package protovcs_test

import (
	"context"
	"errors"
	"testing"
	"time"

	protovcs "github.com/nextgui/protovcs"
	"github.com/nextgui/protovcs/memtransport"
	testpb "github.com/nextgui/protovcs/proto/protovcs/testpb"
)

// What a client is told when a branch moves.
//
// The transport reports every branch, because telling the server which one a
// client cares about would tell it something it does not otherwise know. The
// filtering down to one branch is [protovcs.Branch.Watch], so these are the
// tests of that: a caller watching main must not be woken by a write to a
// release branch it has nothing to do with.

const (
	watchArrivesWithin = 5 * time.Second
	watchQuietFor      = 250 * time.Millisecond
)

// branchWatch is a Watch running in the background and the moves it reported.
type branchWatch struct {
	moves  chan protovcs.RefState
	cancel context.CancelFunc

	// Closed when Watch has returned. err is written before the close.
	finished chan struct{}
	err      error
}

func watchBranch(t *testing.T, b *protovcs.Branch) *branchWatch {
	t.Helper()

	ctx, cancel := context.WithCancel(context.Background())
	w := &branchWatch{
		moves:    make(chan protovcs.RefState, 32),
		cancel:   cancel,
		finished: make(chan struct{}),
	}
	go func() {
		defer close(w.finished)
		w.err = b.Watch(ctx, func(state protovcs.RefState) error {
			w.moves <- state
			return nil
		})
	}()
	t.Cleanup(func() { w.stop(t) })
	return w
}

func (w *branchWatch) next(t *testing.T, what string) protovcs.RefState {
	t.Helper()
	select {
	case state := <-w.moves:
		return state
	case <-w.finished:
		t.Fatalf("%s: the watch ended instead, with %v", what, w.err)
		return protovcs.RefState{}
	case <-time.After(watchArrivesWithin):
		t.Fatalf("%s: nothing arrived in %v", what, watchArrivesWithin)
		return protovcs.RefState{}
	}
}

func (w *branchWatch) quiet(t *testing.T, what string) {
	t.Helper()
	select {
	case state := <-w.moves:
		t.Fatalf("%s: the watch reported a move to %s", what, state.Commit)
	case <-time.After(watchQuietFor):
	}
}

func (w *branchWatch) stop(t *testing.T) error {
	t.Helper()
	w.cancel()
	select {
	case <-w.finished:
		return w.err
	case <-time.After(watchArrivesWithin):
		t.Fatal("the watch did not return after its context was cancelled")
		return nil
	}
}

func write(t *testing.T, b *protovcs.Branch, name string, n int) protovcs.CommitID {
	t.Helper()

	id, err := b.Write(context.Background(), meta("write "+name), &testpb.Store{
		Manifest: &testpb.Manifest{SchemaVersion: 1},
		Name:     name,
		Records:  records(n),
	})
	if err != nil {
		t.Fatalf("Write(%s): %v", name, err)
	}
	return id
}

func TestAWatcherIsToldAboutACommitMadeAfterItSubscribed(t *testing.T) {
	s := newStore(t, memtransport.New(), keyring(t))
	main := s.Branch("main")

	first := write(t, main, "first", 10)

	w := watchBranch(t, main)
	if got := w.next(t, "where the branch is"); got.Commit != first {
		t.Fatalf("the first report put main at %s, want %s", got.Commit, first)
	}

	second := write(t, main, "second", 12)
	if got := w.next(t, "the move"); got.Commit != second {
		t.Fatalf("the watcher was told main is at %s, want %s", got.Commit, second)
	}
}

// TestAWatcherOnOneBranchIgnoresAnother is the filtering that makes a watch
// usable.
//
// The server sends every branch to every watcher, on purpose: a request that
// named a branch would tell the server which one this client is following, and
// nothing else in the protocol does. Watch drops the branches the caller did
// not ask about.
func TestAWatcherOnOneBranchIgnoresAnother(t *testing.T) {
	s := newStore(t, memtransport.New(), keyring(t))
	main, release := s.Branch("main"), s.Branch("release")

	write(t, main, "first", 10)

	w := watchBranch(t, main)
	w.next(t, "where main is")

	write(t, release, "on the other branch", 8)
	w.quiet(t, "a write to another branch")

	// And the watch is still live: the next write to main does arrive.
	second := write(t, main, "second", 11)
	if got := w.next(t, "a later move on main"); got.Commit != second {
		t.Fatalf("after ignoring another branch the watcher reported %s, want %s", got.Commit, second)
	}
}

func TestTwoWatchersOnOneBranchBothSeeAMove(t *testing.T) {
	s := newStore(t, memtransport.New(), keyring(t))
	main := s.Branch("main")

	write(t, main, "first", 10)

	a, b := watchBranch(t, main), watchBranch(t, main)
	a.next(t, "where a thinks main is")
	b.next(t, "where b thinks main is")

	second := write(t, main, "second", 12)

	for name, w := range map[string]*branchWatch{"a": a, "b": b} {
		if got := w.next(t, name+" seeing the move"); got.Commit != second {
			t.Fatalf("watcher %s reported %s, want %s", name, got.Commit, second)
		}
	}
}

// TestAWatchOfABranchThatDoesNotExistWaitsForIt, because the first thing a new
// vault does is watch a branch nobody has written yet.
func TestAWatchOfABranchThatDoesNotExistWaitsForIt(t *testing.T) {
	s := newStore(t, memtransport.New(), keyring(t))
	main, other := s.Branch("main"), s.Branch("other")

	w := watchBranch(t, main)

	// A branch that has never existed is not a move, so nothing is reported —
	// not even when a different branch appears.
	write(t, other, "elsewhere", 5)
	w.quiet(t, "a branch that has never existed")

	first := write(t, main, "first", 10)
	if got := w.next(t, "the branch being created"); got.Commit != first {
		t.Fatalf("creating main reported %s, want %s", got.Commit, first)
	}
}

// TestADeletedBranchIsReportedAsAZeroState is how a caller tells a branch that
// went away from one that has not moved.
func TestADeletedBranchIsReportedAsAZeroState(t *testing.T) {
	ctx := context.Background()
	tr := memtransport.New()
	s := newStore(t, tr, keyring(t))
	main := s.Branch("main")

	write(t, main, "first", 10)

	w := watchBranch(t, main)
	state := w.next(t, "where main is")

	if _, err := tr.UpdateRef(ctx, state.Name, protovcs.CommitID{}, state.CASToken, false); err != nil {
		t.Fatalf("deleting the branch: %v", err)
	}

	got := w.next(t, "the deletion")
	if !got.Commit.IsZero() {
		t.Fatalf("a deleted branch was reported at %s, want a zero commit", got.Commit)
	}
}

// TestADisconnectedWatcherReleasesItsSubscription asserts the release rather
// than assuming it. A watch outlives any single call, so nothing else would
// notice one that was never let go.
func TestADisconnectedWatcherReleasesItsSubscription(t *testing.T) {
	tr := memtransport.New()
	s := newStore(t, tr, keyring(t))
	main := s.Branch("main")

	write(t, main, "first", 10)

	var watchers []*branchWatch
	for i := 0; i < 3; i++ {
		w := watchBranch(t, main)
		w.next(t, "where main is")
		watchers = append(watchers, w)
	}
	if got := tr.WatchCount(); got != 3 {
		t.Fatalf("three watches registered %d subscriptions", got)
	}

	for i, w := range watchers {
		if err := w.stop(t); !errors.Is(err, context.Canceled) {
			t.Fatalf("watcher %d returned %v, want the cancellation", i, err)
		}
	}
	if got := tr.WatchCount(); got != 0 {
		t.Fatalf("after every watcher left, %d subscriptions were still registered", got)
	}
}

// TestAWatcherThatRefusesEndsItsOwnWatch covers the caller stopping without
// cancelling: returning an error from the callback.
func TestAWatcherThatRefusesEndsItsOwnWatch(t *testing.T) {
	tr := memtransport.New()
	s := newStore(t, tr, keyring(t))
	main := s.Branch("main")

	write(t, main, "first", 10)

	enough := errors.New("that will do")
	err := main.Watch(context.Background(), func(protovcs.RefState) error { return enough })
	if !errors.Is(err, enough) {
		t.Fatalf("Watch returned %v, want the error the caller gave it", err)
	}
	if got := tr.WatchCount(); got != 0 {
		t.Fatalf("a refused watch left %d subscriptions behind", got)
	}
}

// TestAWatchReportsAMoveOnce, because a caller that acts on every report should
// not act twice on one commit.
func TestAWatchReportsAMoveOnce(t *testing.T) {
	ctx := context.Background()
	tr := memtransport.New()
	s := newStore(t, tr, keyring(t))
	main := s.Branch("main")

	first := write(t, main, "first", 10)

	w := watchBranch(t, main)
	w.next(t, "where main is")

	// A write to a second branch wakes every watcher of the store, which is
	// what the transport promises. Main has not moved, so Watch says nothing.
	write(t, s.Branch("release"), "elsewhere", 6)
	w.quiet(t, "another branch moving")

	// Nor does re-reading the same state produce a second report.
	if _, err := tr.ListRefs(ctx); err != nil {
		t.Fatalf("ListRefs: %v", err)
	}
	w.quiet(t, "a repeated state")

	head, _, err := main.Head(ctx)
	if err != nil {
		t.Fatalf("Head: %v", err)
	}
	if got, _ := protovcs.CommitName(head); got != first {
		t.Fatalf("main is at %s, want %s", got, first)
	}
}

func TestADeletionIsReportedOnceWhileOtherBranchesMove(t *testing.T) {
	ctx := context.Background()
	tr := memtransport.New()
	s := newStore(t, tr, keyring(t))
	main := s.Branch("main")
	write(t, main, "first", 3)

	w := watchBranch(t, main)
	state := w.next(t, "where main is")
	if _, err := tr.UpdateRef(ctx, state.Name, protovcs.CommitID{}, state.CASToken, false); err != nil {
		t.Fatalf("deleting the branch: %v", err)
	}
	w.next(t, "the deletion")

	write(t, s.Branch("other"), "other", 3)
	w.quiet(t, "another branch moved")

	write(t, main, "again", 3)
	if got := w.next(t, "main created again"); got.Commit.IsZero() {
		t.Fatal("main created again was reported as a deletion")
	}
}
