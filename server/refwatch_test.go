package server_test

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	protovcs "github.com/nextgui/protovcs"
	pb "github.com/nextgui/protovcs/proto/protovcs/v1"
	"github.com/nextgui/protovcs/server"
)

// How long a test waits for something that should happen, and how long it waits
// to be satisfied that something has not.
//
// The first is generous because a loaded machine running every test at once is
// the normal case and a flaky test is worse than a slow one. The second is
// short because it is spent on every run.
const (
	shouldArriveWithin = 5 * time.Second
	shouldStayQuietFor = 250 * time.Millisecond
)

// pollFast is short enough that a test waiting for a poll finishes quickly, and
// long enough that a wake still usually beats the tick, so the two paths stay
// distinguishable.
const pollFast = 20 * time.Millisecond

// watchCommit records a commit in a named store and returns its name.
//
// Every commit gets a distinct nonce, because a commit is named by hashing its
// own record and two commits with the same parents would otherwise be one.
func watchCommit(t *testing.T, s *server.Server, storeID string, nonce byte, parents ...protovcs.CommitID) protovcs.CommitID {
	t.Helper()

	raw := make([][]byte, 0, len(parents))
	for _, p := range parents {
		raw = append(raw, p.Bytes())
	}
	id, err := s.PutCommit(context.Background(), storeID, "", &pb.Commit{
		FormatVersion: protovcs.FormatVersion,
		Parents:       raw,
		NamingNonce:   []byte{nonce},
	})
	if err != nil {
		t.Fatalf("PutCommit: %v", err)
	}
	return id
}

// moveRef moves a branch through the server, which is what a client does.
func moveRef(t *testing.T, s *server.Server, storeID, branch string, to protovcs.CommitID, expected string) protovcs.RefState {
	t.Helper()

	state, err := s.UpdateRef(context.Background(), storeID, []byte(branch), to, expected, false)
	if err != nil {
		t.Fatalf("UpdateRef(%s): %v", branch, err)
	}
	return state
}

// watcher is a watch running in the background, and what it has been told.
type watcher struct {
	updates chan []protovcs.RefState
	cancel  context.CancelFunc

	// Closed when the watch has returned. err is written before the close and
	// read after it, so no lock is needed and every waiter may look.
	finished chan struct{}
	err      error
}

// watchStore opens a watch on a store and starts collecting what it delivers.
func watchStore(t *testing.T, s *server.Server, storeID string) *watcher {
	t.Helper()

	ctx, cancel := context.WithCancel(context.Background())
	w := &watcher{
		// Deep enough that deliver never blocks, so a test measures the server
		// rather than its own reader.
		updates:  make(chan []protovcs.RefState, 64),
		cancel:   cancel,
		finished: make(chan struct{}),
	}
	go func() {
		defer close(w.finished)
		w.err = s.WatchRefs(ctx, storeID, func(refs []protovcs.RefState) error {
			w.updates <- refs
			return nil
		})
	}()
	t.Cleanup(func() { w.stop(t) })
	return w
}

// next returns the watch's next delivery, failing the test if none arrives.
func (w *watcher) next(t *testing.T, what string) []protovcs.RefState {
	t.Helper()
	select {
	case refs := <-w.updates:
		return refs
	case <-w.finished:
		t.Fatalf("%s: the watch ended instead, with %v", what, w.err)
		return nil
	case <-time.After(shouldArriveWithin):
		t.Fatalf("%s: nothing arrived in %v", what, shouldArriveWithin)
		return nil
	}
}

// quiet fails the test if anything arrives.
func (w *watcher) quiet(t *testing.T, what string) {
	t.Helper()
	select {
	case refs := <-w.updates:
		t.Fatalf("%s: was told about %d refs", what, len(refs))
	case <-time.After(shouldStayQuietFor):
	}
}

// ended waits for the watch to finish on its own and returns why.
func (w *watcher) ended(t *testing.T, what string) error {
	t.Helper()
	select {
	case <-w.finished:
		return w.err
	case <-time.After(shouldArriveWithin):
		t.Fatalf("%s: the watch was still running after %v", what, shouldArriveWithin)
		return nil
	}
}

// stop ends the watch and returns what it reported. Safe to call twice, which
// it is: once by a test and once by the cleanup.
func (w *watcher) stop(t *testing.T) error {
	t.Helper()
	w.cancel()
	return w.ended(t, "stopping the watch")
}

// commitAt returns where a named branch points in a delivered set.
func commitAt(refs []protovcs.RefState, branch string) (protovcs.CommitID, bool) {
	for _, r := range refs {
		if string(r.Name) == branch {
			return r.Commit, true
		}
	}
	return protovcs.CommitID{}, false
}

// waitFor retries until cond holds, which is how a test waits for something a
// background goroutine does without sleeping for a guess.
func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(shouldArriveWithin)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("%s did not happen in %v", what, shouldArriveWithin)
}

// TestAWatcherIsToldWhereTheBranchIsBeforeItMoves is the deliberate decision
// about what a watch reports first.
//
// A commit made before the watch opened is not replayed as an event. What the
// watcher gets is the branch's current position, once, and then only changes.
// The alternative — say nothing until something moves — would force every
// caller to read the head separately and would lose a write that landed between
// the two calls.
func TestAWatcherIsToldWhereTheBranchIsBeforeItMoves(t *testing.T) {
	s, _ := newServer(t, &pb.Quota{MaxBytes: 1 << 20})

	before := watchCommit(t, s, storeID, 1)
	moveRef(t, s, storeID, "main", before, "")

	w := watchStore(t, s, storeID)

	got, ok := commitAt(w.next(t, "the state at subscription"), "main")
	if !ok || got != before {
		t.Fatalf("the first delivery put main at %v, want %s", got, before)
	}

	// And the commit made before it subscribed is not delivered a second time
	// as though it had just happened.
	w.quiet(t, "a commit made before the watch opened")
}

func TestAWatcherSeesACommitMadeAfterItSubscribed(t *testing.T) {
	s, _ := newServer(t, &pb.Quota{MaxBytes: 1 << 20})

	first := watchCommit(t, s, storeID, 1)
	state := moveRef(t, s, storeID, "main", first, "")

	w := watchStore(t, s, storeID)
	w.next(t, "the state at subscription")

	second := watchCommit(t, s, storeID, 2, first)
	moveRef(t, s, storeID, "main", second, state.CASToken)

	got, ok := commitAt(w.next(t, "the move"), "main")
	if !ok || got != second {
		t.Fatalf("the watcher was told main is at %v, want %s", got, second)
	}
}

// TestAWatchOnAStoreThatNeverHadARefStartsEmpty covers the ordinary first use:
// a vault nobody has written to yet.
func TestAWatchOnAStoreThatNeverHadARefStartsEmpty(t *testing.T) {
	s, _ := newServer(t, &pb.Quota{MaxBytes: 1 << 20})

	w := watchStore(t, s, storeID)
	if refs := w.next(t, "the state at subscription"); len(refs) != 0 {
		t.Fatalf("a store with no branches delivered %d refs", len(refs))
	}

	first := watchCommit(t, s, storeID, 1)
	moveRef(t, s, storeID, "main", first, "")

	if got, ok := commitAt(w.next(t, "the branch being created"), "main"); !ok || got != first {
		t.Fatalf("creating a branch delivered %v, want %s", got, first)
	}
}

func TestTwoWatchersOnOneStoreBothSeeAMove(t *testing.T) {
	s, _ := newServer(t, &pb.Quota{MaxBytes: 1 << 20})

	first := watchCommit(t, s, storeID, 1)
	state := moveRef(t, s, storeID, "main", first, "")

	a, b := watchStore(t, s, storeID), watchStore(t, s, storeID)
	a.next(t, "a's state at subscription")
	b.next(t, "b's state at subscription")

	second := watchCommit(t, s, storeID, 2, first)
	moveRef(t, s, storeID, "main", second, state.CASToken)

	for name, w := range map[string]*watcher{"a": a, "b": b} {
		got, ok := commitAt(w.next(t, name+" seeing the move"), "main")
		if !ok || got != second {
			t.Fatalf("watcher %s was told main is at %v, want %s", name, got, second)
		}
	}
}

// TestAWatcherOnOneStoreDoesNotSeeAnother is the isolation the watch has to
// keep whatever the token check decides: a subscription is per store, and one
// store's writes must not reach another store's watchers.
func TestAWatcherOnOneStoreDoesNotSeeAnother(t *testing.T) {
	ctx := context.Background()
	s, _ := newServer(t, &pb.Quota{MaxBytes: 1 << 20})

	const other = "store-2"
	if _, err := s.CreateStore(ctx, &pb.CreateStoreRequest{
		StoreId: other, Owner: "someone", Quota: &pb.Quota{MaxBytes: 1 << 20},
	}); err != nil {
		t.Fatalf("CreateStore: %v", err)
	}

	w := watchStore(t, s, storeID)
	w.next(t, "the state at subscription")

	moveRef(t, s, other, "main", watchCommit(t, s, other, 9), "")
	w.quiet(t, "a move in another store")
}

// TestAPollNoticesAMoveThisServerDidNotMake is the mechanism the design rests
// on.
//
// Storage is a directory or a bucket and neither announces a change, so the
// server polls. A move made through this server takes a shortcut and wakes the
// poller, which would hide a broken poll — so this one moves the ref through a
// second server over the same storage, exactly as a second replica would.
// The unconditional listing is pushed out of reach, so only the generation
// check can be what noticed.
func TestAPollNoticesAMoveThisServerDidNotMake(t *testing.T) {
	s, storage := newCountingServer(t, time.Hour)
	replica := server.New(storage)

	first := watchCommit(t, s, storeID, 1)
	second := watchCommit(t, s, storeID, 2, first)
	state := moveRef(t, s, storeID, "main", first, "")

	w := watchStore(t, s, storeID)
	w.next(t, "the state at subscription")
	storage.settle(t)

	moveRef(t, replica, storeID, "main", second, state.CASToken)

	got, ok := commitAt(w.next(t, "the poll"), "main")
	if !ok || got != second {
		t.Fatalf("the poll reported main at %v, want %s", got, second)
	}
}

// TestTheRelistingNoticesAMoveTheGenerationMissed covers a server that moved a
// ref and stopped before writing the store's record.
//
// The generation check cannot see that move, so it is made in storage
// directly, which leaves the record untouched. The unconditional listing is
// what has to report it.
func TestTheRelistingNoticesAMoveTheGenerationMissed(t *testing.T) {
	ctx := context.Background()
	s, storage := newCountingServer(t, 10*pollFast)

	first := watchCommit(t, s, storeID, 1)
	second := watchCommit(t, s, storeID, 2, first)
	state := moveRef(t, s, storeID, "main", first, "")

	w := watchStore(t, s, storeID)
	w.next(t, "the state at subscription")
	storage.settle(t)

	if _, err := storage.UpdateRef(ctx, storeID, []byte("main"), second, state.CASToken, false); err != nil {
		t.Fatalf("moving the ref in storage: %v", err)
	}

	got, ok := commitAt(w.next(t, "the relisting"), "main")
	if !ok || got != second {
		t.Fatalf("the relisting reported main at %v, want %s", got, second)
	}
}

// TestAnIdleWatchedStoreIsNotListed is the cost claim: a watched store nobody
// writes to costs a generation read per tick and no listing.
//
// On Cloud Storage a listing is billed as a write and a generation read as a
// read, twelve times cheaper, so listing on every tick is what this replaced.
func TestAnIdleWatchedStoreIsNotListed(t *testing.T) {
	s, storage := newCountingServer(t, time.Hour)

	first := watchCommit(t, s, storeID, 1)
	moveRef(t, s, storeID, "main", first, "")

	// UpdateRef lists the refs itself, so count from here.
	base := storage.listings.Load()
	w := watchStore(t, s, storeID)
	w.next(t, "the state at subscription")
	storage.settle(t)

	listed := storage.listings.Load()
	checked := storage.generations.Load()
	waitFor(t, "ten more generation checks", func() bool { return storage.generations.Load() >= checked+10 })
	if got := storage.listings.Load(); got != listed {
		t.Fatalf("an idle store was listed %d times over ten checks, want 0", got-listed)
	}
	// One by the subscription, one by the poller's first check.
	if listed-base > 2 {
		t.Fatalf("starting a watch listed the store %d times, want at most 2", listed-base)
	}
}

// newCountingServer returns a server polling every pollFast and listing
// unconditionally every relist, over storage that counts its reads.
func newCountingServer(t *testing.T, relist time.Duration) (*server.Server, *countingStorage) {
	t.Helper()
	storage := &countingStorage{Storage: server.NewMemStorage()}
	s := server.New(storage).WithRefPollInterval(pollFast).WithRefRelistInterval(relist)
	if _, err := s.CreateStore(context.Background(), &pb.CreateStoreRequest{
		StoreId: storeID, Owner: "someone", Quota: &pb.Quota{MaxBytes: 1 << 20},
	}); err != nil {
		t.Fatalf("CreateStore: %v", err)
	}
	return s, storage
}

// countingStorage counts the two reads a poll makes.
type countingStorage struct {
	server.Storage
	listings    atomic.Int64
	generations atomic.Int64
}

func (c *countingStorage) ListRefs(ctx context.Context, storeID string) ([]protovcs.RefState, error) {
	c.listings.Add(1)
	return c.Storage.ListRefs(ctx, storeID)
}

func (c *countingStorage) StoreGeneration(ctx context.Context, storeID string) (int64, error) {
	c.generations.Add(1)
	return c.Storage.StoreGeneration(ctx, storeID)
}

// settle waits until a watch's poller has checked the store twice, so its
// first check, which always lists, is over.
func (c *countingStorage) settle(t *testing.T) {
	t.Helper()
	from := c.generations.Load()
	waitFor(t, "two generation checks", func() bool { return c.generations.Load() >= from+2 })
}

// TestManyWatchersOnOneStoreCostOnePoll is the cost claim, asserted rather than
// asserted in a comment.
//
// The reason a server may poll its own storage on behalf of idle watchers is
// that the poll is per store, not per watcher. If that ever stopped being true
// the cost of the design would change by a factor of however many tabs a person
// has open.
func TestManyWatchersOnOneStoreCostOnePoll(t *testing.T) {
	s, _ := newServer(t, &pb.Quota{MaxBytes: 1 << 20})

	const n = 8
	for i := 0; i < n; i++ {
		watchStore(t, s, storeID).next(t, "the state at subscription")
	}

	stats := s.WatchStats()
	if stats.Watchers != n {
		t.Fatalf("%d watchers registered %d subscriptions", n, stats.Watchers)
	}
	if stats.Stores != 1 || stats.Pollers != 1 {
		t.Fatalf("%d watchers of one store started %d pollers over %d stores, want 1 and 1",
			n, stats.Pollers, stats.Stores)
	}
}

// TestADisconnectedWatcherReleasesEverything is the leak this design could most
// easily have.
//
// A watch is a goroutine, a subscription and a poller that outlives any single
// request. A client that walks away must take all three with it, and a server
// holding a vault open for a browser tab that closed an hour ago is not
// something an operator would notice until it mattered.
func TestADisconnectedWatcherReleasesEverything(t *testing.T) {
	s, _ := newServer(t, &pb.Quota{MaxBytes: 1 << 20})
	s.WithRefPollInterval(pollFast)

	before := runtime.NumGoroutine()

	var watchers []*watcher
	for i := 0; i < 4; i++ {
		w := watchStore(t, s, storeID)
		w.next(t, "the state at subscription")
		watchers = append(watchers, w)
	}
	if got := s.WatchStats(); got.Watchers != 4 {
		t.Fatalf("four watches registered %d subscriptions", got.Watchers)
	}

	for i, w := range watchers {
		if err := w.stop(t); !errors.Is(err, context.Canceled) {
			t.Fatalf("watcher %d returned %v, want the cancellation", i, err)
		}
	}

	// The subscriptions and the store's entry go as the last watcher leaves.
	// The poller goroutine is asked to stop at the same moment and stops just
	// after, so this waits rather than checking once.
	waitFor(t, "the server releasing every watcher", func() bool {
		return s.WatchStats() == server.WatchStats{}
	})

	// And no goroutine is left behind. The counter above proves the server let
	// go of its own bookkeeping; this proves nothing is still running.
	waitFor(t, "the goroutines returning", func() bool {
		return runtime.NumGoroutine() <= before
	})
}

// TestAWatcherThatRefusesEndsItsOwnWatch covers the other way a watch stops:
// the caller deciding it has seen enough.
func TestAWatcherThatRefusesEndsItsOwnWatch(t *testing.T) {
	s, _ := newServer(t, &pb.Quota{MaxBytes: 1 << 20})

	enough := errors.New("that will do")
	err := s.WatchRefs(context.Background(), storeID, func([]protovcs.RefState) error {
		return enough
	})
	if !errors.Is(err, enough) {
		t.Fatalf("WatchRefs returned %v, want the error the caller gave it", err)
	}
	waitFor(t, "a refused watch releasing what it held", func() bool {
		return s.WatchStats() == server.WatchStats{}
	})
}

// TestASlowWatcherGetsTheNewestStateAndDelaysNobody is why the wire carries the
// whole ref set rather than what changed.
//
// A watcher that is not reading is sent one state, not a queue, and the state
// it eventually reads is the current one rather than the oldest one it missed.
// A watcher that stalls must also not stall the writes or the other watchers,
// which is what the second half checks.
func TestASlowWatcherGetsTheNewestStateAndDelaysNobody(t *testing.T) {
	s, _ := newServer(t, &pb.Quota{MaxBytes: 1 << 20})
	s.WithRefPollInterval(pollFast)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// The slow watcher takes its first delivery, then blocks until released.
	release := make(chan struct{})
	seen := make(chan []protovcs.RefState, 8)
	slowDone := make(chan error, 1)
	var once sync.Once
	go func() {
		slowDone <- s.WatchRefs(ctx, storeID, func(refs []protovcs.RefState) error {
			seen <- refs
			once.Do(func() { <-release })
			return nil
		})
	}()
	<-seen

	fast := watchStore(t, s, storeID)
	fast.next(t, "the state at subscription")

	// Ten moves while the slow watcher is not reading. None of them may block.
	var last protovcs.CommitID
	state := protovcs.RefState{}
	for i := byte(1); i <= 10; i++ {
		next := watchCommit(t, s, storeID, i, parentsOf(last)...)
		state = moveRef(t, s, storeID, "main", next, state.CASToken)
		last = next
	}

	// The fast watcher is up to date, so the writes were not held up by the
	// watcher that stopped reading.
	waitFor(t, "the fast watcher reaching the last commit", func() bool {
		select {
		case refs := <-fast.updates:
			got, _ := commitAt(refs, "main")
			return got == last
		default:
			return false
		}
	})

	close(release)

	// The slow watcher's next delivery is the newest state. It is not walked
	// through the nine it slept through.
	select {
	case refs := <-seen:
		got, ok := commitAt(refs, "main")
		if !ok || got != last {
			t.Fatalf("the slow watcher was next told main is at %v, want the newest %s", got, last)
		}
	case <-time.After(shouldArriveWithin):
		t.Fatal("the slow watcher was told nothing after it started reading again")
	}

	cancel()
	if err := <-slowDone; !errors.Is(err, context.Canceled) {
		t.Fatalf("the slow watch returned %v, want the cancellation", err)
	}
}

func parentsOf(id protovcs.CommitID) []protovcs.CommitID {
	if id.IsZero() {
		return nil
	}
	return []protovcs.CommitID{id}
}

// TestAFailingPollEndsTheWatchRatherThanGoingQuiet.
//
// A watch that has stopped reporting looks exactly like a store nobody is
// writing to, so a poll that cannot read storage has to be visible. Every
// watcher on that store is told, and the client's answer is to open the watch
// again — which starts by delivering the current state, so nothing is missed by
// having been away.
func TestAFailingPollEndsTheWatchRatherThanGoingQuiet(t *testing.T) {
	ctx := context.Background()
	storage := &failingRefs{Storage: server.NewMemStorage()}
	s := server.New(storage).WithRefPollInterval(pollFast)

	if _, err := s.CreateStore(ctx, &pb.CreateStoreRequest{
		StoreId: storeID, Owner: "someone", Quota: &pb.Quota{MaxBytes: 1 << 20},
	}); err != nil {
		t.Fatalf("CreateStore: %v", err)
	}

	w := watchStore(t, s, storeID)
	w.next(t, "the state at subscription")

	storage.fail()

	if err := w.ended(t, "a watch whose poll could not read storage"); !errors.Is(err, errStorageIsGone) {
		t.Fatalf("the watch ended with %v, want the storage failure", err)
	}

	// And the failure took the poller with it rather than leaving one retrying
	// against storage that is not answering.
	waitFor(t, "the failed poller retiring", func() bool {
		return s.WatchStats() == server.WatchStats{}
	})
}

var errStorageIsGone = errors.New("storage is gone")

// failingRefs is storage whose ref listing and generation reads can be made to
// fail, which MemStorage on its own cannot do.
type failingRefs struct {
	server.Storage

	mu     sync.Mutex
	broken bool
}

func (f *failingRefs) fail() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.broken = true
}

func (f *failingRefs) ListRefs(ctx context.Context, storeID string) ([]protovcs.RefState, error) {
	f.mu.Lock()
	broken := f.broken
	f.mu.Unlock()

	if broken {
		return nil, fmt.Errorf("%w: %s", errStorageIsGone, storeID)
	}
	return f.Storage.ListRefs(ctx, storeID)
}

func (f *failingRefs) StoreGeneration(ctx context.Context, storeID string) (int64, error) {
	f.mu.Lock()
	broken := f.broken
	f.mu.Unlock()

	if broken {
		return 0, fmt.Errorf("%w: %s", errStorageIsGone, storeID)
	}
	return f.Storage.StoreGeneration(ctx, storeID)
}

func TestWatchingAStoreThatDoesNotExistIsRefused(t *testing.T) {
	s, _ := newServer(t, &pb.Quota{MaxBytes: 1 << 20})

	err := s.WatchRefs(context.Background(), "store-nobody-created", func([]protovcs.RefState) error {
		t.Error("a watch on a store that does not exist delivered something")
		return nil
	})
	if !errors.Is(err, server.ErrNoSuchStore) {
		t.Fatalf("watching a store that does not exist returned %v, want ErrNoSuchStore", err)
	}
	if got := s.WatchStats(); got != (server.WatchStats{}) {
		t.Fatalf("a refused watch left %+v behind", got)
	}
}

// TestABranchBeingDeletedIsAMove, because a client watching a branch that is
// removed has to find out.
func TestABranchBeingDeletedIsAMove(t *testing.T) {
	s, _ := newServer(t, &pb.Quota{MaxBytes: 1 << 20})

	first := watchCommit(t, s, storeID, 1)
	state := moveRef(t, s, storeID, "main", first, "")

	w := watchStore(t, s, storeID)
	w.next(t, "the state at subscription")

	if _, err := s.UpdateRef(context.Background(), storeID, []byte("main"),
		protovcs.CommitID{}, state.CASToken, false); err != nil {
		t.Fatalf("deleting the branch: %v", err)
	}

	if refs := w.next(t, "the deletion"); len(refs) != 0 {
		t.Fatalf("after the only branch was deleted the watcher was told about %d", len(refs))
	}
}
