package protovcs_test

import (
	"context"
	"strings"
	"sync"
	"testing"

	"google.golang.org/protobuf/proto"

	protovcs "github.com/nextgui/protovcs"
	"github.com/nextgui/protovcs/memtransport"
	testpb "github.com/nextgui/protovcs/proto/protovcs/testpb"
	pb "github.com/nextgui/protovcs/proto/protovcs/v1"
)

// dropSecrets removes every record whose id starts with "secret".
func dropSecrets(_ context.Context, m proto.Message) error {
	st := m.(*testpb.Store)
	kept := st.Records[:0]
	for _, r := range st.GetRecords() {
		if !strings.HasPrefix(r.GetId(), "secret") {
			kept = append(kept, r)
		}
	}
	st.Records = kept
	return nil
}

func secret(id string) *testpb.Record {
	return &testpb.Record{Id: id, Group: "alpha", Date: &testpb.Date{Year: 2026, Month: 3, Day: 3}, Amount: 7}
}

func appendRecord(r *testpb.Record) func(context.Context, proto.Message) error {
	return func(_ context.Context, m proto.Message) error {
		st := m.(*testpb.Store)
		st.Records = append(st.Records, proto.Clone(r).(*testpb.Record))
		return nil
	}
}

// history builds main: seed -> withSecret -> unrelated -> merge(side), where
// side forks from withSecret and adds a second secret. It returns the seed
// commit, which holds nothing to erase.
func history(t *testing.T, s *protovcs.Store) protovcs.CommitID {
	t.Helper()
	ctx := context.Background()
	main := s.Branch("main")
	seed, err := main.Write(ctx, meta("seed"), &testpb.Store{
		Manifest: &testpb.Manifest{SchemaVersion: 1}, Name: "h", Records: records(20),
	})
	if err != nil {
		t.Fatalf("Write: %v", err)
	}
	withSecret, err := main.Change(ctx, meta("add a secret"), appendRecord(secret("secret-1")))
	if err != nil {
		t.Fatalf("Change: %v", err)
	}
	unrelated, err := main.Change(ctx, meta("unrelated"), appendRecord(&testpb.Record{
		Id: "plain-1", Group: "beta", Date: &testpb.Date{Year: 2026, Month: 5, Day: 5}}))
	if err != nil {
		t.Fatalf("Change: %v", err)
	}

	sideMsg, err := s.ReadAt(ctx, withSecret)
	if err != nil {
		t.Fatalf("ReadAt: %v", err)
	}
	sideMsg.(*testpb.Store).Records = append(sideMsg.(*testpb.Store).Records, secret("secret-2"),
		&testpb.Record{Id: "side-1", Group: "gamma", Date: &testpb.Date{Year: 2026, Month: 6, Day: 6}})
	sideHead, err := protovcs.PublishWithParentsForTest(ctx, s.Branch("side"), meta("side"),
		[]protovcs.CommitID{withSecret}, sideMsg)
	if err != nil {
		t.Fatalf("side: %v", err)
	}

	merged, err := s.ReadAt(ctx, sideHead)
	if err != nil {
		t.Fatalf("ReadAt: %v", err)
	}
	merged.(*testpb.Store).Records = append(merged.(*testpb.Store).Records, &testpb.Record{
		Id: "plain-1", Group: "beta", Date: &testpb.Date{Year: 2026, Month: 5, Day: 5}})
	if _, err := protovcs.PublishWithParentsForTest(ctx, main, meta("merge side"),
		[]protovcs.CommitID{unrelated, sideHead}, merged); err != nil {
		t.Fatalf("merge: %v", err)
	}
	return seed
}

// reachable returns every commit reachable from any branch.
func reachable(t *testing.T, tr protovcs.Transport) map[protovcs.CommitID]*pb.Commit {
	t.Helper()
	ctx := context.Background()
	refs, err := tr.ListRefs(ctx)
	if err != nil {
		t.Fatalf("ListRefs: %v", err)
	}
	out := map[protovcs.CommitID]*pb.Commit{}
	for _, r := range refs {
		commits, err := tr.GetCommits(ctx, r.Commit, 10000)
		if err != nil {
			t.Fatalf("GetCommits: %v", err)
		}
		for _, c := range commits {
			id, _ := protovcs.CommitIDFromBytes(c.GetId())
			out[id] = c
		}
	}
	return out
}

func TestRewriteRemovesARecordFromEveryReachableCommitAndKeepsTheRest(t *testing.T) {
	ctx := context.Background()
	tr := memtransport.New()
	s := newStore(t, tr, keyring(t))
	seed := history(t, s)
	before := reachable(t, tr)

	res, err := s.Rewrite(ctx, dropSecrets)
	if err != nil {
		t.Fatalf("Rewrite: %v", err)
	}
	if res.BranchesMoved() != 2 {
		t.Errorf("moved %d branches, want 2", res.BranchesMoved())
	}
	rewritten := res.Rewritten()
	if _, ok := rewritten[seed]; ok {
		t.Error("the seed commit holds no secret and was rewritten anyway")
	}

	// A fresh client, so nothing is answered from the rewriter's cache.
	fresh := newStore(t, tr, keyring(t))
	after := reachable(t, tr)
	for id := range after {
		m, err := fresh.ReadAt(ctx, id)
		if err != nil {
			t.Fatalf("ReadAt(%s): %v", id, err)
		}
		for _, r := range m.(*testpb.Store).GetRecords() {
			if strings.HasPrefix(r.GetId(), "secret") {
				t.Errorf("commit %s still holds %s", id, r.GetId())
			}
		}
	}

	for old, next := range rewritten {
		if _, ok := after[old]; ok {
			t.Errorf("replaced commit %s is still reachable", old)
		}
		was, err := fresh.ReadAt(ctx, old)
		if err != nil {
			t.Fatalf("ReadAt(old %s): %v", old, err)
		}
		if err := dropSecrets(ctx, was); err != nil {
			t.Fatal(err)
		}
		now, err := fresh.ReadAt(ctx, next)
		if err != nil {
			t.Fatalf("ReadAt(new %s): %v", next, err)
		}
		if !proto.Equal(was, now) {
			t.Errorf("commit %s changed more than the secrets when rewritten to %s", old, next)
		}

		// Same shape: each parent is the rewrite of the old parent.
		oldParents, newParents := before[old].GetParents(), after[next].GetParents()
		if len(oldParents) != len(newParents) {
			t.Fatalf("commit %s had %d parents and its rewrite has %d", old, len(oldParents), len(newParents))
		}
		for i := range oldParents {
			p, _ := protovcs.CommitIDFromBytes(oldParents[i])
			want := p
			if n, ok := rewritten[p]; ok {
				want = n
			}
			got, _ := protovcs.CommitIDFromBytes(newParents[i])
			if got != want {
				t.Errorf("parent %d of %s's rewrite is %s, want %s", i, old, got, want)
			}
		}

		// The commit's own record of why and when is kept.
		oldInfo, err := fresh.CommitDetails(ctx, before[old])
		if err != nil {
			t.Fatal(err)
		}
		newInfo, err := fresh.CommitDetails(ctx, after[next])
		if err != nil {
			t.Fatal(err)
		}
		if oldInfo.Title != newInfo.Title || !oldInfo.Time.Equal(newInfo.Time) {
			t.Errorf("rewrite of %s says %q at %v, want %q at %v", old, newInfo.Title, newInfo.Time, oldInfo.Title, oldInfo.Time)
		}
	}
	if len(rewritten) != 4 {
		t.Errorf("rewrote %d commits, want 4 (secret, unrelated, side, merge)", len(rewritten))
	}

	// A second pass finds nothing to do.
	again, err := s.Rewrite(ctx, dropSecrets)
	if err != nil {
		t.Fatalf("second Rewrite: %v", err)
	}
	if len(again.Rewritten()) != 0 || again.BranchesMoved() != 0 {
		t.Errorf("a second rewrite replaced %d commits and moved %d branches", len(again.Rewritten()), again.BranchesMoved())
	}

	// The rewriting client can keep writing to the branch.
	if _, err := s.Branch("main").Change(ctx, meta("after"), appendRecord(&testpb.Record{
		Id: "plain-2", Group: "beta", Date: &testpb.Date{Year: 2026, Month: 7, Day: 7}})); err != nil {
		t.Fatalf("Change after rewrite: %v", err)
	}
}

// racingTransport lets another writer commit just before the first branch move.
type racingTransport struct {
	protovcs.Transport
	once   sync.Once
	before func()
}

func (r *racingTransport) UpdateRef(ctx context.Context, name []byte, commit protovcs.CommitID, expected string, force bool) (protovcs.RefState, error) {
	if force {
		r.once.Do(r.before)
	}
	return r.Transport.UpdateRef(ctx, name, commit, expected, force)
}

func TestRewriteAlsoRewritesACommitThatLandsWhileItRuns(t *testing.T) {
	ctx := context.Background()
	mem := memtransport.New()
	writer := newStore(t, mem, keyring(t))
	history(t, writer)

	race := &racingTransport{Transport: mem}
	race.before = func() {
		other := newStore(t, mem, keyring(t))
		if _, err := other.Branch("main").Change(ctx, meta("late"), func(_ context.Context, m proto.Message) error {
			st := m.(*testpb.Store)
			st.Records = append(st.Records, secret("secret-late"), &testpb.Record{
				Id: "plain-late", Group: "beta", Date: &testpb.Date{Year: 2026, Month: 8, Day: 8}})
			return nil
		}); err != nil {
			t.Errorf("the racing write failed: %v", err)
		}
	}
	rewriter := newStore(t, race, keyring(t))
	if _, err := rewriter.Rewrite(ctx, dropSecrets); err != nil {
		t.Fatalf("Rewrite: %v", err)
	}

	fresh := newStore(t, mem, keyring(t))
	got, err := fresh.Branch("main").Read(ctx)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	var sawLate bool
	for _, r := range got.(*testpb.Store).GetRecords() {
		if strings.HasPrefix(r.GetId(), "secret") {
			t.Errorf("main still holds %s", r.GetId())
		}
		sawLate = sawLate || r.GetId() == "plain-late"
	}
	if !sawLate {
		t.Error("the write that landed during the rewrite was lost")
	}
	for id := range reachable(t, mem) {
		m, err := fresh.ReadAt(ctx, id)
		if err != nil {
			t.Fatal(err)
		}
		for _, r := range m.(*testpb.Store).GetRecords() {
			if strings.HasPrefix(r.GetId(), "secret") {
				t.Errorf("commit %s still holds %s", id, r.GetId())
			}
		}
	}
}
