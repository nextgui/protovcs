package protovcs_test

import (
	"context"
	"math/rand"
	"testing"

	"google.golang.org/protobuf/proto"

	protovcs "github.com/nextgui/protovcs"
	"github.com/nextgui/protovcs/memtransport"
	testpb "github.com/nextgui/protovcs/proto/protovcs/testpb"
	pb "github.com/nextgui/protovcs/proto/protovcs/v1"
)

// UpdateManifest and AddedSince were written to make a commit cost what it
// changed. Both are new, both are only exercised indirectly by the operations
// above them, and both fail in ways nothing else would notice: a tree that
// disagrees with the one a full write builds means two clients holding the same
// data name it differently, and an added set that is missing an entry means
// collection deletes something a commit still references.

// TestIncrementalAndFullBuildAgree is the strongest statement available about
// UpdateManifest: a store built one commit at a time must end up byte-identical
// to the same store written in one go.
//
// If it did not, a vault written by a phone would name its chunks differently
// from the same vault written by a laptop, nothing would deduplicate between
// them, and neither would be able to tell the other had the same data.
func TestIncrementalAndFullBuildAgree(t *testing.T) {
	ctx := context.Background()

	// Built one commit at a time, through the partial path that calls
	// UpdateManifest.
	incremental, incTr := dailyStore(t, 0)
	records := dailyRecords(90)
	for _, r := range records {
		key, err := incremental.Layout().ItemKeyFor("records", r)
		if err != nil {
			t.Fatalf("ItemKeyFor: %v", err)
		}
		part, err := incremental.Branch("main").Checkout(ctx, protovcs.Prefix(string(key)))
		if err != nil {
			t.Fatalf("Checkout: %v", err)
		}
		frag := &testpb.Store{Records: []*testpb.Record{r}}
		if err := part.Put(protovcs.Item{Key: key, Fragment: frag}); err != nil {
			t.Fatalf("Put: %v", err)
		}
		if _, err := part.Commit(ctx, meta("one at a time")); err != nil {
			t.Fatalf("Commit: %v", err)
		}
	}

	// Built in one write, through BuildManifest. The same root fields as the
	// seed, or the two stores would not hold the same data and the comparison
	// below would be measuring that instead.
	full, _ := dailyStore(t, 0)
	if _, err := full.Branch("main").Write(ctx, meta("all at once"), &testpb.Store{
		Manifest: &testpb.Manifest{SchemaVersion: 1},
		Records:  records,
	}); err != nil {
		t.Fatalf("Write: %v", err)
	}

	incHead, _, err := incremental.Branch("main").Head(ctx)
	if err != nil {
		t.Fatalf("Head: %v", err)
	}
	fullHead, _, err := full.Branch("main").Head(ctx)
	if err != nil {
		t.Fatalf("Head: %v", err)
	}

	incRoot := manifestRoot(t, incremental, incHead)
	fullRoot := manifestRoot(t, full, fullHead)
	if incRoot != fullRoot {
		t.Fatalf("a store built one commit at a time has manifest root %s;"+
			" the same store written at once has %s", incRoot, fullRoot)
	}

	// And the data itself, which the root already implies but which is worth
	// asserting separately in case the root is computed from the wrong thing.
	got, err := incremental.Branch("main").Read(ctx)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	want, err := full.Branch("main").Read(ctx)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if !proto.Equal(got, want) {
		t.Fatal("the two stores hold different data despite the same inputs")
	}
	_ = incTr
}

// TestIncrementalBuildAgreesUnderShuffledOrder is the same property when the
// commits arrive in a different order, which is what two clients writing
// concurrently produce.
func TestIncrementalBuildAgreesUnderShuffledOrder(t *testing.T) {
	ctx := context.Background()
	records := dailyRecords(60)

	build := func(order []int) protovcs.ChunkID {
		s, _ := dailyStore(t, 0)
		for _, i := range order {
			r := records[i]
			key, err := s.Layout().ItemKeyFor("records", r)
			if err != nil {
				t.Fatalf("ItemKeyFor: %v", err)
			}
			part, err := s.Branch("main").Checkout(ctx, protovcs.Prefix(string(key)))
			if err != nil {
				t.Fatalf("Checkout: %v", err)
			}
			if err := part.Put(protovcs.Item{
				Key: key, Fragment: &testpb.Store{Records: []*testpb.Record{r}},
			}); err != nil {
				t.Fatalf("Put: %v", err)
			}
			if _, err := part.Commit(ctx, meta("x")); err != nil {
				t.Fatalf("Commit: %v", err)
			}
		}
		head, _, err := s.Branch("main").Head(ctx)
		if err != nil {
			t.Fatalf("Head: %v", err)
		}
		return manifestRoot(t, s, head)
	}

	forward := make([]int, len(records))
	for i := range forward {
		forward[i] = i
	}
	shuffled := make([]int, len(forward))
	copy(shuffled, forward)
	rand.New(rand.NewSource(7)).Shuffle(len(shuffled), func(i, j int) {
		shuffled[i], shuffled[j] = shuffled[j], shuffled[i]
	})

	if a, b := build(forward), build(shuffled); a != b {
		t.Fatalf("commit order changed the manifest root: %s vs %s", a, b)
	}
}

// TestEveryLiveChunkIsInSomeAddedSet is the test that stands between a working
// store and collection deleting it.
//
// A server computes what is reachable as the union of the added sets over every
// commit reachable from a branch. If a commit references a chunk that no commit
// in its history ever declared, that chunk is unreachable as far as the server
// can tell, and collection removes it. The store then reads until the moment it
// does not.
func TestEveryLiveChunkIsInSomeAddedSet(t *testing.T) {
	ctx := context.Background()
	s, tr := dailyStore(t, 60)

	// A handful of commits of different shapes: a partial edit, a full write, a
	// delete, and an append.
	edit(t, s, "records/2000/01/05", func(st *testpb.Store) {
		st.Records[0].Amount = 999
	})
	if _, err := s.Branch("main").Change(ctx, meta("full write"), func(_ context.Context, m proto.Message) error {
		st := m.(*testpb.Store)
		st.Records = append(st.Records, &testpb.Record{
			Id: "extra", Group: "g", Date: &testpb.Date{Year: 2001, Month: 3, Day: 3},
		})
		return nil
	}); err != nil {
		t.Fatalf("Change: %v", err)
	}
	edit(t, s, "records/2000/02/03", func(st *testpb.Store) {
		st.Records = nil // emptied, which is a deletion
	})

	head, _, err := s.Branch("main").Head(ctx)
	if err != nil {
		t.Fatalf("Head: %v", err)
	}

	// What the server would consider reachable.
	declared := map[protovcs.ChunkID]bool{}
	id, err := protovcs.CommitIDFromBytes(head.GetId())
	if err != nil {
		t.Fatalf("CommitIDFromBytes: %v", err)
	}
	for {
		commits, err := tr.GetCommits(ctx, id, 1)
		if err != nil || len(commits) == 0 {
			t.Fatalf("GetCommits: %v", err)
		}
		for _, raw := range commits[0].GetAdded() {
			cid, err := protovcs.ChunkIDFromBytes(raw)
			if err != nil {
				t.Fatalf("ChunkIDFromBytes: %v", err)
			}
			declared[cid] = true
		}
		parents := commits[0].GetParents()
		if len(parents) == 0 {
			break
		}
		if id, err = protovcs.CommitIDFromBytes(parents[0]); err != nil {
			t.Fatalf("CommitIDFromBytes: %v", err)
		}
	}

	// What the head actually needs.
	for _, id := range liveChunks(t, s, head) {
		if !declared[id] {
			t.Fatalf("chunk %s is referenced by the head and declared by no commit;"+
				" collection would delete it and the store would stop reading", id)
		}
	}
}

// TestDeletingTheLastItemUnderAPrefixPrunesTheNode covers the path that is
// easiest to get wrong in an incremental update: emptying a directory.
func TestDeletingTheLastItemUnderAPrefixPrunesTheNode(t *testing.T) {
	ctx := context.Background()
	s, _ := dailyStore(t, 60)

	before, err := s.Branch("main").Read(ctx)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	beforeCount := len(before.(*testpb.Store).GetRecords())

	// 2000/01/05 holds exactly one record, so emptying it removes the only
	// entry under that day.
	edit(t, s, "records/2000/01/05", func(st *testpb.Store) { st.Records = nil })

	after, err := s.Branch("main").Read(ctx)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	got := after.(*testpb.Store).GetRecords()
	if len(got) != beforeCount-1 {
		t.Fatalf("after deleting one item the store holds %d records, want %d", len(got), beforeCount-1)
	}

	// Nothing under the deleted key is still reachable.
	fresh := freshClient(t, transportOf(t, s))
	part, err := fresh.Branch("main").Checkout(ctx, protovcs.Prefix("records/2000/01/05"))
	if err != nil {
		t.Fatalf("Checkout: %v", err)
	}
	if len(part.Items()) != 0 {
		t.Fatalf("a deleted item is still in the manifest: %v", part.Items())
	}

	// And the store still reads whole, so the pruning did not take a sibling
	// with it.
	if _, err := fresh.Branch("main").Read(ctx); err != nil {
		t.Fatalf("reading after a prune: %v", err)
	}
}

func TestDeletingEveryItemLeavesAReadableStore(t *testing.T) {
	ctx := context.Background()
	s, _ := dailyStore(t, 12)

	current, err := s.Branch("main").Read(ctx)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	for _, r := range current.(*testpb.Store).GetRecords() {
		key, err := s.Layout().ItemKeyFor("records", r)
		if err != nil {
			t.Fatalf("ItemKeyFor: %v", err)
		}
		edit(t, s, string(key), func(st *testpb.Store) { st.Records = nil })
	}

	got, err := s.Branch("main").Read(ctx)
	if err != nil {
		t.Fatalf("reading an emptied store: %v", err)
	}
	if n := len(got.(*testpb.Store).GetRecords()); n != 0 {
		t.Fatalf("an emptied store holds %d records", n)
	}
}

// edit checks out one item, mutates it, and commits, which is the path that
// exercises UpdateManifest.
func edit(t *testing.T, s *protovcs.Store, key string, mutate func(*testpb.Store)) {
	t.Helper()
	ctx := context.Background()

	part, err := s.Branch("main").Checkout(ctx, protovcs.Prefix(key))
	if err != nil {
		t.Fatalf("Checkout(%s): %v", key, err)
	}
	items := part.Items()
	if len(items) != 1 {
		t.Fatalf("Checkout(%s) returned %d items, want 1", key, len(items))
	}
	mutate(items[0].Fragment.(*testpb.Store))
	if err := part.Put(items[0]); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if _, err := part.Commit(ctx, meta("edit "+key)); err != nil {
		t.Fatalf("Commit: %v", err)
	}
}

func manifestRoot(t *testing.T, s *protovcs.Store, head *pb.Commit) protovcs.ChunkID {
	t.Helper()

	root, err := s.ManifestRootOf(context.Background(), head)
	if err != nil {
		t.Fatalf("ManifestRootOf: %v", err)
	}
	return root
}

// liveChunks returns every chunk the head version needs: its manifest nodes, its
// items, and its schema.
func liveChunks(t *testing.T, s *protovcs.Store, head *pb.Commit) []protovcs.ChunkID {
	t.Helper()

	ids, err := s.ReachableChunks(context.Background(), head)
	if err != nil {
		t.Fatalf("ReachableChunks: %v", err)
	}
	return ids
}

func transportOf(t *testing.T, s *protovcs.Store) *memtransport.Transport {
	t.Helper()
	tr, ok := s.Transport().(*memtransport.Transport)
	if !ok {
		t.Fatalf("store is not on a memtransport")
	}
	return tr
}
