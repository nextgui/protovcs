package protovcs_test

import (
	"context"
	"fmt"
	"testing"

	"google.golang.org/protobuf/proto"

	protovcs "github.com/nextgui/protovcs"
	"github.com/nextgui/protovcs/memtransport"
	testpb "github.com/nextgui/protovcs/proto/protovcs/testpb"
	pb "github.com/nextgui/protovcs/proto/protovcs/v1"
)

// The shape of a cost is what these test, not its size. An operation that is
// quietly linear where it should be logarithmic is correct, passes every other
// test in this package, and is unusable at the size protovcs exists for — so it
// is measured at two sizes and the two are compared.
//
// Counted rather than timed. A count of chunks fetched does not depend on what
// else the machine is doing, and it is what the claims are about.

// The two sizes. Ten times apart, which is enough to tell linear from constant
// without making the test slow.
const (
	smallStore = 120
	largeStore = 1200
)

// dailyLayout shards one item per day, so a store of n records is a store of
// about n items and growing the store grows the item count with it.
func dailyLayout() *pb.Layout {
	return &pb.Layout{
		Name:          "daily",
		RootMessage:   "nextgui.protovcs.testpb.Store",
		SchemaVersion: 1,
		Splits: []*pb.Split{{
			Field: "records",
			By:    []*pb.ShardKey{{Field: "date", Transform: pb.Transform_TRANSFORM_DAY}},
			Order: []string{"id"},
		}},
	}
}

// dailyRecords spreads n records over n distinct days, one each.
func dailyRecords(n int) []*testpb.Record {
	out := make([]*testpb.Record, 0, n)
	for i := 0; i < n; i++ {
		y := 2000 + i/336
		m := 1 + (i/28)%12
		d := 1 + i%28
		out = append(out, &testpb.Record{
			Id:    fmt.Sprintf("rec-%06d", i),
			Group: "g",
			Date:  &testpb.Date{Year: int32(y), Month: int32(m), Day: int32(d)},
		})
	}
	return out
}

// dailyStore returns a written store of n records and its transport, counters
// zeroed so the next operation is what gets measured.
func dailyStore(t *testing.T, n int) (*protovcs.Store, *memtransport.Transport) {
	t.Helper()

	tr := memtransport.New()
	c, err := protovcs.CompileLayout(dailyLayout(), (&testpb.Store{}).ProtoReflect().Descriptor())
	if err != nil {
		t.Fatalf("CompileLayout: %v", err)
	}
	s, err := protovcs.NewBuilder().WithTransport(tr).WithSealer(keyring(t)).WithLayout(c).Build()
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	if _, err := s.Branch("main").Write(context.Background(), meta("seed"), &testpb.Store{
		Manifest: &testpb.Manifest{SchemaVersion: 1},
		Records:  dailyRecords(n),
	}); err != nil {
		t.Fatalf("Write: %v", err)
	}
	tr.ResetCounts()
	return s, tr
}

// freshClient opens a second client on the same store, so nothing is served from
// the writer's cache.
func freshClient(t *testing.T, tr protovcs.Transport) *protovcs.Store {
	t.Helper()

	c, err := protovcs.CompileLayout(dailyLayout(), (&testpb.Store{}).ProtoReflect().Descriptor())
	if err != nil {
		t.Fatalf("CompileLayout: %v", err)
	}
	s, err := protovcs.NewBuilder().WithTransport(tr).WithSealer(keyring(t)).WithLayout(c).Build()
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	return s
}

// assertDoesNotGrow fails when a count that should be independent of store size
// grew with it.
//
// Slack of two, not one: a ten-times-larger store has a deeper manifest, so a
// logarithmic operation legitimately touches a few more nodes. Wide enough that
// depth does not fail the build, narrow enough that linear cannot hide.
func assertDoesNotGrow(t *testing.T, what string, small, large int) {
	t.Helper()
	t.Logf("%s: %d at %d records, %d at %d", what, small, smallStore, large, largeStore)
	if large > small*2+8 {
		t.Fatalf("%s grew from %d to %d when the store grew ten times;"+
			" it is meant to be independent of store size", what, small, large)
	}
}

// TestReadingOneItemDoesNotGrowWithTheStore is the claim a scoped read rests on.
func TestReadingOneItemDoesNotGrowWithTheStore(t *testing.T) {
	ctx := context.Background()

	measure := func(n int) int {
		_, tr := dailyStore(t, n)
		fresh := freshClient(t, tr)
		tr.ResetCounts()

		if _, err := fresh.Branch("main").Checkout(ctx, protovcs.Prefix("records/2000/01/05")); err != nil {
			t.Fatalf("Checkout: %v", err)
		}
		fetched, _, _ := tr.Counts()
		return fetched
	}

	assertDoesNotGrow(t, "chunks fetched to read one item", measure(smallStore), measure(largeStore))
}

// TestAScopedCheckoutDoesNotGrowWithWhatIsOutsideIt is what makes partial
// checkout worth having. Fetching a month must not depend on how many other
// months exist.
func TestAScopedCheckoutDoesNotGrowWithWhatIsOutsideIt(t *testing.T) {
	ctx := context.Background()

	measure := func(n int) int {
		_, tr := dailyStore(t, n)
		fresh := freshClient(t, tr)
		tr.ResetCounts()

		if _, err := fresh.Branch("main").Checkout(ctx, protovcs.Prefix("records/2000/01")); err != nil {
			t.Fatalf("Checkout: %v", err)
		}
		fetched, _, _ := tr.Counts()
		return fetched
	}

	assertDoesNotGrow(t, "chunks fetched for one month", measure(smallStore), measure(largeStore))
}

// TestASecondCommitUploadsOnlyWhatChanged is the claim the whole design is for.
func TestASecondCommitUploadsOnlyWhatChanged(t *testing.T) {
	ctx := context.Background()

	measure := func(n int) int {
		s, tr := dailyStore(t, n)

		part, err := s.Branch("main").Checkout(ctx, protovcs.Prefix("records/2000/01/05"))
		if err != nil {
			t.Fatalf("Checkout: %v", err)
		}
		items := part.Items()
		if len(items) != 1 {
			t.Fatalf("checked out %d items, want 1", len(items))
		}
		frag := items[0].Fragment.(*testpb.Store)
		frag.Records = append(frag.Records, &testpb.Record{
			Id: "added", Group: "g", Date: &testpb.Date{Year: 2000, Month: 1, Day: 5},
		})
		if err := part.Put(items[0]); err != nil {
			t.Fatalf("Put: %v", err)
		}

		tr.ResetCounts()
		if _, err := part.Commit(ctx, meta("one more")); err != nil {
			t.Fatalf("Commit: %v", err)
		}
		_, uploaded, _ := tr.Counts()
		return uploaded
	}

	assertDoesNotGrow(t, "chunks uploaded by a one-item change", measure(smallStore), measure(largeStore))
}

// TestAOneItemCommitDoesNotReadTheWholeStore is the failure this file exists to
// catch: a commit that walks the parent's entire manifest to work out what it
// added is correct, passes every other test, and is linear in the store.
func TestAOneItemCommitDoesNotReadTheWholeStore(t *testing.T) {
	ctx := context.Background()

	measure := func(n int) int {
		_, tr := dailyStore(t, n)
		fresh := freshClient(t, tr)

		part, err := fresh.Branch("main").Checkout(ctx, protovcs.Prefix("records/2000/01/05"))
		if err != nil {
			t.Fatalf("Checkout: %v", err)
		}
		items := part.Items()
		frag := items[0].Fragment.(*testpb.Store)
		frag.Records = append(frag.Records, &testpb.Record{
			Id: "added", Group: "g", Date: &testpb.Date{Year: 2000, Month: 1, Day: 5},
		})
		if err := part.Put(items[0]); err != nil {
			t.Fatalf("Put: %v", err)
		}

		tr.ResetCounts()
		if _, err := part.Commit(ctx, meta("one more")); err != nil {
			t.Fatalf("Commit: %v", err)
		}
		fetched, _, _ := tr.Counts()
		return fetched
	}

	assertDoesNotGrow(t, "chunks fetched by a one-item commit", measure(smallStore), measure(largeStore))
}

// TestAppendingDoesNotReadTheStore is the property the purity of a shard key
// buys: the item an element belongs to is computable from the element, so
// nothing else has to be read.
func TestAppendingDoesNotReadTheStore(t *testing.T) {
	ctx := context.Background()

	measure := func(n int) int {
		_, tr := dailyStore(t, n)
		fresh := freshClient(t, tr)
		tr.ResetCounts()

		// The item is known from the element alone, so only it is fetched.
		part, err := fresh.Branch("main").Checkout(ctx, protovcs.Prefix("records/2000/02/03"))
		if err != nil {
			t.Fatalf("Checkout: %v", err)
		}
		items := part.Items()
		frag := items[0].Fragment.(*testpb.Store)
		frag.Records = append(frag.Records, &testpb.Record{
			Id: "appended", Group: "g", Date: &testpb.Date{Year: 2000, Month: 2, Day: 3},
		})
		if err := part.Put(items[0]); err != nil {
			t.Fatalf("Put: %v", err)
		}
		fetched, _, _ := tr.Counts()
		return fetched
	}

	assertDoesNotGrow(t, "chunks fetched to append one element", measure(smallStore), measure(largeStore))
}

// TestAnIncrementalFetchDoesNotGrowWithTheStore checks the answer the server
// computes from plaintext fields alone.
func TestAnIncrementalFetchDoesNotGrowWithTheStore(t *testing.T) {
	ctx := context.Background()

	measure := func(n int) int {
		s, tr := dailyStore(t, n)

		before, _, err := s.Branch("main").Head(ctx)
		if err != nil {
			t.Fatalf("Head: %v", err)
		}
		beforeID, err := protovcs.CommitIDFromBytes(before.GetId())
		if err != nil {
			t.Fatalf("CommitIDFromBytes: %v", err)
		}

		if _, err := s.Branch("main").Change(ctx, meta("one more"),
			func(_ context.Context, m proto.Message) error {
				st := m.(*testpb.Store)
				st.Records = append(st.Records, &testpb.Record{
					Id: "added", Group: "g", Date: &testpb.Date{Year: 2000, Month: 1, Day: 5},
				})
				return nil
			}); err != nil {
			t.Fatalf("Change: %v", err)
		}

		head, _, err := s.Branch("main").Head(ctx)
		if err != nil {
			t.Fatalf("Head: %v", err)
		}
		headID, err := protovcs.CommitIDFromBytes(head.GetId())
		if err != nil {
			t.Fatalf("CommitIDFromBytes: %v", err)
		}

		missing, _, err := tr.MissingBetween(ctx, []protovcs.CommitID{beforeID}, headID)
		if err != nil {
			t.Fatalf("MissingBetween: %v", err)
		}
		return len(missing)
	}

	assertDoesNotGrow(t, "chunks an incremental fetch needs", measure(smallStore), measure(largeStore))
}

// TestManifestNodesRewrittenDoNotGrowWithTheStore is the tree property, measured
// through the store rather than against BuildManifest directly.
func TestManifestNodesRewrittenDoNotGrowWithTheStore(t *testing.T) {
	ctx := context.Background()

	measure := func(n int) int {
		s, tr := dailyStore(t, n)

		part, err := s.Branch("main").Checkout(ctx, protovcs.Prefix("records/2000/01/05"))
		if err != nil {
			t.Fatalf("Checkout: %v", err)
		}
		items := part.Items()
		frag := items[0].Fragment.(*testpb.Store)
		frag.Records = append(frag.Records, &testpb.Record{
			Id: "x", Group: "g", Date: &testpb.Date{Year: 2000, Month: 1, Day: 5},
		})
		if err := part.Put(items[0]); err != nil {
			t.Fatalf("Put: %v", err)
		}

		tr.ResetCounts()
		if _, err := part.Commit(ctx, meta("one")); err != nil {
			t.Fatalf("Commit: %v", err)
		}
		_, uploaded, _ := tr.Counts()
		// One item plus the nodes on its path plus the commit's own chunks.
		return uploaded
	}

	assertDoesNotGrow(t, "chunks written by a one-item change", measure(smallStore), measure(largeStore))
}

// TestAppendingWithoutCheckingOutTheCollection is the question the generated
// API's shape raises: adding one element must not require reading the rest.
//
// Which item an element belongs to is a pure function of the element, so the
// only thing that has to be read is the item it lands in — one item and the
// nodes above it, whatever the store holds.
func TestAppendingWithoutCheckingOutTheCollection(t *testing.T) {
	ctx := context.Background()

	measure := func(n int) int {
		_, tr := dailyStore(t, n)
		fresh := freshClient(t, tr)
		tr.ResetCounts()

		// Nothing is checked out.
		part, err := fresh.Branch("main").Checkout(ctx, protovcs.Nothing())
		if err != nil {
			t.Fatalf("Checkout: %v", err)
		}
		if len(part.Items()) != 0 {
			t.Fatalf("a checkout of nothing fetched %d items", len(part.Items()))
		}

		if err := part.PutElement(ctx, "records", &testpb.Record{
			Id: "appended", Group: "g", Date: &testpb.Date{Year: 2000, Month: 2, Day: 3},
		}); err != nil {
			t.Fatalf("PutElement: %v", err)
		}
		if _, err := part.Commit(ctx, meta("appended")); err != nil {
			t.Fatalf("Commit: %v", err)
		}

		fetched, _, _ := tr.Counts()
		return fetched
	}

	assertDoesNotGrow(t, "chunks fetched to append without a checkout",
		measure(smallStore), measure(largeStore))
}

// TestAppendingManyElementsToOneItemReadsItOnce is the other half: the cost is
// per item touched, not per element added.
func TestAppendingManyElementsToOneItemReadsItOnce(t *testing.T) {
	ctx := context.Background()

	measure := func(elements int) int {
		_, tr := dailyStore(t, smallStore)
		fresh := freshClient(t, tr)
		tr.ResetCounts()

		part, err := fresh.Branch("main").Checkout(ctx, protovcs.Nothing())
		if err != nil {
			t.Fatalf("Checkout: %v", err)
		}
		for i := 0; i < elements; i++ {
			if err := part.PutElement(ctx, "records", &testpb.Record{
				Id:    fmt.Sprintf("appended-%04d", i),
				Group: "g",
				Date:  &testpb.Date{Year: 2000, Month: 2, Day: 3},
			}); err != nil {
				t.Fatalf("PutElement: %v", err)
			}
		}
		if _, err := part.Commit(ctx, meta("appended")); err != nil {
			t.Fatalf("Commit: %v", err)
		}
		fetched, _, _ := tr.Counts()
		return fetched
	}

	one, many := measure(1), measure(200)
	t.Logf("chunks fetched: %d for one element, %d for two hundred into the same item", one, many)
	if many > one {
		t.Fatalf("appending 200 elements to one item fetched %d chunks against %d for one;"+
			" the item is meant to be read once", many, one)
	}
}

// TestReadingAfterAnotherWriterDoesNotGrowWithHistory covers the check that a
// server has not moved a branch backwards. A client that last saw an older head
// must confirm the new one descends from it, and that must cost the commits
// made since rather than every commit the branch has.
func TestReadingAfterAnotherWriterDoesNotGrowWithHistory(t *testing.T) {
	ctx := context.Background()

	measure := func(commits int) int {
		writer, tr := dailyStore(t, 10)
		appendOne := func(i int) {
			p, err := writer.Branch("main").Checkout(ctx, protovcs.Nothing())
			if err != nil {
				t.Fatalf("Checkout: %v", err)
			}
			if err := p.PutElement(ctx, "records", &testpb.Record{
				Id:    fmt.Sprintf("extra-%06d", i),
				Group: "g",
				Date:  &testpb.Date{Year: 2030, Month: 1, Day: 1},
			}); err != nil {
				t.Fatalf("PutElement: %v", err)
			}
			if _, err := p.Commit(ctx, meta("append")); err != nil {
				t.Fatalf("Commit: %v", err)
			}
		}
		for i := 0; i < commits; i++ {
			appendOne(i)
		}

		counted := &commitCounter{Transport: tr}
		reader := freshClient(t, counted)
		if _, _, err := reader.Branch("main").Head(ctx); err != nil {
			t.Fatalf("Head: %v", err)
		}
		appendOne(commits)

		counted.calls = 0
		if _, _, err := reader.Branch("main").Head(ctx); err != nil {
			t.Fatalf("Head after another writer: %v", err)
		}
		return counted.calls
	}

	small, large := measure(smallStore), measure(largeStore)
	t.Logf("commit fetches after one new commit: %d with %d commits of history, %d with %d",
		small, smallStore, large, largeStore)
	if large > small+2 {
		t.Fatalf("commit fetches grew from %d to %d when the history grew ten times; "+
			"confirming one new commit is meant to cost one commit's worth", small, large)
	}
}

// commitCounter counts GetCommits calls, which memtransport's counters leave out.
type commitCounter struct {
	protovcs.Transport
	calls int
}

func (c *commitCounter) GetCommits(ctx context.Context, from protovcs.CommitID, depth uint32) ([]*pb.Commit, error) {
	c.calls++
	return c.Transport.GetCommits(ctx, from, depth)
}
