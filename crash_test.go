package protovcs_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"

	"google.golang.org/protobuf/proto"

	protovcs "github.com/nextgui/protovcs"
	"github.com/nextgui/protovcs/memtransport"
	testpb "github.com/nextgui/protovcs/proto/protovcs/testpb"
)

// The write path stores chunks, then records a commit, then moves the branch.
// The order is its entire crash safety, and until now it was asserted by reading
// the code: no test could stop between two of those steps.
//
// A probe can. Each test below fails the write at one point and then checks what
// a client that came along afterwards would see.

var errCrashed = errors.New("crashed")

func crashAt(point protovcs.ProbePoint) protovcs.Probe {
	return protovcs.ProbeFunc(func(_ context.Context, p protovcs.ProbePoint) error {
		if p == point {
			return errCrashed
		}
		return nil
	})
}

func storeWithProbe(t *testing.T, tr protovcs.Transport, p protovcs.Probe) *protovcs.Store {
	t.Helper()

	c, err := protovcs.CompileLayout(layout(), (&testpb.Store{}).ProtoReflect().Descriptor())
	if err != nil {
		t.Fatalf("CompileLayout: %v", err)
	}
	s, err := protovcs.NewBuilder().
		WithTransport(tr).WithSealer(keyring(t)).WithLayout(c).WithProbe(p).Build()
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	return s
}

// TestACrashAfterUploadLeavesGarbageNotDamage covers the first half of the
// window. Chunks are stored, nothing references them.
func TestACrashAfterUploadLeavesGarbageNotDamage(t *testing.T) {
	ctx := context.Background()
	tr := memtransport.New()

	good := newStore(t, tr, keyring(t))
	if _, err := good.Branch("main").Write(ctx, meta("first"), &testpb.Store{
		Manifest: &testpb.Manifest{SchemaVersion: 1},
		Records:  records(20),
	}); err != nil {
		t.Fatalf("Write: %v", err)
	}
	before := tr.ChunkCount()

	crashing := storeWithProbe(t, tr, crashAt(protovcs.ProbeAfterUpload))
	_, err := crashing.Branch("main").Change(ctx, meta("interrupted"),
		func(_ context.Context, m proto.Message) error {
			st := m.(*testpb.Store)
			st.Records = append(st.Records, &testpb.Record{
				Id: "lost", Group: "alpha", Date: &testpb.Date{Year: 2026, Month: 9, Day: 9},
			})
			return nil
		})
	if !errors.Is(err, errCrashed) {
		t.Fatalf("the probe did not stop the write: %v", err)
	}

	// Chunks landed and nothing points at them, which is the state the ordering
	// is chosen to produce. Collection removes them later.
	if tr.ChunkCount() <= before {
		t.Fatal("the fixture crashed before any chunk was stored, so it proves nothing")
	}

	// The store is exactly as it was, read by a client that knows nothing about
	// what happened.
	fresh := newStore(t, tr, keyring(t))
	got, err := fresh.Branch("main").Read(ctx)
	if err != nil {
		t.Fatalf("reading after a crash: %v", err)
	}
	if n := len(got.(*testpb.Store).GetRecords()); n != 20 {
		t.Fatalf("after a crash the store holds %d records, want the 20 it held before", n)
	}
	for _, r := range got.(*testpb.Store).GetRecords() {
		if r.GetId() == "lost" {
			t.Fatal("an interrupted write became visible")
		}
	}
}

// TestACrashAfterTheCommitLeavesTheBranchWhereItWas covers the dangerous half.
// A complete version exists and no branch reaches it.
func TestACrashAfterTheCommitLeavesTheBranchWhereItWas(t *testing.T) {
	ctx := context.Background()
	tr := memtransport.New()

	good := newStore(t, tr, keyring(t))
	if _, err := good.Branch("main").Write(ctx, meta("first"), &testpb.Store{
		Manifest: &testpb.Manifest{SchemaVersion: 1},
		Records:  records(20),
	}); err != nil {
		t.Fatalf("Write: %v", err)
	}
	head, state, err := good.Branch("main").Head(ctx)
	if err != nil {
		t.Fatalf("Head: %v", err)
	}
	wasAt := head.GetId()

	crashing := storeWithProbe(t, tr, crashAt(protovcs.ProbeAfterCommit))
	if _, err := crashing.Branch("main").Change(ctx, meta("interrupted"),
		func(_ context.Context, m proto.Message) error {
			st := m.(*testpb.Store)
			st.Records = append(st.Records, &testpb.Record{
				Id: "lost", Group: "alpha", Date: &testpb.Date{Year: 2026, Month: 9, Day: 9},
			})
			return nil
		}); !errors.Is(err, errCrashed) {
		t.Fatalf("the probe did not stop the write: %v", err)
	}

	fresh := newStore(t, tr, keyring(t))
	after, afterState, err := fresh.Branch("main").Head(ctx)
	if err != nil {
		t.Fatalf("Head: %v", err)
	}
	if string(after.GetId()) != string(wasAt) {
		t.Fatal("a crash between the commit and the ref moved the branch")
	}
	if afterState.CASToken != state.CASToken {
		t.Fatal("the branch was touched by a write that did not finish")
	}

	// And the store reads, which is the property the ordering buys: an
	// unreferenced commit is garbage, while a branch pointing at an incomplete
	// version would be damage.
	got, err := fresh.Branch("main").Read(ctx)
	if err != nil {
		t.Fatalf("reading after a crash: %v", err)
	}
	if n := len(got.(*testpb.Store).GetRecords()); n != 20 {
		t.Fatalf("after a crash the store holds %d records, want 20", n)
	}
}

// TestAWriteRetriedAfterACrashSucceeds is what makes the crash survivable
// rather than merely harmless. The chunks the interrupted attempt stored are
// content-addressed, so the retry finds them present and uploads nothing.
func TestAWriteRetriedAfterACrashSucceeds(t *testing.T) {
	ctx := context.Background()
	tr := memtransport.New()

	good := newStore(t, tr, keyring(t))
	if _, err := good.Branch("main").Write(ctx, meta("first"), &testpb.Store{
		Manifest: &testpb.Manifest{SchemaVersion: 1},
		Records:  records(20),
	}); err != nil {
		t.Fatalf("Write: %v", err)
	}

	add := func(_ context.Context, m proto.Message) error {
		st := m.(*testpb.Store)
		st.Records = append(st.Records, &testpb.Record{
			Id: "retried", Group: "alpha", Date: &testpb.Date{Year: 2026, Month: 9, Day: 9},
		})
		return nil
	}

	crashing := storeWithProbe(t, tr, crashAt(protovcs.ProbeAfterCommit))
	if _, err := crashing.Branch("main").Change(ctx, meta("attempt one"), add); !errors.Is(err, errCrashed) {
		t.Fatalf("the probe did not stop the write: %v", err)
	}
	afterCrash := tr.ChunkCount()

	// The same write again, from a client that knows nothing about the first.
	retry := newStore(t, tr, keyring(t))
	if _, err := retry.Branch("main").Change(ctx, meta("attempt two"), add); err != nil {
		t.Fatalf("retrying after a crash: %v", err)
	}

	if tr.ChunkCount() != afterCrash {
		t.Fatalf("the retry stored %d new chunks; the first attempt's chunks should have been found present",
			tr.ChunkCount()-afterCrash)
	}

	got, err := retry.Branch("main").Read(ctx)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	found := false
	for _, r := range got.(*testpb.Store).GetRecords() {
		if r.GetId() == "retried" {
			found = true
		}
	}
	if !found {
		t.Fatal("the retried write did not land")
	}
}

// TestAPreciselyInterleavedRaceIsRetried uses the probe to produce the losing
// interleaving deliberately rather than by arranging for it inside a mutation
// closure and hoping.
func TestAPreciselyInterleavedRaceIsRetried(t *testing.T) {
	ctx := context.Background()
	tr := memtransport.New()

	base := newStore(t, tr, keyring(t))
	if _, err := base.Branch("main").Write(ctx, meta("base"), &testpb.Store{
		Manifest: &testpb.Manifest{SchemaVersion: 1},
		Records:  records(10),
	}); err != nil {
		t.Fatalf("Write: %v", err)
	}

	other := newStore(t, tr, keyring(t))

	// The first time the slow writer has read the head, the other writer
	// commits. That is the exact window where a compare-and-swap must lose.
	var once sync.Once
	slow := storeWithProbe(t, tr, protovcs.ProbeFunc(func(ctx context.Context, p protovcs.ProbePoint) error {
		if p != protovcs.ProbeAfterHead {
			return nil
		}
		var err error
		once.Do(func() {
			_, err = other.Branch("main").Change(ctx, meta("from the other"),
				func(_ context.Context, m proto.Message) error {
					st := m.(*testpb.Store)
					st.Records = append(st.Records, &testpb.Record{
						Id: "other", Group: "beta", Date: &testpb.Date{Year: 2026, Month: 7, Day: 7},
					})
					return nil
				})
		})
		return err
	}))

	if _, err := slow.Branch("main").Change(ctx, meta("from the slow one"),
		func(_ context.Context, m proto.Message) error {
			st := m.(*testpb.Store)
			st.Records = append(st.Records, &testpb.Record{
				Id: "slow", Group: "gamma", Date: &testpb.Date{Year: 2026, Month: 8, Day: 8},
			})
			return nil
		}); err != nil {
		t.Fatalf("Change: %v", err)
	}

	got, err := slow.Branch("main").Read(ctx)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	ids := map[string]bool{}
	for _, r := range got.(*testpb.Store).GetRecords() {
		ids[r.GetId()] = true
	}
	if !ids["slow"] || !ids["other"] {
		t.Fatalf("a precisely interleaved race lost work: slow=%v other=%v", ids["slow"], ids["other"])
	}
}

// TestConcurrentWritersAllLand runs real goroutines, which nothing did before.
//
// Every one of them contends for the same branch, so all but one lose a race on
// each attempt and retry. The property is that none of their work is lost, and
// that the retry loop terminates.
func TestConcurrentWritersAllLand(t *testing.T) {
	ctx := context.Background()
	tr := memtransport.New()

	base := newStore(t, tr, keyring(t))
	if _, err := base.Branch("main").Write(ctx, meta("base"), &testpb.Store{
		Manifest: &testpb.Manifest{SchemaVersion: 1},
	}); err != nil {
		t.Fatalf("Write: %v", err)
	}

	const writers = 4
	var wg sync.WaitGroup
	errs := make([]error, writers)

	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			// A client each, as separate machines would have.
			s := newStore(t, tr, keyring(t))
			for i := 0; i < 3; i++ {
				id := fmt.Sprintf("w%d-%d", w, i)
				if _, err := s.Branch("main").Change(ctx, meta(id),
					func(_ context.Context, m proto.Message) error {
						st := m.(*testpb.Store)
						st.Records = append(st.Records, &testpb.Record{
							Id:    id,
							Group: fmt.Sprintf("g%d", w),
							Date:  &testpb.Date{Year: 2026, Month: int32(1 + i), Day: 1},
						})
						return nil
					}); err != nil {
					errs[w] = err
					return
				}
			}
		}(w)
	}
	wg.Wait()

	for w, err := range errs {
		if err != nil {
			t.Fatalf("writer %d: %v", w, err)
		}
	}

	got, err := newStore(t, tr, keyring(t)).Branch("main").Read(ctx)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	have := map[string]bool{}
	for _, r := range got.(*testpb.Store).GetRecords() {
		if have[r.GetId()] {
			t.Fatalf("%s landed twice", r.GetId())
		}
		have[r.GetId()] = true
	}
	for w := 0; w < writers; w++ {
		for i := 0; i < 3; i++ {
			id := fmt.Sprintf("w%d-%d", w, i)
			if !have[id] {
				t.Fatalf("%s was lost by a concurrent write", id)
			}
		}
	}
}
