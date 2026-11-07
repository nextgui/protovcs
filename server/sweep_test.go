package server_test

import (
	"context"
	"errors"
	"testing"
	"time"

	protovcs "github.com/nextgui/protovcs"
	pb "github.com/nextgui/protovcs/proto/protovcs/v1"
	"github.com/nextgui/protovcs/server"
)

// sweepFixture is storage with a fixed clock and the server over it.
//
// The clock is fixed because collection spares a chunk younger than
// server.CollectionGrace, so a test that wants a chunk collected has to move
// time rather than wait an hour.
type sweepFixture struct {
	server  *server.Server
	storage *server.MemStorage
	now     time.Time
}

func newSweepFixture(t *testing.T) *sweepFixture {
	t.Helper()

	f := &sweepFixture{now: time.Unix(1_800_000_000, 0).UTC()}
	f.storage = server.NewMemStorage().WithClock(func() time.Time { return f.now })
	f.server = server.New(f.storage).WithClock(func() time.Time { return f.now })
	return f
}

// createStore makes a store the sweep will find.
func (f *sweepFixture) createStore(t *testing.T, id string) {
	t.Helper()

	if _, err := f.server.CreateStore(context.Background(), &pb.CreateStoreRequest{
		StoreId: id,
		Owner:   "someone",
		Quota:   &pb.Quota{MaxBytes: 1 << 20},
	}); err != nil {
		t.Fatalf("CreateStore(%s): %v", id, err)
	}
}

// writeGarbage uploads one chunk, commits nothing and lets the reservation
// expire, which is how a store ends up holding a chunk nothing reaches.
func (f *sweepFixture) writeGarbage(t *testing.T, id string, n byte) protovcs.ChunkID {
	t.Helper()

	ctx := context.Background()
	r, err := f.server.BeginWrite(ctx, id, 10, 10000)
	if err != nil {
		t.Fatalf("BeginWrite(%s): %v", id, err)
	}
	c := chunk(n, 100)
	if err := f.server.PutChunks(ctx, id, r.ID, []protovcs.SealedChunk{c}); err != nil {
		t.Fatalf("PutChunks(%s): %v", id, err)
	}
	return c.ID
}

// findStore returns the record for one store in a sweep result.
func findStore(t *testing.T, res server.SweepResult, id string) server.StoreSweep {
	t.Helper()

	for _, s := range res.Stores {
		if s.StoreID == id {
			return s
		}
	}
	t.Fatalf("the sweep visited %d stores and none of them was %s", len(res.Stores), id)
	return server.StoreSweep{}
}

// TestASweepCollectsEveryStore is what the scheduled sweep is for.
//
// The pass takes no store id. A caller that had to name one would need a list
// of stores to name, and the thing driving the sweep is a schedule rather than
// a client that knows which stores exist.
func TestASweepCollectsEveryStore(t *testing.T) {
	ctx := context.Background()
	f := newSweepFixture(t)
	f.createStore(t, "store-a")
	f.createStore(t, "store-b")
	garbageA := f.writeGarbage(t, "store-a", 1)
	garbageB := f.writeGarbage(t, "store-b", 2)

	// Past the grace period and past the reservation, so the chunks are
	// collectable and the reservations are expired.
	// The first sweep releases the reservations and marks the chunks; the
	// next, a grace period later, deletes them.
	f.now = f.now.Add(2 * server.CollectionGrace)
	first, err := f.server.Sweep(ctx)
	if err != nil {
		t.Fatalf("Sweep: %v", err)
	}
	f.now = f.now.Add(2 * server.CollectionGrace)

	res, err := f.server.Sweep(ctx)
	if err != nil {
		t.Fatalf("Sweep: %v", err)
	}
	if len(res.Stores) != 2 {
		t.Fatalf("the sweep visited %d stores, want 2", len(res.Stores))
	}
	if failed := res.Failed(); len(failed) != 0 {
		t.Fatalf("the sweep failed on %d stores, first: %v", len(failed), failed[0].Err)
	}

	for _, want := range []struct {
		store string
		chunk protovcs.ChunkID
	}{{"store-a", garbageA}, {"store-b", garbageB}} {
		got := findStore(t, res, want.store)
		if got.Collected.Deleted != 1 {
			t.Errorf("%s: the sweep deleted %d chunks, want 1", want.store, got.Collected.Deleted)
		}
		if released := findStore(t, first, want.store).ReservationsReleased; released != 1 {
			t.Errorf("%s: the first sweep released %d reservations, want 1", want.store, released)
		}
		if has, err := f.storage.HasChunk(ctx, want.store, want.chunk); err != nil || has {
			t.Errorf("%s: the sweep kept an unreachable chunk", want.store)
		}
	}
}

// TestASweepReportsWhatItDid is the half of this that stops a sweep failing
// silently.
//
// A sweep that never runs and one that ran and found nothing look the same from
// outside. The result says which, so the caller can log it and a reader can
// tell.
func TestASweepReportsWhatItDid(t *testing.T) {
	f := newSweepFixture(t)
	f.createStore(t, "store-a")

	res, err := f.server.Sweep(context.Background())
	if err != nil {
		t.Fatalf("Sweep: %v", err)
	}
	got := findStore(t, res, "store-a")
	if got.Collected.Deleted != 0 {
		t.Errorf("a sweep over an empty store deleted %d chunks", got.Collected.Deleted)
	}
	if got.UsageAfter == nil {
		t.Error("the sweep reconciled nothing, so nothing reports whether usage had drifted")
	}
	if got.UsageDrifted() {
		t.Errorf("usage on an untouched store read as drifted: %v then %v", got.UsageBefore, got.UsageAfter)
	}
}

// failAfterFirstStore is storage that refuses to list the chunks of one store
// and behaves normally for every other.
//
// Collection is the operation that deletes things, so it fails closed: any
// error abandons the run for that store. This is how that failure is produced
// without breaking anything else.
//
// It embeds the concrete MemStorage rather than the Storage interface.
// Collection asks its backend when a chunk was written through a second
// interface that Storage does not carry, so embedding the interface would hide
// that method and nothing would ever be collected.
type failAfterFirstStore struct {
	*server.MemStorage

	failFor string
	err     error
}

func (f *failAfterFirstStore) ListChunks(ctx context.Context, storeID string) ([]protovcs.ChunkID, error) {
	if storeID == f.failFor {
		return nil, f.err
	}
	return f.MemStorage.ListChunks(ctx, storeID)
}

// TestAFailedStoreDoesNotStopTheSweep keeps the property the in-process ticker
// had.
//
// The ticker logged a failure and the next tick tried again, so one store that
// could not be collected never suspended collection for the rest. A scheduled
// pass has to behave the same way: stopping at the first failure would mean one
// broken store stops every store listed after it from ever being collected.
func TestAFailedStoreDoesNotStopTheSweep(t *testing.T) {
	ctx := context.Background()
	f := newSweepFixture(t)
	f.createStore(t, "store-a")
	f.createStore(t, "store-b")
	garbage := f.writeGarbage(t, "store-b", 2)
	f.now = f.now.Add(2 * server.CollectionGrace)
	if _, err := f.server.Sweep(ctx); err != nil {
		t.Fatalf("Sweep: %v", err)
	}
	f.now = f.now.Add(2 * server.CollectionGrace)

	broken := errors.New("the bucket said no")
	s := server.New(&failAfterFirstStore{MemStorage: f.storage, failFor: "store-a", err: broken}).
		WithClock(func() time.Time { return f.now })

	res, err := s.Sweep(ctx)
	if err != nil {
		t.Fatalf("Sweep returned an error for one broken store, which abandons the pass: %v", err)
	}

	failed := res.Failed()
	if len(failed) != 1 || failed[0].StoreID != "store-a" {
		t.Fatalf("the sweep reported %d failures, want just store-a", len(failed))
	}
	if !errors.Is(failed[0].Err, broken) {
		t.Errorf("store-a failed with %v, which does not wrap what storage returned", failed[0].Err)
	}

	if got := findStore(t, res, "store-b"); got.Collected.Deleted != 1 {
		t.Errorf("store-b, listed after the broken store, deleted %d chunks, want 1", got.Collected.Deleted)
	}
	if has, err := f.storage.HasChunk(ctx, "store-b", garbage); err != nil || has {
		t.Error("a store listed after a broken one was not collected")
	}
}

// TestASweepWithNoStoreListFails separates the two failures.
//
// A pass that visited every store and failed on some of them has done work and
// reports it. A pass that could not read the store list has done nothing at
// all, and a caller that treated the two alike would report an empty sweep as a
// successful one.
func TestASweepWithNoStoreListFails(t *testing.T) {
	broken := errors.New("the bucket said no")
	s := server.New(&noStoreList{MemStorage: server.NewMemStorage(), err: broken})

	if _, err := s.Sweep(context.Background()); !errors.Is(err, broken) {
		t.Fatalf("Sweep with an unreadable store list returned %v, want the storage error", err)
	}
}

// noStoreList is storage whose store list cannot be read.
type noStoreList struct {
	*server.MemStorage

	err error
}

func (n *noStoreList) ListStores(ctx context.Context, req *pb.ListStoresRequest) ([]*pb.Store, error) {
	return nil, n.err
}
