package server_test

import (
	"context"
	"testing"
	"time"

	protovcs "github.com/nextgui/protovcs"
	pb "github.com/nextgui/protovcs/proto/protovcs/v1"
	"github.com/nextgui/protovcs/server"
)

// hooked wraps MemStorage so a test can act at one exact point of another
// operation.
type hooked struct {
	*server.MemStorage
	onListChunks        func()
	onDeleteReservation func(id string)
	onGetCondemned      func()
	onListRefs          func()
}

func (h *hooked) ListChunks(ctx context.Context, storeID string) ([]protovcs.ChunkID, error) {
	if f := h.onListChunks; f != nil {
		h.onListChunks = nil
		f()
	}
	return h.MemStorage.ListChunks(ctx, storeID)
}

func (h *hooked) DeleteReservation(ctx context.Context, storeID, id string) error {
	if f := h.onDeleteReservation; f != nil {
		h.onDeleteReservation = nil
		f(id)
	}
	return h.MemStorage.DeleteReservation(ctx, storeID, id)
}

func (h *hooked) GetCondemned(ctx context.Context, storeID string) (*server.Condemned, error) {
	if f := h.onGetCondemned; f != nil {
		h.onGetCondemned = nil
		f()
	}
	return h.MemStorage.GetCondemned(ctx, storeID)
}

func (h *hooked) ListRefs(ctx context.Context, storeID string) ([]protovcs.RefState, error) {
	if f := h.onListRefs; f != nil {
		h.onListRefs = nil
		f()
	}
	return h.MemStorage.ListRefs(ctx, storeID)
}

func raceServer(t *testing.T, quota *pb.Quota) (*server.Server, *hooked, *time.Time) {
	t.Helper()
	now := time.Unix(1_800_000_000, 0).UTC()
	h := &hooked{MemStorage: server.NewMemStorage().WithClock(func() time.Time { return now })}
	s := server.New(h).WithClock(func() time.Time { return now })
	if _, err := s.CreateStore(context.Background(), &pb.CreateStoreRequest{StoreId: storeID, Quota: quota}); err != nil {
		t.Fatal(err)
	}
	return s, h, &now
}

func writeCommit(t *testing.T, s *server.Server, n byte, c protovcs.SealedChunk) protovcs.CommitID {
	t.Helper()
	ctx := context.Background()
	r, err := s.BeginWrite(ctx, storeID, 1, uint64(len(c.Sealed)))
	if err != nil {
		t.Fatalf("BeginWrite: %v", err)
	}
	if err := s.PutChunks(ctx, storeID, r.ID, []protovcs.SealedChunk{c}); err != nil {
		t.Fatalf("PutChunks: %v", err)
	}
	id, err := s.PutCommit(ctx, storeID, r.ID, commitNumbered(n, c.ID))
	if err != nil {
		t.Fatalf("PutCommit: %v", err)
	}
	return id
}

func TestABranchMovedDuringCollectionKeepsItsChunks(t *testing.T) {
	ctx := context.Background()
	s, h, now := raceServer(t, &pb.Quota{MaxBytes: 1 << 20})

	x := chunk(1, 100)
	c1 := writeCommit(t, s, 1, x)
	main := []byte("main")
	st, err := s.UpdateRef(ctx, storeID, main, c1, "", false)
	if err != nil {
		t.Fatal(err)
	}
	c2 := writeCommit(t, s, 2, chunk(2, 100))
	if _, err := s.UpdateRef(ctx, storeID, main, c2, st.CASToken, true); err != nil {
		t.Fatal(err)
	}

	*now = now.Add(2 * time.Hour)
	if res, err := s.Collect(ctx, storeID); err != nil || res.Marked != 1 {
		t.Fatalf("first run: %+v %v", res, err)
	}

	*now = now.Add(2 * time.Hour)
	// After reachable() has run, somebody restores C1 onto a new branch.
	h.onListChunks = func() {
		if _, err := s.UpdateRef(ctx, storeID, []byte("restored"), c1, "", false); err != nil {
			t.Errorf("UpdateRef: %v", err)
		}
	}
	res, err := s.Collect(ctx, storeID)
	if err != nil {
		t.Fatal(err)
	}
	has, _ := h.HasChunk(ctx, storeID, x.ID)
	if !has {
		t.Errorf("branch 'restored' names commit %s, but collection (%+v) deleted its chunk", c1, res)
	}
}

func TestACommitCannotNameAChunkDeletedBetweenItsChecks(t *testing.T) {
	ctx := context.Background()
	s, h, now := raceServer(t, &pb.Quota{MaxBytes: 1 << 20})

	x := chunk(1, 100)
	r, _ := s.BeginWrite(ctx, storeID, 1, 100)
	if err := s.PutChunks(ctx, storeID, r.ID, []protovcs.SealedChunk{x}); err != nil {
		t.Fatal(err)
	}
	*now = now.Add(server.ReservationTTL + time.Minute)
	s.ExpireReservations(ctx, storeID)

	*now = now.Add(2 * time.Hour)
	if res, err := s.Collect(ctx, storeID); err != nil || res.Marked != 1 {
		t.Fatalf("first run: %+v %v", res, err)
	}
	*now = now.Add(2 * time.Hour)

	// A writer whose HasChunks answer predates the mark commits X with no
	// reservation. Collection claims and deletes X after PutCommit's HasChunk.
	h.onGetCondemned = func() {
		if res, err := s.Collect(ctx, storeID); err != nil || res.Deleted != 1 {
			t.Errorf("second run: %+v %v", res, err)
		}
	}
	id, err := s.PutCommit(ctx, storeID, "", commitNumbered(1, x.ID))
	if err == nil {
		has, _ := h.HasChunk(ctx, storeID, x.ID)
		t.Errorf("PutCommit stored %s naming chunk %s; chunk present: %v", id, x.ID, has)
	}
}
