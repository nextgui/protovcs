package server_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	protovcs "github.com/nextgui/protovcs"
	pb "github.com/nextgui/protovcs/proto/protovcs/v1"
	"github.com/nextgui/protovcs/server"
)

// deletedStore sets up a store holding one committed chunk, then deletes it.
func deletedStore(t *testing.T, retention time.Duration) (*server.Server, *server.MemStorage, *time.Time, protovcs.ChunkID) {
	t.Helper()
	ctx := context.Background()

	now := time.Unix(1_800_000_000, 0).UTC()
	storage := server.NewMemStorage().WithClock(func() time.Time { return now })
	s := server.New(storage).WithClock(func() time.Time { return now }).WithDeletionRetention(retention)
	if _, err := s.CreateStore(ctx, &pb.CreateStoreRequest{StoreId: storeID, Quota: &pb.Quota{MaxBytes: 1 << 20}}); err != nil {
		t.Fatalf("CreateStore: %v", err)
	}
	r, err := s.BeginWrite(ctx, storeID, 1, 100)
	if err != nil {
		t.Fatalf("BeginWrite: %v", err)
	}
	c := chunk(7, 100)
	if err := s.PutChunks(ctx, storeID, r.ID, []protovcs.SealedChunk{c}); err != nil {
		t.Fatalf("PutChunks: %v", err)
	}
	id, err := s.PutCommit(ctx, storeID, r.ID, commitNumbered(1, c.ID))
	if err != nil {
		t.Fatalf("PutCommit: %v", err)
	}
	if _, err := s.UpdateRef(ctx, storeID, []byte("main"), id, "", false); err != nil {
		t.Fatalf("UpdateRef: %v", err)
	}

	st, err := s.DeleteStore(ctx, storeID)
	if err != nil {
		t.Fatalf("DeleteStore: %v", err)
	}
	if st.GetState() != pb.Store_STATE_DELETING || !st.GetDeletedAt().AsTime().Equal(now) {
		t.Fatalf("DeleteStore left %v at %v, want deleting at %v", st.GetState(), st.GetDeletedAt().AsTime(), now)
	}
	return s, storage, &now, c.ID
}

func hasChunk(t *testing.T, storage *server.MemStorage, id protovcs.ChunkID) bool {
	t.Helper()
	has, err := storage.HasChunk(context.Background(), storeID, id)
	if err != nil {
		t.Fatalf("HasChunk: %v", err)
	}
	return has
}

func TestADeletedStoreCanBeRestoredInsideItsWindow(t *testing.T) {
	ctx := context.Background()
	s, storage, now, id := deletedStore(t, time.Hour)

	*now = now.Add(30 * time.Minute)
	res, err := s.Collect(ctx, storeID)
	if err != nil || !res.DeletionPending || res.Deleted != 0 {
		t.Fatalf("collection inside the window returned %+v, %v, want pending and nothing deleted", res, err)
	}
	if _, err := s.BeginWrite(ctx, storeID, 1, 10); err == nil {
		t.Fatal("a deleted store accepted a write")
	}

	st, err := s.RestoreStore(ctx, storeID)
	if err != nil {
		t.Fatalf("RestoreStore: %v", err)
	}
	if st.GetState() != pb.Store_STATE_ACTIVE || st.GetDeletedAt() != nil {
		t.Fatalf("RestoreStore left %v, deleted at %v", st.GetState(), st.GetDeletedAt())
	}
	if !hasChunk(t, storage, id) {
		t.Fatal("a restored store lost its chunk")
	}
	if _, err := s.BeginWrite(ctx, storeID, 1, 10); err != nil {
		t.Fatalf("a restored store refused a write: %v", err)
	}

	// Long after the original window, the restored store is ordinary.
	*now = now.Add(24 * time.Hour)
	if _, err := s.Collect(ctx, storeID); err != nil {
		t.Fatalf("Collect: %v", err)
	}
	if !hasChunk(t, storage, id) {
		t.Fatal("collection deleted a reachable chunk of a restored store")
	}
}

func TestADeletedStoreIsPurgedAfterItsWindow(t *testing.T) {
	ctx := context.Background()
	s, storage, now, id := deletedStore(t, time.Hour)

	*now = now.Add(time.Hour + time.Minute)
	res, err := s.Collect(ctx, storeID)
	if err != nil || res.DeletionPending || res.Deleted == 0 {
		t.Fatalf("collection after the window returned %+v, %v, want the store purged", res, err)
	}
	if hasChunk(t, storage, id) {
		t.Fatal("a purged store kept its chunk")
	}
	st, err := s.GetStore(ctx, storeID)
	if err != nil || st.GetState() != pb.Store_STATE_PURGING {
		t.Fatalf("after purging the store is %v, %v, want purging", st.GetState(), err)
	}
	if _, err := s.RestoreStore(ctx, storeID); !errors.Is(err, server.ErrNotRestorable) {
		t.Fatalf("RestoreStore on a purged store returned %v, want ErrNotRestorable", err)
	}
}

func TestNoWindowPurgesOnTheNextCollection(t *testing.T) {
	s, storage, _, id := deletedStore(t, 0)
	if res, err := s.Collect(context.Background(), storeID); err != nil || res.Deleted == 0 {
		t.Fatalf("Collect returned %+v, %v, want the store purged at once", res, err)
	}
	if hasChunk(t, storage, id) {
		t.Fatal("a store deleted with no window kept its chunk")
	}
}

func TestDeletingTwiceKeepsTheFirstTime(t *testing.T) {
	ctx := context.Background()
	s, _, now, _ := deletedStore(t, time.Hour)
	first := *now
	*now = now.Add(50 * time.Minute)
	st, err := s.DeleteStore(ctx, storeID)
	if err != nil || !st.GetDeletedAt().AsTime().Equal(first) {
		t.Fatalf("a second DeleteStore moved deleted_at to %v (%v), want %v", st.GetDeletedAt().AsTime(), err, first)
	}
}

// A store marked deleting before deleted_at existed has no window.
func TestAStoreMarkedWithoutATimeHasNoWindow(t *testing.T) {
	ctx := context.Background()
	s, storage, _, id := deletedStore(t, time.Hour)
	if _, err := storage.Update(ctx, storeID, func(st *pb.Store) error {
		st.DeletedAt = nil
		return nil
	}); err != nil {
		t.Fatalf("Update: %v", err)
	}
	if res, err := s.Collect(ctx, storeID); err != nil || res.Deleted == 0 {
		t.Fatalf("Collect returned %+v, %v, want the store purged", res, err)
	}
	if hasChunk(t, storage, id) {
		t.Fatal("a store marked without a time kept its chunk")
	}
}

// restoreDuringCollection restores the store just after collection has read
// it, the moment a restore and a purge race.
type restoreDuringCollection struct {
	*server.MemStorage
	once    sync.Once
	restore func()
}

func (r *restoreDuringCollection) GetStore(ctx context.Context, id string) (*pb.Store, error) {
	st, err := r.MemStorage.GetStore(ctx, id)
	r.once.Do(r.restore)
	return st, err
}

func TestARestoreDuringCollectionWins(t *testing.T) {
	ctx := context.Background()
	s, mem, now, id := deletedStore(t, time.Hour)
	*now = now.Add(2 * time.Hour)

	storage := &restoreDuringCollection{MemStorage: mem}
	racing := server.New(storage).WithClock(func() time.Time { return *now })
	storage.restore = func() {
		if _, err := s.RestoreStore(ctx, storeID); err != nil {
			t.Errorf("RestoreStore: %v", err)
		}
	}

	if _, err := racing.Collect(ctx, storeID); err != nil {
		t.Fatalf("Collect: %v", err)
	}
	if !hasChunk(t, mem, id) {
		t.Fatal("collection purged a store restored after it read the store")
	}
	st, err := s.GetStore(ctx, storeID)
	if err != nil || st.GetState() != pb.Store_STATE_ACTIVE {
		t.Fatalf("the store is %v, %v, want active", st.GetState(), err)
	}
}

// commitsUnreadable fails every commit read, as storage does once a store's
// history is damaged.
type commitsUnreadable struct {
	*server.MemStorage
}

func (c commitsUnreadable) GetCommit(ctx context.Context, storeID string, id protovcs.CommitID) (*pb.Commit, error) {
	return nil, errors.New("the commit could not be read")
}

// Purging deletes every chunk, so it does not read the history, and a commit
// that cannot be read does not keep the store from being purged or swept.
func TestAPurgeFinishesWhenTheHistoryCannotBeRead(t *testing.T) {
	ctx := context.Background()
	_, mem, now, id := deletedStore(t, 0)
	broken := server.New(commitsUnreadable{mem}).WithClock(func() time.Time { return *now }).WithDeletionRetention(0)
	sweepRes, err := broken.Sweep(ctx)
	if err != nil || len(sweepRes.Failed()) != 0 {
		t.Fatalf("Sweep returned %+v, %v, want the store purged without failure", sweepRes, err)
	}
	if hasChunk(t, mem, id) {
		t.Fatal("a purge left a chunk behind")
	}
}

