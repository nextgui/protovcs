package server_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	protovcs "github.com/nextgui/protovcs"
	pb "github.com/nextgui/protovcs/proto/protovcs/v1"
	"github.com/nextgui/protovcs/server"
)

// inParallel runs fn n times at once and reports how many returned nil.
func inParallel(n int, fn func(i int) error) int {
	var ok atomic.Int32
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			if fn(i) == nil {
				ok.Add(1)
			}
		}()
	}
	close(start)
	wg.Wait()
	return int(ok.Load())
}

func usage(t *testing.T, s *server.Server) *pb.Usage {
	t.Helper()
	st, err := s.GetStore(context.Background(), storeID)
	if err != nil {
		t.Fatalf("GetStore: %v", err)
	}
	return st.GetUsage()
}

func TestParallelReservationsCannotOverrunTheQuota(t *testing.T) {
	ctx := context.Background()
	s, _ := newServer(t, &pb.Quota{MaxBytes: 1000})

	granted := inParallel(20, func(int) error {
		_, err := s.BeginWrite(ctx, storeID, 1, 300)
		return err
	})
	if granted > 3 {
		t.Fatalf("%d reservations of 300 bytes were granted under a 1000 byte limit", granted)
	}
}

func TestParallelUploadsCannotOverrunAReservation(t *testing.T) {
	ctx := context.Background()
	s, _ := newServer(t, &pb.Quota{MaxBytes: 1 << 20})

	r, err := s.BeginWrite(ctx, storeID, 1, 100)
	if err != nil {
		t.Fatalf("BeginWrite: %v", err)
	}
	stored := inParallel(10, func(i int) error {
		return s.PutChunks(ctx, storeID, r.ID, []protovcs.SealedChunk{chunk(byte(i+1), 100)})
	})
	if stored != 1 {
		t.Fatalf("%d uploads fitted in a reservation for one chunk", stored)
	}
	if u := usage(t, s); u.GetObjects() != 1 || u.GetBytes() != 100 {
		t.Fatalf("usage is %d objects and %d bytes, want 1 and 100", u.GetObjects(), u.GetBytes())
	}
}

func TestAReservationIDCannotNameAPath(t *testing.T) {
	ctx := context.Background()
	s, _ := newServer(t, &pb.Quota{MaxBytes: 1 << 20})

	for _, id := range []string{"../../other/reservations/0123", "ABCDEF0123456789ABCDEF0123456789", ""} {
		err := s.PutChunks(ctx, storeID, id, []protovcs.SealedChunk{chunk(1, 10)})
		if !errors.Is(err, server.ErrNoReservation) {
			t.Errorf("PutChunks with reservation %q returned %v, want ErrNoReservation", id, err)
		}
	}
}

// commitNumbered returns a root commit that adds nothing, named by n.
func commitNumbered(n byte, added ...protovcs.ChunkID) *pb.Commit {
	c := &pb.Commit{FormatVersion: protovcs.FormatVersion, NamingNonce: []byte{n}}
	for _, id := range added {
		c.Added = append(c.Added, id.Bytes())
	}
	return c
}

func TestCommitsCountAgainstTheQuota(t *testing.T) {
	ctx := context.Background()

	t.Run("max_commits", func(t *testing.T) {
		s, _ := newServer(t, &pb.Quota{MaxCommits: 2})
		for n := byte(1); n <= 2; n++ {
			if _, err := s.PutCommit(ctx, storeID, "", commitNumbered(n)); err != nil {
				t.Fatalf("PutCommit %d: %v", n, err)
			}
		}
		if _, err := s.PutCommit(ctx, storeID, "", commitNumbered(3)); !errors.Is(err, server.ErrQuotaExceeded) {
			t.Fatalf("a third commit under max_commits 2 returned %v, want ErrQuotaExceeded", err)
		}
	})

	t.Run("max_bytes", func(t *testing.T) {
		s, _ := newServer(t, &pb.Quota{MaxBytes: 1000})
		small := commitNumbered(1)
		if _, err := s.PutCommit(ctx, storeID, "", small); err != nil {
			t.Fatalf("PutCommit: %v", err)
		}
		if u := usage(t, s); u.GetBytes() == 0 || u.GetCommits() != 1 {
			t.Fatalf("after one commit usage is %d bytes and %d commits", u.GetBytes(), u.GetCommits())
		}

		large := commitNumbered(2)
		large.Payload = make([]byte, 2000)
		if _, err := s.PutCommit(ctx, storeID, "", large); !errors.Is(err, server.ErrQuotaExceeded) {
			t.Fatalf("a commit past max_bytes returned %v, want ErrQuotaExceeded", err)
		}
	})

	t.Run("max_chunk_bytes", func(t *testing.T) {
		s, _ := newServer(t, &pb.Quota{MaxChunkBytes: 100})
		large := commitNumbered(1)
		large.Payload = make([]byte, 200)
		if _, err := s.PutCommit(ctx, storeID, "", large); !errors.Is(err, server.ErrQuotaExceeded) {
			t.Fatalf("a commit past max_chunk_bytes returned %v, want ErrQuotaExceeded", err)
		}
	})

	t.Run("reconcile agrees", func(t *testing.T) {
		s, _ := newServer(t, &pb.Quota{MaxBytes: 1 << 20})
		c := commitNumbered(1)
		c.Payload = make([]byte, 300)
		if _, err := s.PutCommit(ctx, storeID, "", c); err != nil {
			t.Fatalf("PutCommit: %v", err)
		}
		before, after, err := s.Reconcile(ctx, storeID)
		if err != nil {
			t.Fatalf("Reconcile: %v", err)
		}
		if before.GetBytes() != after.GetBytes() {
			t.Fatalf("Reconcile moved bytes from %d to %d", before.GetBytes(), after.GetBytes())
		}
	})
}

// orphanedChunk sets up a store holding one chunk that no branch reaches and
// that is older than the grace period: written under a commit, then left behind
// by a forced move of the branch to a new root.
func orphanedChunk(t *testing.T) (*server.Server, *server.MemStorage, *time.Time, protovcs.ChunkID) {
	t.Helper()
	ctx := context.Background()

	now := time.Unix(1_800_000_000, 0).UTC()
	storage := server.NewMemStorage().WithClock(func() time.Time { return now })
	s := server.New(storage).WithClock(func() time.Time { return now })
	if _, err := s.CreateStore(ctx, &pb.CreateStoreRequest{StoreId: storeID, Quota: &pb.Quota{MaxBytes: 1 << 20}}); err != nil {
		t.Fatalf("CreateStore: %v", err)
	}

	r, err := s.BeginWrite(ctx, storeID, 1, 100)
	if err != nil {
		t.Fatalf("BeginWrite: %v", err)
	}
	old := chunk(9, 100)
	if err := s.PutChunks(ctx, storeID, r.ID, []protovcs.SealedChunk{old}); err != nil {
		t.Fatalf("PutChunks: %v", err)
	}
	first, err := s.PutCommit(ctx, storeID, r.ID, commitNumbered(1, old.ID))
	if err != nil {
		t.Fatalf("PutCommit: %v", err)
	}
	state, err := s.UpdateRef(ctx, storeID, []byte("main"), first, "", false)
	if err != nil {
		t.Fatalf("UpdateRef: %v", err)
	}
	root, err := s.PutCommit(ctx, storeID, "", commitNumbered(2))
	if err != nil {
		t.Fatalf("PutCommit: %v", err)
	}
	if _, err := s.UpdateRef(ctx, storeID, []byte("main"), root, state.CASToken, true); err != nil {
		t.Fatalf("UpdateRef: %v", err)
	}

	now = now.Add(2 * server.CollectionGrace)
	return s, storage, &now, old.ID
}

// A writer stores its commit before it moves the branch. A commit that reuses
// an old orphaned chunk must keep it through a collection in that gap.
func TestCollectionSparesAChunkACommitJustReused(t *testing.T) {
	ctx := context.Background()
	s, storage, _, old := orphanedChunk(t)

	if _, err := s.PutCommit(ctx, storeID, "", commitNumbered(3, old)); err != nil {
		t.Fatalf("PutCommit: %v", err)
	}
	if _, err := s.Collect(ctx, storeID); err != nil {
		t.Fatalf("Collect: %v", err)
	}
	if has, err := storage.HasChunk(ctx, storeID, old); err != nil || !has {
		t.Fatal("collection deleted a chunk named by a commit whose branch had not moved yet")
	}
}

// commitDuringCollection stores a commit the first time collection asks about
// reservations, which is after it has worked out what is reachable.
type commitDuringCollection struct {
	*server.MemStorage
	once   sync.Once
	commit func()
}

func (c *commitDuringCollection) ListReservations(ctx context.Context, storeID string) ([]*server.Reservation, error) {
	c.once.Do(c.commit)
	return c.MemStorage.ListReservations(ctx, storeID)
}

func TestCollectionDeletesNothingWhenACommitLandsDuringIt(t *testing.T) {
	ctx := context.Background()
	s, mem, now, old := orphanedChunk(t)

	storage := &commitDuringCollection{MemStorage: mem}
	racing := server.New(storage).WithClock(func() time.Time { return *now })
	storage.commit = func() {
		if _, err := s.PutCommit(ctx, storeID, "", commitNumbered(3, old)); err != nil {
			t.Errorf("PutCommit: %v", err)
		}
	}

	res, err := racing.Collect(ctx, storeID)
	if err != nil {
		t.Fatalf("Collect: %v", err)
	}
	if !res.Interrupted || res.Deleted != 0 {
		t.Fatalf("collection with a commit landing during it returned %+v, want it interrupted", res)
	}
	if has, err := mem.HasChunk(ctx, storeID, old); err != nil || !has {
		t.Fatal("collection deleted a chunk a commit written during it named")
	}

	// Once that commit is old and no branch reaches it, the orphan goes.
	*now = now.Add(2 * server.CollectionGrace)
	if _, err := racing.Collect(ctx, storeID); err != nil {
		t.Fatalf("Collect: %v", err)
	}
	*now = now.Add(2 * server.CollectionGrace)
	if res, err := racing.Collect(ctx, storeID); err != nil || res.Deleted != 1 {
		t.Fatalf("the next collection returned %+v, %v, want one chunk deleted", res, err)
	}
}

// commitDuringDeletes stores a commit the first time collection deletes a
// chunk, which is after every check it makes.
type commitDuringDeletes struct {
	*server.MemStorage
	once   sync.Once
	commit func()
}

func (c *commitDuringDeletes) DeleteChunk(ctx context.Context, storeID string, id protovcs.ChunkID) error {
	c.once.Do(c.commit)
	return c.MemStorage.DeleteChunk(ctx, storeID, id)
}

// A commit recorded between collection's last check and its deletes must not
// end up naming a deleted chunk.
func TestACommitDuringTheDeletesCannotLoseAChunk(t *testing.T) {
	ctx := context.Background()
	s, mem, now, old := orphanedChunk(t)

	storage := &commitDuringDeletes{MemStorage: mem}
	racing := server.New(storage).WithClock(func() time.Time { return *now })
	var commitErr error
	committed := false
	storage.commit = func() {
		_, commitErr = s.PutCommit(ctx, storeID, "", commitNumbered(3, old))
		committed = commitErr == nil
	}

	for range 2 {
		if _, err := racing.Collect(ctx, storeID); err != nil {
			t.Fatalf("Collect: %v", err)
		}
		*now = now.Add(2 * server.CollectionGrace)
	}

	has, err := mem.HasChunk(ctx, storeID, old)
	if err != nil {
		t.Fatalf("HasChunk: %v", err)
	}
	if committed && !has {
		t.Fatal("a commit recorded during collection's deletes names a chunk collection deleted")
	}
	if !errors.Is(commitErr, protovcs.ErrRaceLost) {
		t.Fatalf("the commit during the deletes returned %v, want it refused as a lost race so the writer starts again", commitErr)
	}
}

// Once a chunk is marked, a writer is told it is missing, cannot commit it
// without uploading it, and keeps it by uploading it.
func TestAWriterTakesBackAMarkedChunk(t *testing.T) {
	ctx := context.Background()
	s, mem, now, old := orphanedChunk(t)

	if res, err := s.Collect(ctx, storeID); err != nil || res.Marked != 1 || res.Deleted != 0 {
		t.Fatalf("the first collection returned %+v, %v, want the orphan marked", res, err)
	}

	missing, err := s.HasChunks(ctx, storeID, []protovcs.ChunkID{old})
	if err != nil {
		t.Fatalf("HasChunks: %v", err)
	}
	if len(missing) != 1 {
		t.Fatal("HasChunks reported a chunk marked for collection as present")
	}
	if _, err := s.PutCommit(ctx, storeID, "", commitNumbered(3, old)); !errors.Is(err, protovcs.ErrRaceLost) {
		t.Fatalf("PutCommit adding a marked chunk returned %v, want a lost race", err)
	}

	r, err := s.BeginWrite(ctx, storeID, 1, 100)
	if err != nil {
		t.Fatalf("BeginWrite: %v", err)
	}
	if err := s.PutChunks(ctx, storeID, r.ID, []protovcs.SealedChunk{chunk(9, 100)}); err != nil {
		t.Fatalf("PutChunks: %v", err)
	}
	if _, err := s.PutCommit(ctx, storeID, r.ID, commitNumbered(3, old)); err != nil {
		t.Fatalf("PutCommit after uploading the chunk again: %v", err)
	}

	*now = now.Add(2 * server.CollectionGrace)
	if res, err := s.Collect(ctx, storeID); err != nil || res.Deleted != 0 {
		t.Fatalf("collection after the chunk was taken back returned %+v, %v, want nothing deleted", res, err)
	}
	if has, err := mem.HasChunk(ctx, storeID, old); err != nil || !has {
		t.Fatal("collection deleted a chunk a writer took back")
	}
}

// Commit bytes count towards max_bytes_per_day, and the count starts again at
// midnight in Los Angeles, as it does for chunks.
func TestCommitBytesTodayStartAgainTheNextDay(t *testing.T) {
	ctx := context.Background()
	// 23:00 in Los Angeles on 2026-11-06.
	now := time.Date(2026, 11, 7, 7, 0, 0, 0, time.UTC)
	storage := server.NewMemStorage().WithClock(func() time.Time { return now })
	s := server.New(storage).WithClock(func() time.Time { return now })

	// The server stores a commit with its 32-byte id, so a limit of one and a
	// half stored commits fits exactly one.
	first := commitNumbered(1)
	id, err := protovcs.CommitName(first)
	if err != nil {
		t.Fatalf("CommitName: %v", err)
	}
	first.Id = id.Bytes()
	size := uint64(proto.Size(first))
	if _, err := s.CreateStore(ctx, &pb.CreateStoreRequest{StoreId: storeID, Quota: &pb.Quota{MaxBytesPerDay: size + size/2}}); err != nil {
		t.Fatalf("CreateStore: %v", err)
	}
	if _, err := s.PutCommit(ctx, storeID, "", commitNumbered(1)); err != nil {
		t.Fatalf("the first commit of the day: %v", err)
	}
	if _, err := s.PutCommit(ctx, storeID, "", commitNumbered(2)); !errors.Is(err, server.ErrQuotaExceeded) {
		t.Fatalf("a second commit past max_bytes_per_day returned %v, want ErrQuotaExceeded", err)
	}

	now = now.Add(2 * time.Hour)
	if _, err := s.PutCommit(ctx, storeID, "", commitNumbered(2)); err != nil {
		t.Fatalf("a commit after midnight in Los Angeles: %v", err)
	}
}
