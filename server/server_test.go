package server_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	protovcs "github.com/nextgui/protovcs"
	pb "github.com/nextgui/protovcs/proto/protovcs/v1"
	"github.com/nextgui/protovcs/server"
)

const storeID = "store-1"

func newServer(t *testing.T, quota *pb.Quota) (*server.Server, *server.MemStorage) {
	t.Helper()

	storage := server.NewMemStorage()
	s := server.New(storage)
	if _, err := s.CreateStore(context.Background(), &pb.CreateStoreRequest{
		StoreId: storeID,
		Owner:   "someone",
		Quota:   quota,
	}); err != nil {
		t.Fatalf("CreateStore: %v", err)
	}
	return s, storage
}

func chunk(n byte, size int) protovcs.SealedChunk {
	sealed := make([]byte, size)
	for i := range sealed {
		sealed[i] = n
	}
	var id protovcs.ChunkID
	id[0] = n
	return protovcs.SealedChunk{ID: id, Sealed: sealed}
}

func TestCreateStoreIsIdempotent(t *testing.T) {
	ctx := context.Background()
	s, _ := newServer(t, &pb.Quota{MaxBytes: 1 << 20})

	// A retry after an unknown outcome must not leave a second store.
	again, err := s.CreateStore(ctx, &pb.CreateStoreRequest{StoreId: storeID, Owner: "someone"})
	if err != nil {
		t.Fatalf("CreateStore: %v", err)
	}
	if again.GetStoreId() != storeID {
		t.Fatalf("a retry created %q", again.GetStoreId())
	}

	stores, err := s.ListStores(ctx, &pb.ListStoresRequest{Owner: "someone"})
	if err != nil {
		t.Fatalf("ListStores: %v", err)
	}
	if len(stores) != 1 {
		t.Fatalf("a retried creation produced %d stores", len(stores))
	}
}

// TestQuotaIsCheckedBeforeUpload is the property the whole reservation
// mechanism exists for. Checking at commit time would be too late: the chunks
// are already stored.
func TestQuotaIsCheckedBeforeUpload(t *testing.T) {
	ctx := context.Background()
	s, _ := newServer(t, &pb.Quota{MaxBytes: 1000})

	if _, err := s.BeginWrite(ctx, storeID, 1, 5000); !errors.Is(err, server.ErrQuotaExceeded) {
		t.Fatalf("BeginWrite over the limit returned %v, want ErrQuotaExceeded", err)
	}
}

func TestQuotaErrorSaysWhatIsWrong(t *testing.T) {
	ctx := context.Background()
	s, _ := newServer(t, &pb.Quota{MaxObjects: 2})

	_, err := s.BeginWrite(ctx, storeID, 10, 100)
	if err == nil {
		t.Fatal("BeginWrite over the object limit succeeded")
	}
	// "quota exceeded" is not actionable. The message has to name the limit,
	// what is in use and what the write needed.
	for _, want := range []string{"max_objects", "2", "10"} {
		if !contains(err.Error(), want) {
			t.Fatalf("error %q does not mention %q", err, want)
		}
	}
}

func TestReservationsCountAgainstTheLimit(t *testing.T) {
	ctx := context.Background()
	s, _ := newServer(t, &pb.Quota{MaxBytes: 1000})

	if _, err := s.BeginWrite(ctx, storeID, 1, 800); err != nil {
		t.Fatalf("BeginWrite: %v", err)
	}
	// Two writers must not each be told there is room for the last of it.
	if _, err := s.BeginWrite(ctx, storeID, 1, 800); !errors.Is(err, server.ErrQuotaExceeded) {
		t.Fatalf("a second reservation returned %v, want ErrQuotaExceeded", err)
	}
}

func TestUploadingBeyondAReservationIsRefused(t *testing.T) {
	ctx := context.Background()
	s, _ := newServer(t, &pb.Quota{MaxBytes: 1 << 20})

	r, err := s.BeginWrite(ctx, storeID, 1, 100)
	if err != nil {
		t.Fatalf("BeginWrite: %v", err)
	}
	if err := s.PutChunks(ctx, storeID, r.ID, []protovcs.SealedChunk{chunk(1, 500)}); !errors.Is(err, server.ErrNoReservation) {
		t.Fatalf("uploading past the reservation returned %v, want ErrNoReservation", err)
	}
}

func TestUploadingWithNoReservationIsRefused(t *testing.T) {
	ctx := context.Background()
	s, _ := newServer(t, &pb.Quota{MaxBytes: 1 << 20})

	if err := s.PutChunks(ctx, storeID, "made-up", []protovcs.SealedChunk{chunk(1, 10)}); !errors.Is(err, server.ErrNoReservation) {
		t.Fatalf("uploading with no reservation returned %v, want ErrNoReservation", err)
	}
}

func TestAnExpiredReservationIsRefused(t *testing.T) {
	ctx := context.Background()
	storage := server.NewMemStorage()
	now := time.Unix(1_800_000_000, 0).UTC()
	s := server.New(storage).WithClock(func() time.Time { return now })

	if _, err := s.CreateStore(ctx, &pb.CreateStoreRequest{StoreId: storeID, Quota: &pb.Quota{MaxBytes: 1 << 20}}); err != nil {
		t.Fatalf("CreateStore: %v", err)
	}
	r, err := s.BeginWrite(ctx, storeID, 4, 4000)
	if err != nil {
		t.Fatalf("BeginWrite: %v", err)
	}

	now = now.Add(2 * server.ReservationTTL)
	if err := s.PutChunks(ctx, storeID, r.ID, []protovcs.SealedChunk{chunk(1, 10)}); !errors.Is(err, server.ErrNoReservation) {
		t.Fatalf("uploading against an expired reservation returned %v, want ErrNoReservation", err)
	}

	released, err := s.ExpireReservations(ctx, storeID)
	if err != nil {
		t.Fatalf("ExpireReservations: %v", err)
	}
	if released != 1 {
		t.Fatalf("released %d reservations, want 1", released)
	}

	store, err := s.GetStore(ctx, storeID)
	if err != nil {
		t.Fatalf("GetStore: %v", err)
	}
	if store.GetUsage().GetReservedBytes() != 0 {
		t.Fatalf("an expired reservation still holds %d bytes", store.GetUsage().GetReservedBytes())
	}
}

// TestAnAbandonedWriteDoesNotWaitForTheSweep covers a writer that reserved
// quota and died. Its reservation counts as used, and the sweep that releases
// it runs weekly, so a write it would block releases it first.
func TestAnAbandonedWriteDoesNotWaitForTheSweep(t *testing.T) {
	ctx := context.Background()
	storage := server.NewMemStorage()
	now := time.Unix(1_800_000_000, 0).UTC()
	s := server.New(storage).WithClock(func() time.Time { return now })

	if _, err := s.CreateStore(ctx, &pb.CreateStoreRequest{StoreId: storeID, Quota: &pb.Quota{MaxBytes: 1000}}); err != nil {
		t.Fatalf("CreateStore: %v", err)
	}
	if _, err := s.BeginWrite(ctx, storeID, 1, 900); err != nil {
		t.Fatalf("BeginWrite: %v", err)
	}

	// While the reservation is live, a second write is refused.
	if _, err := s.BeginWrite(ctx, storeID, 1, 500); !errors.Is(err, server.ErrQuotaExceeded) {
		t.Fatalf("a write past a live reservation returned %v, want ErrQuotaExceeded", err)
	}

	// Once it has expired, the same write goes through with no sweep between.
	now = now.Add(2 * server.ReservationTTL)
	if _, err := s.BeginWrite(ctx, storeID, 1, 500); err != nil {
		t.Fatalf("a write past an expired reservation returned %v, want it to succeed", err)
	}
}

func TestTheNumberOfOpenReservationsIsLimited(t *testing.T) {
	ctx := context.Background()
	storage := server.NewMemStorage()
	now := time.Unix(1_800_000_000, 0).UTC()
	s := server.New(storage).WithClock(func() time.Time { return now })

	if _, err := s.CreateStore(ctx, &pb.CreateStoreRequest{StoreId: storeID, Quota: &pb.Quota{MaxBytes: 1 << 20}}); err != nil {
		t.Fatalf("CreateStore: %v", err)
	}
	var first *server.Reservation
	for i := range server.MaxReservations {
		r, err := s.BeginWrite(ctx, storeID, 0, 0)
		if err != nil {
			t.Fatalf("reservation %d of %d: %v", i+1, server.MaxReservations, err)
		}
		if first == nil {
			first = r
		}
	}
	if _, err := s.BeginWrite(ctx, storeID, 0, 0); !errors.Is(err, server.ErrQuotaExceeded) {
		t.Fatalf("a reservation past MaxReservations returned %v, want ErrQuotaExceeded", err)
	}

	// A finished write gives its place back.
	if _, err := s.PutCommit(ctx, storeID, first.ID, commitNumbered(1)); err != nil {
		t.Fatalf("PutCommit: %v", err)
	}
	if _, err := s.BeginWrite(ctx, storeID, 0, 0); err != nil {
		t.Fatalf("a reservation after one was released: %v", err)
	}

	// Expired ones are released by the refusal, with no sweep between.
	now = now.Add(2 * server.ReservationTTL)
	if _, err := s.BeginWrite(ctx, storeID, 0, 0); err != nil {
		t.Fatalf("a reservation once the others expired: %v", err)
	}
	st, err := s.GetStore(ctx, storeID)
	if err != nil {
		t.Fatalf("GetStore: %v", err)
	}
	if got := st.GetUsage().GetReservations(); got != 1 {
		t.Errorf("reservations after the expired ones were released = %d, want 1", got)
	}
}

func TestStoringAChunkTwiceIsChargedOnce(t *testing.T) {
	ctx := context.Background()
	s, _ := newServer(t, &pb.Quota{MaxBytes: 1 << 20})

	r, err := s.BeginWrite(ctx, storeID, 4, 4000)
	if err != nil {
		t.Fatalf("BeginWrite: %v", err)
	}
	c := chunk(1, 100)
	for i := 0; i < 3; i++ {
		if err := s.PutChunks(ctx, storeID, r.ID, []protovcs.SealedChunk{c}); err != nil {
			t.Fatalf("PutChunks: %v", err)
		}
	}

	store, err := s.GetStore(ctx, storeID)
	if err != nil {
		t.Fatalf("GetStore: %v", err)
	}
	// A chunk's name determines its bytes, so a retry is not a second chunk.
	// Charging for it twice would make usage depend on how often a client
	// retried.
	if got := store.GetUsage().GetObjects(); got != 1 {
		t.Fatalf("usage counts %d objects after three identical writes, want 1", got)
	}
	if got := store.GetUsage().GetBytes(); got != 100 {
		t.Fatalf("usage counts %d bytes, want 100", got)
	}
}

func TestOversizedChunksAreRefused(t *testing.T) {
	ctx := context.Background()
	s, _ := newServer(t, &pb.Quota{MaxBytes: 1 << 20, MaxChunkBytes: 50})

	r, err := s.BeginWrite(ctx, storeID, 1, 1000)
	if err != nil {
		t.Fatalf("BeginWrite: %v", err)
	}
	if err := s.PutChunks(ctx, storeID, r.ID, []protovcs.SealedChunk{chunk(1, 500)}); !errors.Is(err, server.ErrQuotaExceeded) {
		t.Fatalf("an oversized chunk returned %v, want ErrQuotaExceeded", err)
	}
}

func TestACommitNamingAnAbsentChunkIsRefused(t *testing.T) {
	ctx := context.Background()
	s, _ := newServer(t, &pb.Quota{MaxBytes: 1 << 20})

	r, err := s.BeginWrite(ctx, storeID, 1, 1000)
	if err != nil {
		t.Fatalf("BeginWrite: %v", err)
	}

	var absent protovcs.ChunkID
	absent[0] = 0xEE
	_, err = s.PutCommit(ctx, storeID, r.ID, &pb.Commit{
		FormatVersion: protovcs.FormatVersion,
		Added:         [][]byte{absent.Bytes()},
	})
	if err == nil {
		t.Fatal("a commit naming a chunk that was never stored was accepted")
	}
}

func TestACommitCannotClaimAName(t *testing.T) {
	ctx := context.Background()
	s, _ := newServer(t, &pb.Quota{MaxBytes: 1 << 20})

	var lie protovcs.CommitID
	lie[0] = 0x99
	_, err := s.PutCommit(ctx, storeID, "", &pb.Commit{
		FormatVersion: protovcs.FormatVersion,
		Id:            lie.Bytes(),
	})
	if err == nil {
		t.Fatal("a commit was stored under a name it chose for itself")
	}
}

// TestCollectionKeepsWhatIsReachable is the operation that deletes things, so
// it is the one with the most ways to be catastrophic.
func TestCollectionKeepsWhatIsReachable(t *testing.T) {
	ctx := context.Background()
	storage := server.NewMemStorage()
	now := time.Unix(1_800_000_000, 0).UTC()
	storage.WithClock(func() time.Time { return now })
	s := server.New(storage).WithClock(func() time.Time { return now })

	if _, err := s.CreateStore(ctx, &pb.CreateStoreRequest{StoreId: storeID, Quota: &pb.Quota{MaxBytes: 1 << 20}}); err != nil {
		t.Fatalf("CreateStore: %v", err)
	}

	r, err := s.BeginWrite(ctx, storeID, 10, 10000)
	if err != nil {
		t.Fatalf("BeginWrite: %v", err)
	}
	kept, dropped := chunk(1, 100), chunk(2, 100)
	if err := s.PutChunks(ctx, storeID, r.ID, []protovcs.SealedChunk{kept, dropped}); err != nil {
		t.Fatalf("PutChunks: %v", err)
	}

	commit, err := s.PutCommit(ctx, storeID, r.ID, &pb.Commit{
		FormatVersion: protovcs.FormatVersion,
		Added:         [][]byte{kept.ID.Bytes()},
	})
	if err != nil {
		t.Fatalf("PutCommit: %v", err)
	}
	if _, err := s.UpdateRef(ctx, storeID, []byte("main"), commit, "", false); err != nil {
		t.Fatalf("UpdateRef: %v", err)
	}

	// Immediately: the unreferenced chunk is younger than the grace period, so
	// it could still belong to a write in progress. Nothing is deleted.
	res, err := s.Collect(ctx, storeID)
	if err != nil {
		t.Fatalf("Collect: %v", err)
	}
	if res.Deleted != 0 {
		t.Fatalf("collection deleted %d chunks inside the grace period", res.Deleted)
	}

	// Past the grace period, with no live reservation, the unreferenced chunk
	// goes and the reachable one stays.
	now = now.Add(2 * server.CollectionGrace)
	if _, err := s.Collect(ctx, storeID); err != nil {
		t.Fatalf("Collect: %v", err)
	}
	now = now.Add(2 * server.CollectionGrace)
	res, err = s.Collect(ctx, storeID)
	if err != nil {
		t.Fatalf("Collect: %v", err)
	}
	if res.Deleted != 1 {
		t.Fatalf("collection deleted %d chunks, want 1", res.Deleted)
	}

	if has, err := storage.HasChunk(ctx, storeID, kept.ID); err != nil || !has {
		t.Fatal("collection deleted a reachable chunk")
	}
	if has, err := storage.HasChunk(ctx, storeID, dropped.ID); err != nil || has {
		t.Fatal("collection kept an unreachable chunk")
	}
}

func TestCollectionSparesALiveWriter(t *testing.T) {
	ctx := context.Background()
	storage := server.NewMemStorage()
	now := time.Unix(1_800_000_000, 0).UTC()
	storage.WithClock(func() time.Time { return now })
	s := server.New(storage).WithClock(func() time.Time { return now })

	if _, err := s.CreateStore(ctx, &pb.CreateStoreRequest{StoreId: storeID, Quota: &pb.Quota{MaxBytes: 1 << 20}}); err != nil {
		t.Fatalf("CreateStore: %v", err)
	}

	// A writer uploads and has not committed yet. Its chunks are unreachable by
	// construction, and deleting them would destroy a write in progress.
	r, err := s.BeginWrite(ctx, storeID, 10, 10000)
	if err != nil {
		t.Fatalf("BeginWrite: %v", err)
	}
	inFlight := chunk(7, 100)
	if err := s.PutChunks(ctx, storeID, r.ID, []protovcs.SealedChunk{inFlight}); err != nil {
		t.Fatalf("PutChunks: %v", err)
	}

	now = now.Add(2 * server.CollectionGrace)
	// The reservation is still live even though the chunk is old, because the
	// clock only moved for the grace period check in this test's construction.
	if err := storage.PutReservation(ctx, storeID, &server.Reservation{
		ID: r.ID, Chunks: r.Chunks, Bytes: r.Bytes, ExpiresUnix: now.Add(time.Hour).Unix(),
	}); err != nil {
		t.Fatalf("PutReservation: %v", err)
	}

	res, err := s.Collect(ctx, storeID)
	if err != nil {
		t.Fatalf("Collect: %v", err)
	}
	if res.Deleted != 0 {
		t.Fatalf("collection deleted %d chunks while a write was in flight", res.Deleted)
	}
	if has, err := storage.HasChunk(ctx, storeID, inFlight.ID); err != nil || !has {
		t.Fatal("collection deleted a chunk belonging to a live write")
	}
}

func TestReconcileCorrectsDrift(t *testing.T) {
	ctx := context.Background()
	s, storage := newServer(t, &pb.Quota{MaxBytes: 1 << 20})

	r, err := s.BeginWrite(ctx, storeID, 5, 5000)
	if err != nil {
		t.Fatalf("BeginWrite: %v", err)
	}
	if err := s.PutChunks(ctx, storeID, r.ID, []protovcs.SealedChunk{chunk(1, 100), chunk(2, 200)}); err != nil {
		t.Fatalf("PutChunks: %v", err)
	}

	// Drift the counters, as a process dying between two writes would.
	if _, err := storage.Update(ctx, storeID, func(st *pb.Store) error {
		st.Usage.Bytes = 999999
		st.Usage.Objects = 42
		return nil
	}); err != nil {
		t.Fatalf("Update: %v", err)
	}

	before, after, err := s.Reconcile(ctx, storeID)
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if before.GetObjects() == after.GetObjects() {
		t.Fatal("Reconcile reported no drift when there was some")
	}
	if after.GetObjects() != 2 || after.GetBytes() != 300 {
		t.Fatalf("after reconciling: %d objects and %d bytes, want 2 and 300", after.GetObjects(), after.GetBytes())
	}
}

func TestHeadSchemaVersionIsRecordedForRetirement(t *testing.T) {
	ctx := context.Background()
	s, _ := newServer(t, &pb.Quota{MaxBytes: 1 << 20})

	r, err := s.BeginWrite(ctx, storeID, 2, 2000)
	if err != nil {
		t.Fatalf("BeginWrite: %v", err)
	}
	c := chunk(1, 10)
	if err := s.PutChunks(ctx, storeID, r.ID, []protovcs.SealedChunk{c}); err != nil {
		t.Fatalf("PutChunks: %v", err)
	}
	if _, err := s.PutCommit(ctx, storeID, r.ID, &pb.Commit{
		FormatVersion: protovcs.FormatVersion,
		SchemaVersion: 3,
		Added:         [][]byte{c.ID.Bytes()},
	}); err != nil {
		t.Fatalf("PutCommit: %v", err)
	}

	// Deleting the code that reads an old schema is safe only when no store is
	// still below the new version. This is how that is answered.
	behind, err := s.ListStores(ctx, &pb.ListStoresRequest{HeadSchemaVersionBelow: 3})
	if err != nil {
		t.Fatalf("ListStores: %v", err)
	}
	if len(behind) != 0 {
		t.Fatalf("%d stores reported as behind version 3, want 0", len(behind))
	}

	behind, err = s.ListStores(ctx, &pb.ListStoresRequest{HeadSchemaVersionBelow: 4})
	if err != nil {
		t.Fatalf("ListStores: %v", err)
	}
	if len(behind) != 1 {
		t.Fatalf("%d stores reported as behind version 4, want 1", len(behind))
	}
}

func TestADeletedStoreRefusesWrites(t *testing.T) {
	ctx := context.Background()
	s, _ := newServer(t, &pb.Quota{MaxBytes: 1 << 20})

	if _, err := s.DeleteStore(ctx, storeID); err != nil {
		t.Fatalf("DeleteStore: %v", err)
	}
	if _, err := s.BeginWrite(ctx, storeID, 1, 10); !errors.Is(err, server.ErrNoSuchStore) {
		t.Fatalf("writing to a deleted store returned %v, want ErrNoSuchStore", err)
	}
}

func contains(haystack, needle string) bool {
	return len(haystack) >= len(needle) && indexOf(haystack, needle) >= 0
}

func indexOf(haystack, needle string) int {
	for i := 0; i+len(needle) <= len(haystack); i++ {
		if haystack[i:i+len(needle)] == needle {
			return i
		}
	}
	return -1
}

func TestARequestPastItsReservationStoresNothing(t *testing.T) {
	ctx := context.Background()
	s, storage := newServer(t, &pb.Quota{MaxBytes: 1 << 20})

	r, err := s.BeginWrite(ctx, storeID, 2, 2000)
	if err != nil {
		t.Fatalf("BeginWrite: %v", err)
	}
	batch := []protovcs.SealedChunk{chunk(1, 100), chunk(2, 100), chunk(3, 100)}
	if err := s.PutChunks(ctx, storeID, r.ID, batch); !errors.Is(err, server.ErrNoReservation) {
		t.Fatalf("three chunks against a reservation for two returned %v, want ErrNoReservation", err)
	}

	// Checked before anything is written, so the chunks that fit are not left
	// behind uncounted.
	for _, c := range batch {
		if has, err := storage.HasChunk(ctx, storeID, c.ID); err != nil || has {
			t.Fatalf("chunk %d was stored by a refused request (err %v)", c.ID[0], err)
		}
	}
	store, err := s.GetStore(ctx, storeID)
	if err != nil {
		t.Fatalf("GetStore: %v", err)
	}
	if got := store.GetUsage().GetObjects(); got != 0 {
		t.Fatalf("usage counts %d objects after a refused request, want 0", got)
	}
}

func TestOneRequestIsChargedForEachNewChunkOnce(t *testing.T) {
	ctx := context.Background()
	s, _ := newServer(t, &pb.Quota{MaxBytes: 1 << 20})

	r, err := s.BeginWrite(ctx, storeID, 40, 40000)
	if err != nil {
		t.Fatalf("BeginWrite: %v", err)
	}
	if err := s.PutChunks(ctx, storeID, r.ID, []protovcs.SealedChunk{chunk(1, 100)}); err != nil {
		t.Fatalf("PutChunks: %v", err)
	}

	// Chunk 1 is already held, and chunk 2 is named twice. Enough chunks that
	// the writes run concurrently.
	batch := []protovcs.SealedChunk{chunk(1, 100), chunk(2, 200), chunk(2, 200)}
	for n := byte(3); n < 35; n++ {
		batch = append(batch, chunk(n, 10))
	}
	if err := s.PutChunks(ctx, storeID, r.ID, batch); err != nil {
		t.Fatalf("PutChunks: %v", err)
	}

	store, err := s.GetStore(ctx, storeID)
	if err != nil {
		t.Fatalf("GetStore: %v", err)
	}
	if got, want := store.GetUsage().GetObjects(), uint64(34); got != want {
		t.Fatalf("usage counts %d objects, want %d", got, want)
	}
	if got, want := store.GetUsage().GetBytes(), uint64(100+200+32*10); got != want {
		t.Fatalf("usage counts %d bytes, want %d", got, want)
	}
	if got, want := store.GetUsage().GetBytesToday(), uint64(100+200+32*10); got != want {
		t.Fatalf("usage counts %d bytes today, want %d", got, want)
	}
}

// TestTheDailyLimitResetsAtMidnightInLosAngeles writes up to
// max_bytes_per_day, is still refused after UTC midnight, and can write again
// once it is the next day in Los Angeles.
func TestTheDailyLimitResetsAtMidnightInLosAngeles(t *testing.T) {
	ctx := context.Background()
	la, err := time.LoadLocation("America/Los_Angeles")
	if err != nil {
		t.Fatalf("LoadLocation: %v", err)
	}
	now := time.Date(2026, 10, 29, 16, 0, 0, 0, la)
	storage := server.NewMemStorage()
	s := server.New(storage).WithClock(func() time.Time { return now })
	if _, err := s.CreateStore(ctx, &pb.CreateStoreRequest{
		StoreId: storeID,
		Owner:   "someone",
		Quota:   &pb.Quota{MaxBytesPerDay: 1000},
	}); err != nil {
		t.Fatalf("CreateStore: %v", err)
	}

	r, err := s.BeginWrite(ctx, storeID, 1, 1000)
	if err != nil {
		t.Fatalf("BeginWrite: %v", err)
	}
	if err := s.PutChunks(ctx, storeID, r.ID, []protovcs.SealedChunk{chunk(1, 1000)}); err != nil {
		t.Fatalf("PutChunks: %v", err)
	}
	if _, err := s.BeginWrite(ctx, storeID, 1, 1); !errors.Is(err, server.ErrQuotaExceeded) {
		t.Fatalf("BeginWrite past the daily limit returned %v, want ErrQuotaExceeded", err)
	}

	// 18:00 in Los Angeles is 01:00 the next day in UTC.
	now = time.Date(2026, 10, 29, 18, 0, 0, 0, la)
	if _, err := s.BeginWrite(ctx, storeID, 1, 1); !errors.Is(err, server.ErrQuotaExceeded) {
		t.Fatalf("BeginWrite after UTC midnight returned %v, want ErrQuotaExceeded", err)
	}

	now = time.Date(2026, 10, 30, 0, 30, 0, 0, la)
	r, err = s.BeginWrite(ctx, storeID, 1, 1000)
	if err != nil {
		t.Fatalf("BeginWrite on the next day: %v", err)
	}
	if err := s.PutChunks(ctx, storeID, r.ID, []protovcs.SealedChunk{chunk(2, 1000)}); err != nil {
		t.Fatalf("PutChunks on the next day: %v", err)
	}

	store, err := s.GetStore(ctx, storeID)
	if err != nil {
		t.Fatalf("GetStore: %v", err)
	}
	if got, want := store.GetUsage().GetBytesToday(), uint64(1000); got != want {
		t.Fatalf("usage counts %d bytes today, want %d", got, want)
	}
}

// failingStorage is MemStorage, except that storing one chosen chunk fails
// while failing is set.
type failingStorage struct {
	*server.MemStorage
	chunk   protovcs.ChunkID
	failing atomic.Bool
}

var errDiskFull = errors.New("disk full")

func (f *failingStorage) PutChunk(ctx context.Context, storeID string, id protovcs.ChunkID, sealed []byte) error {
	if f.failing.Load() && id == f.chunk {
		return errDiskFull
	}
	return f.MemStorage.PutChunk(ctx, storeID, id, sealed)
}

// When one write in a request fails, the chunks that did land must still be
// charged. A retry finds them present and skips them, so nothing else would
// ever count them, and the store's usage would fall short of what it holds.
func TestAFailedWriteChargesTheChunksThatLanded(t *testing.T) {
	ctx := context.Background()
	storage := &failingStorage{MemStorage: server.NewMemStorage()}
	s := server.New(storage)
	if _, err := s.CreateStore(ctx, &pb.CreateStoreRequest{
		StoreId: storeID, Quota: &pb.Quota{MaxBytes: 1 << 20},
	}); err != nil {
		t.Fatalf("CreateStore: %v", err)
	}

	// Enough chunks that the writes run concurrently, sized so that every
	// chunk's contribution to the byte count is distinct.
	var batch []protovcs.SealedChunk
	var total uint64
	for n := byte(1); n <= 80; n++ {
		batch = append(batch, chunk(n, int(n)))
		total += uint64(n)
	}
	r, err := s.BeginWrite(ctx, storeID, uint32(len(batch)), total)
	if err != nil {
		t.Fatalf("BeginWrite: %v", err)
	}

	storage.chunk = batch[40].ID
	storage.failing.Store(true)
	if err := s.PutChunks(ctx, storeID, r.ID, batch); !errors.Is(err, errDiskFull) {
		t.Fatalf("PutChunks returned %v, want the storage error", err)
	}

	var heldObjects, heldBytes uint64
	for _, c := range batch {
		has, err := storage.HasChunk(ctx, storeID, c.ID)
		if err != nil {
			t.Fatalf("HasChunk: %v", err)
		}
		if has {
			heldObjects++
			heldBytes += uint64(len(c.Sealed))
		}
	}
	if heldObjects == 0 || heldObjects == uint64(len(batch)) {
		t.Fatalf("%d of %d chunks landed; the test needs some to land and some not", heldObjects, len(batch))
	}
	store, err := s.GetStore(ctx, storeID)
	if err != nil {
		t.Fatalf("GetStore: %v", err)
	}
	if got := store.GetUsage().GetObjects(); got != heldObjects {
		t.Fatalf("after a failed request usage counts %d objects, and the store holds %d", got, heldObjects)
	}
	if got := store.GetUsage().GetBytes(); got != heldBytes {
		t.Fatalf("after a failed request usage counts %d bytes, and the store holds %d", got, heldBytes)
	}

	// The retry stores the rest. The reservation covers exactly the batch, so
	// this also fails if the failed request charged the reservation for chunks
	// it did not store.
	storage.failing.Store(false)
	if err := s.PutChunks(ctx, storeID, r.ID, batch); err != nil {
		t.Fatalf("retrying PutChunks: %v", err)
	}
	store, err = s.GetStore(ctx, storeID)
	if err != nil {
		t.Fatalf("GetStore: %v", err)
	}
	if got, want := store.GetUsage().GetObjects(), uint64(len(batch)); got != want {
		t.Fatalf("after the retry usage counts %d objects, want %d", got, want)
	}
	if got := store.GetUsage().GetBytes(); got != total {
		t.Fatalf("after the retry usage counts %d bytes, want %d", got, total)
	}
}
