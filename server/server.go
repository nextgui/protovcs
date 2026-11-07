package server

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	protovcs "github.com/nextgui/protovcs"
	pb "github.com/nextgui/protovcs/proto/protovcs/v1"
)

// ReservationTTL is how long a write may hold quota before it is released.
//
// Long enough that a slow upload over a poor connection is not cut off, short
// enough that an abandoned write does not hold a store's ceiling down until
// somebody notices.
const ReservationTTL = 30 * time.Minute

// CollectionGrace is how long an unreferenced chunk is kept before it is
// deleted.
//
// A writer uploads chunks before the commit that references them, so between
// those two points its chunks are unreachable and look exactly like garbage.
// Deleting them would destroy a live write. The grace period is what
// distinguishes the two, and it is deliberately longer than ReservationTTL.
const CollectionGrace = time.Hour

// DeletionRetention is how long a deleted store can be restored before
// collection starts removing it, unless the server is given another window
// with [Server.WithDeletionRetention].
const DeletionRetention = time.Hour

// Server enforces the rules. It holds no keys and reads nothing it stores.
type Server struct {
	storage Storage
	now     func() time.Time

	// Open ref watches, and how often the stores behind them are re-read.
	// Storage does not announce a change, so knowing one happened means looking
	// — see refwatch.go.
	watch             *refWatchers
	refPollInterval   time.Duration
	refRelistInterval time.Duration

	// Asked to run a pack pass when a hint arrives, or nil, in which case only
	// the sweep packs. See pack.go.
	packScheduler PackScheduler

	// How long a deleted store waits before collection purges it.
	deletionRetention time.Duration
}

// New returns a server over a storage implementation.
func New(storage Storage) *Server {
	return &Server{
		storage:           storage,
		now:               func() time.Time { return time.Now().UTC() },
		watch:             newRefWatchers(),
		refPollInterval:   RefPollInterval,
		refRelistInterval: RefRelistInterval,
		deletionRetention: DeletionRetention,
	}
}

// WithDeletionRetention sets how long a deleted store can be restored before
// collection purges it. Zero purges it on the next collection, for an
// application whose deletion must be immediate.
func (s *Server) WithDeletionRetention(d time.Duration) *Server {
	s.deletionRetention = d
	return s
}

// WithClock replaces the clock, for tests that need to reach an expiry without
// waiting for one.
func (s *Server) WithClock(now func() time.Time) *Server {
	s.now = now
	return s
}

// CreateStore records a new store with its limits.
func (s *Server) CreateStore(ctx context.Context, req *pb.CreateStoreRequest) (*pb.Store, error) {
	if req.GetStoreId() == "" {
		return nil, fmt.Errorf("protovcs: a store needs an id supplied by the caller, so that a retry is not a second store")
	}
	return s.storage.CreateStore(ctx, &pb.Store{
		StoreId:   req.GetStoreId(),
		Owner:     req.GetOwner(),
		Quota:     req.GetQuota(),
		Usage:     &pb.Usage{},
		CreatedAt: timestamppb.New(s.now()),
		State:     pb.Store_STATE_ACTIVE,
	})
}

// GetStore returns a store's record.
func (s *Server) GetStore(ctx context.Context, storeID string) (*pb.Store, error) {
	return s.storage.GetStore(ctx, storeID)
}

// ListStores answers operational questions about a fleet.
//
// The one that motivated it: retiring a schema means deleting the code that
// reads it, and that is safe only when no store is still below the new version.
// head_schema_version_below is how that is checked rather than assumed.
func (s *Server) ListStores(ctx context.Context, req *pb.ListStoresRequest) ([]*pb.Store, error) {
	return s.storage.ListStores(ctx, req)
}

// SetQuota changes a store's limits.
func (s *Server) SetQuota(ctx context.Context, storeID string, quota *pb.Quota) (*pb.Store, error) {
	return s.storage.Update(ctx, storeID, func(st *pb.Store) error {
		st.Quota = quota
		return nil
	})
}

// DeleteStore marks a store for deletion without removing anything.
//
// The chunks go when collection runs after the retention window, and until
// then [Server.RestoreStore] undoes it. A deletion that cannot be undone in its
// first hour is one that will be regretted. Deleting a store already marked
// keeps the time it was first marked.
func (s *Server) DeleteStore(ctx context.Context, storeID string) (*pb.Store, error) {
	return s.storage.Update(ctx, storeID, func(st *pb.Store) error {
		if st.GetState() != pb.Store_STATE_ACTIVE {
			return nil
		}
		st.State = pb.Store_STATE_DELETING
		st.DeletedAt = timestamppb.New(s.now())
		return nil
	})
}

// RestoreStore makes a deleted store active again, if collection has not
// started purging it.
//
// The check and the change are one conditional update of the store's record,
// and collection moves a store to purging the same way, so a restore and a
// purge cannot both succeed.
func (s *Server) RestoreStore(ctx context.Context, storeID string) (*pb.Store, error) {
	return s.storage.Update(ctx, storeID, func(st *pb.Store) error {
		switch st.GetState() {
		case pb.Store_STATE_ACTIVE:
			return nil
		case pb.Store_STATE_DELETING:
			st.State = pb.Store_STATE_ACTIVE
			st.DeletedAt = nil
			return nil
		default:
			return fmt.Errorf("%w: %s is being purged", ErrNotRestorable, storeID)
		}
	})
}

// BeginWrite reserves quota before anything is uploaded.
func (s *Server) BeginWrite(ctx context.Context, storeID string, chunks uint32, bytes uint64) (*Reservation, error) {
	store, err := s.storage.GetStore(ctx, storeID)
	if err != nil {
		return nil, err
	}
	if store.GetState() != pb.Store_STATE_ACTIVE {
		return nil, fmt.Errorf("%w: %s is being deleted", ErrNoSuchStore, storeID)
	}

	// The quota is checked inside the update that counts the reservation, so
	// two writers cannot each be told there is room for the last of it.
	reserve := func() error {
		_, err := s.storage.Update(ctx, storeID, func(st *pb.Store) error {
			return accountOf(st, s.now()).reserve(chunks, bytes)
		})
		return err
	}
	if err := reserve(); err != nil {
		// Reserved amounts count as used, and the sweep that releases a
		// reservation whose writer died runs once a week. So a refusal first
		// releases this store's expired reservations and asks again, and an
		// abandoned write holds the ceiling down for ReservationTTL rather than
		// until the next sweep.
		if !errors.Is(err, ErrQuotaExceeded) {
			return nil, err
		}
		released, expireErr := s.ExpireReservations(ctx, storeID)
		if expireErr != nil || released == 0 {
			return nil, err
		}
		if err := reserve(); err != nil {
			return nil, err
		}
	}

	var raw [16]byte
	if _, err := rand.Read(raw[:]); err != nil {
		_, refundErr := s.storage.Update(ctx, storeID, func(st *pb.Store) error {
			accountOf(st, s.now()).release(&Reservation{Chunks: chunks, Bytes: bytes})
			return nil
		})
		return nil, errors.Join(fmt.Errorf("protovcs: generating a reservation id: %w", err), refundErr)
	}
	r := &Reservation{
		ID:          hex.EncodeToString(raw[:]),
		Chunks:      chunks,
		Bytes:       bytes,
		ExpiresUnix: s.now().Add(ReservationTTL).Unix(),
	}
	if err := s.storage.PutReservation(ctx, storeID, r); err != nil {
		_, refundErr := s.storage.Update(ctx, storeID, func(st *pb.Store) error {
			accountOf(st, s.now()).release(r)
			return nil
		})
		return nil, errors.Join(err, refundErr)
	}
	return r, nil
}

// HasChunks returns the ids a store does not hold.
//
// The first call of every write. Returning the absent set rather than the
// present one keeps the common answer — a small change to a large store — small.
func (s *Server) HasChunks(ctx context.Context, storeID string, ids []protovcs.ChunkID) ([]protovcs.ChunkID, error) {
	if _, err := s.storage.GetStore(ctx, storeID); err != nil {
		return nil, err
	}

	// A chunk marked for collection is reported missing, so the writer uploads
	// it again and PutChunks takes it back. A writer told it was present could
	// commit a chunk a later run deletes.
	condemned, err := s.storage.GetCondemned(ctx, storeID)
	if err != nil {
		return nil, err
	}

	var missing []protovcs.ChunkID
	for _, id := range ids {
		if _, marked := condemned.Chunks[id]; marked {
			missing = append(missing, id)
			continue
		}
		has, err := s.storage.HasChunk(ctx, storeID, id)
		if err != nil {
			return nil, err
		}
		if !has {
			missing = append(missing, id)
		}
	}
	return missing, nil
}

// errCollecting reports a chunk that collection has claimed for deletion. It
// wraps [protovcs.ErrRaceLost] so that a client's Change starts the write
// again, by which time the chunk is gone and is uploaded as new.
func errCollecting(id protovcs.ChunkID) error {
	return fmt.Errorf("%w: chunk %s is being collected", protovcs.ErrRaceLost, id)
}

// takeBack removes chunks a writer is uploading from the set marked for
// collection, so a run does not delete what a commit is about to name. It
// fails if a run has already claimed one.
func (s *Server) takeBack(ctx context.Context, storeID string, chunks []protovcs.SealedChunk) error {
	condemned, err := s.storage.GetCondemned(ctx, storeID)
	if err != nil {
		return err
	}
	var anyMarked bool
	for _, c := range chunks {
		if _, ok := condemned.Chunks[c.ID]; ok {
			anyMarked = true
			break
		}
	}
	if !anyMarked {
		return nil
	}
	_, err = s.storage.UpdateCondemned(ctx, storeID, func(c *Condemned) error {
		for _, ch := range chunks {
			if c.Chunks[ch.ID].Deleting {
				return errCollecting(ch.ID)
			}
		}
		for _, ch := range chunks {
			delete(c.Chunks, ch.ID)
		}
		return nil
	})
	return err
}

// putChunksParallelism is how many chunk lookups or chunk writes one
// PutChunks request has in flight at once.
//
// Chunks used to be checked and written one after another, so each new chunk
// waited for the one before it. On a directory bucket the wait is a flush to
// disk, about 10 ms on a workstation. On Cloud Storage it is a network round
// trip. Concurrent calls overlap the waits.
//
// Writing a store of about 1,150 chunks through a
// directory bucket on a workstation took 10.9 s one at a time, 5.2 s with 8 in
// flight, 2.1 s with 32 and 1.1 s with 128. 32 takes most of that gain without
// opening a hundred connections to Cloud Storage for every request.
//
// Writing concurrently does not weaken what a caller can rely on:
//   - Each chunk is still written and made durable on its own, exactly as it
//     was when they were written in turn.
//   - PutChunks returns only after every chunk in the request is stored, and a
//     client writes the commit naming them only after that.
//   - Nothing names a chunk until that commit exists, so the order in which a
//     request's chunks land is not visible to any reader.
const putChunksParallelism = 32

// PutChunks stores chunks against a live reservation.
//
// Every chunk is checked against the store's max_chunk_bytes and the
// reservation's remaining room before any is written, so a request that would
// overrun either stores nothing. The new chunks are then written concurrently,
// and the store's usage is updated once for the request rather than once per
// chunk.
func (s *Server) PutChunks(ctx context.Context, storeID, reservationID string, chunks []protovcs.SealedChunk) error {
	store, err := s.storage.GetStore(ctx, storeID)
	if err != nil {
		return err
	}
	if store.GetState() != pb.Store_STATE_ACTIVE {
		return fmt.Errorf("%w: %s is being deleted", ErrNoSuchStore, storeID)
	}

	if err := checkReservationID(reservationID); err != nil {
		return err
	}
	r, err := s.storage.GetReservation(ctx, storeID, reservationID)
	if err != nil {
		return err
	}
	if r == nil {
		return fmt.Errorf("%w: %q", ErrNoReservation, reservationID)
	}

	if err := s.takeBack(ctx, storeID, chunks); err != nil {
		return err
	}
	fresh, err := s.chunksToStore(ctx, storeID, store, r, chunks)
	if err != nil {
		return err
	}

	// The room is claimed before anything is written, in one update of the
	// reservation. Checking it and recording it separately would let parallel
	// requests on one reservation each find the same room and together store
	// several times what it covers.
	if err := s.claim(ctx, storeID, reservationID, fresh); err != nil {
		return err
	}
	stored, putErr := s.putChunksConcurrently(ctx, storeID, fresh)

	// Charged even when a write failed. The chunks that did land are in the
	// store, and a retry finds them present and skips them, so this is the only
	// point at which they can be counted. What failed is handed back to the
	// reservation, so the retry has room for it.
	err = s.charge(ctx, storeID, stored)
	if len(stored) < len(fresh) {
		landed := make(map[protovcs.ChunkID]bool, len(stored))
		for _, c := range stored {
			landed[c.ID] = true
		}
		var failed []protovcs.SealedChunk
		for _, c := range fresh {
			if !landed[c.ID] {
				failed = append(failed, c)
			}
		}
		err = errors.Join(err, s.unclaim(ctx, storeID, reservationID, failed))
	}
	return errors.Join(putErr, err)
}

// claim records chunks as used by a reservation, refusing if they do not fit
// in what it has left or it has expired.
func (s *Server) claim(ctx context.Context, storeID, reservationID string, chunks []protovcs.SealedChunk) error {
	n, bytes := sealedSize(chunks)
	_, err := s.storage.UpdateReservation(ctx, storeID, reservationID, func(r *Reservation) error {
		if s.now().Unix() > r.ExpiresUnix {
			return fmt.Errorf("%w: %q expired", ErrNoReservation, reservationID)
		}
		if r.UsedChunks+n > r.Chunks || r.UsedBytes+bytes > r.Bytes {
			return fmt.Errorf("%w: reservation %q covered %d chunks and %d bytes",
				ErrNoReservation, r.ID, r.Chunks, r.Bytes)
		}
		r.UsedChunks += n
		r.UsedBytes += bytes
		return nil
	})
	return err
}

// unclaim returns chunks that were claimed and never written to a reservation.
func (s *Server) unclaim(ctx context.Context, storeID, reservationID string, chunks []protovcs.SealedChunk) error {
	n, bytes := sealedSize(chunks)
	_, err := s.storage.UpdateReservation(ctx, storeID, reservationID, func(r *Reservation) error {
		r.UsedChunks -= min(n, r.UsedChunks)
		r.UsedBytes = saturatingSub(r.UsedBytes, bytes)
		return nil
	})
	return err
}

func sealedSize(chunks []protovcs.SealedChunk) (uint32, uint64) {
	var bytes uint64
	for _, c := range chunks {
		bytes += uint64(len(c.Sealed))
	}
	return uint32(len(chunks)), bytes
}

// charge adds stored chunks to the store's usage, with one update of the
// store's record.
func (s *Server) charge(ctx context.Context, storeID string, stored []protovcs.SealedChunk) error {
	if len(stored) == 0 {
		return nil
	}
	n, bytes := sealedSize(stored)
	_, err := s.storage.Update(ctx, storeID, func(st *pb.Store) error {
		accountOf(st, s.now()).storeChunks(n, bytes)
		return nil
	})
	return err
}

// checkReservationID refuses an id BeginWrite could not have issued. The id
// becomes part of a storage key, so anything other than the hex it is minted
// as could name an object outside the store.
func checkReservationID(id string) error {
	if len(id) != 32 {
		return fmt.Errorf("%w: %q is not a reservation id", ErrNoReservation, id)
	}
	for _, c := range id {
		if !('0' <= c && c <= '9' || 'a' <= c && c <= 'f') {
			return fmt.Errorf("%w: %q is not a reservation id", ErrNoReservation, id)
		}
	}
	return nil
}

// chunksToStore returns the chunks of a request that the store does not hold
// yet, each once, in the order given.
//
// It fails if any chunk is larger than the store's max_chunk_bytes, or if the
// new chunks would take the reservation past what it covers.
func (s *Server) chunksToStore(ctx context.Context, storeID string, store *pb.Store, r *Reservation, chunks []protovcs.SealedChunk) ([]protovcs.SealedChunk, error) {
	for _, c := range chunks {
		if err := checkObjectSize(store.GetQuota(), "chunk "+c.ID.String(), uint64(len(c.Sealed))); err != nil {
			return nil, err
		}
	}

	// Looked up concurrently, because on Cloud Storage each lookup is a round
	// trip. The client asked which chunks were missing just before sending
	// these, so nearly every lookup finds nothing.
	held := make([]bool, len(chunks))
	_, errs := inParallel(len(chunks), func(i int) error {
		has, err := s.storage.HasChunk(ctx, storeID, chunks[i].ID)
		held[i] = has
		return err
	})
	if err := firstError(errs); err != nil {
		return nil, err
	}

	var fresh []protovcs.SealedChunk
	seen := make(map[protovcs.ChunkID]bool, len(chunks))
	usedChunks, usedBytes := r.UsedChunks, r.UsedBytes
	for i, c := range chunks {
		// A chunk's name determines its bytes, so storing one twice is storing
		// it once. Charging for it twice would make usage depend on how many
		// times a client retried, or how many times a request named it.
		if held[i] || seen[c.ID] {
			continue
		}
		seen[c.ID] = true

		if usedChunks+1 > r.Chunks || usedBytes+uint64(len(c.Sealed)) > r.Bytes {
			return nil, fmt.Errorf("%w: reservation %q covered %d chunks and %d bytes",
				ErrNoReservation, r.ID, r.Chunks, r.Bytes)
		}
		usedChunks++
		usedBytes += uint64(len(c.Sealed))
		fresh = append(fresh, c)
	}
	return fresh, nil
}

// putChunksConcurrently writes chunks with at most [putChunksParallelism] in
// flight. It returns the chunks that were stored, and the error of the
// earliest chunk in the request that failed.
func (s *Server) putChunksConcurrently(ctx context.Context, storeID string, chunks []protovcs.SealedChunk) ([]protovcs.SealedChunk, error) {
	started, errs := inParallel(len(chunks), func(i int) error {
		return s.storage.PutChunk(ctx, storeID, chunks[i].ID, chunks[i].Sealed)
	})

	var stored []protovcs.SealedChunk
	for i, c := range chunks {
		if started[i] && errs[i] == nil {
			stored = append(stored, c)
		}
	}
	return stored, firstError(errs)
}

// inParallel calls fn for each index below n, with at most
// [putChunksParallelism] calls in flight.
//
// After a call fails no further calls start, but the ones already started are
// waited for. It reports which calls started and what each returned.
func inParallel(n int, fn func(i int) error) (started []bool, errs []error) {
	started = make([]bool, n)
	errs = make([]error, n)
	slots := make(chan struct{}, putChunksParallelism)
	var failed atomic.Bool
	var wg sync.WaitGroup

	for i := 0; i < n; i++ {
		slots <- struct{}{}
		if failed.Load() {
			<-slots
			break
		}
		started[i] = true
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() { <-slots }()
			if err := fn(i); err != nil {
				errs[i] = err
				failed.Store(true)
			}
		}()
	}
	wg.Wait()
	return started, errs
}

func firstError(errs []error) error {
	for _, err := range errs {
		if err != nil {
			return err
		}
	}
	return nil
}

// GetChunks returns chunks by name.
func (s *Server) GetChunks(ctx context.Context, storeID string, ids []protovcs.ChunkID) ([]protovcs.SealedChunk, error) {
	if _, err := s.storage.GetStore(ctx, storeID); err != nil {
		return nil, err
	}

	if batch, ok := s.storage.(ChunkBatchReader); ok {
		sealed, err := batch.GetChunks(ctx, storeID, ids)
		if err != nil {
			return nil, err
		}
		out := make([]protovcs.SealedChunk, len(ids))
		for i, id := range ids {
			out[i] = protovcs.SealedChunk{ID: id, Sealed: sealed[i]}
		}
		return out, nil
	}

	out := make([]protovcs.SealedChunk, 0, len(ids))
	for _, id := range ids {
		sealed, err := s.storage.GetChunk(ctx, storeID, id)
		if err != nil {
			return nil, err
		}
		out = append(out, protovcs.SealedChunk{ID: id, Sealed: sealed})
	}
	return out, nil
}

// PutCommit records a version and releases what the write reserved.
func (s *Server) PutCommit(ctx context.Context, storeID, reservationID string, commit *pb.Commit) (protovcs.CommitID, error) {
	store, err := s.storage.GetStore(ctx, storeID)
	if err != nil {
		return protovcs.CommitID{}, err
	}
	if store.GetState() != pb.Store_STATE_ACTIVE {
		return protovcs.CommitID{}, fmt.Errorf("%w: %s is being deleted", ErrNoSuchStore, storeID)
	}
	if reservationID != "" {
		if err := checkReservationID(reservationID); err != nil {
			return protovcs.CommitID{}, err
		}
	}

	// A commit may not add a chunk marked for collection. Its writer was told
	// the chunk was present before the mark, and did not upload it. Starting
	// the write again uploads it, which takes it back.
	//
	// Checked before presence, not after. A run can claim a marked chunk,
	// delete it and drop its mark between two checks, and in the other order a
	// chunk seen present and then no longer marked would be committed after it
	// was deleted.
	condemned, err := s.storage.GetCondemned(ctx, storeID)
	if err != nil {
		return protovcs.CommitID{}, err
	}
	for _, raw := range commit.GetAdded() {
		id, err := protovcs.ChunkIDFromBytes(raw)
		if err != nil {
			return protovcs.CommitID{}, err
		}
		if _, marked := condemned.Chunks[id]; marked {
			return protovcs.CommitID{}, errCollecting(id)
		}
	}

	// Every chunk the commit adds must be present, or a reachable commit would
	// name something that is not there — which is a corrupt store rather than
	// an incomplete one.
	for _, raw := range commit.GetAdded() {
		id, err := protovcs.ChunkIDFromBytes(raw)
		if err != nil {
			return protovcs.CommitID{}, err
		}
		has, err := s.storage.HasChunk(ctx, storeID, id)
		if err != nil {
			return protovcs.CommitID{}, err
		}
		if !has {
			return protovcs.CommitID{}, fmt.Errorf("protovcs: commit adds chunk %s, which was never stored", id)
		}
	}
	for _, raw := range commit.GetParents() {
		pid, err := protovcs.CommitIDFromBytes(raw)
		if err != nil {
			return protovcs.CommitID{}, err
		}
		if _, err := s.storage.GetCommit(ctx, storeID, pid); err != nil {
			return protovcs.CommitID{}, fmt.Errorf("protovcs: commit names parent %s: %w", pid, err)
		}
	}

	// A commit MUST NOT be written below the floor an operator set. A client is
	// already supposed to refuse this, but "supposed to" is a property of the
	// binary somebody is running, and an old build left on a laptop for a year
	// is exactly the case this exists for.
	if min := store.GetMinWritableSchemaVersion(); min > 0 && commit.GetSchemaVersion() < min {
		return protovcs.CommitID{}, fmt.Errorf(
			"%w: this store accepts schema %d and above, this commit is %d",
			ErrSchemaTooOld, min, commit.GetSchemaVersion())
	}

	// Nor below its own parents. An older client writing over a store that has
	// already migrated would otherwise reinterpret every value in it, and the
	// server can tell without reading anything.
	for _, raw := range commit.GetParents() {
		pid, err := protovcs.CommitIDFromBytes(raw)
		if err != nil {
			return protovcs.CommitID{}, err
		}
		parent, err := s.storage.GetCommit(ctx, storeID, pid)
		if err != nil {
			return protovcs.CommitID{}, err
		}
		if commit.GetSchemaVersion() < parent.GetSchemaVersion() {
			return protovcs.CommitID{}, fmt.Errorf(
				"%w: parent %s is schema %d and this commit is %d",
				ErrSchemaTooOld, pid, parent.GetSchemaVersion(), commit.GetSchemaVersion())
		}
	}

	// The name is recomputed rather than believed. A client free to name a
	// commit could overwrite another commit's record.
	id, err := protovcs.CommitName(commit)
	if err != nil {
		return protovcs.CommitID{}, err
	}
	if claimed := commit.GetId(); len(claimed) > 0 {
		want, err := protovcs.CommitIDFromBytes(claimed)
		if err != nil {
			return protovcs.CommitID{}, err
		}
		if want != id {
			return protovcs.CommitID{}, fmt.Errorf("protovcs: commit claims the name %s but is named %s", want, id)
		}
	}

	stored := proto.Clone(commit).(*pb.Commit)
	stored.Id = id.Bytes()

	// A commit is named by its contents, so one already stored under this
	// name is this commit, from a retry whose first response was lost. It is
	// not charged again.
	if _, err := s.storage.GetCommit(ctx, storeID, id); err == nil {
		if reservationID != "" {
			if err := s.releaseReservation(ctx, storeID, reservationID); err != nil {
				return protovcs.CommitID{}, err
			}
		}
		return id, nil
	} else if !errors.Is(err, protovcs.ErrNoSuchCommit) {
		return protovcs.CommitID{}, err
	}

	// Commits are never collected, and a commit's payload is opaque, so a
	// commit is charged like a chunk: against max_chunk_bytes for its size, and
	// against max_bytes and max_bytes_per_day for what it adds. Otherwise a
	// write token could store anything as commit payloads with no limit. The
	// charge is taken before the record is written so that two commits cannot
	// both be told there is room, and handed back if the write fails.
	size := uint64(proto.Size(stored))
	if err := checkObjectSize(store.GetQuota(), "commit "+id.String(), size); err != nil {
		return protovcs.CommitID{}, err
	}
	if reservationID != "" {
		if err := s.releaseReservation(ctx, storeID, reservationID); err != nil {
			return protovcs.CommitID{}, err
		}
	}
	now := s.now()
	if _, err := s.storage.Update(ctx, storeID, func(st *pb.Store) error {
		if err := accountOf(st, now).storeCommit(size); err != nil {
			return err
		}
		if v := commit.GetSchemaVersion(); v > st.HeadSchemaVersion {
			st.HeadSchemaVersion = v
		}
		return nil
	}); err != nil {
		return protovcs.CommitID{}, err
	}
	if err := s.storage.PutCommit(ctx, storeID, id, stored); err != nil {
		_, refundErr := s.storage.Update(ctx, storeID, func(st *pb.Store) error {
			accountOf(st, s.now()).unstoreCommit(size, now)
			return nil
		})
		return protovcs.CommitID{}, errors.Join(err, refundErr)
	}
	return id, nil
}

// releaseReservation gives a reservation's quota back, once.
//
// The reservation is taken before the quota is returned, so two releases of
// one reservation, such as a PutCommit racing ExpireReservations, return it
// once. A crash between the two leaves the quota held, which Reconcile
// recounts from the reservations that remain.
func (s *Server) releaseReservation(ctx context.Context, storeID, reservationID string) error {
	r, err := s.storage.TakeReservation(ctx, storeID, reservationID)
	if err != nil || r == nil {
		return err
	}
	_, err = s.storage.Update(ctx, storeID, func(st *pb.Store) error {
		accountOf(st, s.now()).release(r)
		return nil
	})
	return err
}

// GetCommits walks back from a commit, at most depth records.
func (s *Server) GetCommits(ctx context.Context, storeID string, from protovcs.CommitID, depth uint32) ([]*pb.Commit, error) {
	if _, err := s.storage.GetStore(ctx, storeID); err != nil {
		return nil, err
	}
	if depth == 0 {
		depth = 1
	}

	var out []*pb.Commit
	seen := map[protovcs.CommitID]bool{}
	queue := []protovcs.CommitID{from}

	for len(queue) > 0 && uint32(len(out)) < depth {
		id := queue[0]
		queue = queue[1:]
		if seen[id] {
			continue
		}
		seen[id] = true

		c, err := s.storage.GetCommit(ctx, storeID, id)
		if err != nil {
			return nil, err
		}
		out = append(out, c)

		for _, raw := range c.GetParents() {
			pid, err := protovcs.CommitIDFromBytes(raw)
			if err != nil {
				return nil, err
			}
			queue = append(queue, pid)
		}
	}
	return out, nil
}

// ListRefs returns every branch.
func (s *Server) ListRefs(ctx context.Context, storeID string) ([]protovcs.RefState, error) {
	if _, err := s.storage.GetStore(ctx, storeID); err != nil {
		return nil, err
	}
	return s.storage.ListRefs(ctx, storeID)
}

// UpdateRef moves a branch under compare-and-swap, and counts branches.
//
// A move to a commit that does not descend from where the branch is needs
// force. The server can check that from plaintext parents alone, and the
// accident it prevents is silent: the history in between is orphaned, the
// chunks only it reached become collectable, and nothing about the write fails.
func (s *Server) UpdateRef(ctx context.Context, storeID string, name []byte, commit protovcs.CommitID, expected string, force bool) (protovcs.RefState, error) {
	store, err := s.storage.GetStore(ctx, storeID)
	if err != nil {
		return protovcs.RefState{}, err
	}
	if store.GetState() != pb.Store_STATE_ACTIVE {
		return protovcs.RefState{}, fmt.Errorf("%w: %s is being deleted", ErrNoSuchStore, storeID)
	}

	if expected == "" && !commit.IsZero() {
		refs, err := s.storage.ListRefs(ctx, storeID)
		if err != nil {
			return protovcs.RefState{}, err
		}
		if err := checkNewRef(store.GetQuota(), len(refs)); err != nil {
			return protovcs.RefState{}, err
		}
	}

	// A move can make chunks reachable again that collection has marked,
	// such as a branch forced back to an old commit. Taking them back before
	// the move means a run cannot delete them after it; one already claimed
	// refuses the move.
	if !commit.IsZero() {
		if err := s.takeBackReachable(ctx, storeID, name, commit); err != nil {
			return protovcs.RefState{}, err
		}
	}

	// Compare-and-swap is checked first, and by the storage layer, so that a
	// writer which simply lost a race is told exactly that. The ancestry check
	// below would otherwise report a lost race as a non-fast-forward, and a
	// caller retrying on one and not the other would give up on an ordinary
	// collision.
	state, err := s.storage.UpdateRef(ctx, storeID, name, commit, expected, force)
	if err != nil {
		return state, err
	}

	// Tell this process's watchers now instead of leaving them to notice on the
	// next poll. A move made by another replica still waits for the poll, so
	// this is an optimisation of the common case rather than the mechanism.
	s.watch.wake(storeID)

	refs, err := s.storage.ListRefs(ctx, storeID)
	if err != nil {
		return state, err
	}
	if _, err := s.storage.Update(ctx, storeID, func(st *pb.Store) error {
		accountOf(st, s.now()).setRefs(uint32(len(refs)))
		return nil
	}); err != nil {
		return state, err
	}
	return state, nil
}

// takeBackReachable removes from the condemned set every chunk that moving
// branch name to commit would make reachable, and fails if a run has already
// claimed one. Nothing is walked while nothing is marked, which is the common
// case.
func (s *Server) takeBackReachable(ctx context.Context, storeID string, name []byte, commit protovcs.CommitID) error {
	condemned, err := s.storage.GetCondemned(ctx, storeID)
	if err != nil || len(condemned.Chunks) == 0 {
		return err
	}
	var have []protovcs.CommitID
	refs, err := s.storage.ListRefs(ctx, storeID)
	if err != nil {
		return err
	}
	for _, r := range refs {
		if string(r.Name) == string(name) {
			have = append(have, r.Commit)
		}
	}
	ids, _, err := s.MissingBetween(ctx, storeID, have, commit)
	if err != nil {
		return err
	}
	var anyMarked bool
	for _, id := range ids {
		if _, ok := condemned.Chunks[id]; ok {
			anyMarked = true
			break
		}
	}
	if !anyMarked {
		return nil
	}
	_, err = s.storage.UpdateCondemned(ctx, storeID, func(c *Condemned) error {
		for _, id := range ids {
			if c.Chunks[id].Deleting {
				return errCollecting(id)
			}
		}
		for _, id := range ids {
			delete(c.Chunks, id)
		}
		return nil
	})
	return err
}

// MissingBetween returns the chunk ids a client holding `have` might not hold
// at `want`.
//
// Computed from the plaintext parents and added sets alone, so the server
// decrypts nothing. Answering it in one call is what makes an incremental fetch
// one round trip instead of a manifest walk.
func (s *Server) MissingBetween(ctx context.Context, storeID string, have []protovcs.CommitID, want protovcs.CommitID) ([]protovcs.ChunkID, bool, error) {
	if _, err := s.storage.GetStore(ctx, storeID); err != nil {
		return nil, false, err
	}

	held := map[protovcs.CommitID]bool{}
	for _, h := range have {
		held[h] = true
	}

	var out []protovcs.ChunkID
	seen := map[protovcs.CommitID]bool{}
	queue := []protovcs.CommitID{want}
	metOne := len(have) == 0

	for len(queue) > 0 {
		id := queue[0]
		queue = queue[1:]
		if held[id] {
			metOne = true
			continue
		}
		if seen[id] {
			continue
		}
		seen[id] = true

		c, err := s.storage.GetCommit(ctx, storeID, id)
		if err != nil {
			return nil, false, err
		}
		for _, raw := range c.GetAdded() {
			cid, err := protovcs.ChunkIDFromBytes(raw)
			if err != nil {
				return nil, false, err
			}
			out = append(out, cid)
		}
		for _, raw := range c.GetParents() {
			pid, err := protovcs.CommitIDFromBytes(raw)
			if err != nil {
				return nil, false, err
			}
			queue = append(queue, pid)
		}
	}
	return out, !metOne, nil
}
