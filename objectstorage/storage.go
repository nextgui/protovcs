package objectstorage

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	mathrand "math/rand/v2"
	"slices"
	"strings"
	"time"

	"google.golang.org/protobuf/proto"

	protovcs "github.com/nextgui/protovcs"
	pb "github.com/nextgui/protovcs/proto/protovcs/v1"
	"github.com/nextgui/protovcs/server"
)

// Storage is a [server.Storage] over any [Bucket].
//
// Every rule about what protovcs keeps and where lives here, so a filesystem
// and a bucket differ only in how their objects are written and never in what
// a store looks like.
type Storage struct {
	bucket Bucket

	// Each store's pack index, as last read. See pack.go.
	packs *packCache
}

// New returns storage over a bucket.
func New(b Bucket) *Storage { return &Storage{bucket: b, packs: newPackCache()} }

// Close releases the bucket.
func (s *Storage) Close() error { return s.bucket.Close() }

// Bucket returns the objects underneath, for a caller that has to reach past
// the store — a migration, or a test asserting what was written.
func (s *Storage) Bucket() Bucket { return s.bucket }

// Keys.
//
// A store id comes from a caller, so it is hex-encoded rather than pasted into
// a path: a store called "../../escaped" must name an object and not a
// location. Chunks shard one level by the first byte of their name, because a
// store holds thousands and one flat prefix makes every listing slow.
func storeKey(storeID string) string {
	return "stores/" + hex.EncodeToString([]byte(storeID)) + "/store.pb"
}

func storePrefix(storeID string) string {
	return "stores/" + hex.EncodeToString([]byte(storeID)) + "/"
}

func chunkKey(storeID string, id protovcs.ChunkID) string {
	name := id.String()
	return storePrefix(storeID) + "chunks/" + name[:2] + "/" + name
}

func chunkPrefix(storeID string) string { return storePrefix(storeID) + "chunks/" }

func commitKey(storeID string, id protovcs.CommitID) string {
	return storePrefix(storeID) + "commits/" + id.String()
}

func commitPrefix(storeID string) string { return storePrefix(storeID) + "commits/" }

func refKey(storeID string, name []byte) string {
	return storePrefix(storeID) + "refs/" + hex.EncodeToString(name)
}

func refPrefix(storeID string) string { return storePrefix(storeID) + "refs/" }

func condemnedKey(storeID string) string { return storePrefix(storeID) + "condemned.txt" }

// reservationKey refuses an id that is not the hex the server mints. It is the
// one key component that arrives from a request as text, so anything else could
// climb out of the store's prefix.
func reservationKey(storeID, id string) (string, error) {
	if len(id) == 0 || len(id) > 64 || strings.Trim(id, "0123456789abcdef") != "" {
		return "", fmt.Errorf("%w: %q is not a reservation id", server.ErrNoReservation, id)
	}
	return storePrefix(storeID) + "reservations/" + id, nil
}

func reservationPrefix(storeID string) string { return storePrefix(storeID) + "reservations/" }

func (s *Storage) CreateStore(ctx context.Context, store *pb.Store) (*pb.Store, error) {
	raw, err := proto.Marshal(store)
	if err != nil {
		return nil, err
	}

	// Only if absent, so two callers creating the same store cannot each
	// believe they made it and write different quotas.
	_, err = s.bucket.PutIfGeneration(ctx, storeKey(store.GetStoreId()), raw, 0)
	if errors.Is(err, ErrGenerationMismatch) {
		// Idempotent: a retry after an unknown outcome must not leave a second
		// store nobody knows about, and must not overwrite the first.
		return s.GetStore(ctx, store.GetStoreId())
	}
	if err != nil {
		return nil, err
	}
	return proto.Clone(store).(*pb.Store), nil
}

func (s *Storage) StoreGeneration(ctx context.Context, storeID string) (int64, error) {
	info, err := s.bucket.Stat(ctx, storeKey(storeID))
	if err != nil {
		if errors.Is(err, ErrNoSuchObject) {
			return 0, fmt.Errorf("%w: %s", server.ErrNoSuchStore, storeID)
		}
		return 0, err
	}
	return info.Generation, nil
}

func (s *Storage) GetStore(ctx context.Context, storeID string) (*pb.Store, error) {
	store, _, err := s.getStore(ctx, storeID)
	return store, err
}

func (s *Storage) getStore(ctx context.Context, storeID string) (*pb.Store, int64, error) {
	raw, gen, err := s.bucket.Get(ctx, storeKey(storeID))
	if err != nil {
		if errors.Is(err, ErrNoSuchObject) {
			return nil, 0, fmt.Errorf("%w: %s", server.ErrNoSuchStore, storeID)
		}
		return nil, 0, err
	}
	store := &pb.Store{}
	if err := proto.Unmarshal(raw, store); err != nil {
		return nil, 0, err
	}
	return store, gen, nil
}

func (s *Storage) ListStores(ctx context.Context, req *pb.ListStoresRequest) ([]*pb.Store, error) {
	objects, err := s.bucket.List(ctx, "stores/")
	if err != nil {
		return nil, err
	}

	var out []*pb.Store
	var unreadable *server.UnreadableStoresError
	for _, o := range objects {
		if !strings.HasSuffix(o.Key, "/store.pb") {
			continue
		}
		raw, _, err := s.bucket.Get(ctx, o.Key)
		if err != nil {
			if errors.Is(err, ErrNoSuchObject) {
				continue
			}
			return nil, err
		}
		store := &pb.Store{}
		if err := proto.Unmarshal(raw, store); err != nil {
			id, _ := hex.DecodeString(strings.TrimSuffix(strings.TrimPrefix(o.Key, "stores/"), "/store.pb"))
			if unreadable == nil {
				unreadable = &server.UnreadableStoresError{Err: err}
			}
			unreadable.IDs = append(unreadable.IDs, string(id))
			continue
		}
		if o := req.GetOwner(); o != "" && store.GetOwner() != o {
			continue
		}
		if v := req.GetHeadSchemaVersionBelow(); v != 0 && store.GetHeadSchemaVersion() >= v {
			continue
		}
		if st := req.GetState(); st != pb.Store_STATE_UNSPECIFIED && store.GetState() != st {
			continue
		}
		out = append(out, store)
	}
	if unreadable != nil {
		return out, unreadable
	}
	return out, nil
}

// updateAttempts is how many times Update rereads and tries again.
//
// The usage counters move here, and two writers that read the same figure would
// each write back their own increment and lose the other's — so the write is
// conditional on the generation that was read, and a loss is retried rather
// than reported. There is no caller who would do anything else with it.
//
// Sized above the branch budget because this object is contended harder than
// any branch: every commit in the store touches it, whichever branch it was
// on. It is the one place in this design where writers who are not competing
// still queue.
const updateAttempts = 32

// updateBase is the first delay after a lost race here, doubling per attempt.
//
// Without a delay the loop is wrong in a way that only appears under real
// concurrency: every loser rereads the same generation at once, recomputes, and
// collides again, so the budget is spent with nobody making progress. Four
// concurrent writers exhausted eight immediate attempts. The delay is drawn
// uniformly from the window rather than being the window, because writers that
// back off by the same amount collide again at the same moment.
const updateBase = 2 * time.Millisecond

// updateCap is the longest window a delay is drawn from, above the cost of an
// attempt so that backing off actually separates writers.
const updateCap = time.Second

func updateBackoff(attempt int) time.Duration {
	window := updateBase << uint(attempt)
	if window > updateCap || window <= 0 {
		window = updateCap
	}
	return time.Duration(mathrand.Int64N(int64(window)) + int64(updateBase))
}

func (s *Storage) Update(ctx context.Context, storeID string, fn func(*pb.Store) error) (*pb.Store, error) {
	var last error
	for attempt := 0; attempt < updateAttempts; attempt++ {
		if attempt > 0 {
			timer := time.NewTimer(updateBackoff(attempt))
			select {
			case <-ctx.Done():
				timer.Stop()
				return nil, ctx.Err()
			case <-timer.C:
			}
		}

		store, gen, err := s.getStore(ctx, storeID)
		if err != nil {
			return nil, err
		}
		if err := fn(store); err != nil {
			return nil, err
		}

		raw, err := proto.Marshal(store)
		if err != nil {
			return nil, err
		}
		if _, err := s.bucket.PutIfGeneration(ctx, storeKey(storeID), raw, gen); err == nil {
			return proto.Clone(store).(*pb.Store), nil
		} else if !errors.Is(err, ErrGenerationMismatch) {
			return nil, err
		} else {
			last = err
		}
	}
	return nil, fmt.Errorf("protovcs: gave up updating store %s after %d attempts: %w",
		storeID, updateAttempts, last)
}

func (s *Storage) PutChunk(ctx context.Context, storeID string, id protovcs.ChunkID, sealed []byte) error {
	key := chunkKey(storeID, id)

	// A chunk's name determines its bytes, so writing one twice is writing it
	// once. Skipping also preserves the original write time, which collection
	// uses to tell an abandoned upload from a live one — rewriting would make
	// every re-upload look young and spare orphans forever.
	if _, err := s.bucket.Stat(ctx, key); err == nil {
		return nil
	} else if !errors.Is(err, ErrNoSuchObject) {
		return err
	}
	return s.bucket.Put(ctx, key, sealed)
}

func (s *Storage) GetChunk(ctx context.Context, storeID string, id protovcs.ChunkID) ([]byte, error) {
	raw, _, err := s.bucket.Get(ctx, chunkKey(storeID, id))
	if err != nil {
		if errors.Is(err, ErrNoSuchObject) {
			return nil, fmt.Errorf("%w: %s", protovcs.ErrNoSuchChunk, id)
		}
		return nil, err
	}
	return raw, nil
}

func (s *Storage) HasChunk(ctx context.Context, storeID string, id protovcs.ChunkID) (bool, error) {
	_, err := s.bucket.Stat(ctx, chunkKey(storeID, id))
	if err == nil {
		return true, nil
	}
	if errors.Is(err, ErrNoSuchObject) {
		return false, nil
	}
	return false, err
}

func (s *Storage) DeleteChunk(ctx context.Context, storeID string, id protovcs.ChunkID) error {
	return s.bucket.Delete(ctx, chunkKey(storeID, id))
}

// ListChunkInfo lists every chunk with the size and write time the bucket
// listing already carries.
func (s *Storage) ListChunkInfo(ctx context.Context, storeID string) ([]server.ChunkInfo, error) {
	objects, err := s.bucket.List(ctx, chunkPrefix(storeID))
	if err != nil {
		return nil, err
	}
	out := make([]server.ChunkInfo, 0, len(objects))
	for _, o := range objects {
		id, ok := idFromKey(o.Key)
		if !ok {
			continue
		}
		out = append(out, server.ChunkInfo{ID: id, Size: uint64(o.Size), Written: o.Updated})
	}
	return out, nil
}

func (s *Storage) ListChunks(ctx context.Context, storeID string) ([]protovcs.ChunkID, error) {
	objects, err := s.bucket.List(ctx, chunkPrefix(storeID))
	if err != nil {
		return nil, err
	}

	out := make([]protovcs.ChunkID, 0, len(objects))
	for _, o := range objects {
		id, ok := idFromKey(o.Key)
		if !ok {
			continue
		}
		out = append(out, id)
	}
	return out, nil
}

func idFromKey(key string) (protovcs.ChunkID, bool) {
	name := key[strings.LastIndex(key, "/")+1:]
	raw, err := hex.DecodeString(name)
	if err != nil {
		return protovcs.ChunkID{}, false
	}
	id, err := protovcs.ChunkIDFromBytes(raw)
	if err != nil {
		return protovcs.ChunkID{}, false
	}
	return id, true
}

func (s *Storage) ChunkSize(ctx context.Context, storeID string, id protovcs.ChunkID) (uint64, error) {
	info, err := s.bucket.Stat(ctx, chunkKey(storeID, id))
	if err != nil {
		if errors.Is(err, ErrNoSuchObject) {
			return 0, nil
		}
		return 0, err
	}
	return uint64(info.Size), nil
}

// ChunkWrittenAt reports when a chunk was stored.
//
// What lets collection spare a chunk younger than the grace period, which is
// how an upload still in flight is told from an abandoned one. A bucket that
// cannot report this honestly makes collection unsafe, which is why it is on
// the interface rather than inferred.
func (s *Storage) ChunkWrittenAt(ctx context.Context, storeID string, id protovcs.ChunkID) (time.Time, error) {
	info, err := s.bucket.Stat(ctx, chunkKey(storeID, id))
	if err != nil {
		if errors.Is(err, ErrNoSuchObject) {
			return time.Time{}, fmt.Errorf("%w: %s", protovcs.ErrNoSuchChunk, id)
		}
		return time.Time{}, err
	}
	return info.Updated, nil
}

func (s *Storage) PutCommit(ctx context.Context, storeID string, id protovcs.CommitID, commit *pb.Commit) error {
	raw, err := proto.Marshal(commit)
	if err != nil {
		return err
	}
	return s.bucket.Put(ctx, commitKey(storeID, id), raw)
}

// ListCommitInfo lists a store's commits with their sizes and write times,
// from one listing.
func (s *Storage) ListCommitInfo(ctx context.Context, storeID string) ([]server.CommitInfo, error) {
	objects, err := s.bucket.List(ctx, commitPrefix(storeID))
	if err != nil {
		return nil, err
	}
	out := make([]server.CommitInfo, 0, len(objects))
	for _, o := range objects {
		raw, err := hex.DecodeString(o.Key[strings.LastIndex(o.Key, "/")+1:])
		if err != nil {
			continue
		}
		id, err := protovcs.CommitIDFromBytes(raw)
		if err != nil {
			continue
		}
		out = append(out, server.CommitInfo{ID: id, Size: uint64(o.Size), Written: o.Updated})
	}
	return out, nil
}

func (s *Storage) GetCommit(ctx context.Context, storeID string, id protovcs.CommitID) (*pb.Commit, error) {
	raw, _, err := s.bucket.Get(ctx, commitKey(storeID, id))
	if err != nil {
		if errors.Is(err, ErrNoSuchObject) {
			return nil, fmt.Errorf("%w: %s", protovcs.ErrNoSuchCommit, id)
		}
		return nil, err
	}
	commit := &pb.Commit{}
	if err := proto.Unmarshal(raw, commit); err != nil {
		return nil, err
	}
	return commit, nil
}

func (s *Storage) ListCommits(ctx context.Context, storeID string) ([]protovcs.CommitID, error) {
	objects, err := s.bucket.List(ctx, commitPrefix(storeID))
	if err != nil {
		return nil, err
	}

	out := make([]protovcs.CommitID, 0, len(objects))
	for _, o := range objects {
		name := o.Key[strings.LastIndex(o.Key, "/")+1:]
		raw, err := hex.DecodeString(name)
		if err != nil {
			continue
		}
		id, err := protovcs.CommitIDFromBytes(raw)
		if err != nil {
			continue
		}
		out = append(out, id)
	}
	return out, nil
}

// A ref object holds the commit it points at. The compare-and-swap token is the
// object's own generation, so nothing has to be stored to make it work and
// nothing can disagree with it.
func (s *Storage) readRef(ctx context.Context, storeID string, name []byte) (protovcs.CommitID, int64, bool, error) {
	raw, gen, err := s.bucket.Get(ctx, refKey(storeID, name))
	if err != nil {
		if errors.Is(err, ErrNoSuchObject) {
			return protovcs.CommitID{}, 0, false, nil
		}
		return protovcs.CommitID{}, 0, false, err
	}
	id, err := protovcs.CommitIDFromBytes(raw)
	if err != nil {
		return protovcs.CommitID{}, 0, false, err
	}
	return id, gen, true, nil
}

func (s *Storage) ListRefs(ctx context.Context, storeID string) ([]protovcs.RefState, error) {
	objects, err := s.bucket.List(ctx, refPrefix(storeID))
	if err != nil {
		return nil, err
	}

	var out []protovcs.RefState
	for _, o := range objects {
		encoded := o.Key[strings.LastIndex(o.Key, "/")+1:]
		name, err := hex.DecodeString(encoded)
		if err != nil {
			continue
		}
		commit, gen, ok, err := s.readRef(ctx, storeID, name)
		if err != nil {
			return nil, err
		}
		if !ok {
			continue
		}
		out = append(out, protovcs.RefState{Name: name, Commit: commit, CASToken: tokenOf(gen)})
	}
	return out, nil
}

// tokenOf renders a generation as the opaque token a client returns.
//
// Opaque to the client on purpose: it is a generation here and could be
// something else in another bucket, and a client that parsed it would be a
// client that broke when it changed.
func tokenOf(gen int64) string { return fmt.Sprintf("%d", gen) }

func generationOf(token string) (int64, error) {
	if token == "" {
		return 0, nil
	}
	var gen int64
	if _, err := fmt.Sscanf(token, "%d", &gen); err != nil {
		return 0, fmt.Errorf("protovcs: %q is not a token this store issued", token)
	}
	return gen, nil
}

// UpdateRef moves a branch under compare-and-swap.
//
// The whole of protovcs's concurrency control, and here it is one conditional
// write. What makes it correct is the bucket's precondition, not anything in
// this function — which is why a bucket that cannot offer one may only be used
// by a single process.
func (s *Storage) UpdateRef(ctx context.Context, storeID string, name []byte, commit protovcs.CommitID, expected string, force bool) (protovcs.RefState, error) {
	current, gen, exists, err := s.readRef(ctx, storeID, name)
	if err != nil {
		return protovcs.RefState{}, err
	}
	state := protovcs.RefState{Name: name, Commit: current, CASToken: tokenOf(gen)}

	switch {
	case !exists && expected != "":
		return protovcs.RefState{}, fmt.Errorf("%w: the branch no longer exists", protovcs.ErrRaceLost)
	case exists && expected == "":
		return state, fmt.Errorf("%w: the branch already exists", protovcs.ErrRaceLost)
	case exists && expected != tokenOf(gen):
		return state, protovcs.ErrRaceLost
	}

	if commit.IsZero() {
		// Conditional on the generation read, as a move is, so a move that
		// lands in between is reported rather than deleted.
		if err := s.bucket.DeleteIfGeneration(ctx, refKey(storeID, name), gen); err != nil {
			if errors.Is(err, ErrGenerationMismatch) {
				return protovcs.RefState{}, protovcs.ErrRaceLost
			}
			return protovcs.RefState{}, err
		}
		return protovcs.RefState{}, nil
	}

	if _, err := s.bucket.Stat(ctx, commitKey(storeID, commit)); err != nil {
		return protovcs.RefState{}, fmt.Errorf("protovcs: ref points at commit %s, which is not stored", commit)
	}

	// Checked after the token, so a writer that simply lost a race is told
	// exactly that. A caller retries on one and gives up on the other.
	if exists && !force {
		descends, err := s.descendsFrom(ctx, storeID, commit, current)
		if err != nil {
			return protovcs.RefState{}, err
		}
		if !descends {
			return state, fmt.Errorf("%w: %s does not descend from %s",
				protovcs.ErrNotFastForward, commit, current)
		}
	}

	expectedGen, err := generationOf(expected)
	if err != nil {
		return protovcs.RefState{}, err
	}
	next, err := s.bucket.PutIfGeneration(ctx, refKey(storeID, name), commit.Bytes(), expectedGen)
	if errors.Is(err, ErrGenerationMismatch) {
		// Somebody moved it between the read above and this write. Exactly the
		// case compare-and-swap exists for, and it is reported as a lost race
		// rather than as a failure.
		latest, latestGen, _, rerr := s.readRef(ctx, storeID, name)
		if rerr != nil {
			return protovcs.RefState{}, rerr
		}
		return protovcs.RefState{Name: name, Commit: latest, CASToken: tokenOf(latestGen)}, protovcs.ErrRaceLost
	}
	if err != nil {
		return protovcs.RefState{}, err
	}
	return protovcs.RefState{Name: name, Commit: commit, CASToken: tokenOf(next)}, nil
}

// descendsFrom reports whether want is reachable from start by walking parents.
func (s *Storage) descendsFrom(ctx context.Context, storeID string, start, want protovcs.CommitID) (bool, error) {
	seen := map[protovcs.CommitID]bool{}
	queue := []protovcs.CommitID{start}

	for len(queue) > 0 {
		id := queue[0]
		queue = queue[1:]
		if id == want {
			return true, nil
		}
		if seen[id] {
			continue
		}
		seen[id] = true

		commit, err := s.GetCommit(ctx, storeID, id)
		if err != nil {
			// A parent that is not stored ends this path rather than the walk:
			// a truncated history is a history, and the answer is simply that
			// nothing below it descends from want.
			continue
		}
		for _, raw := range commit.GetParents() {
			pid, err := protovcs.CommitIDFromBytes(raw)
			if err != nil {
				return false, err
			}
			queue = append(queue, pid)
		}
	}
	return false, nil
}

func (s *Storage) PutReservation(ctx context.Context, storeID string, r *server.Reservation) error {
	key, err := reservationKey(storeID, r.ID)
	if err != nil {
		return err
	}
	return s.bucket.Put(ctx, key, encodeReservation(r))
}

func (s *Storage) GetReservation(ctx context.Context, storeID, id string) (*server.Reservation, error) {
	r, _, err := s.readReservation(ctx, storeID, id)
	return r, err
}

// UpdateReservation is conditional on the generation read, as Update is for the
// store's record, and retries a lost race the same way.
func (s *Storage) UpdateReservation(ctx context.Context, storeID, id string, fn func(*server.Reservation) error) (*server.Reservation, error) {
	key, err := reservationKey(storeID, id)
	if err != nil {
		return nil, err
	}
	var last error
	for attempt := 0; attempt < updateAttempts; attempt++ {
		if attempt > 0 {
			timer := time.NewTimer(updateBackoff(attempt))
			select {
			case <-ctx.Done():
				timer.Stop()
				return nil, ctx.Err()
			case <-timer.C:
			}
		}

		r, gen, err := s.readReservation(ctx, storeID, id)
		if err != nil {
			return nil, err
		}
		if r == nil {
			return nil, fmt.Errorf("%w: %q", server.ErrNoReservation, id)
		}
		if err := fn(r); err != nil {
			return nil, err
		}
		if _, err := s.bucket.PutIfGeneration(ctx, key, encodeReservation(r), gen); err == nil {
			return r, nil
		} else if !errors.Is(err, ErrGenerationMismatch) {
			return nil, err
		} else {
			last = err
		}
	}
	return nil, fmt.Errorf("protovcs: gave up updating reservation %s after %d attempts: %w",
		id, updateAttempts, last)
}

func (s *Storage) readReservation(ctx context.Context, storeID, id string) (*server.Reservation, int64, error) {
	key, err := reservationKey(storeID, id)
	if err != nil {
		return nil, 0, err
	}
	raw, gen, err := s.bucket.Get(ctx, key)
	if err != nil {
		if errors.Is(err, ErrNoSuchObject) {
			return nil, 0, nil
		}
		return nil, 0, err
	}

	r := &server.Reservation{ID: id}
	if _, err := fmt.Sscanf(string(raw), "%d %d %d %d %d",
		&r.Chunks, &r.Bytes, &r.UsedChunks, &r.UsedBytes, &r.ExpiresUnix); err != nil {
		return nil, 0, fmt.Errorf("protovcs: reservation %q is malformed: %w", id, err)
	}
	return r, gen, nil
}

func encodeReservation(r *server.Reservation) []byte {
	return []byte(fmt.Sprintf("%d %d %d %d %d", r.Chunks, r.Bytes, r.UsedChunks, r.UsedBytes, r.ExpiresUnix))
}

// TakeReservation is a delete conditional on the generation read, so of two
// callers racing on one reservation only one deletes it.
func (s *Storage) TakeReservation(ctx context.Context, storeID, id string) (*server.Reservation, error) {
	key, err := reservationKey(storeID, id)
	if err != nil {
		return nil, err
	}
	for attempt := 0; attempt < updateAttempts; attempt++ {
		r, gen, err := s.readReservation(ctx, storeID, id)
		if err != nil || r == nil {
			return nil, err
		}
		err = s.bucket.DeleteIfGeneration(ctx, key, gen)
		if err == nil {
			return r, nil
		}
		if !errors.Is(err, ErrGenerationMismatch) {
			return nil, err
		}
	}
	return nil, fmt.Errorf("protovcs: gave up taking reservation %s after %d attempts", id, updateAttempts)
}

func (s *Storage) ListReservations(ctx context.Context, storeID string) ([]*server.Reservation, error) {
	objects, err := s.bucket.List(ctx, reservationPrefix(storeID))
	if err != nil {
		return nil, err
	}

	var out []*server.Reservation
	for _, o := range objects {
		id := o.Key[strings.LastIndex(o.Key, "/")+1:]
		// A reservation that cannot be read is an error, not one to skip:
		// collection reads "no live reservation" as "no write in progress".
		// One deleted since the listing is gone, and is skipped.
		r, err := s.GetReservation(ctx, storeID, id)
		if err != nil {
			return nil, err
		}
		if r != nil {
			out = append(out, r)
		}
	}
	return out, nil
}

var _ server.Storage = (*Storage)(nil)

func (s *Storage) GetCondemned(ctx context.Context, storeID string) (*server.Condemned, error) {
	c, _, err := s.readCondemned(ctx, storeID)
	return c, err
}

// UpdateCondemned is conditional on the generation read, as Update is for the
// store's record. A set that does not exist yet is created at generation zero,
// which fails if another writer created it first.
func (s *Storage) UpdateCondemned(ctx context.Context, storeID string, fn func(*server.Condemned) error) (*server.Condemned, error) {
	var last error
	for attempt := 0; attempt < updateAttempts; attempt++ {
		if attempt > 0 {
			timer := time.NewTimer(updateBackoff(attempt))
			select {
			case <-ctx.Done():
				timer.Stop()
				return nil, ctx.Err()
			case <-timer.C:
			}
		}

		c, gen, err := s.readCondemned(ctx, storeID)
		if err != nil {
			return nil, err
		}
		if err := fn(c); err != nil {
			return nil, err
		}
		if _, err := s.bucket.PutIfGeneration(ctx, condemnedKey(storeID), encodeCondemned(c), gen); err == nil {
			return c, nil
		} else if !errors.Is(err, ErrGenerationMismatch) {
			return nil, err
		} else {
			last = err
		}
	}
	return nil, fmt.Errorf("protovcs: gave up updating the condemned chunks of %s after %d attempts: %w",
		storeID, updateAttempts, last)
}

// readCondemned reads the set as one line per chunk: its id in hex, when it was
// marked, and 1 if a run is deleting it.
func (s *Storage) readCondemned(ctx context.Context, storeID string) (*server.Condemned, int64, error) {
	raw, gen, err := s.bucket.Get(ctx, condemnedKey(storeID))
	if err != nil {
		if errors.Is(err, ErrNoSuchObject) {
			return server.NewCondemned(), 0, nil
		}
		return nil, 0, err
	}

	c := server.NewCondemned()
	for _, line := range strings.Split(string(raw), "\n") {
		if line == "" {
			continue
		}
		var name string
		var marked int64
		var deleting int
		if _, err := fmt.Sscanf(line, "%s %d %d", &name, &marked, &deleting); err != nil {
			return nil, 0, fmt.Errorf("protovcs: condemned chunks of %s are malformed: %w", storeID, err)
		}
		raw, err := hex.DecodeString(name)
		if err != nil {
			return nil, 0, fmt.Errorf("protovcs: condemned chunks of %s are malformed: %w", storeID, err)
		}
		id, err := protovcs.ChunkIDFromBytes(raw)
		if err != nil {
			return nil, 0, fmt.Errorf("protovcs: condemned chunks of %s are malformed: %w", storeID, err)
		}
		c.Chunks[id] = server.CondemnedChunk{MarkedUnix: marked, Deleting: deleting == 1}
	}
	return c, gen, nil
}

func encodeCondemned(c *server.Condemned) []byte {
	ids := make([]protovcs.ChunkID, 0, len(c.Chunks))
	for id := range c.Chunks {
		ids = append(ids, id)
	}
	slices.SortFunc(ids, func(a, b protovcs.ChunkID) int { return bytes.Compare(a[:], b[:]) })

	var b strings.Builder
	for _, id := range ids {
		e := c.Chunks[id]
		deleting := 0
		if e.Deleting {
			deleting = 1
		}
		fmt.Fprintf(&b, "%s %d %d\n", id, e.MarkedUnix, deleting)
	}
	return []byte(b.String())
}
