package server

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"time"

	"google.golang.org/protobuf/proto"

	protovcs "github.com/nextgui/protovcs"
	pb "github.com/nextgui/protovcs/proto/protovcs/v1"
)

// MemStorage keeps everything in memory.
//
// For tests and for a single-process deployment. It records when each chunk was
// written, which is what lets collection tell an abandoned write from one still
// in progress.
type MemStorage struct {
	mu     sync.Mutex
	stores map[string]*storeState
	now    func() time.Time
}

type storeState struct {
	record        *pb.Store
	chunks        map[protovcs.ChunkID][]byte
	chunkWritten  map[protovcs.ChunkID]time.Time
	commits       map[protovcs.CommitID]*pb.Commit
	commitWritten map[protovcs.CommitID]time.Time
	refs          map[string]*protovcs.RefState
	reservations  map[string]*Reservation
	condemned     *Condemned
	casSeq        uint64

	// Counts writes of record, standing in for a bucket object's generation.
	// A ref move does not change it, exactly as writing a ref object does not
	// change the store record's generation in a bucket.
	generation int64
}

// NewMemStorage returns empty storage.
func NewMemStorage() *MemStorage {
	return &MemStorage{
		stores: map[string]*storeState{},
		now:    func() time.Time { return time.Now().UTC() },
	}
}

// WithClock replaces the clock used to age chunks.
func (m *MemStorage) WithClock(now func() time.Time) *MemStorage {
	m.now = now
	return m
}

func (m *MemStorage) state(storeID string) (*storeState, error) {
	st, ok := m.stores[storeID]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrNoSuchStore, storeID)
	}
	return st, nil
}

func (m *MemStorage) CreateStore(ctx context.Context, store *pb.Store) (*pb.Store, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if existing, ok := m.stores[store.GetStoreId()]; ok {
		// Idempotent: a retry after an unknown outcome must not leave a second
		// store nobody knows about.
		return proto.Clone(existing.record).(*pb.Store), nil
	}
	m.stores[store.GetStoreId()] = &storeState{
		record:        proto.Clone(store).(*pb.Store),
		chunks:        map[protovcs.ChunkID][]byte{},
		chunkWritten:  map[protovcs.ChunkID]time.Time{},
		commits:       map[protovcs.CommitID]*pb.Commit{},
		commitWritten: map[protovcs.CommitID]time.Time{},
		refs:          map[string]*protovcs.RefState{},
		reservations:  map[string]*Reservation{},
		condemned:     NewCondemned(),
		generation:    1,
	}
	return proto.Clone(store).(*pb.Store), nil
}

func (m *MemStorage) GetStore(ctx context.Context, storeID string) (*pb.Store, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	st, err := m.state(storeID)
	if err != nil {
		return nil, err
	}
	return proto.Clone(st.record).(*pb.Store), nil
}

func (m *MemStorage) ListStores(ctx context.Context, req *pb.ListStoresRequest) ([]*pb.Store, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	var out []*pb.Store
	for _, st := range m.stores {
		if o := req.GetOwner(); o != "" && st.record.GetOwner() != o {
			continue
		}
		if v := req.GetHeadSchemaVersionBelow(); v != 0 && st.record.GetHeadSchemaVersion() >= v {
			continue
		}
		if s := req.GetState(); s != pb.Store_STATE_UNSPECIFIED && st.record.GetState() != s {
			continue
		}
		out = append(out, proto.Clone(st.record).(*pb.Store))
	}
	return out, nil
}

func (m *MemStorage) Update(ctx context.Context, storeID string, fn func(*pb.Store) error) (*pb.Store, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	st, err := m.state(storeID)
	if err != nil {
		return nil, err
	}
	if err := fn(st.record); err != nil {
		return nil, err
	}
	st.generation++
	return proto.Clone(st.record).(*pb.Store), nil
}

func (m *MemStorage) StoreGeneration(ctx context.Context, storeID string) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	st, err := m.state(storeID)
	if err != nil {
		return 0, err
	}
	return st.generation, nil
}

func (m *MemStorage) PutChunk(ctx context.Context, storeID string, id protovcs.ChunkID, sealed []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	st, err := m.state(storeID)
	if err != nil {
		return err
	}
	if _, ok := st.chunks[id]; ok {
		return nil
	}
	st.chunks[id] = append([]byte(nil), sealed...)
	st.chunkWritten[id] = m.now()
	return nil
}

func (m *MemStorage) GetChunk(ctx context.Context, storeID string, id protovcs.ChunkID) ([]byte, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	st, err := m.state(storeID)
	if err != nil {
		return nil, err
	}
	sealed, ok := st.chunks[id]
	if !ok {
		return nil, fmt.Errorf("%w: %s", protovcs.ErrNoSuchChunk, id)
	}
	return append([]byte(nil), sealed...), nil
}

func (m *MemStorage) HasChunk(ctx context.Context, storeID string, id protovcs.ChunkID) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	st, err := m.state(storeID)
	if err != nil {
		return false, err
	}
	_, ok := st.chunks[id]
	return ok, nil
}

func (m *MemStorage) DeleteChunk(ctx context.Context, storeID string, id protovcs.ChunkID) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	st, err := m.state(storeID)
	if err != nil {
		return err
	}
	delete(st.chunks, id)
	delete(st.chunkWritten, id)
	return nil
}

func (m *MemStorage) ListChunks(ctx context.Context, storeID string) ([]protovcs.ChunkID, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	st, err := m.state(storeID)
	if err != nil {
		return nil, err
	}
	out := make([]protovcs.ChunkID, 0, len(st.chunks))
	for id := range st.chunks {
		out = append(out, id)
	}
	return out, nil
}

func (m *MemStorage) ChunkSize(ctx context.Context, storeID string, id protovcs.ChunkID) (uint64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	st, err := m.state(storeID)
	if err != nil {
		return 0, err
	}
	return uint64(len(st.chunks[id])), nil
}

// ChunkWrittenAt reports when a chunk was stored, which is how collection tells
// an abandoned write from one still in progress.
func (m *MemStorage) ChunkWrittenAt(ctx context.Context, storeID string, id protovcs.ChunkID) (time.Time, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	st, err := m.state(storeID)
	if err != nil {
		return time.Time{}, err
	}
	at, ok := st.chunkWritten[id]
	if !ok {
		return time.Time{}, fmt.Errorf("%w: %s", protovcs.ErrNoSuchChunk, id)
	}
	return at, nil
}

func (m *MemStorage) PutCommit(ctx context.Context, storeID string, id protovcs.CommitID, commit *pb.Commit) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	st, err := m.state(storeID)
	if err != nil {
		return err
	}
	st.commits[id] = proto.Clone(commit).(*pb.Commit)
	st.commitWritten[id] = m.now()
	return nil
}

// ListCommitInfo lists a store's commits with their sizes and write times.
func (m *MemStorage) ListCommitInfo(ctx context.Context, storeID string) ([]CommitInfo, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	st, err := m.state(storeID)
	if err != nil {
		return nil, err
	}
	out := make([]CommitInfo, 0, len(st.commits))
	for id, c := range st.commits {
		out = append(out, CommitInfo{ID: id, Size: uint64(proto.Size(c)), Written: st.commitWritten[id]})
	}
	return out, nil
}

func (m *MemStorage) GetCommit(ctx context.Context, storeID string, id protovcs.CommitID) (*pb.Commit, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	st, err := m.state(storeID)
	if err != nil {
		return nil, err
	}
	c, ok := st.commits[id]
	if !ok {
		return nil, fmt.Errorf("%w: %s", protovcs.ErrNoSuchCommit, id)
	}
	return proto.Clone(c).(*pb.Commit), nil
}

func (m *MemStorage) ListCommits(ctx context.Context, storeID string) ([]protovcs.CommitID, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	st, err := m.state(storeID)
	if err != nil {
		return nil, err
	}
	out := make([]protovcs.CommitID, 0, len(st.commits))
	for id := range st.commits {
		out = append(out, id)
	}
	return out, nil
}

func (m *MemStorage) ListRefs(ctx context.Context, storeID string) ([]protovcs.RefState, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	st, err := m.state(storeID)
	if err != nil {
		return nil, err
	}
	out := make([]protovcs.RefState, 0, len(st.refs))
	for _, r := range st.refs {
		out = append(out, *r)
	}
	return out, nil
}

func (m *MemStorage) UpdateRef(ctx context.Context, storeID string, name []byte, commit protovcs.CommitID, expected string, force bool) (protovcs.RefState, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	st, err := m.state(storeID)
	if err != nil {
		return protovcs.RefState{}, err
	}

	cur, exists := st.refs[string(name)]
	switch {
	case !exists && expected != "":
		return protovcs.RefState{}, fmt.Errorf("%w: the branch no longer exists", protovcs.ErrRaceLost)
	case exists && expected == "":
		return *cur, fmt.Errorf("%w: the branch already exists", protovcs.ErrRaceLost)
	case exists && cur.CASToken != expected:
		return *cur, protovcs.ErrRaceLost
	}

	if commit.IsZero() {
		delete(st.refs, string(name))
		return protovcs.RefState{}, nil
	}
	if _, ok := st.commits[commit]; !ok {
		return protovcs.RefState{}, fmt.Errorf("protovcs: ref points at commit %s, which is not stored", commit)
	}

	// Checked after the token, so a lost race is reported as one. A caller
	// retries on that and gives up on this, and telling them apart is what
	// makes each response correct.
	if exists && !force && !st.descendsFrom(commit, cur.Commit) {
		return *cur, fmt.Errorf("%w: %s does not descend from %s", protovcs.ErrNotFastForward, commit, cur.Commit)
	}

	st.casSeq++
	next := &protovcs.RefState{
		Name:     append([]byte(nil), name...),
		Commit:   commit,
		CASToken: strconv.FormatUint(st.casSeq, 10),
	}
	st.refs[string(name)] = next
	return *next, nil
}

// descendsFrom reports whether want is reachable from start by walking parents.
//
// Plaintext parents make this something a server can answer without reading
// anything it stores.
func (st *storeState) descendsFrom(start, want protovcs.CommitID) bool {
	seen := map[protovcs.CommitID]bool{}
	queue := []protovcs.CommitID{start}
	for len(queue) > 0 {
		id := queue[0]
		queue = queue[1:]
		if id == want {
			return true
		}
		if seen[id] {
			continue
		}
		seen[id] = true
		c, ok := st.commits[id]
		if !ok {
			continue
		}
		for _, raw := range c.GetParents() {
			pid, err := protovcs.CommitIDFromBytes(raw)
			if err != nil {
				continue
			}
			queue = append(queue, pid)
		}
	}
	return false
}

func (m *MemStorage) PutReservation(ctx context.Context, storeID string, r *Reservation) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	st, err := m.state(storeID)
	if err != nil {
		return err
	}
	copied := *r
	st.reservations[r.ID] = &copied
	return nil
}

func (m *MemStorage) GetReservation(ctx context.Context, storeID, id string) (*Reservation, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	st, err := m.state(storeID)
	if err != nil {
		return nil, err
	}
	r, ok := st.reservations[id]
	if !ok {
		return nil, nil
	}
	copied := *r
	return &copied, nil
}

func (m *MemStorage) UpdateReservation(ctx context.Context, storeID, id string, fn func(*Reservation) error) (*Reservation, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	st, err := m.state(storeID)
	if err != nil {
		return nil, err
	}
	r, ok := st.reservations[id]
	if !ok {
		return nil, fmt.Errorf("%w: %q", ErrNoReservation, id)
	}
	copied := *r
	if err := fn(&copied); err != nil {
		return nil, err
	}
	st.reservations[id] = &copied
	out := copied
	return &out, nil
}

func (m *MemStorage) TakeReservation(ctx context.Context, storeID, id string) (*Reservation, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	st, err := m.state(storeID)
	if err != nil {
		return nil, err
	}
	r, ok := st.reservations[id]
	if !ok {
		return nil, nil
	}
	delete(st.reservations, id)
	taken := *r
	return &taken, nil
}

func (m *MemStorage) ListReservations(ctx context.Context, storeID string) ([]*Reservation, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	st, err := m.state(storeID)
	if err != nil {
		return nil, err
	}
	out := make([]*Reservation, 0, len(st.reservations))
	for _, r := range st.reservations {
		copied := *r
		out = append(out, &copied)
	}
	return out, nil
}

func (m *MemStorage) GetCondemned(ctx context.Context, storeID string) (*Condemned, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	st, err := m.state(storeID)
	if err != nil {
		return nil, err
	}
	return st.condemned.Clone(), nil
}

func (m *MemStorage) UpdateCondemned(ctx context.Context, storeID string, fn func(*Condemned) error) (*Condemned, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	st, err := m.state(storeID)
	if err != nil {
		return nil, err
	}
	updated := st.condemned.Clone()
	if err := fn(updated); err != nil {
		return nil, err
	}
	st.condemned = updated
	return updated.Clone(), nil
}
