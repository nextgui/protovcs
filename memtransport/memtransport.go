// Package memtransport is a protovcs store held in memory.
//
// For tests, and for anything that wants a store without a server. It enforces
// the same rules a real server does — a commit naming a chunk that is not there
// is refused, a ref moves only against the token the writer read — because a
// test that passes against a permissive fake proves nothing about production.
//
// It does not enforce quotas. Reservations are accepted and ignored, since the
// limits are a control plane concern and there is no control plane here.
package memtransport

import (
	"context"
	"fmt"
	"strconv"
	"sync"

	"google.golang.org/protobuf/proto"

	protovcs "github.com/nextgui/protovcs"
	pb "github.com/nextgui/protovcs/proto/protovcs/v1"
)

// Transport is an in-memory store. Safe for concurrent use, because two
// goroutines writing to it race the same way two machines do.
type Transport struct {
	mu       sync.Mutex
	chunks   map[protovcs.ChunkID][]byte
	commits  map[protovcs.CommitID]*pb.Commit
	refs     map[string]*protovcs.RefState
	casSeq   uint64
	failNext error

	// Open watches. There is no storage to poll here: every write goes through
	// this process, so a ref change can wake the watchers directly.
	watchers  map[int64]chan struct{}
	nextWatch int64

	// What has crossed the wire. Counted rather than timed, because the claims
	// worth testing are about how many chunks an operation touches and a count
	// is exact.
	fetched  int
	uploaded int
	calls    int

	// The last pack hint accepted. Nothing here packs; it is kept so a test can
	// see what a client sent.
	hintCommit protovcs.CommitID
	hintIDs    []protovcs.ChunkID
	hints      int
}

// New returns an empty store.
func New() *Transport {
	return &Transport{
		chunks:   map[protovcs.ChunkID][]byte{},
		commits:  map[protovcs.CommitID]*pb.Commit{},
		refs:     map[string]*protovcs.RefState{},
		watchers: map[int64]chan struct{}{},
	}
}

// FailNext makes the next call return err. For testing what a client does when
// a server misbehaves.
func (t *Transport) FailNext(err error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.failNext = err
}

// Corrupt replaces a chunk's stored bytes without changing its name, which is
// what a hostile or broken server would do. The client must refuse it.
func (t *Transport) Corrupt(id protovcs.ChunkID, sealed []byte) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.chunks[id] = sealed
}

// Swap serves one chunk's bytes under another's name.
func (t *Transport) Swap(a, b protovcs.ChunkID) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.chunks[a], t.chunks[b] = t.chunks[b], t.chunks[a]
}

// SetRef moves a branch with no regard for what a writer expected, which is how
// a test rolls a store back to an older commit.
func (t *Transport) SetRef(name []byte, commit protovcs.CommitID) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.casSeq++
	t.refs[string(name)] = &protovcs.RefState{
		Name:     append([]byte(nil), name...),
		Commit:   commit,
		CASToken: strconv.FormatUint(t.casSeq, 10),
	}
	t.wakeWatchers()
}

// ChunkCount reports how many chunks the store holds, for tests that assert a
// commit cost only what changed.
func (t *Transport) ChunkCount() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return len(t.chunks)
}

// Counts reports chunks fetched, chunks newly stored, and calls made since the
// last Reset.
//
// The unit the complexity tests are written in. A count does not depend on what
// else the machine is doing, and it is what a claim like "a scoped checkout is
// linear in the scope rather than the store" is actually about.
func (t *Transport) Counts() (fetched, uploaded, calls int) {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.fetched, t.uploaded, t.calls
}

// ResetCounts zeroes the counters, so a test can measure one operation rather
// than everything that set it up.
func (t *Transport) ResetCounts() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.fetched, t.uploaded, t.calls = 0, 0, 0
}

func (t *Transport) take() error {
	if t.failNext != nil {
		err := t.failNext
		t.failNext = nil
		return err
	}
	return nil
}

func (t *Transport) ListRefs(ctx context.Context) ([]protovcs.RefState, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if err := t.take(); err != nil {
		return nil, err
	}
	return t.refStates(), nil
}

// WatchCount reports how many watches are open, so a test can assert that a
// watcher which went away released what it held rather than assuming it.
func (t *Transport) WatchCount() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return len(t.watchers)
}

// WatchRefs calls deliver with the store's branches and again whenever one
// moves.
//
// The first call carries the current state rather than waiting for a change, so
// a watcher needs no separate ListRefs. Every call carries the whole set, and a
// watcher that was slow is woken once for however many writes it slept through
// rather than once each.
func (t *Transport) WatchRefs(ctx context.Context, deliver func([]protovcs.RefState) error) error {
	id, wake, err := t.subscribe()
	if err != nil {
		return err
	}
	defer t.unsubscribe(id)

	refs := t.snapshot()
	if err := deliver(refs); err != nil {
		return err
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-wake:
			next := t.snapshot()
			if protovcs.SameRefs(refs, next) {
				continue
			}
			refs = next
			if err := deliver(refs); err != nil {
				return err
			}
		}
	}
}

// subscribe registers a watch before its first read, so that a write landing
// between the two is reported rather than falling into the gap.
func (t *Transport) subscribe() (int64, chan struct{}, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if err := t.take(); err != nil {
		return 0, nil, err
	}

	id := t.nextWatch
	t.nextWatch++
	// Buffered by one. A watch is woken to go and look, so a second wake while
	// one is pending would tell it nothing the first has not.
	wake := make(chan struct{}, 1)
	t.watchers[id] = wake
	return id, wake, nil
}

func (t *Transport) unsubscribe(id int64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	delete(t.watchers, id)
}

func (t *Transport) snapshot() []protovcs.RefState {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.refStates()
}

// refStates copies the ref table. The caller must hold the mutex.
func (t *Transport) refStates() []protovcs.RefState {
	out := make([]protovcs.RefState, 0, len(t.refs))
	for _, r := range t.refs {
		out = append(out, *r)
	}
	return out
}

// wakeWatchers tells every open watch to go and look. The caller must hold the
// mutex.
func (t *Transport) wakeWatchers() {
	for _, wake := range t.watchers {
		select {
		case wake <- struct{}{}:
		default:
		}
	}
}

func (t *Transport) UpdateRef(ctx context.Context, name []byte, commit protovcs.CommitID, expected string, force bool) (protovcs.RefState, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if err := t.take(); err != nil {
		return protovcs.RefState{}, err
	}

	cur, exists := t.refs[string(name)]
	switch {
	case !exists && expected != "":
		return protovcs.RefState{}, fmt.Errorf("%w: the branch no longer exists", protovcs.ErrRaceLost)
	case exists && expected == "":
		return *cur, fmt.Errorf("%w: the branch already exists", protovcs.ErrRaceLost)
	case exists && cur.CASToken != expected:
		return *cur, protovcs.ErrRaceLost
	}

	if commit.IsZero() {
		delete(t.refs, string(name))
		t.wakeWatchers()
		return protovcs.RefState{}, nil
	}
	if _, ok := t.commits[commit]; !ok {
		return protovcs.RefState{}, fmt.Errorf("protovcs: ref points at commit %s, which is not stored", commit)
	}
	if exists && !force && !t.descendsFrom(commit, cur.Commit) {
		return *cur, fmt.Errorf("%w: %s does not descend from %s", protovcs.ErrNotFastForward, commit, cur.Commit)
	}

	t.casSeq++
	next := &protovcs.RefState{
		Name:     append([]byte(nil), name...),
		Commit:   commit,
		CASToken: strconv.FormatUint(t.casSeq, 10),
	}
	t.refs[string(name)] = next
	t.wakeWatchers()
	return *next, nil
}

// descendsFrom reports whether want is reachable from start by walking parents.
func (t *Transport) descendsFrom(start, want protovcs.CommitID) bool {
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
		c, ok := t.commits[id]
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

func (t *Transport) GetCommits(ctx context.Context, from protovcs.CommitID, depth uint32) ([]*pb.Commit, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if err := t.take(); err != nil {
		return nil, err
	}
	if depth == 0 {
		depth = 1
	}

	var out []*pb.Commit
	next := []protovcs.CommitID{from}
	seen := map[protovcs.CommitID]bool{}

	for len(next) > 0 && uint32(len(out)) < depth {
		id := next[0]
		next = next[1:]
		if seen[id] {
			continue
		}
		seen[id] = true

		c, ok := t.commits[id]
		if !ok {
			return nil, fmt.Errorf("%w: %s", protovcs.ErrNoSuchCommit, id)
		}
		out = append(out, proto.Clone(c).(*pb.Commit))

		for _, p := range c.GetParents() {
			pid, err := protovcs.CommitIDFromBytes(p)
			if err != nil {
				return nil, err
			}
			next = append(next, pid)
		}
	}
	return out, nil
}

func (t *Transport) PutCommit(ctx context.Context, commit *pb.Commit, reservation string) (protovcs.CommitID, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if err := t.take(); err != nil {
		return protovcs.CommitID{}, err
	}

	// Every chunk the commit adds has to be present. This is what stops a
	// commit that points at nothing from becoming reachable, and a real server
	// enforces the same thing for the same reason.
	for _, raw := range commit.GetAdded() {
		id, err := protovcs.ChunkIDFromBytes(raw)
		if err != nil {
			return protovcs.CommitID{}, err
		}
		if _, ok := t.chunks[id]; !ok {
			return protovcs.CommitID{}, fmt.Errorf("protovcs: commit adds chunk %s, which was never stored", id)
		}
	}
	for _, raw := range commit.GetParents() {
		pid, err := protovcs.CommitIDFromBytes(raw)
		if err != nil {
			return protovcs.CommitID{}, err
		}
		if _, ok := t.commits[pid]; !ok {
			return protovcs.CommitID{}, fmt.Errorf("protovcs: commit names parent %s, which is not stored", pid)
		}
	}

	// The name is recomputed rather than believed. A client that could name a
	// commit whatever it liked could overwrite another commit's record.
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
	t.commits[id] = stored
	return id, nil
}

func (t *Transport) MissingBetween(ctx context.Context, have []protovcs.CommitID, want protovcs.CommitID) ([]protovcs.ChunkID, bool, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if err := t.take(); err != nil {
		return nil, false, err
	}

	held := map[protovcs.CommitID]bool{}
	for _, h := range have {
		held[h] = true
	}

	// Walk back from want, collecting added sets, and stop at any commit the
	// client already holds. Everything past that point is already theirs.
	//
	// Reaching a root without meeting one of theirs means they are not on this
	// history, so the answer is everything rather than a delta computed against
	// an ancestor that does not exist.
	var out []protovcs.ChunkID
	seen := map[protovcs.CommitID]bool{}
	queue := []protovcs.CommitID{want}
	metOne := len(have) == 0

	for len(queue) > 0 {
		id := queue[0]
		queue = queue[1:]
		if seen[id] || held[id] {
			if held[id] {
				metOne = true
			}
			continue
		}
		seen[id] = true

		c, ok := t.commits[id]
		if !ok {
			return nil, false, fmt.Errorf("%w: %s", protovcs.ErrNoSuchCommit, id)
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

func (t *Transport) BeginWrite(ctx context.Context, chunks uint32, bytes uint64) (string, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if err := t.take(); err != nil {
		return "", err
	}
	t.casSeq++
	return "reservation-" + strconv.FormatUint(t.casSeq, 10), nil
}

func (t *Transport) HasChunks(ctx context.Context, ids []protovcs.ChunkID) ([]protovcs.ChunkID, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if err := t.take(); err != nil {
		return nil, err
	}

	var missing []protovcs.ChunkID
	for _, id := range ids {
		if _, ok := t.chunks[id]; !ok {
			missing = append(missing, id)
		}
	}
	return missing, nil
}

func (t *Transport) PutChunks(ctx context.Context, reservation string, chunks []protovcs.SealedChunk) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if err := t.take(); err != nil {
		return err
	}

	t.calls++
	for _, c := range chunks {
		if _, ok := t.chunks[c.ID]; ok {
			// Writing a chunk twice is writing it once. A retry after a failure
			// of unknown outcome is always safe.
			continue
		}
		t.chunks[c.ID] = append([]byte(nil), c.Sealed...)
		t.uploaded++
	}
	return nil
}

func (t *Transport) GetChunks(ctx context.Context, ids []protovcs.ChunkID) ([]protovcs.SealedChunk, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if err := t.take(); err != nil {
		return nil, err
	}

	t.calls++
	out := make([]protovcs.SealedChunk, 0, len(ids))
	for _, id := range ids {
		sealed, ok := t.chunks[id]
		if !ok {
			return nil, fmt.Errorf("%w: %s", protovcs.ErrNoSuchChunk, id)
		}
		t.fetched++
		out = append(out, protovcs.SealedChunk{ID: id, Sealed: append([]byte(nil), sealed...)})
	}
	return out, nil
}

// PackHint keeps the hint when commit is the head of some ref. Nothing is
// packed, because nothing here is slow to read.
func (t *Transport) PackHint(ctx context.Context, commit protovcs.CommitID, ids []protovcs.ChunkID) (bool, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if err := t.take(); err != nil {
		return false, err
	}
	for _, r := range t.refs {
		if r.Commit == commit {
			t.hintCommit = commit
			t.hintIDs = append([]protovcs.ChunkID(nil), ids...)
			t.hints++
			return true, nil
		}
	}
	return false, nil
}

// LastPackHint returns the last hint accepted and how many have been accepted.
func (t *Transport) LastPackHint() (protovcs.CommitID, []protovcs.ChunkID, int) {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.hintCommit, append([]protovcs.ChunkID(nil), t.hintIDs...), t.hints
}
