package protovcs

import (
	"context"
	"fmt"
	"sync"
)

// HeadMarks keeps the newest commit this client has seen on each branch of one
// store, where a later [Store] can find it, such as the browser's IndexedDB.
//
// [Branch.Head] refuses a head that is an ancestor of the newest one seen.
// Without HeadMarks a Store remembers only in memory, so a new process or a
// page reload accepts any head, including one a server rolled back to. With
// them, the Store loads the marks on first use and writes each mark as it
// advances.
//
// A record names a commit by its hash, which the server already knows, so it
// holds nothing secret. It is not protected against local tampering: anything
// able to change it runs with the page's own access and could read the vault
// key instead.
//
// One HeadMarks serves one store. A caller holding several stores gives each
// its own, because branch names repeat across stores.
type HeadMarks interface {
	// Load returns the recorded commit for each branch.
	Load(ctx context.Context) (map[string]CommitID, error)

	// CompareAndSwap records next for branch when the record holds old, where
	// a zero old means there is no record. It returns what the record holds
	// afterwards: next when it swapped, and the record it found otherwise.
	CompareAndSwap(ctx context.Context, branch string, old, next CommitID) (CommitID, error)
}

// WithHeadMarks makes the store load the newest head seen on each branch from
// marks, and write there each one as it advances, so the rollback check in
// [Branch.Head] holds across Store instances. See [HeadMarks].
func (b *StoreBuilder) WithHeadMarks(marks HeadMarks) *StoreBuilder {
	b.headMarks = marks
	return b
}

// saveMarkAttempts bounds how often saveMark retries after another writer
// changed the record.
const saveMarkAttempts = 4

// loadMarks reads the store's HeadMarks into memory, once.
//
// A failed load fails the caller and is retried by the next one. Reading on
// without the marks would accept any head, which is the gap they close.
func (s *Store) loadMarks(ctx context.Context) error {
	if s.headMarks == nil {
		return nil
	}
	s.loadMu.Lock()
	defer s.loadMu.Unlock()
	if s.marksLoaded {
		return nil
	}
	loaded, err := s.headMarks.Load(ctx)
	if err != nil {
		return fmt.Errorf("protovcs: loading the newest heads seen: %w", err)
	}
	s.mu.Lock()
	for branch, id := range loaded {
		s.stored[branch] = id
		if _, ok := s.highWater[branch]; !ok {
			s.highWater[branch] = id
		}
	}
	s.mu.Unlock()
	s.marksLoaded = true
	return nil
}

// saveMark writes the mark held in memory for branch to the store's HeadMarks.
//
// Best effort: a failed write leaves the mark in memory, and only a later Store
// misses it. Another Store sharing the record may have written a newer commit
// in the meantime. The record then keeps it and this store adopts it, so the
// record moves only to a descendant, or elsewhere after a forced move. When the
// two cannot be compared, the record is left as it is.
func (s *Store) saveMark(ctx context.Context, branch string) {
	if s.headMarks == nil || s.loadMarks(ctx) != nil {
		return
	}
	s.saveMu.Lock()
	defer s.saveMu.Unlock()
	for attempt := 0; attempt < saveMarkAttempts; attempt++ {
		s.mu.Lock()
		want, old := s.highWater[branch], s.stored[branch]
		s.mu.Unlock()
		if want == old {
			return
		}
		now, err := s.headMarks.CompareAndSwap(ctx, branch, old, want)
		if err != nil {
			return
		}
		s.mu.Lock()
		s.stored[branch] = now
		s.mu.Unlock()
		if now == want {
			return
		}
		if now.IsZero() {
			// The record was cleared, as deleting the database does.
			continue
		}
		newer, err := s.isAncestor(ctx, now, want)
		if err != nil {
			return
		}
		if newer {
			if err := s.Branch(branch).raiseHighWater(ctx, want, true, now); err != nil {
				return
			}
		}
	}
}

// MemoryHeadMarks is a [HeadMarks] held in memory. Stores built over the same
// one share their marks, as stores in one process would share a file.
type MemoryHeadMarks struct {
	mu    sync.Mutex
	marks map[string]CommitID
}

// NewMemoryHeadMarks returns an empty MemoryHeadMarks.
func NewMemoryHeadMarks() *MemoryHeadMarks {
	return &MemoryHeadMarks{marks: map[string]CommitID{}}
}

// Load returns a copy of the marks.
func (m *MemoryHeadMarks) Load(context.Context) (map[string]CommitID, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make(map[string]CommitID, len(m.marks))
	for branch, id := range m.marks {
		out[branch] = id
	}
	return out, nil
}

// CompareAndSwap records next for branch when the mark is old. See [HeadMarks].
func (m *MemoryHeadMarks) CompareAndSwap(_ context.Context, branch string, old, next CommitID) (CommitID, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.marks[branch] != old {
		return m.marks[branch], nil
	}
	m.marks[branch] = next
	return next, nil
}
