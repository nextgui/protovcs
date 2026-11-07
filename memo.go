package protovcs

import (
	"context"
	"sync"

	pb "github.com/nextgui/protovcs/proto/protovcs/v1"
)

// memo is what a store remembers by content: chunk plaintext by chunk name and
// commits by commit id, and the chunk fetches in progress.
//
// Shared by a store and every store derived from it, under its own lock. Both
// kinds of name are fingerprints of what they name, so an entry is correct
// forever, does not depend on the layout, and never needs invalidating.
type memo struct {
	mu       sync.Mutex
	chunks   map[ChunkID][]byte
	commits  map[CommitID]*pb.Commit
	fetching map[ChunkID]*chunkFetch
}

// chunkFetch is one GetChunks request that other reads wanting the same chunks
// wait on rather than repeating.
type chunkFetch struct {
	done chan struct{}
	err  error
}

func newMemo() *memo {
	return &memo{
		chunks:   map[ChunkID][]byte{},
		commits:  map[CommitID]*pb.Commit{},
		fetching: map[ChunkID]*chunkFetch{},
	}
}

// chunk returns a chunk's plaintext when it is held.
func (m *memo) chunk(id ChunkID) ([]byte, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	plaintext, ok := m.chunks[id]
	return plaintext, ok
}

// putChunks records plaintext by chunk name.
func (m *memo) putChunks(plaintext map[ChunkID][]byte) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for id, p := range plaintext {
		m.chunks[id] = p
	}
}

// commit returns a commit when it is held.
func (m *memo) commit(id CommitID) (*pb.Commit, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	c, ok := m.commits[id]
	return c, ok
}

// putCommit records a commit by id.
func (m *memo) putCommit(id CommitID, c *pb.Commit) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.commits[id] = c
}

// warm makes every chunk named held, fetching what is missing in one request.
//
// A chunk another read is already fetching is waited for rather than fetched
// again. A dashboard starts several full reads of the same commit at once, and
// without this each of them downloads the whole vault.
//
// fetch is called with the chunks to request and returns their plaintext. A
// chunk the server does not have is simply absent from what it returns; the
// caller reading it reports that.
func (m *memo) warm(ctx context.Context, ids []ChunkID,
	fetch func([]ChunkID) (map[ChunkID][]byte, error)) error {

	for {
		m.mu.Lock()
		var want []ChunkID
		waits := map[*chunkFetch]bool{}
		mine := &chunkFetch{done: make(chan struct{})}
		for _, id := range ids {
			if _, ok := m.chunks[id]; ok {
				continue
			}
			if other, ok := m.fetching[id]; ok {
				waits[other] = true
				continue
			}
			m.fetching[id] = mine
			want = append(want, id)
		}
		m.mu.Unlock()

		if len(want) > 0 {
			opened, err := fetch(want)
			m.mu.Lock()
			for id, p := range opened {
				m.chunks[id] = p
			}
			for _, id := range want {
				delete(m.fetching, id)
			}
			m.mu.Unlock()
			mine.err = err
			close(mine.done)
			if err != nil {
				return err
			}
		}

		// A fetch that failed belongs to another read, maybe one whose context
		// ended. Its chunks are asked for again rather than failing this read
		// over it.
		retry := false
		for w := range waits {
			select {
			case <-w.done:
				if w.err != nil {
					retry = true
				}
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		if !retry {
			return nil
		}
		if err := ctx.Err(); err != nil {
			return err
		}
	}
}
