package protovcs

import "context"

// ChunkCache keeps sealed chunks where a later store can find them, such as
// the browser's IndexedDB, so a page load does not download again what an
// earlier one already did.
//
// A store asks it before the server and tells it about every chunk it fetches
// or uploads. What it holds is sealed exactly as the server holds it, so it
// keeps nothing a server does not already have, and every chunk is checked
// when it is opened: an entry that was damaged or replaced fails to open, is
// dropped, and is fetched from the server instead.
//
// Every method is best effort. An error is treated as a miss, never as a
// failed read, because the server still has everything.
type ChunkCache interface {
	// Get returns the chunks it holds among ids, in any order, and nothing for
	// the rest.
	Get(ctx context.Context, ids []ChunkID) ([]SealedChunk, error)

	// Put keeps chunks. It may return before they are written.
	Put(ctx context.Context, chunks []SealedChunk) error

	// Drop removes chunks that failed to open.
	Drop(ctx context.Context, ids []ChunkID) error
}

// WithChunkCache makes the store look for chunks in cache before asking the
// server, and keep there what it fetches and uploads.
//
// Several stores may share one cache. A chunk's name is a fingerprint of its
// contents, keyed by the store's sealer, so stores with different keys never
// share a name.
func (b *StoreBuilder) WithChunkCache(cache ChunkCache) *StoreBuilder {
	b.chunkCache = cache
	return b
}

// fetchChunks opens every chunk in want that the cache or the server has.
//
// The cache is asked first. A cached chunk that fails to open is dropped from
// it and fetched from the server with the rest; a chunk from the server that
// fails to open fails the read, as it always has.
func (s *Store) fetchChunks(ctx context.Context, want []ChunkID) (map[ChunkID][]byte, error) {
	opened := make(map[ChunkID][]byte, len(want))
	missing := want
	if s.chunkCache != nil {
		held, _ := s.chunkCache.Get(ctx, want)
		var bad []ChunkID
		for _, c := range held {
			plaintext, err := s.sealer.Open(c.ID, c.Sealed)
			if err != nil {
				bad = append(bad, c.ID)
				continue
			}
			opened[c.ID] = plaintext
		}
		if len(bad) > 0 {
			_ = s.chunkCache.Drop(ctx, bad)
		}
		missing = make([]ChunkID, 0, len(want)-len(opened))
		for _, id := range want {
			if _, ok := opened[id]; !ok {
				missing = append(missing, id)
			}
		}
		if len(missing) == 0 {
			return opened, nil
		}
	}

	fetched, err := s.transport.GetChunks(ctx, missing)
	if err != nil {
		return nil, err
	}
	for _, c := range fetched {
		plaintext, err := s.sealer.Open(c.ID, c.Sealed)
		if err != nil {
			return nil, err
		}
		opened[c.ID] = plaintext
	}
	s.keepChunks(ctx, fetched)
	return opened, nil
}

// keepChunks hands chunks to the cache, when the store has one.
func (s *Store) keepChunks(ctx context.Context, chunks []SealedChunk) {
	if s.chunkCache != nil && len(chunks) > 0 {
		_ = s.chunkCache.Put(ctx, chunks)
	}
}
