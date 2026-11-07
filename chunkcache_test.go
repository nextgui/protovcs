package protovcs_test

import (
	"context"
	"errors"
	"sync"
	"testing"

	protovcs "github.com/nextgui/protovcs"
	"github.com/nextgui/protovcs/memtransport"
	testpb "github.com/nextgui/protovcs/proto/protovcs/testpb"
)

// memCache is a ChunkCache held in memory, standing in for IndexedDB.
type memCache struct {
	mu      sync.Mutex
	chunks  map[protovcs.ChunkID][]byte
	dropped []protovcs.ChunkID
	fail    bool
}

func newMemCache() *memCache { return &memCache{chunks: map[protovcs.ChunkID][]byte{}} }

func (c *memCache) Get(_ context.Context, ids []protovcs.ChunkID) ([]protovcs.SealedChunk, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.fail {
		return nil, errors.New("the cache is unavailable")
	}
	var out []protovcs.SealedChunk
	for _, id := range ids {
		if sealed, ok := c.chunks[id]; ok {
			out = append(out, protovcs.SealedChunk{ID: id, Sealed: sealed})
		}
	}
	return out, nil
}

func (c *memCache) Put(_ context.Context, chunks []protovcs.SealedChunk) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.fail {
		return errors.New("the cache is unavailable")
	}
	for _, ch := range chunks {
		c.chunks[ch.ID] = append([]byte(nil), ch.Sealed...)
	}
	return nil
}

func (c *memCache) Drop(_ context.Context, ids []protovcs.ChunkID) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, id := range ids {
		delete(c.chunks, id)
		c.dropped = append(c.dropped, id)
	}
	return nil
}

// cachedStore opens a store over tr that keeps its chunks in cache.
func cachedStore(t *testing.T, tr protovcs.Transport, cache protovcs.ChunkCache) *protovcs.Store {
	t.Helper()
	c, err := protovcs.CompileLayout(layout(), (&testpb.Store{}).ProtoReflect().Descriptor())
	if err != nil {
		t.Fatalf("CompileLayout: %v", err)
	}
	s, err := protovcs.NewBuilder().WithTransport(tr).WithSealer(keyring(t)).
		WithLayout(c).WithChunkCache(cache).Build()
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	return s
}

// seededStore writes a store of n records through a store with no cache.
func seededStore(t *testing.T, tr protovcs.Transport, n int) {
	t.Helper()
	if _, err := newStore(t, tr, keyring(t)).Branch("main").Write(context.Background(), meta("seed"),
		&testpb.Store{Name: "a store", Records: records(n)}); err != nil {
		t.Fatalf("Write: %v", err)
	}
}

// TestASecondPageLoadReadsFromTheCache checks that a store opened over a cache
// an earlier store filled fetches no chunks from the server, and reads the same
// message.
func TestASecondPageLoadReadsFromTheCache(t *testing.T) {
	ctx := context.Background()
	tr := memtransport.New()
	seededStore(t, tr, 60)
	cache := newMemCache()

	first, err := cachedStore(t, tr, cache).Branch("main").Read(ctx)
	if err != nil {
		t.Fatalf("first Read: %v", err)
	}

	counting := &countingTransport{Transport: tr}
	second, err := cachedStore(t, counting, cache).Branch("main").Read(ctx)
	if err != nil {
		t.Fatalf("second Read: %v", err)
	}
	if got := counting.getChunks.Load(); got != 0 {
		t.Fatalf("a read over a filled cache made %d GetChunks requests, want 0", got)
	}
	if got, want := len(second.(*testpb.Store).GetRecords()), len(first.(*testpb.Store).GetRecords()); got != want {
		t.Fatalf("the cached read has %d records, the first had %d", got, want)
	}
}

// TestADamagedCacheEntryIsDroppedAndFetched checks that a cached chunk that
// fails to open is removed from the cache and read from the server instead,
// rather than failing the read.
func TestADamagedCacheEntryIsDroppedAndFetched(t *testing.T) {
	ctx := context.Background()
	tr := memtransport.New()
	seededStore(t, tr, 20)
	cache := newMemCache()
	if _, err := cachedStore(t, tr, cache).Branch("main").Read(ctx); err != nil {
		t.Fatalf("first Read: %v", err)
	}

	var damaged protovcs.ChunkID
	cache.mu.Lock()
	for id, sealed := range cache.chunks {
		damaged = id
		flipped := append([]byte(nil), sealed...)
		flipped[len(flipped)-1] ^= 1
		cache.chunks[id] = flipped
		break
	}
	cache.mu.Unlock()

	got, err := cachedStore(t, tr, cache).Branch("main").Read(ctx)
	if err != nil {
		t.Fatalf("Read over a damaged entry: %v", err)
	}
	if n := len(got.(*testpb.Store).GetRecords()); n != 20 {
		t.Fatalf("read %d records, want 20", n)
	}
	if len(cache.dropped) != 1 || cache.dropped[0] != damaged {
		t.Fatalf("dropped %v, want only the damaged chunk %s", cache.dropped, damaged)
	}
}

// TestAFailingCacheIsAMiss checks that a cache that errors on every call costs
// a read nothing but the cache's help.
func TestAFailingCacheIsAMiss(t *testing.T) {
	ctx := context.Background()
	tr := memtransport.New()
	seededStore(t, tr, 20)
	cache := newMemCache()
	cache.fail = true

	got, err := cachedStore(t, tr, cache).Branch("main").Read(ctx)
	if err != nil {
		t.Fatalf("Read with a failing cache: %v", err)
	}
	if n := len(got.(*testpb.Store).GetRecords()); n != 20 {
		t.Fatalf("read %d records, want 20", n)
	}
}

// TestAWriteKeepsWhatItUploaded checks that the chunks a write sends to the
// server are cached too, so the writer's next page load does not download what
// it uploaded.
func TestAWriteKeepsWhatItUploaded(t *testing.T) {
	ctx := context.Background()
	tr := memtransport.New()
	cache := newMemCache()
	if _, err := cachedStore(t, tr, cache).Branch("main").Write(ctx, meta("seed"),
		&testpb.Store{Name: "a store", Records: records(30)}); err != nil {
		t.Fatalf("Write: %v", err)
	}

	counting := &countingTransport{Transport: tr}
	if _, err := cachedStore(t, counting, cache).Branch("main").Read(ctx); err != nil {
		t.Fatalf("Read: %v", err)
	}
	if got := counting.getChunks.Load(); got != 0 {
		t.Fatalf("reading what this cache's writer uploaded made %d GetChunks requests, want 0", got)
	}
}
