//go:build js

package idbcache

import (
	"context"
	"fmt"
	"testing"
	"time"

	protovcs "github.com/nextgui/protovcs"
)

// open opens a database of the test's own, so tests do not share one, and
// deletes it afterwards.
func open(t *testing.T, capBytes int64) *Cache {
	t.Helper()
	name := "idbcache-test-" + t.Name()
	c, err := Open(context.Background(), name, capBytes)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() {
		c.close()
		_ = deleteDatabase(context.Background(), name)
	})
	return c
}

// chunk returns a sealed chunk of n bytes named after i. The bytes are not
// sealed by anything: this package stores what it is given.
func chunk(i, n int) protovcs.SealedChunk {
	var id protovcs.ChunkID
	copy(id[:], fmt.Sprintf("chunk-%04d", i))
	sealed := make([]byte, n)
	for j := range sealed {
		sealed[j] = byte(i)
	}
	return protovcs.SealedChunk{ID: id, Sealed: sealed}
}

// put keeps chunks and waits for them to be written.
func put(t *testing.T, c *Cache, chunks ...protovcs.SealedChunk) {
	t.Helper()
	if err := c.Put(context.Background(), chunks); err != nil {
		t.Fatalf("Put: %v", err)
	}
	c.writes.Wait()
}

// held returns which of the chunks numbered 0..n-1 the cache holds.
func held(t *testing.T, c *Cache, n int) map[int]bool {
	t.Helper()
	ids := make([]protovcs.ChunkID, n)
	index := map[protovcs.ChunkID]int{}
	for i := range ids {
		ids[i] = chunk(i, 0).ID
		index[ids[i]] = i
	}
	got, err := c.Get(context.Background(), ids)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	c.writes.Wait()
	out := map[int]bool{}
	for _, g := range got {
		out[index[g.ID]] = true
	}
	return out
}

func total(t *testing.T, c *Cache) int64 {
	t.Helper()
	n, err := c.bytes(context.Background())
	if err != nil {
		t.Fatalf("Bytes: %v", err)
	}
	return n
}

func TestAChunkPutIsGotBack(t *testing.T) {
	c := open(t, 1<<20)
	want := chunk(1, 300)
	put(t, c, want)

	got, err := c.Get(context.Background(), []protovcs.ChunkID{want.ID, chunk(2, 0).ID})
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if len(got) != 1 || got[0].ID != want.ID || string(got[0].Sealed) != string(want.Sealed) {
		t.Fatalf("Get returned %d chunks, want the one put", len(got))
	}
	if n := total(t, c); n != 300 {
		t.Fatalf("total = %d, want 300", n)
	}
}

// TestPuttingAChunkTwiceCountsItOnce checks that the total is the bytes held,
// not the bytes ever put, including when a single Put names a chunk twice.
func TestPuttingAChunkTwiceCountsItOnce(t *testing.T) {
	c := open(t, 1<<20)
	put(t, c, chunk(1, 300), chunk(1, 300))
	put(t, c, chunk(1, 300))
	if n := total(t, c); n != 300 {
		t.Fatalf("total = %d, want 300", n)
	}
}

// TestPassingTheLimitEvictsTheLeastRecentlyUsed fills a cache past its limit
// and checks that the chunks read most recently are the ones kept.
func TestPassingTheLimitEvictsTheLeastRecentlyUsed(t *testing.T) {
	c := open(t, 1000)
	clock := time.Date(2026, 11, 4, 9, 0, 0, 0, time.UTC)
	c.now = func() time.Time { return clock }

	// Ten chunks of 100 bytes fill the limit exactly, one minute apart.
	for i := 0; i < 10; i++ {
		put(t, c, chunk(i, 100))
		clock = clock.Add(time.Minute)
	}
	// Reading chunk 0 two hours on makes it the most recently used.
	clock = clock.Add(2 * time.Hour)
	if got := held(t, c, 1); !got[0] {
		t.Fatalf("chunk 0 is missing before anything was evicted")
	}

	clock = clock.Add(time.Minute)
	put(t, c, chunk(10, 100))

	got := held(t, c, 11)
	// 1100 bytes is over the limit, so eviction goes down to 900: chunks 1 and
	// 2, the two used least recently, go.
	for i, want := range map[int]bool{0: true, 1: false, 2: false, 3: true, 10: true} {
		if got[i] != want {
			t.Errorf("chunk %d held = %v, want %v", i, got[i], want)
		}
	}
	if n := total(t, c); n != 900 {
		t.Fatalf("total = %d after eviction, want 900", n)
	}
}

func TestDropRemovesAChunk(t *testing.T) {
	c := open(t, 1<<20)
	put(t, c, chunk(0, 100), chunk(1, 100))
	if err := c.Drop(context.Background(), []protovcs.ChunkID{chunk(0, 0).ID, chunk(0, 0).ID}); err != nil {
		t.Fatalf("Drop: %v", err)
	}
	if got := held(t, c, 2); got[0] || !got[1] {
		t.Fatalf("held %v after dropping chunk 0", got)
	}
	if n := total(t, c); n != 100 {
		t.Fatalf("total = %d, want 100", n)
	}
}

// TestDeletingTheDatabaseElsewhereClosesThisConnection checks that deleting
// the database, as a sign-out does, is not held up by an open cache, and that
// the cache then starts again empty.
func TestDeletingTheDatabaseElsewhereClosesThisConnection(t *testing.T) {
	c := open(t, 1<<20)
	put(t, c, chunk(0, 100))

	// Blocks until this cache's connection closes, which it does on the
	// versionchange event the deletion fires.
	if err := deleteDatabase(context.Background(), c.name); err != nil {
		t.Fatalf("deleting the database: %v", err)
	}

	if got := held(t, c, 1); len(got) != 0 {
		t.Fatalf("the cache holds %v after its database was deleted", got)
	}
	put(t, c, chunk(1, 100))
	if got := held(t, c, 2); !got[1] {
		t.Fatalf("a chunk put after the deletion was not kept")
	}
}
