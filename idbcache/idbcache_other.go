//go:build !(js && wasm)

package idbcache

import (
	"context"
	"errors"
	"sync"
	"time"

	protovcs "github.com/nextgui/protovcs"
)

// Cache is the browser's chunk cache. Outside a browser there is none, and
// [Open] says so; the type exists so callers and tests compile everywhere.
type Cache struct {
	name     string
	capBytes int64
	now      func() time.Time
	version  int
	writes   sync.WaitGroup
}

var errNoBrowser = errors.New("idbcache: IndexedDB exists only in a js/wasm build")

// Open fails outside a browser.
func Open(context.Context, string, int64) (*Cache, error) { return nil, errNoBrowser }

func (c *Cache) Get(context.Context, []protovcs.ChunkID) ([]protovcs.SealedChunk, error) {
	return nil, errNoBrowser
}

func (c *Cache) Put(context.Context, []protovcs.SealedChunk) error { return errNoBrowser }

func (c *Cache) Drop(context.Context, []protovcs.ChunkID) error { return errNoBrowser }

func (c *Cache) bytes(context.Context) (int64, error) { return 0, errNoBrowser }

func (c *Cache) close() {}

func deleteDatabase(context.Context, string) error { return errNoBrowser }
