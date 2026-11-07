// Package idbcache keeps protovcs chunks in the browser's IndexedDB, so a page
// load does not download again what an earlier one did.
//
// It implements [protovcs.ChunkCache] for js/wasm builds only. What it stores
// is sealed exactly as the server stores it. [Cache.HeadMarks] keeps the
// newest head seen on each branch in the same database, so a reload still
// refuses a rolled-back head and deleting the database clears both.
//
// # Layout
//
// One database, [DatabaseName], with three object stores:
//
//   - "chunks", keyed by the chunk id as hex. Each value is
//     {s: sealed bytes, n: their length, u: last used, in Unix milliseconds},
//     with an index "used" on u.
//   - "meta", holding "total": the bytes of sealed chunks held. Every write
//     that adds or removes a chunk updates it in the same transaction.
//   - "heads", keyed by scope, a NUL byte, and branch name. Each value is the
//     hex id of the newest commit seen on that branch. Added in version 2.
//
// A page clears it, on sign-out for example, by deleting the database by
// name.
package idbcache

import (
	"time"

	protovcs "github.com/nextgui/protovcs"
)

// Both builds of Cache must satisfy the interface. This file has no build
// constraint, so the import here also keeps the library's deps out of a
// per-platform select() when gazelle regenerates BUILD.bazel.
var (
	_ protovcs.ChunkCache = (*Cache)(nil)
	_ protovcs.HeadMarks  = (*headMarks)(nil)
)

// DatabaseName is the IndexedDB database the cache lives in.
const DatabaseName = "protovcs-chunks"

// DefaultCapBytes is how many bytes of sealed chunks a browser keeps before
// the least recently used are evicted.
const DefaultCapBytes = 100 << 20

const (
	schemaVersion = 2
	chunkStore    = "chunks"
	metaStore     = "meta"
	headStore     = "heads"
	usedIndex     = "used"
	totalKey      = "total"

	// touchAfter is how stale a chunk's last-used time may be before a read
	// moves it to now.
	touchAfter = time.Hour

	// readTimeout bounds how long a read waits on the cache before treating
	// it as a miss. IndexedDB can stall, for example while a deletion waits on
	// another connection, and a read must never wait on a cache.
	readTimeout = 2 * time.Second

	// writeTimeout bounds a background write. Nobody waits on one, so it only
	// stops a stalled write holding its goroutine for good.
	writeTimeout = 30 * time.Second
)
