//go:build js && wasm

package idbcache

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"syscall/js"
	"time"

	protovcs "github.com/nextgui/protovcs"
)

// Cache is the browser's chunk cache, shared by every store the page opens.
//
// One cache serves every store. A chunk's name is a keyed hash of its contents
// under the store's key, so two stores share a name only when they share a key
// and the contents, and then either copy opens for both.
type Cache struct {
	name     string
	capBytes int64
	now      func() time.Time
	// The schema version opened. Tests lower it to make an older database.
	version int

	mu sync.Mutex
	// The open database, or undefined once another connection asked to
	// delete or upgrade it, so a deletion in another tab never waits on this
	// one. See connect.
	db js.Value

	// Writes Put started and has not finished. Tests wait on it.
	writes sync.WaitGroup
}

// Open returns a cache over the database called name, with a limit of
// capBytes of sealed chunks.
//
// It does not touch the database: the connection is made on first use. A
// js/wasm module's main must install itself before it first blocks, and
// waiting here for IndexedDB would block it. It fails only when the browser has
// no IndexedDB, as some private windows do, and a page then runs without a
// cache.
func Open(_ context.Context, name string, capBytes int64) (*Cache, error) {
	if js.Global().Get("indexedDB").Type() != js.TypeObject {
		return nil, errors.New("idbcache: this browser has no IndexedDB")
	}
	return &Cache{name: name, capBytes: capBytes, now: time.Now, version: schemaVersion, db: js.Undefined()}, nil
}

// connect returns the open database, opening it when it is not.
//
// A connection closes when another one deletes the database, as a departure
// from a vault does. The next use opens it again, empty, so the vaults still
// open keep a cache.
func (c *Cache) connect(ctx context.Context) (js.Value, error) {
	c.mu.Lock()
	db := c.db
	c.mu.Unlock()
	if db.Type() == js.TypeObject {
		return db, nil
	}

	factory := js.Global().Get("indexedDB")
	if factory.Type() != js.TypeObject {
		return js.Undefined(), errors.New("idbcache: this browser has no IndexedDB")
	}
	var opened js.Value
	err := run(ctx, "opening "+c.name, func(done func(error)) []js.Func {
		req := factory.Call("open", c.name, c.version)
		upgrade := js.FuncOf(func(_ js.Value, args []js.Value) any {
			db := req.Get("result")
			from := args[0].Get("oldVersion").Int()
			if from < 1 {
				chunks := db.Call("createObjectStore", chunkStore)
				chunks.Call("createIndex", usedIndex, "u")
				db.Call("createObjectStore", metaStore)
			}
			if from < 2 && c.version >= 2 {
				db.Call("createObjectStore", headStore)
			}
			return nil
		})
		success := js.FuncOf(func(js.Value, []js.Value) any {
			opened = req.Get("result")
			done(nil)
			return nil
		})
		failure := js.FuncOf(func(js.Value, []js.Value) any {
			done(domError(req.Get("error")))
			return nil
		})
		req.Set("onupgradeneeded", upgrade)
		req.Set("onsuccess", success)
		req.Set("onerror", failure)
		return []js.Func{upgrade, success, failure}
	})
	if err != nil {
		return js.Undefined(), err
	}

	// Kept for the life of the connection, which is the life of the page or
	// until another connection deletes the database.
	var onVersionChange js.Func
	onVersionChange = js.FuncOf(func(js.Value, []js.Value) any {
		c.closeIf(opened)
		onVersionChange.Release()
		return nil
	})
	opened.Set("onversionchange", onVersionChange)

	c.mu.Lock()
	defer c.mu.Unlock()
	if c.db.Type() == js.TypeObject {
		// Another goroutine connected first. Keep its connection.
		opened.Call("close")
		return c.db, nil
	}
	c.db = opened
	return opened, nil
}

// deleteDatabase deletes the database called name, waiting until every other
// connection to it has closed.
func deleteDatabase(ctx context.Context, name string) error {
	return run(ctx, "deleting "+name, func(done func(error)) []js.Func {
		req := js.Global().Get("indexedDB").Call("deleteDatabase", name)
		success := js.FuncOf(func(js.Value, []js.Value) any { done(nil); return nil })
		failure := js.FuncOf(func(js.Value, []js.Value) any {
			done(domError(req.Get("error")))
			return nil
		})
		req.Set("onsuccess", success)
		req.Set("onerror", failure)
		return []js.Func{success, failure}
	})
}

// bytes reports how many bytes of sealed chunks the cache holds.
func (c *Cache) bytes(ctx context.Context) (int64, error) {
	var total int64
	err := c.transact(ctx, "reading the total", func(tx js.Value, done func(error)) []js.Func {
		req := tx.Call("objectStore", metaStore).Call("get", totalKey)
		got := js.FuncOf(func(js.Value, []js.Value) any {
			total = intOf(req.Get("result"))
			return nil
		})
		req.Set("onsuccess", got)
		return []js.Func{got}
	}, readonly)
	return total, err
}

// close stops using the database.
func (c *Cache) close() { c.closeIf(c.current()) }

// closeIf closes db, and forgets it when it is still the connection in use.
func (c *Cache) closeIf(db js.Value) {
	if db.Type() != js.TypeObject {
		return
	}
	db.Call("close")
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.db.Equal(db) {
		c.db = js.Undefined()
	}
}

func (c *Cache) current() js.Value {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.db
}

// key is a chunk's key in the chunks object store.
func key(id protovcs.ChunkID) js.Value { return js.ValueOf(id.String()) }

// Get returns the chunks held among ids.
//
// A chunk read here has its last-used time moved to now, so eviction takes the
// chunks nobody has read for longest. The move is skipped for a chunk used in
// the last hour, because writing on every read would make a read cost a write.
func (c *Cache) Get(ctx context.Context, ids []protovcs.ChunkID) ([]protovcs.SealedChunk, error) {
	ctx, cancel := context.WithTimeout(ctx, readTimeout)
	defer cancel()
	now := c.now().UnixMilli()
	var out []protovcs.SealedChunk
	var stale []js.Value
	err := c.transact(ctx, "reading chunks", func(tx js.Value, done func(error)) []js.Func {
		chunks := tx.Call("objectStore", chunkStore)
		var funcs []js.Func
		for _, id := range ids {
			id := id
			req := chunks.Call("get", key(id))
			got := js.FuncOf(func(js.Value, []js.Value) any {
				value := req.Get("result")
				if value.Type() != js.TypeObject {
					return nil
				}
				raw := value.Get("s")
				sealed := make([]byte, raw.Get("length").Int())
				js.CopyBytesToGo(sealed, raw)
				out = append(out, protovcs.SealedChunk{ID: id, Sealed: sealed})
				if now-int64(value.Get("u").Float()) > touchAfter.Milliseconds() {
					stale = append(stale, js.ValueOf([]any{key(id), value}))
				}
				return nil
			})
			req.Set("onsuccess", got)
			funcs = append(funcs, got)
		}
		return funcs
	}, readonly)
	if err != nil {
		return nil, err
	}
	if len(stale) > 0 {
		c.background(func() {
			ctx, cancel := context.WithTimeout(context.Background(), writeTimeout)
			defer cancel()
			_ = c.touch(ctx, stale, now)
		})
	}
	return out, nil
}

// Put keeps chunks, and returns before they are written.
//
// A chunk already held is left as it is, so the total stays the bytes actually
// held. When the total passes the limit, the chunks used least recently are
// removed until it is back under nine tenths of it, so a full cache does not
// evict on every Put.
func (c *Cache) Put(_ context.Context, chunks []protovcs.SealedChunk) error {
	now := c.now().UnixMilli()
	kept := make([]protovcs.SealedChunk, len(chunks))
	for i, ch := range chunks {
		kept[i] = protovcs.SealedChunk{ID: ch.ID, Sealed: append([]byte(nil), ch.Sealed...)}
	}
	c.background(func() {
		ctx, cancel := context.WithTimeout(context.Background(), writeTimeout)
		defer cancel()
		total, err := c.put(ctx, kept, now)
		if err == nil && total > c.capBytes {
			_ = c.evict(ctx)
		}
	})
	return nil
}

// put writes the chunks not already held, and returns the new total.
func (c *Cache) put(ctx context.Context, chunks []protovcs.SealedChunk, now int64) (int64, error) {
	var total int64
	err := c.transact(ctx, "keeping chunks", func(tx js.Value, done func(error)) []js.Func {
		store := tx.Call("objectStore", chunkStore)
		var added int64
		pending := len(chunks)
		var funcs []js.Func
		for _, ch := range chunks {
			ch := ch
			k := key(ch.ID)
			req := store.Call("count", k)
			counted := js.FuncOf(func(js.Value, []js.Value) any {
				if req.Get("result").Int() == 0 {
					raw := js.Global().Get("Uint8Array").New(len(ch.Sealed))
					js.CopyBytesToJS(raw, ch.Sealed)
					store.Call("put", js.ValueOf(map[string]any{"s": raw, "n": len(ch.Sealed), "u": now}), k)
					added += int64(len(ch.Sealed))
				}
				if pending--; pending == 0 {
					adjustTotalThen(tx, added, func(t int64) { total = t })
				}
				return nil
			})
			req.Set("onsuccess", counted)
			funcs = append(funcs, counted)
		}
		return funcs
	})
	return total, err
}

// Drop removes chunks that failed to open.
func (c *Cache) Drop(ctx context.Context, ids []protovcs.ChunkID) error {
	ctx, cancel := context.WithTimeout(ctx, readTimeout)
	defer cancel()
	return c.transact(ctx, "dropping chunks", func(tx js.Value, done func(error)) []js.Func {
		store := tx.Call("objectStore", chunkStore)
		var freed int64
		pending := len(ids)
		var funcs []js.Func
		for _, id := range ids {
			k := key(id)
			req := store.Call("get", k)
			got := js.FuncOf(func(js.Value, []js.Value) any {
				if value := req.Get("result"); value.Type() == js.TypeObject {
					freed += int64(value.Get("n").Int())
					store.Call("delete", k)
				}
				if pending--; pending == 0 {
					adjustTotal(tx, -freed)
				}
				return nil
			})
			req.Set("onsuccess", got)
			funcs = append(funcs, got)
		}
		return funcs
	})
}

// touch moves chunks' last-used time to now. Each entry is [key, value].
func (c *Cache) touch(ctx context.Context, entries []js.Value, now int64) error {
	return c.transact(ctx, "marking chunks used", func(tx js.Value, done func(error)) []js.Func {
		store := tx.Call("objectStore", chunkStore)
		for _, e := range entries {
			value := e.Index(1)
			value.Set("u", now)
			store.Call("put", value, e.Index(0))
		}
		return nil
	})
}

// evict removes the chunks used least recently until the total is at most
// nine tenths of the limit.
func (c *Cache) evict(ctx context.Context) error {
	target := c.capBytes / 10 * 9
	return c.transact(ctx, "evicting", func(tx js.Value, done func(error)) []js.Func {
		meta := tx.Call("objectStore", metaStore)
		chunks := tx.Call("objectStore", chunkStore)
		read := meta.Call("get", totalKey)
		var total int64
		var walk js.Value
		var step js.Func
		step = js.FuncOf(func(js.Value, []js.Value) any {
			cursor := walk.Get("result")
			if cursor.IsNull() || total <= target {
				meta.Call("put", total, totalKey)
				return nil
			}
			total -= int64(cursor.Get("value").Get("n").Int())
			cursor.Call("delete")
			cursor.Call("continue")
			return nil
		})
		start := js.FuncOf(func(js.Value, []js.Value) any {
			total = intOf(read.Get("result"))
			if total <= target {
				return nil
			}
			walk = chunks.Call("index", usedIndex).Call("openCursor")
			walk.Set("onsuccess", step)
			return nil
		})
		read.Set("onsuccess", start)
		return []js.Func{start, step}
	})
}

// background runs a write the caller does not wait for.
func (c *Cache) background(write func()) {
	c.writes.Add(1)
	go func() {
		defer c.writes.Done()
		write()
	}()
}

type txMode int

const (
	readwrite txMode = iota
	readonly
)

// transact runs one transaction over both object stores and waits for it to
// finish.
//
// body issues the transaction's first requests and returns the callbacks it
// made, which are released when the transaction ends. A request issued from one
// of those callbacks belongs to the same transaction, which is how a body reads
// and then writes atomically: IndexedDB commits a transaction only once no
// request is left pending, so the body's work must be chained through callbacks
// rather than done after this returns. done fails the transaction early.
func (c *Cache) transact(ctx context.Context, operation string,
	body func(tx js.Value, done func(error)) []js.Func, mode ...txMode) error {
	return c.transactOn(ctx, []any{chunkStore, metaStore}, operation, body, mode...)
}

// transactOn is transact over the named object stores.
func (c *Cache) transactOn(ctx context.Context, stores []any, operation string,
	body func(tx js.Value, done func(error)) []js.Func, mode ...txMode) error {

	db, err := c.connect(ctx)
	if err != nil {
		return err
	}
	m := "readwrite"
	if len(mode) > 0 && mode[0] == readonly {
		m = "readonly"
	}
	return run(ctx, operation, func(done func(error)) (funcs []js.Func) {
		// A connection closed since it was read above throws here rather than
		// failing a request.
		defer func() {
			if r := recover(); r != nil {
				done(fmt.Errorf("%v", r))
			}
		}()
		tx := db.Call("transaction", js.ValueOf(stores), m)
		complete := js.FuncOf(func(js.Value, []js.Value) any { done(nil); return nil })
		failed := js.FuncOf(func(js.Value, []js.Value) any {
			done(domError(tx.Get("error")))
			return nil
		})
		tx.Set("oncomplete", complete)
		tx.Set("onerror", failed)
		tx.Set("onabort", failed)
		funcs = append(funcs, complete, failed)
		abort := func(err error) {
			tx.Call("abort")
			done(err)
		}
		return append(funcs, body(tx, abort)...)
	})
}

// run starts an operation and blocks until it calls done or ctx ends.
//
// A caller giving up does not stop the operation. The browser finishes it, and
// what it wrote stays written.
//
// Blocking a goroutine on a channel is safe under js/wasm: the runtime returns
// to the JavaScript event loop while every goroutine is blocked, so the
// callbacks that call done get to run.
func run(ctx context.Context, operation string, start func(done func(error)) []js.Func) error {
	result := make(chan error, 1)
	var once sync.Once
	done := func(err error) { once.Do(func() { result <- err }) }
	funcs := start(done)
	release := func() {
		for _, f := range funcs {
			f.Release()
		}
	}
	select {
	case err := <-result:
		release()
		if err != nil {
			return fmt.Errorf("idbcache: %s: %w", operation, err)
		}
		return nil
	case <-ctx.Done():
		// The browser may still call these, and calling a released function
		// panics, so they are released only once it has finished.
		go func() {
			<-result
			release()
		}()
		return fmt.Errorf("idbcache: %s: %w", operation, ctx.Err())
	}
}

// adjustTotal adds delta to the stored total, inside tx.
func adjustTotal(tx js.Value, delta int64) {
	adjustTotalThen(tx, delta, func(int64) {})
}

// adjustTotalThen adds delta to the stored total inside tx and hands the new
// total to then.
func adjustTotalThen(tx js.Value, delta int64, then func(int64)) {
	meta := tx.Call("objectStore", metaStore)
	req := meta.Call("get", totalKey)
	var got js.Func
	got = js.FuncOf(func(js.Value, []js.Value) any {
		defer got.Release()
		total := max(intOf(req.Get("result"))+delta, 0)
		meta.Call("put", total, totalKey)
		then(total)
		return nil
	})
	req.Set("onsuccess", got)
}

// intOf reads a stored number, which is undefined before the first write.
func intOf(v js.Value) int64 {
	if v.Type() != js.TypeNumber {
		return 0
	}
	return int64(v.Float())
}

// domError turns a DOMException into an error.
func domError(v js.Value) error {
	if v.Type() == js.TypeObject && v.Get("message").Type() == js.TypeString {
		return errors.New(v.Get("name").String() + ": " + v.Get("message").String())
	}
	return errors.New("the browser gave no reason")
}
