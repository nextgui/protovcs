//go:build js && wasm

package idbcache

import (
	"context"
	"strings"
	"syscall/js"

	protovcs "github.com/nextgui/protovcs"
)

// Load returns the commit recorded for each of the store's branches.
func (h *headMarks) Load(ctx context.Context) (map[string]protovcs.CommitID, error) {
	ctx, cancel := context.WithTimeout(ctx, markTimeout)
	defer cancel()
	prefix := h.scope + "\x00"
	out := map[string]protovcs.CommitID{}
	err := h.cache.transactOn(ctx, []any{headStore}, "loading head marks",
		func(tx js.Value, done func(error)) []js.Func {
			// Every key that starts with prefix sorts below scope+"\x01".
			bounds := js.Global().Get("IDBKeyRange").Call("bound", prefix, h.scope+"\x01", false, true)
			walk := tx.Call("objectStore", headStore).Call("openCursor", bounds)
			step := js.FuncOf(func(js.Value, []js.Value) any {
				cursor := walk.Get("result")
				if cursor.IsNull() {
					return nil
				}
				if value := cursor.Get("value"); value.Type() == js.TypeString {
					if id := parseMark(value.String()); !id.IsZero() {
						out[strings.TrimPrefix(cursor.Get("key").String(), prefix)] = id
					}
				}
				cursor.Call("continue")
				return nil
			})
			walk.Set("onsuccess", step)
			return []js.Func{step}
		}, readonly)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// CompareAndSwap records next for branch when the record holds old. The read
// and the write share one transaction, so two tabs cannot both swap from the
// same record.
func (h *headMarks) CompareAndSwap(ctx context.Context, branch string, old, next protovcs.CommitID) (protovcs.CommitID, error) {
	ctx, cancel := context.WithTimeout(ctx, markTimeout)
	defer cancel()
	var now protovcs.CommitID
	err := h.cache.transactOn(ctx, []any{headStore}, "recording a head mark",
		func(tx js.Value, done func(error)) []js.Func {
			store := tx.Call("objectStore", headStore)
			k := h.key(branch)
			req := store.Call("get", k)
			got := js.FuncOf(func(js.Value, []js.Value) any {
				var held protovcs.CommitID
				if value := req.Get("result"); value.Type() == js.TypeString {
					held = parseMark(value.String())
				}
				if held != old {
					now = held
					return nil
				}
				store.Call("put", next.String(), k)
				now = next
				return nil
			})
			req.Set("onsuccess", got)
			return []js.Func{got}
		})
	if err != nil {
		return protovcs.CommitID{}, err
	}
	return now, nil
}
