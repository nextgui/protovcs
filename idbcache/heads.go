package idbcache

import (
	"encoding/hex"
	"time"

	protovcs "github.com/nextgui/protovcs"
)

// markTimeout bounds how long loading or recording a head mark waits on
// IndexedDB. A failed load fails the read that needed it, so this is longer
// than readTimeout.
const markTimeout = 10 * time.Second

// HeadMarks returns the [protovcs.HeadMarks] of one store, kept in this
// cache's database under scope.
//
// scope names the store. Branch names repeat across stores, so each store
// needs a scope of its own. Each record holds a commit id, which the server
// already knows. Nothing protects it against local change: whatever could
// change it runs with the page's access and could read the vault key instead.
func (c *Cache) HeadMarks(scope string) protovcs.HeadMarks {
	return &headMarks{cache: c, scope: scope}
}

// headMarks is one store's marks in the "heads" object store.
type headMarks struct {
	cache *Cache
	scope string
}

// key is branch's key in the heads object store.
func (h *headMarks) key(branch string) string { return h.scope + "\x00" + branch }

// parseMark reads a stored commit id. Anything else reads as no record.
func parseMark(s string) protovcs.CommitID {
	raw, err := hex.DecodeString(s)
	if err != nil {
		return protovcs.CommitID{}
	}
	id, err := protovcs.CommitIDFromBytes(raw)
	if err != nil {
		return protovcs.CommitID{}
	}
	return id
}
