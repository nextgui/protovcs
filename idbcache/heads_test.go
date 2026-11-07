//go:build js

package idbcache

import (
	"context"
	"testing"

	protovcs "github.com/nextgui/protovcs"
)

func commit(b byte) protovcs.CommitID {
	var id protovcs.CommitID
	for i := range id {
		id[i] = b
	}
	return id
}

func load(t *testing.T, m protovcs.HeadMarks) map[string]protovcs.CommitID {
	t.Helper()
	got, err := m.Load(context.Background())
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	return got
}

func swap(t *testing.T, m protovcs.HeadMarks, branch string, old, next protovcs.CommitID) protovcs.CommitID {
	t.Helper()
	now, err := m.CompareAndSwap(context.Background(), branch, old, next)
	if err != nil {
		t.Fatalf("CompareAndSwap: %v", err)
	}
	return now
}

// TestAHeadMarkSwapsOnlyFromWhatItHolds checks the compare in CompareAndSwap,
// which is what keeps one tab from lowering a mark another tab raised.
func TestAHeadMarkSwapsOnlyFromWhatItHolds(t *testing.T) {
	m := open(t, 1<<20).HeadMarks("vault-a")

	if now := swap(t, m, "main", protovcs.CommitID{}, commit(1)); now != commit(1) {
		t.Fatalf("the first swap left %s, want %s", now, commit(1))
	}
	if now := swap(t, m, "main", protovcs.CommitID{}, commit(2)); now != commit(1) {
		t.Fatalf("a swap from no record returned %s, want the held %s", now, commit(1))
	}
	if now := swap(t, m, "main", commit(1), commit(3)); now != commit(3) {
		t.Fatalf("a swap from the held mark left %s, want %s", now, commit(3))
	}
	if got := load(t, m); len(got) != 1 || got["main"] != commit(3) {
		t.Fatalf("Load = %v, want main at %s", got, commit(3))
	}
}

// TestHeadMarksAreScopedByStore checks that two vaults with a branch of the
// same name keep separate marks.
func TestHeadMarksAreScopedByStore(t *testing.T) {
	c := open(t, 1<<20)
	a, b := c.HeadMarks("vault-a"), c.HeadMarks("vault-a2")
	swap(t, a, "main", protovcs.CommitID{}, commit(1))
	swap(t, b, "main", protovcs.CommitID{}, commit(2))

	if got := load(t, a); got["main"] != commit(1) || len(got) != 1 {
		t.Fatalf("vault-a loaded %v", got)
	}
	if got := load(t, b); got["main"] != commit(2) || len(got) != 1 {
		t.Fatalf("vault-a2 loaded %v", got)
	}
}

// TestDeletingTheDatabaseClearsHeadMarks checks that a sign-out, which deletes
// the database, clears the marks along with the chunks.
func TestDeletingTheDatabaseClearsHeadMarks(t *testing.T) {
	c := open(t, 1<<20)
	m := c.HeadMarks("vault-a")
	swap(t, m, "main", protovcs.CommitID{}, commit(1))

	if err := deleteDatabase(context.Background(), c.name); err != nil {
		t.Fatalf("deleting the database: %v", err)
	}
	if got := load(t, m); len(got) != 0 {
		t.Fatalf("Load = %v after the database was deleted, want nothing", got)
	}
}

// TestAVersionOneDatabaseGainsHeadMarks opens a database made by the cache
// before head marks existed, and checks that its chunks survive the upgrade
// and that it then keeps marks.
func TestAVersionOneDatabaseGainsHeadMarks(t *testing.T) {
	old := open(t, 1<<20)
	old.version = 1
	put(t, old, chunk(0, 100))
	old.close()

	c, err := Open(context.Background(), old.name, 1<<20)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(c.close)
	if got := held(t, c, 1); !got[0] {
		t.Fatalf("a chunk put before the upgrade is missing after it")
	}
	m := c.HeadMarks("vault-a")
	swap(t, m, "main", protovcs.CommitID{}, commit(1))
	if got := load(t, m); got["main"] != commit(1) {
		t.Fatalf("Load = %v after the upgrade", got)
	}
}
