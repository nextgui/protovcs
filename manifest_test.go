package protovcs

import (
	"fmt"
	"testing"
)

// dayItems builds the item set a day-batched layout produces over `years`,
// which is the shape a real vault has: entries/YYYY/MM/DD.
func dayItems(years int) []ItemRef {
	var out []ItemRef
	for y := 2022; y < 2022+years; y++ {
		for m := 1; m <= 12; m++ {
			for d := 1; d <= 28; d++ {
				key := ItemKey(fmt.Sprintf("entries/%04d/%02d/%02d", y, m, d))
				var id ChunkID
				copy(id[:], fmt.Sprintf("%s-contents", key))
				out = append(out, ItemRef{Key: key, ID: id, Bytes: 3000, Elements: 14})
			}
		}
	}
	return out
}

// TestTheManifestIsATreeNotAList is the property that makes a manifest cheap to
// change and cheap to read part of.
//
// A flat list naming every item would have to be rewritten whole for any change
// and downloaded whole to read one item. Nesting along item key paths means a
// node's fanout is the number of distinct values at one position of a shard key,
// which is small, and the node count is a small fraction of the item count.
func TestTheManifestIsATreeNotAList(t *testing.T) {
	s := testKeyring(t)
	items := dayItems(5)

	nodes, err := BuildManifest(s, items)
	if err != nil {
		t.Fatalf("BuildManifest: %v", err)
	}

	// 1680 items over five years. A flat list would be one node holding 1680
	// entries; the tree is a root, one for the field, one per year, one per
	// month.
	if len(nodes) >= len(items)/10 {
		t.Fatalf("%d items produced %d manifest nodes, which is not a tree", len(items), len(nodes))
	}

	depth := 0
	for _, n := range nodes {
		if d := len(splitPath(n.Prefix)); d > depth {
			depth = d
		}
		// Fanout stays small at every level, which is what keeps a node inside
		// one chunk without any splitting rule.
		if got := len(n.Node.GetEntries()); got > 32 {
			t.Fatalf("node %q holds %d entries, which is more fanout than a date shard key should produce", n.Prefix, got)
		}
	}
	if depth < 3 {
		t.Fatalf("the deepest node is at depth %d, so the tree is not nesting along item keys", depth)
	}
	t.Logf("%d items → %d nodes, deepest at %d", len(items), len(nodes), depth)
}

// TestChangingOneItemRewritesOnlyItsPath is the property the tree exists for.
//
// Every node not on the path keeps its chunk id, so it is neither re-uploaded
// nor re-fetched, and two versions share almost their whole manifest.
func TestChangingOneItemRewritesOnlyItsPath(t *testing.T) {
	s := testKeyring(t)
	items := dayItems(5)

	before, err := BuildManifest(s, items)
	if err != nil {
		t.Fatalf("BuildManifest: %v", err)
	}

	// One item's contents change, which changes its chunk id and nothing else.
	changed := make([]ItemRef, len(items))
	copy(changed, items)
	for i := range changed {
		if changed[i].Key == "entries/2024/06/15" {
			copy(changed[i].ID[:], "different-contents")
		}
	}

	after, err := BuildManifest(s, changed)
	if err != nil {
		t.Fatalf("BuildManifest: %v", err)
	}
	if len(before) != len(after) {
		t.Fatalf("the node count changed from %d to %d for the same key set", len(before), len(after))
	}

	byPrefix := map[string]ChunkID{}
	for _, n := range before {
		byPrefix[n.Prefix] = n.ID
	}

	var rewritten []string
	for _, n := range after {
		if byPrefix[n.Prefix] != n.ID {
			rewritten = append(rewritten, n.Prefix)
		}
	}

	// The root, the field, the year and the month. Nothing else.
	want := map[string]bool{
		"":                true,
		"entries":         true,
		"entries/2024":    true,
		"entries/2024/06": true,
	}
	if len(rewritten) != len(want) {
		t.Fatalf("changing one item rewrote %d nodes (%v), want %d", len(rewritten), rewritten, len(want))
	}
	for _, p := range rewritten {
		if !want[p] {
			t.Fatalf("changing one item rewrote node %q, which is not on its path", p)
		}
	}
	t.Logf("one item changed → %d of %d nodes rewritten", len(rewritten), len(after))
}

// TestAnUnchangedSubtreeKeepsItsName is what a reader relies on. A subtree whose
// id matches is equal wholesale, so a diff never descends into it and a fetch
// never asks for it.
func TestAnUnchangedSubtreeKeepsItsName(t *testing.T) {
	s := testKeyring(t)

	before, err := BuildManifest(s, dayItems(5))
	if err != nil {
		t.Fatalf("BuildManifest: %v", err)
	}

	// A sixth year is appended, which is what a store that keeps growing does.
	after, err := BuildManifest(s, dayItems(6))
	if err != nil {
		t.Fatalf("BuildManifest: %v", err)
	}

	byPrefix := map[string]ChunkID{}
	for _, n := range after {
		byPrefix[n.Prefix] = n.ID
	}

	for _, n := range before {
		if n.Prefix == "" || n.Prefix == "entries" {
			// The root and the field gained a child, so both legitimately move.
			continue
		}
		if byPrefix[n.Prefix] != n.ID {
			t.Fatalf("adding a later year changed node %q, which holds none of it", n.Prefix)
		}
	}
}

// TestTheTreeDoesNotDependOnInsertionOrder is what makes two clients agree.
func TestTheTreeDoesNotDependOnInsertionOrder(t *testing.T) {
	s := testKeyring(t)
	items := dayItems(3)

	forward, err := BuildManifest(s, items)
	if err != nil {
		t.Fatalf("BuildManifest: %v", err)
	}

	reversed := make([]ItemRef, len(items))
	for i, it := range items {
		reversed[len(items)-1-i] = it
	}
	backward, err := BuildManifest(s, reversed)
	if err != nil {
		t.Fatalf("BuildManifest: %v", err)
	}

	if len(forward) != len(backward) {
		t.Fatalf("two orders produced %d and %d nodes", len(forward), len(backward))
	}
	if forward[len(forward)-1].ID != backward[len(backward)-1].ID {
		t.Fatal("two clients holding the same items built different trees, so their chunk names would never agree")
	}
}

func TestBuildManifestRefusesADuplicateItem(t *testing.T) {
	s := testKeyring(t)
	items := dayItems(1)
	items = append(items, items[0])

	if _, err := BuildManifest(s, items); err == nil {
		t.Fatal("two items with the same key were accepted")
	}
}

func splitPath(prefix string) []string {
	if prefix == "" {
		return nil
	}
	var out []string
	start := 0
	for i := 0; i < len(prefix); i++ {
		if prefix[i] == '/' {
			out = append(out, prefix[start:i])
			start = i + 1
		}
	}
	return append(out, prefix[start:])
}
