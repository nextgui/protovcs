package protovcs

import (
	"testing"

	pb "github.com/nextgui/protovcs/proto/protovcs/v1"
)

// readerOver returns a reader over nodes, as BuildManifest or UpdateManifest
// returned them, with the root last.
func readerOver(t *testing.T, s Sealer, nodes []ManifestChunk) *ManifestReader {
	t.Helper()
	byID := map[ChunkID]*pb.ManifestNode{}
	for _, n := range nodes {
		byID[n.ID] = n.Node
	}
	return NewManifestReader(s, nodes[len(nodes)-1].ID, func(_ string, id ChunkID) (*pb.ManifestNode, error) {
		return byID[id], nil
	})
}

// A segment that is a prefix of a sibling, followed by a character that sorts
// below "/", used to put the two entries out of order, and then neither item
// could be looked up.
func TestASegmentThatPrefixesASiblingIsFound(t *testing.T) {
	s := PlainSealer()
	items := []ItemRef{
		{Key: "x/a/1", ID: ChunkID{1}},
		{Key: "x/a-b/1", ID: ChunkID{2}},
		{Key: "x/a.c/2", ID: ChunkID{3}},
		{Key: "x/a%25/3", ID: ChunkID{4}},
	}
	nodes, err := BuildManifest(s, items)
	if err != nil {
		t.Fatalf("BuildManifest: %v", err)
	}
	r := readerOver(t, s, nodes)
	for _, it := range items {
		got, err := r.Lookup(it.Key)
		if err != nil || got.ID != it.ID {
			t.Errorf("Lookup(%s) = %v, %v; want %v", it.Key, got.ID, err, it.ID)
		}
	}
	for _, n := range nodes {
		if n.Prefix != "x" {
			continue
		}
		var segs []string
		for _, e := range n.Node.GetEntries() {
			segs = append(segs, e.GetSegment())
		}
		for i := 1; i < len(segs); i++ {
			if segs[i-1] >= segs[i] {
				t.Errorf("node x is not in segment order: %v", segs)
			}
		}
	}
}

// A version written before the fix has a node out of segment order. Its items
// are still found, and the next update writes the node in order.
func TestAManifestWrittenOutOfOrderIsStillRead(t *testing.T) {
	s := PlainSealer()
	nodes, err := BuildManifest(s, []ItemRef{
		{Key: "x/a/1", ID: ChunkID{1}},
		{Key: "x/a-b/1", ID: ChunkID{2}},
	})
	if err != nil {
		t.Fatalf("BuildManifest: %v", err)
	}
	// Put node x back in the order the old sort wrote: "a-b" before "a".
	for _, n := range nodes {
		if n.Prefix == "x" {
			e := n.Node.Entries
			e[0], e[1] = e[1], e[0]
		}
	}
	r := readerOver(t, s, nodes)
	for key, want := range map[ItemKey]ChunkID{"x/a/1": {1}, "x/a-b/1": {2}} {
		if got, err := r.Lookup(key); err != nil || got.ID != want {
			t.Errorf("Lookup(%s) on an out-of-order node = %v, %v", key, got.ID, err)
		}
	}

	byID := map[ChunkID]*pb.ManifestNode{}
	for _, n := range nodes {
		byID[n.ID] = n.Node
	}
	updated, _, err := UpdateManifest(s, nodes[len(nodes)-1].ID, map[ItemKey]*ItemRef{
		"x/a/2": {Key: "x/a/2", ID: ChunkID{3}},
	}, func(_ string, id ChunkID) (*pb.ManifestNode, error) { return byID[id], nil })
	if err != nil {
		t.Fatalf("UpdateManifest: %v", err)
	}
	for _, n := range updated {
		if n.Prefix == "x" {
			e := n.Node.GetEntries()
			if len(e) != 2 || e[0].GetSegment() != "a" || e[1].GetSegment() != "a-b" {
				t.Fatalf("the updated node x holds %v, want a then a-b", e)
			}
		}
	}
}
