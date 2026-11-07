package protovcs

import (
	"fmt"
	"sort"
	"strings"

	"google.golang.org/protobuf/proto"

	pb "github.com/nextgui/protovcs/proto/protovcs/v1"
)

// ItemRef is one item as the manifest records it: where it is and how big.
type ItemRef struct {
	Key      ItemKey
	ID       ChunkID
	Bytes    uint64
	Elements uint32
}

// ManifestChunk is one node of the tree, ready to store.
type ManifestChunk struct {
	// The prefix of item keys this node covers. The root node covers "".
	Prefix    string
	Node      *pb.ManifestNode
	Canonical []byte
	ID        ChunkID
}

// maxNodeEntries is the point at which a manifest node is refused.
//
// The tree nests along item key paths, so a node's fanout is the number of
// distinct values at one position of a shard key. That is small for dates and
// ids and large only for a hash with very many buckets. Splitting an oversized
// node further would need boundaries that do not depend on insertion order, and
// nothing has needed it, so this is a refusal with an explanation rather than a
// structure nobody has exercised.
const maxNodeEntries = 65536

// BuildManifest writes the tree naming every item in a version, deepest nodes
// first, and returns them with the root last.
//
// The tree's shape is a function of the item keys alone: keys have path
// structure and the tree nests along it. That is what makes two clients holding
// the same items build the identical tree, which is what makes their chunk
// names agree and unchanged subtrees shared between versions. A tree whose
// shape depended on the order items were added would produce different names
// for the same data and nothing would ever be reused.
func BuildManifest(s Sealer, items []ItemRef) ([]ManifestChunk, error) {
	sorted := make([]ItemRef, len(items))
	copy(sorted, items)
	sort.Slice(sorted, func(i, j int) bool { return keyLess(sorted[i].Key, sorted[j].Key) })

	for i := 1; i < len(sorted); i++ {
		if sorted[i].Key == sorted[i-1].Key {
			return nil, fmt.Errorf("protovcs: item %q appears twice", sorted[i].Key)
		}
	}

	var out []ManifestChunk
	_, err := buildNode(s, sorted, "", 0, &out)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// buildNode builds the node covering one prefix and everything below it.
//
// Children are emitted before their parent, so a caller storing the result in
// order never writes a node naming a chunk that is not there yet. That ordering
// is the same discipline the write path uses at the commit level, and for the
// same reason: a dangling reference is a corrupt store, while an unreferenced
// chunk is only garbage.
func buildNode(s Sealer, items []ItemRef, prefix string, depth int, out *[]ManifestChunk) (ManifestChunk, error) {
	node := &pb.ManifestNode{}

	for i := 0; i < len(items); {
		segment := segmentAt(items[i].Key, depth)

		j := i
		for j < len(items) && segmentAt(items[j].Key, depth) == segment {
			j++
		}
		group := items[i:j]
		i = j

		// One item whose key ends here: a leaf entry naming the item itself.
		if len(group) == 1 && depthOf(group[0].Key) == depth+1 {
			node.Entries = append(node.Entries, &pb.ManifestNode_Entry{
				Segment:      segment,
				Target:       &pb.ManifestNode_Entry_Item{Item: group[0].ID.Bytes()},
				TotalBytes:   group[0].Bytes,
				ElementCount: group[0].Elements,
			})
			continue
		}

		// An item key that is also the prefix of another cannot be represented:
		// one entry names one thing. No layout produces this, because every
		// element of a split reaches the same depth, so it is reported rather
		// than worked around.
		for _, it := range group {
			if depthOf(it.Key) == depth+1 {
				return ManifestChunk{}, fmt.Errorf(
					"protovcs: item %q is also a directory holding other items", it.Key)
			}
		}

		child, err := buildNode(s, group, join(prefix, segment), depth+1, out)
		if err != nil {
			return ManifestChunk{}, err
		}
		var total uint64
		var elements uint32
		for _, it := range group {
			total += it.Bytes
			elements += it.Elements
		}
		node.Entries = append(node.Entries, &pb.ManifestNode_Entry{
			Segment:      segment,
			Target:       &pb.ManifestNode_Entry_Child{Child: child.ID.Bytes()},
			TotalBytes:   total,
			ElementCount: elements,
		})
	}

	if len(node.Entries) > maxNodeEntries {
		return ManifestChunk{}, fmt.Errorf(
			"protovcs: %d items share the prefix %q, which is more than one manifest node holds; "+
				"add a shard key component so they divide further",
			len(node.Entries), prefix)
	}

	// Entries are already in order, because the items were sorted and segments
	// were taken in one pass. Sorting is what makes a range of item keys a
	// contiguous walk rather than a lookup for each one.
	canonical, err := CanonicalBytes(node)
	if err != nil {
		return ManifestChunk{}, fmt.Errorf("protovcs: encoding the manifest node at %q: %w", prefix, err)
	}

	chunk := ManifestChunk{
		Prefix:    prefix,
		Node:      node,
		Canonical: canonical,
		ID:        s.ChunkID(RoleManifest, prefix, canonical),
	}
	*out = append(*out, chunk)
	return chunk, nil
}

// ManifestReader resolves item keys against a stored tree.
//
// It fetches nodes as it needs them, so resolving one key costs the depth of
// the tree rather than the whole manifest, and resolving a range costs the
// nodes that range actually touches.
type ManifestReader struct {
	sealer Sealer
	root   ChunkID
	fetch  func(prefix string, id ChunkID) (*pb.ManifestNode, error)

	// prefetch, when set, is handed every node of one level of the tree that a
	// range read will open, before any of them is opened, so a caller can
	// fetch the level in one request.
	prefetch func([]ChunkID) error
}

// WithPrefetch returns the reader with a function that fetches many nodes at
// once. A range read then costs one request per level of the tree, not one per
// node it reads.
func (r *ManifestReader) WithPrefetch(prefetch func([]ChunkID) error) *ManifestReader {
	r.prefetch = prefetch
	return r
}

// NewManifestReader returns a reader over a stored tree. fetch opens one node,
// and is where a caller puts its cache.
func NewManifestReader(s Sealer, root ChunkID, fetch func(prefix string, id ChunkID) (*pb.ManifestNode, error)) *ManifestReader {
	return &ManifestReader{sealer: s, root: root, fetch: fetch}
}

// Lookup returns the chunk holding one item, or ErrNoSuchChunk.
func (r *ManifestReader) Lookup(key ItemKey) (ItemRef, error) {
	node, err := r.fetch("", r.root)
	if err != nil {
		return ItemRef{}, err
	}

	prefix := ""
	for depth := 0; ; depth++ {
		segment := segmentAt(key, depth)
		entry := findEntry(node, segment)
		if entry == nil {
			return ItemRef{}, fmt.Errorf("%w: no item %q", ErrNoSuchChunk, key)
		}

		if item := entry.GetItem(); item != nil {
			if depthOf(key) != depth+1 {
				return ItemRef{}, fmt.Errorf("%w: no item %q", ErrNoSuchChunk, key)
			}
			id, err := ChunkIDFromBytes(item)
			if err != nil {
				return ItemRef{}, err
			}
			return ItemRef{Key: key, ID: id, Bytes: entry.GetTotalBytes(), Elements: entry.GetElementCount()}, nil
		}

		id, err := ChunkIDFromBytes(entry.GetChild())
		if err != nil {
			return ItemRef{}, err
		}
		prefix = join(prefix, segment)
		if node, err = r.fetch(prefix, id); err != nil {
			return ItemRef{}, err
		}
	}
}

// Range returns every item whose key falls between from and to inclusive.
//
// Only the subtrees the range touches are fetched, so asking for three months
// of a five-year store reads three months of nodes. A subtree entirely outside
// the range is skipped without being opened, which is what makes a date range
// cheap rather than merely correct.
func (r *ManifestReader) Range(from, to ItemKey) ([]ItemRef, error) {
	// Descend only when the subtree can contain part of the range. Its keys all
	// begin with this prefix, so the subtree is outside the range when the
	// prefix sorts after `to`, or when everything under it sorts before `from`.
	descend := func(key ItemKey) bool {
		return string(key) <= string(to) && !prefixEndsBefore(string(key), string(from))
	}
	if err := r.prefetchLevels(descend); err != nil {
		return nil, err
	}

	var out []ItemRef
	err := r.walk("", r.root, 0, func(prefix string, entry *pb.ManifestNode_Entry, key ItemKey) (bool, error) {
		if entry.GetItem() == nil {
			return descend(key), nil
		}
		if key < from || key > to {
			return false, nil
		}
		id, err := ChunkIDFromBytes(entry.GetItem())
		if err != nil {
			return false, err
		}
		out = append(out, ItemRef{Key: key, ID: id, Bytes: entry.GetTotalBytes(), Elements: entry.GetElementCount()})
		return false, nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// prefetchLevels hands prefetch every node the walk will open, one level of the
// tree at a time, so the walk that follows reads them all from the caller's
// cache.
//
// The walk itself goes depth first, because it reports items in key order. Done
// that way, fetching the children of each node together still costs a request
// per node with children, and a deep tree with many small nodes spends most of
// a read waiting on those requests one after another.
func (r *ManifestReader) prefetchLevels(descend func(ItemKey) bool) error {
	if r.prefetch == nil {
		return nil
	}
	type pending struct {
		prefix string
		id     ChunkID
	}
	level := []pending{{prefix: "", id: r.root}}
	for len(level) > 0 {
		ids := make([]ChunkID, len(level))
		for i, n := range level {
			ids[i] = n.id
		}
		if err := r.prefetch(ids); err != nil {
			return err
		}
		var next []pending
		for _, n := range level {
			node, err := r.fetch(n.prefix, n.id)
			if err != nil {
				return err
			}
			for _, entry := range node.GetEntries() {
				if entry.GetItem() != nil {
					continue
				}
				key := ItemKey(join(n.prefix, entry.GetSegment()))
				if !descend(key) {
					continue
				}
				child, err := ChunkIDFromBytes(entry.GetChild())
				if err != nil {
					return err
				}
				next = append(next, pending{prefix: string(key), id: child})
			}
		}
		level = next
	}
	return nil
}

// All returns every item in the version, in key order.
func (r *ManifestReader) All() ([]ItemRef, error) {
	return r.Range("", keyMax)
}

func (r *ManifestReader) walk(prefix string, id ChunkID, depth int,
	visit func(prefix string, entry *pb.ManifestNode_Entry, key ItemKey) (bool, error)) error {

	node, err := r.fetch(prefix, id)
	if err != nil {
		return err
	}

	// Decide which children to descend into first. visit is called on a child
	// entry only to decide, so calling it ahead of the leaves beside it changes
	// nothing it reports.
	entries := node.GetEntries()
	children := make([]ChunkID, len(entries))
	descend := make([]bool, len(entries))
	for i, entry := range entries {
		if entry.GetItem() != nil {
			continue
		}
		key := ItemKey(join(prefix, entry.GetSegment()))
		if descend[i], err = visit(prefix, entry, key); err != nil {
			return err
		}
		if !descend[i] {
			continue
		}
		if children[i], err = ChunkIDFromBytes(entry.GetChild()); err != nil {
			return err
		}
	}

	for i, entry := range entries {
		key := ItemKey(join(prefix, entry.GetSegment()))
		if entry.GetItem() != nil {
			if _, err := visit(prefix, entry, key); err != nil {
				return err
			}
			continue
		}
		if !descend[i] {
			continue
		}
		if err := r.walk(string(key), children[i], depth+1, visit); err != nil {
			return err
		}
	}
	return nil
}

func findEntry(node *pb.ManifestNode, segment string) *pb.ManifestNode_Entry {
	entries := node.GetEntries()
	i := sort.Search(len(entries), func(i int) bool { return entries[i].GetSegment() >= segment })
	if i < len(entries) && entries[i].GetSegment() == segment {
		return entries[i]
	}
	// Versions written before keyLess sorted a node by whole item key, which
	// puts a segment such as "a-b" before "a" when both have children. The
	// binary search misses there, so a node in that order is searched in full.
	for _, e := range entries {
		if e.GetSegment() == segment {
			return e
		}
	}
	return nil
}

// keyLess orders item keys one segment at a time, which is the order a node's
// entries are kept in. Comparing whole keys is not the same: "/" sorts above
// "-", ".", "%" and others, so "x/a-b/1" would come before "x/a/1" while the
// segment "a" comes before "a-b".
func keyLess(a, b ItemKey) bool {
	sa, sb := string(a), string(b)
	for i := 0; i < len(sa) && i < len(sb); i++ {
		ca, cb := sa[i], sb[i]
		if ca == cb {
			continue
		}
		if ca == '/' {
			return true
		}
		if cb == '/' {
			return false
		}
		return ca < cb
	}
	return len(sa) < len(sb)
}

// sortEntries puts a node's entries in segment order. A node from a version
// written before keyLess may not be, and an update keeps it sorted from here.
func sortEntries(node *pb.ManifestNode) {
	sort.SliceStable(node.Entries, func(i, j int) bool {
		return node.Entries[i].GetSegment() < node.Entries[j].GetSegment()
	})
}

// segmentAt returns one segment of an item key. The root item's key is empty
// and has exactly one segment, which is also empty.
func segmentAt(key ItemKey, depth int) string {
	parts := strings.Split(string(key), "/")
	if depth >= len(parts) {
		return ""
	}
	return parts[depth]
}

func depthOf(key ItemKey) int {
	return strings.Count(string(key), "/") + 1
}

func join(prefix, segment string) string {
	if prefix == "" {
		return segment
	}
	return prefix + "/" + segment
}

// prefixEndsBefore reports whether every key beginning with prefix sorts before
// from.
func prefixEndsBefore(prefix, from string) bool {
	if len(prefix) >= len(from) {
		return prefix < from
	}
	// The largest key under this prefix begins with it, so if the prefix is
	// already greater than or equal to the corresponding part of `from`, the
	// subtree can still contain keys at or after it.
	return prefix < from[:len(prefix)]
}

// AddedSince returns the chunks a new manifest holds that an old one did not.
//
// Descends both trees together and stops wherever a subtree's name matches,
// because a matching name means the whole subtree is identical — that is what
// content addressing buys and it is the only reason this is affordable. The
// cost is the number of nodes that actually differ, not the size of the store.
//
// The alternative is listing every item both versions hold and subtracting,
// which is correct, obvious, and linear in the store. A commit that does that
// is indistinguishable from this one until somebody has a large vault.
//
// newNodes are the nodes just built, in hand and not fetched. fetch reads an old
// node, and is called only where the two trees disagree.
func AddedSince(newNodes []ManifestChunk, newRoot, oldRoot ChunkID,
	fetch func(prefix string, id ChunkID) (*pb.ManifestNode, error)) ([]ChunkID, error) {

	byID := make(map[ChunkID]*pb.ManifestNode, len(newNodes))
	for i := range newNodes {
		byID[newNodes[i].ID] = newNodes[i].Node
	}

	var added []ChunkID
	seen := map[ChunkID]bool{}

	var walk func(prefix string, newID, oldID ChunkID) error
	walk = func(prefix string, newID, oldID ChunkID) error {
		if newID == oldID || seen[newID] {
			return nil
		}
		seen[newID] = true
		added = append(added, newID)

		node := byID[newID]
		if node == nil {
			// Not one of the nodes just built, so nothing below it is either.
			return nil
		}

		// What the old tree had at this prefix, if it had anything. A missing
		// old node means every child below is new, which is the ordinary case
		// for a subtree that did not exist before.
		oldEntries := map[string]*pb.ManifestNode_Entry{}
		if !oldID.IsZero() {
			old, err := fetch(prefix, oldID)
			if err != nil {
				return err
			}
			for _, e := range old.GetEntries() {
				oldEntries[e.GetSegment()] = e
			}
		}

		for _, e := range node.GetEntries() {
			prev := oldEntries[e.GetSegment()]
			childPrefix := join(prefix, e.GetSegment())

			if item := e.GetItem(); item != nil {
				id, err := ChunkIDFromBytes(item)
				if err != nil {
					return err
				}
				if prev != nil && prev.GetItem() != nil {
					prevID, err := ChunkIDFromBytes(prev.GetItem())
					if err != nil {
						return err
					}
					if prevID == id {
						continue
					}
				}
				if !seen[id] {
					seen[id] = true
					added = append(added, id)
				}
				continue
			}

			childID, err := ChunkIDFromBytes(e.GetChild())
			if err != nil {
				return err
			}
			var prevChild ChunkID
			if prev != nil && prev.GetChild() != nil {
				if prevChild, err = ChunkIDFromBytes(prev.GetChild()); err != nil {
					return err
				}
			}
			if err := walk(childPrefix, childID, prevChild); err != nil {
				return err
			}
		}
		return nil
	}

	if err := walk("", newRoot, oldRoot); err != nil {
		return nil, err
	}
	return added, nil
}

// UpdateManifest rebuilds only the paths that changed.
//
// BuildManifest takes every item and constructs a tree, which is the right
// thing when every item is in hand — a full write already materialised them all.
// A caller that changed three items out of a million has not, and listing the
// other 999,997 to rebuild an identical tree is linear in the store for a
// constant amount of work.
//
// This descends from the old root along the changed keys only, fetching the
// nodes on those paths and nothing else, and rebuilds them bottom-up. Every
// other node keeps its name and is neither fetched nor rewritten.
//
// A nil ItemRef deletes.
func UpdateManifest(s Sealer, oldRoot ChunkID, changes map[ItemKey]*ItemRef,
	fetch func(prefix string, id ChunkID) (*pb.ManifestNode, error)) ([]ManifestChunk, ChunkID, error) {

	loaded := map[string]*pb.ManifestNode{}
	dirty := map[string]bool{}

	load := func(prefix string, id ChunkID) (*pb.ManifestNode, error) {
		if n, ok := loaded[prefix]; ok {
			return n, nil
		}
		if id.IsZero() {
			n := &pb.ManifestNode{}
			loaded[prefix] = n
			return n, nil
		}
		got, err := fetch(prefix, id)
		if err != nil {
			return nil, err
		}
		n := proto.Clone(got).(*pb.ManifestNode)
		sortEntries(n)
		loaded[prefix] = n
		return n, nil
	}

	if _, err := load("", oldRoot); err != nil {
		return nil, ChunkID{}, err
	}

	// Walk each changed key down from the root, materialising the nodes on its
	// path and applying the change at the leaf. Deletions run before insertions
	// so that replacing an item with a directory of items below it (or a
	// directory of items with an item at its prefix) in one change set does not
	// depend on map iteration order.
	keys := make([]ItemKey, 0, len(changes))
	for k := range changes {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool {
		di, dj := changes[keys[i]] == nil, changes[keys[j]] == nil
		if di != dj {
			return di
		}
		return keys[i] < keys[j]
	})

	for _, key := range keys {
		ref := changes[key]
		segs := strings.Split(string(key), "/")
		prefix := ""

		for i := 0; i < len(segs)-1; i++ {
			node := loaded[prefix]
			entry := findEntry(node, segs[i])
			childPrefix := join(prefix, segs[i])

			var childID ChunkID
			if entry != nil {
				if raw := entry.GetChild(); raw != nil {
					var err error
					if childID, err = ChunkIDFromBytes(raw); err != nil {
						return nil, ChunkID{}, err
					}
				} else {
					return nil, ChunkID{}, fmt.Errorf(
						"protovcs: item %q needs %q to be a directory, and it holds an item", key, childPrefix)
				}
			}
			if _, err := load(childPrefix, childID); err != nil {
				return nil, ChunkID{}, err
			}
			dirty[prefix] = true
			prefix = childPrefix
		}

		leaf := segs[len(segs)-1]
		node := loaded[prefix]
		dirty[prefix] = true

		// One entry names one thing, as in BuildManifest. Writing or deleting
		// an item where a directory is, or one this change set is creating,
		// would replace or drop every item under it.
		if entry := findEntry(node, leaf); entry != nil && entry.GetChild() != nil {
			return nil, ChunkID{}, fmt.Errorf(
				"protovcs: item %q is also a directory holding other items", key)
		}
		if child, ok := loaded[join(prefix, leaf)]; ok && key != RootItemKey && len(child.GetEntries()) > 0 {
			return nil, ChunkID{}, fmt.Errorf(
				"protovcs: item %q is also a directory holding other items", key)
		}

		if ref == nil {
			removeEntry(node, leaf)
			for cur := prefix; cur != "" && len(loaded[cur].GetEntries()) == 0; {
				parentPrefix, seg := splitLast(cur)
				parent := loaded[parentPrefix]
				if parent == nil {
					break
				}
				removeEntry(parent, seg)
				cur = parentPrefix
			}
			continue
		}
		setEntry(node, &pb.ManifestNode_Entry{
			Segment:      leaf,
			Target:       &pb.ManifestNode_Entry_Item{Item: ref.ID.Bytes()},
			TotalBytes:   ref.Bytes,
			ElementCount: ref.Elements,
		})
	}
	dirty[""] = true

	// Rebuild deepest first, so a node is named after its children are.
	prefixes := make([]string, 0, len(dirty))
	for p := range dirty {
		prefixes = append(prefixes, p)
	}
	sort.Slice(prefixes, func(i, j int) bool {
		di, dj := strings.Count(prefixes[i], "/"), strings.Count(prefixes[j], "/")
		if di != dj {
			return di > dj
		}
		return prefixes[i] > prefixes[j]
	})

	var out []ManifestChunk
	var root ChunkID
	for _, prefix := range prefixes {
		node := loaded[prefix]
		if len(node.GetEntries()) > maxNodeEntries {
			return nil, ChunkID{}, fmt.Errorf(
				"protovcs: %d entries share the prefix %q, which is more than one manifest node holds;"+
					" add a shard key component so they divide further",
				len(node.GetEntries()), prefix)
		}

		canonical, err := CanonicalBytes(node)
		if err != nil {
			return nil, ChunkID{}, fmt.Errorf("protovcs: encoding the manifest node at %q: %w", prefix, err)
		}
		id := s.ChunkID(RoleManifest, prefix, canonical)
		out = append(out, ManifestChunk{Prefix: prefix, Node: node, Canonical: canonical, ID: id})

		if prefix == "" {
			root = id
			continue
		}

		// Point the parent at the rebuilt node, or drop it when nothing is left.
		parentPrefix, segment := splitLast(prefix)
		parent := loaded[parentPrefix]
		if parent == nil {
			return nil, ChunkID{}, fmt.Errorf("protovcs: node %q was rebuilt without its parent", prefix)
		}
		if len(node.GetEntries()) == 0 {
			if e := findEntry(parent, segment); e != nil && e.GetChild() != nil {
				removeEntry(parent, segment)
			}
			continue
		}
		var total uint64
		var elements uint32
		for _, e := range node.GetEntries() {
			total += e.GetTotalBytes()
			elements += e.GetElementCount()
		}
		setEntry(parent, &pb.ManifestNode_Entry{
			Segment:      segment,
			Target:       &pb.ManifestNode_Entry_Child{Child: id.Bytes()},
			TotalBytes:   total,
			ElementCount: elements,
		})
	}
	return out, root, nil
}

// setEntry inserts or replaces an entry, keeping the node sorted by segment.
//
// Sorted is not cosmetic: a range of item keys is a contiguous walk only because
// entries are in order, and a lookup binary-searches them.
func setEntry(node *pb.ManifestNode, entry *pb.ManifestNode_Entry) {
	entries := node.GetEntries()
	i := sort.Search(len(entries), func(i int) bool { return entries[i].GetSegment() >= entry.GetSegment() })
	if i < len(entries) && entries[i].GetSegment() == entry.GetSegment() {
		entries[i] = entry
		return
	}
	entries = append(entries, nil)
	copy(entries[i+1:], entries[i:])
	entries[i] = entry
	node.Entries = entries
}

func removeEntry(node *pb.ManifestNode, segment string) {
	entries := node.GetEntries()
	i := sort.Search(len(entries), func(i int) bool { return entries[i].GetSegment() >= segment })
	if i < len(entries) && entries[i].GetSegment() == segment {
		node.Entries = append(entries[:i], entries[i+1:]...)
	}
}

func splitLast(prefix string) (parent, segment string) {
	i := strings.LastIndexByte(prefix, '/')
	if i < 0 {
		return "", prefix
	}
	return prefix[:i], prefix[i+1:]
}
