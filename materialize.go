package protovcs

import (
	"fmt"
	"sort"
	"strings"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/dynamicpb"
)

// Item is one fragment of a message, and the unit a store holds.
//
// The fragment is a partial instance of the root message: an item holding a
// day of records is a Store with only those records set. That is what makes
// reassembly proto.Merge and nothing more.
type Item struct {
	Key      ItemKey
	Fragment proto.Message

	// Canonical bytes of the fragment, and the name they produce. Filled in by
	// the writer; empty when an item has only been built in memory.
	Canonical []byte
	ID        ChunkID
}

// Materialize decomposes a message into the items a store holds.
//
// Every element of a split field goes to the item its shard key names, and
// everything no rule mentions goes to the root item. The result obeys two
// laws, both of which are tested as properties rather than examples:
//
//	Merge(Materialize(m))            equals m
//	Materialize(a merged with b)     equals Materialize(a) merged keywise with Materialize(b)
//
// The second is why appending an element does not require reading the store: a
// new element materializes on its own into exactly the item it belongs in, and
// merging that item is a local operation.
func (c *Compiled) Materialize(m proto.Message) ([]Item, error) {
	if m == nil {
		return nil, nil
	}
	if got := m.ProtoReflect().Descriptor().FullName(); got != c.desc.FullName() {
		return nil, fmt.Errorf("protovcs: layout %q decomposes %s, given %s", c.layout.GetName(), c.desc.FullName(), got)
	}

	byKey := map[ItemKey]protoreflect.Message{}

	// Everything not split, including unknown fields, becomes the root item.
	// Cloned and then emptied of split fields rather than built up field by
	// field, so a field nobody thought about lands in the root rather than
	// being silently dropped.
	root := proto.Clone(m).ProtoReflect()
	for _, s := range c.splits {
		clearAt(root, s.path)
	}
	if hasAnything(root) {
		byKey[RootItemKey] = root
	}

	for _, s := range c.splits {
		if err := c.materializeSplit(m.ProtoReflect(), s, byKey); err != nil {
			return nil, err
		}
	}

	keys := make([]ItemKey, 0, len(byKey))
	for k := range byKey {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })

	items := make([]Item, 0, len(keys))
	for _, k := range keys {
		items = append(items, Item{Key: k, Fragment: byKey[k].Interface()})
	}
	return items, nil
}

func (c *Compiled) materializeSplit(root protoreflect.Message, s *compiledSplit, byKey map[ItemKey]protoreflect.Message) error {
	list, ok := listAt(root, s.path)
	if !ok || list.Len() == 0 {
		return nil
	}

	touched := map[ItemKey]bool{}
	for i := 0; i < list.Len(); i++ {
		elem := list.Get(i).Message()

		key, err := c.itemKeyFor(s, elem)
		if err != nil {
			return err
		}

		frag := byKey[key]
		if frag == nil {
			frag = root.New()
			byKey[key] = frag
		}
		listAtOrCreate(frag, s.path).Append(
			protoreflect.ValueOfMessage(proto.Clone(elem.Interface()).ProtoReflect()))
		touched[key] = true
	}

	// Sorting within each item is what makes a split field a multiset rather
	// than a list: the same elements always come back in the same order,
	// whatever order they were written in, so the same data always produces the
	// same bytes and therefore the same chunk name.
	for key := range touched {
		l, ok := listAt(byKey[key], s.path)
		if !ok {
			continue
		}
		if l.Len() > 1 {
			sortList(l, s.order)
		}
		if err := checkUnique(s, key, l); err != nil {
			return err
		}
	}
	return nil
}

// ItemKeyFor returns the item an element belongs to.
//
// A pure function of the element. That is what lets a writer append without
// reading anything: evaluate this, fetch that one item, append, write.
func (c *Compiled) ItemKeyFor(field string, elem proto.Message) (ItemKey, error) {
	s := c.splitFor(field)
	if s == nil {
		return "", fmt.Errorf("protovcs: layout %q does not split %q", c.layout.GetName(), field)
	}
	return c.itemKeyFor(s, elem.ProtoReflect())
}

func (c *Compiled) itemKeyFor(s *compiledSplit, elem protoreflect.Message) (ItemKey, error) {
	parts := make([]string, 0, len(s.by)+1)
	parts = append(parts, s.field)

	for _, k := range s.by {
		v, present := valueAt(elem, k.path)
		segment, err := shardValue(k, v, present)
		if err != nil {
			return "", fmt.Errorf("protovcs: splitting %q: %w", s.field, err)
		}
		parts = append(parts, segment)
	}
	return ItemKey(strings.Join(parts, "/")), nil
}

// Merge reassembles a message from items.
//
// The whole of reassembly. Items are merged in key order, and protobuf's own
// merge does the rest: repeated fields concatenate, singular fields take the
// last value, and only the root item sets singular fields. Nothing here knows
// what the message means.
func (c *Compiled) Merge(items []Item) (proto.Message, error) {
	sorted := make([]Item, len(items))
	copy(sorted, items)
	sort.SliceStable(sorted, func(i, j int) bool { return sorted[i].Key < sorted[j].Key })

	out := c.New()

	for _, it := range sorted {
		if it.Fragment == nil {
			continue
		}
		if got := it.Fragment.ProtoReflect().Descriptor().FullName(); got != c.desc.FullName() {
			return nil, fmt.Errorf("protovcs: item %q holds %s, not %s", it.Key, got, c.desc.FullName())
		}
		proto.Merge(out.Interface(), it.Fragment)
	}
	return out.Interface(), nil
}

// checkUnique refuses two elements of one item sharing an identity.
//
// A double append, or a deduplication that did not, produces exactly this, and
// the result is a store that reads without complaint and holds one record twice.
// Caught where the grouping happens, which is the last point at which the two
// are still distinguishable from a legitimate pair.
//
// Skipped when the collection declares no identity, because then two elements
// with the same contents are the same element and holding it twice is what the
// caller asked for.
func checkUnique(s *compiledSplit, key ItemKey, list protoreflect.List) error {
	if len(s.identity) == 0 || list.Len() < 2 {
		return nil
	}

	seen := make(map[string]bool, list.Len())
	for i := 0; i < list.Len(); i++ {
		v, present := valueAt(list.Get(i).Message(), s.identity)
		if !present {
			continue
		}
		id := scalarString(s.identity[len(s.identity)-1], v)
		if seen[id] {
			return fmt.Errorf("%w: item %q holds two elements with %s %q",
				ErrDuplicateIdentity, key, fieldPathString(s.identity), id)
		}
		seen[id] = true
	}
	return nil
}

// sortList orders an item's elements by the split's declared order fields, and
// falls back to the elements' canonical bytes.
//
// The fallback matters: a split with no declared order still has to produce one
// order rather than whatever order the caller supplied, or the same data would
// encode two ways and get two chunk names.
func sortList(list protoreflect.List, order [][]protoreflect.FieldDescriptor) {
	n := list.Len()
	elems := make([]protoreflect.Message, n)
	for i := 0; i < n; i++ {
		elems[i] = list.Get(i).Message()
	}

	fallback := make([]string, n)
	for i, e := range elems {
		b, err := appendMessage(nil, e)
		if err == nil {
			fallback[i] = string(b)
		}
	}

	idx := make([]int, n)
	for i := range idx {
		idx[i] = i
	}
	sort.SliceStable(idx, func(a, b int) bool {
		ea, eb := elems[idx[a]], elems[idx[b]]
		for _, path := range order {
			va, pa := valueAt(ea, path)
			vb, pb := valueAt(eb, path)
			if pa != pb {
				return !pa
			}
			if !pa {
				continue
			}
			sa := scalarString(path[len(path)-1], va)
			sb := scalarString(path[len(path)-1], vb)
			if sa != sb {
				return sa < sb
			}
		}
		return fallback[idx[a]] < fallback[idx[b]]
	})

	sorted := make([]protoreflect.Message, n)
	for i, j := range idx {
		sorted[i] = elems[j]
	}
	list.Truncate(0)
	for _, e := range sorted {
		list.Append(protoreflect.ValueOfMessage(e))
	}
}

// New returns an empty instance of the message this layout decomposes.
//
// The generated Go type when this was compiled against it, and a dynamic
// message otherwise. The descriptors are compared rather than the names,
// because a store read through descriptors it carried with it may name a
// message this build also has and mean something different by it. Using the
// compiled-in type then would reinterpret old data as new, which is the failure
// the whole versioning scheme exists to prevent.
func (c *Compiled) New() protoreflect.Message {
	if mt, err := protoregistry.GlobalTypes.FindMessageByName(c.desc.FullName()); err == nil {
		if mt.Descriptor() == c.desc {
			return mt.New()
		}
	}
	return dynamicpb.NewMessage(c.desc)
}

// valueAt reads a field path from a message, reporting whether every step was
// present.
//
// A proto3 scalar with no presence is always reported present, so a shard key
// holding zero renders as zero rather than as absent. The alternative would
// give an element with amount 0 the same item key as one whose amount field
// does not exist.
func valueAt(m protoreflect.Message, path []protoreflect.FieldDescriptor) (protoreflect.Value, bool) {
	cur := m
	for i, fd := range path {
		if i == len(path)-1 {
			if fd.HasPresence() && !cur.Has(fd) {
				return cur.Get(fd), false
			}
			return cur.Get(fd), true
		}
		if !cur.Has(fd) {
			return protoreflect.Value{}, false
		}
		cur = cur.Get(fd).Message()
	}
	return protoreflect.Value{}, false
}

func listAt(m protoreflect.Message, path []protoreflect.FieldDescriptor) (protoreflect.List, bool) {
	cur := m
	for i, fd := range path {
		if i == len(path)-1 {
			if !cur.Has(fd) {
				return nil, false
			}
			return cur.Get(fd).List(), true
		}
		if !cur.Has(fd) {
			return nil, false
		}
		cur = cur.Get(fd).Message()
	}
	return nil, false
}

func listAtOrCreate(m protoreflect.Message, path []protoreflect.FieldDescriptor) protoreflect.List {
	cur := m
	for i, fd := range path {
		if i == len(path)-1 {
			return cur.Mutable(fd).List()
		}
		cur = cur.Mutable(fd).Message()
	}
	return nil
}

func clearAt(m protoreflect.Message, path []protoreflect.FieldDescriptor) {
	cur := m
	for i, fd := range path {
		if i == len(path)-1 {
			cur.Clear(fd)
			return
		}
		if !cur.Has(fd) {
			return
		}
		cur = cur.Get(fd).Message()
	}
}

// hasAnything reports whether a message has any field or unknown bytes set.
//
// An empty root item is not stored. A store whose message has nothing outside
// its split fields would otherwise carry an item holding nothing, in every
// version, forever.
func hasAnything(m protoreflect.Message) bool {
	if len(m.GetUnknown()) > 0 {
		return true
	}
	any := false
	m.Range(func(protoreflect.FieldDescriptor, protoreflect.Value) bool {
		any = true
		return false
	})
	return any
}
