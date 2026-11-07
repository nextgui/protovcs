package protovcs

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"strconv"
	"strings"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"

	pb "github.com/nextgui/protovcs/proto/protovcs/v1"
)

// Compiled is a layout resolved against the message it decomposes.
//
// Resolving happens once, and every check the layout has to pass happens here:
// a field path that does not exist, a transform that cannot apply to the field
// it names, a query no split can serve. Doing it at compile time means a
// mistake in a layout is reported before a store is opened rather than on the
// first write, and the build rule that generates code from a layout runs this
// same code.
type Compiled struct {
	layout *pb.Layout
	desc   protoreflect.MessageDescriptor
	splits []*compiledSplit
	scopes map[string]*pb.Scope

	// The message a commit may carry alongside its text, from
	// Layout.commit_message. Nil when the layout declares none.
	commitMessage protoreflect.MessageDescriptor

	// Repeated fields no rule mentions, so they live in the root item. Kept to
	// make a root item over its ceiling actionable rather than a bare number.
	unsplitCollections []string
}

// UnsplitCollections returns the repeated fields no rule mentions, which
// therefore live in the root item alongside the scalars.
//
// Not a fault. A layout does not have to cover the message: anything it does not
// mention lands in the root item, so omission never loses data. It is a cost —
// every reader fetches the root item — and this is what a caller inspects when
// that cost becomes visible.
func (c *Compiled) UnsplitCollections() []string { return c.unsplitCollections }

type compiledSplit struct {
	// Which field of an element tells one from another, from the identity
	// annotation on the repeated field. Empty when none is declared, in which
	// case an element's identity is its contents.
	identity []protoreflect.FieldDescriptor

	// The layout's own spelling, for errors and item keys.
	field string

	// Resolved path from the root message to the repeated field. More than one
	// element when the field is inside a singular submessage.
	path []protoreflect.FieldDescriptor

	by             []compiledShardKey
	order          [][]protoreflect.FieldDescriptor
	splitByteCount uint32
	maxItemBytes   uint32
}

type compiledShardKey struct {
	path      []protoreflect.FieldDescriptor
	transform pb.Transform
	buckets   uint32
}

// CompileLayout resolves a layout against a message descriptor.
func CompileLayout(l *pb.Layout, desc protoreflect.MessageDescriptor) (*Compiled, error) {
	if l == nil {
		return nil, fmt.Errorf("protovcs: no layout")
	}
	if desc == nil {
		return nil, fmt.Errorf("protovcs: layout %q has no message to resolve against", l.GetName())
	}
	if want := l.GetRootMessage(); want != "" && string(desc.FullName()) != want {
		return nil, fmt.Errorf("protovcs: layout %q is for %s, not %s", l.GetName(), want, desc.FullName())
	}

	c := &Compiled{layout: l, desc: desc, scopes: map[string]*pb.Scope{}}

	seen := map[string]bool{}
	for _, s := range l.GetSplits() {
		cs, err := compileSplit(desc, s)
		if err != nil {
			return nil, err
		}
		if seen[cs.field] {
			return nil, fmt.Errorf("protovcs: layout %q splits %q twice", l.GetName(), cs.field)
		}
		seen[cs.field] = true
		c.splits = append(c.splits, cs)
	}

	for _, s := range l.GetScopes() {
		if s.GetName() == "" {
			return nil, fmt.Errorf("protovcs: layout %q has a scope with no name", l.GetName())
		}
		if c.scopes[s.GetName()] != nil {
			return nil, fmt.Errorf("protovcs: layout %q declares scope %q twice", l.GetName(), s.GetName())
		}
		for _, inc := range s.GetInclude() {
			if _, err := resolvePath(desc, inc.GetField()); err != nil {
				return nil, fmt.Errorf("protovcs: scope %q: %w", s.GetName(), err)
			}
		}
		c.scopes[s.GetName()] = s
	}

	// Which repeated fields are not covered by any rule. Not an error: a short
	// repeated field belongs in the root item, and most are. Recorded so that a
	// root item over its ceiling can name the fields that are probably why.
	for i := 0; i < desc.Fields().Len(); i++ {
		fd := desc.Fields().Get(i)
		if !fd.IsList() && !fd.IsMap() {
			continue
		}
		if seen[string(fd.Name())] {
			continue
		}
		c.unsplitCollections = append(c.unsplitCollections, string(fd.Name()))
	}

	// A query is checked against the splits that would have to serve it. A
	// query nothing can serve is not slow here, it is a download of the whole
	// store, so it is refused rather than allowed to be discovered later.
	for _, q := range l.GetQueries() {
		if err := c.checkQuery(q); err != nil {
			return nil, err
		}
	}

	// The message a commit may carry, resolved now rather than when one is
	// written. A layout naming a type nothing defines is a store whose every
	// commit would fail, and finding that out at the first write is finding it
	// out too late.
	//
	// Resolved against the same files the root message came from, because those
	// are what the SchemaChunk carries and therefore what a client reading an
	// old commit will have.
	if name := l.GetCommitMessage(); name != "" {
		md, err := resolveMessage(desc, protoreflect.FullName(name))
		if err != nil {
			return nil, fmt.Errorf("protovcs: layout %q declares commit_message %q: %w",
				l.GetName(), name, err)
		}
		c.commitMessage = md
	}

	return c, nil
}

// resolveMessage finds a message by name among the files the root message's own
// file can reach.
//
// Its own file, then its imports, transitively. Anything outside that is a type
// the SchemaChunk does not carry, so a client reading an old commit would have
// the layout, the descriptors, and no way to resolve what the layout named.
func resolveMessage(root protoreflect.MessageDescriptor, name protoreflect.FullName) (protoreflect.MessageDescriptor, error) {
	seen := map[string]bool{}
	var find func(protoreflect.FileDescriptor) protoreflect.MessageDescriptor
	find = func(fd protoreflect.FileDescriptor) protoreflect.MessageDescriptor {
		if fd == nil || seen[fd.Path()] {
			return nil
		}
		seen[fd.Path()] = true

		if md := findMessageIn(fd.Messages(), name); md != nil {
			return md
		}
		for i := 0; i < fd.Imports().Len(); i++ {
			if md := find(fd.Imports().Get(i).FileDescriptor); md != nil {
				return md
			}
		}
		return nil
	}

	if md := find(root.ParentFile()); md != nil {
		return md, nil
	}
	return nil, fmt.Errorf("no such message among %s and what it imports", root.ParentFile().Path())
}

// findMessageIn searches a message list and everything nested inside it.
func findMessageIn(list protoreflect.MessageDescriptors, name protoreflect.FullName) protoreflect.MessageDescriptor {
	for i := 0; i < list.Len(); i++ {
		md := list.Get(i)
		if md.FullName() == name {
			return md
		}
		if nested := findMessageIn(md.Messages(), name); nested != nil {
			return nested
		}
	}
	return nil
}

// CommitMessage returns the message a commit may carry, or nil when the layout
// declares none.
func (c *Compiled) CommitMessage() protoreflect.MessageDescriptor { return c.commitMessage }

// Layout returns the layout this was compiled from.
func (c *Compiled) Layout() *pb.Layout { return c.layout }

// Descriptor returns the root message's descriptor.
func (c *Compiled) Descriptor() protoreflect.MessageDescriptor { return c.desc }

// ScopeForField returns the scope a caller needs to read one field.
//
// A split field lives under a key of its own, so its scope is that key's range.
// A field no rule mentions lives in the root item, so its scope is the root.
// Getting this wrong is silent — a scope covering nothing produces a message
// with the field empty rather than an error — which is why the answer comes
// from the layout rather than from the caller's memory of it.
func (c *Compiled) ScopeForField(name string) (Scope, error) {
	if _, err := resolvePath(c.desc, name); err != nil {
		return Scope{}, fmt.Errorf("protovcs: layout %q: %w", c.layout.GetName(), err)
	}
	for _, s := range c.splits {
		if s.field == name {
			return Field(name), nil
		}
	}
	return Root(), nil
}

// SchemaVersion returns what the layout says the data means.
func (c *Compiled) SchemaVersion() uint32 { return c.layout.GetSchemaVersion() }

func compileSplit(root protoreflect.MessageDescriptor, s *pb.Split) (*compiledSplit, error) {
	path, err := resolvePath(root, s.GetField())
	if err != nil {
		return nil, err
	}

	leaf := path[len(path)-1]
	if leaf.IsMap() {
		// Unimplemented, and no longer undecided. The shard key for a map is the
		// map key: protobuf guarantees it is unique, which is the property a
		// shard key needs and a repeated field cannot supply. A value field
		// would be a choice; the key is the only answer that is not one.
		//
		// That uniqueness is also what would let a split reach through a map
		// without the injectivity check a repeated field needs, since the item
		// key prefix would identify exactly one entry by construction.
		return nil, fmt.Errorf(
			"protovcs: field %q is a map, and splitting maps is not implemented.\n"+
				"When it is, the shard key will be the map key — it is unique by\n"+
				"construction, which is what a shard key needs.",
			s.GetField())
	}
	if !leaf.IsList() {
		return nil, fmt.Errorf("protovcs: field %q is not repeated, so it cannot be split; use an extract", s.GetField())
	}
	if leaf.Kind() != protoreflect.MessageKind {
		return nil, fmt.Errorf("protovcs: field %q is a repeated scalar, so it has no shard key to split on", s.GetField())
	}

	elem := leaf.Message()
	cs := &compiledSplit{
		field:          s.GetField(),
		path:           path,
		splitByteCount: s.GetSplitByteCount(),
		maxItemBytes:   s.GetMaxItemBytes(),
	}
	if cs.maxItemBytes > 0 && cs.splitByteCount > cs.maxItemBytes {
		return nil, fmt.Errorf(
			"protovcs: split of %q aims for %d bytes and refuses above %d, so every item it aims for is one it refuses",
			s.GetField(), cs.splitByteCount, cs.maxItemBytes)
	}

	if len(s.GetBy()) == 0 {
		return nil, fmt.Errorf("protovcs: split of %q names no shard key", s.GetField())
	}
	for _, by := range s.GetBy() {
		keyPath, err := resolvePath(elem, by.GetField())
		if err != nil {
			return nil, fmt.Errorf("protovcs: split of %q: %w", s.GetField(), err)
		}
		if err := checkTransform(by, keyPath[len(keyPath)-1]); err != nil {
			return nil, fmt.Errorf("protovcs: split of %q: %w", s.GetField(), err)
		}
		cs.by = append(cs.by, compiledShardKey{
			path:      keyPath,
			transform: by.GetTransform(),
			buckets:   by.GetBuckets(),
		})
	}

	// The identity annotation is on the repeated field rather than on the
	// element's message, because identity is a property of the collection: the
	// same type is identified in one place and a plain bag of values in another.
	if name := identityOf(leaf); name != "" {
		idPath, err := resolvePath(elem, name)
		if err != nil {
			return nil, fmt.Errorf("protovcs: identity of %q: %w", s.GetField(), err)
		}
		last := idPath[len(idPath)-1]
		if last.IsList() || last.IsMap() || last.Kind() == protoreflect.MessageKind {
			return nil, fmt.Errorf(
				"protovcs: identity of %q is %q, which is not a scalar and so cannot tell two elements apart",
				s.GetField(), name)
		}
		cs.identity = idPath
	}

	for _, o := range s.GetOrder() {
		orderPath, err := resolvePath(elem, o)
		if err != nil {
			return nil, fmt.Errorf("protovcs: split of %q order: %w", s.GetField(), err)
		}
		last := orderPath[len(orderPath)-1]
		if last.IsList() || last.IsMap() || last.Kind() == protoreflect.MessageKind {
			return nil, fmt.Errorf("protovcs: split of %q cannot order by %q, which is not a scalar", s.GetField(), o)
		}
		cs.order = append(cs.order, orderPath)
	}

	// An identity is a total order on its own, so a split that declares none of
	// its own gets it rather than falling back to comparing encoded bytes.
	if len(cs.order) == 0 && len(cs.identity) > 0 {
		cs.order = append(cs.order, cs.identity)
	}

	return cs, nil
}

// resolvePath turns "a.b.c" into the field descriptors that reach it.
//
// Only singular message fields may be traversed. A path through a repeated
// field would name many values rather than one, and a shard key has to be a
// single value computed from the element alone.
func resolvePath(md protoreflect.MessageDescriptor, path string) ([]protoreflect.FieldDescriptor, error) {
	if path == "" {
		return nil, fmt.Errorf("an empty field path")
	}

	var out []protoreflect.FieldDescriptor
	cur := md
	parts := strings.Split(path, ".")
	for i, part := range parts {
		if cur == nil {
			return nil, fmt.Errorf("field path %q goes through %q, which is not a message", path, parts[i-1])
		}
		fd := cur.Fields().ByName(protoreflect.Name(part))
		if fd == nil {
			return nil, fmt.Errorf("message %s has no field %q, named in path %q", cur.FullName(), part, path)
		}
		out = append(out, fd)

		if i == len(parts)-1 {
			break
		}
		if fd.IsList() || fd.IsMap() {
			return nil, fmt.Errorf(
				"field path %q goes through repeated field %q, which is not one value but one per element.\n"+
					"Reassembly here is proto.Merge, which concatenates repeated fields rather than\n"+
					"matching them up, so two fragments each holding one %q would merge into two of\n"+
					"them rather than one with both halves.\n"+
					"Flatten instead: give the inner elements a field of their own on the message,\n"+
					"carrying the identity of the %q they belong to, and split that by it.\n"+
					"(A property of the reassembly rule, not of the data. Grouping fragments by\n"+
					"item key prefix before merging would make this work, given the outer split's\n"+
					"shard key identifies one element rather than grouping several.)",
				path, part, part, part)
		}
		if fd.Kind() != protoreflect.MessageKind {
			return nil, fmt.Errorf("field path %q goes through %q, which is not a message", path, part)
		}
		cur = fd.Message()
	}
	return out, nil
}

// checkTransform refuses a transform that cannot apply to the field it names.
//
// A MONTH transform on a string is the mistake this catches, and catching it at
// compile time rather than at the first write is the point of compiling at all.
func checkTransform(by *pb.ShardKey, fd protoreflect.FieldDescriptor) error {
	if fd.IsList() || fd.IsMap() {
		return fmt.Errorf("shard key %q is repeated, so it is not one value", by.GetField())
	}

	switch by.GetTransform() {
	case pb.Transform_TRANSFORM_IDENTITY, pb.Transform_TRANSFORM_UNSPECIFIED:
		if fd.Kind() == protoreflect.MessageKind {
			return fmt.Errorf("shard key %q is a message, which has no identity form", by.GetField())
		}
		return nil

	case pb.Transform_TRANSFORM_DAY, pb.Transform_TRANSFORM_MONTH,
		pb.Transform_TRANSFORM_QUARTER, pb.Transform_TRANSFORM_YEAR:
		if fd.Kind() != protoreflect.MessageKind {
			return fmt.Errorf("shard key %q is %v, and a date transform needs a date or timestamp", by.GetField(), fd.Kind())
		}
		if !isDateShaped(fd.Message()) && !isTimestamp(fd.Message()) {
			return fmt.Errorf("shard key %q is %s, which is neither a date nor a timestamp", by.GetField(), fd.Message().FullName())
		}
		return nil

	case pb.Transform_TRANSFORM_HASH:
		if by.GetBuckets() == 0 {
			return fmt.Errorf("shard key %q hashes into no buckets", by.GetField())
		}
		if fd.Kind() == protoreflect.MessageKind {
			return fmt.Errorf("shard key %q is a message, which cannot be hashed into buckets", by.GetField())
		}
		return nil

	default:
		return fmt.Errorf("shard key %q uses transform %v, which this build does not know", by.GetField(), by.GetTransform())
	}
}

// isDateShaped reports whether a message has the year, month and day fields the
// date transforms read.
//
// Matched by shape rather than by name so that //proto/types.Date,
// google.type.Date and a product's own date all work without this package
// knowing about any of them.
func isDateShaped(md protoreflect.MessageDescriptor) bool {
	for _, name := range []string{"year", "month", "day"} {
		fd := md.Fields().ByName(protoreflect.Name(name))
		if fd == nil || fd.IsList() || fd.Kind() != protoreflect.Int32Kind {
			return false
		}
	}
	return true
}

func isTimestamp(md protoreflect.MessageDescriptor) bool {
	return md.FullName() == "google.protobuf.Timestamp"
}

// checkQuery refuses a query the splits cannot answer.
func (c *Compiled) checkQuery(q *pb.Query) error {
	if q.GetName() == "" {
		return fmt.Errorf("protovcs: layout %q has a query with no name", c.layout.GetName())
	}

	split := c.splitFor(q.GetField())
	if split == nil {
		return fmt.Errorf("protovcs: query %q reads %q, which no split covers, so it would scan the whole store",
			q.GetName(), q.GetField())
	}

	// Predicates must line up with the shard key from its first component
	// onwards. A predicate on the second component with none on the first
	// selects a scatter across every value of the first, which is a scan
	// wearing a query's clothes.
	for i, p := range q.GetBy() {
		if i >= len(split.by) {
			return fmt.Errorf("protovcs: query %q has more conditions than %q has shard key components",
				q.GetName(), q.GetField())
		}
		want := split.by[i]
		if p.GetField() != fieldPathString(want.path) {
			return fmt.Errorf("protovcs: query %q filters on %q where %q's shard key has %q at position %d; "+
				"reorder the query, or add the field to the split",
				q.GetName(), p.GetField(), q.GetField(), fieldPathString(want.path), i)
		}
		// A range on a hashed component cannot work: hashing destroys order.
		if p.GetOp() == pb.Op_OP_RANGE && want.transform == pb.Transform_TRANSFORM_HASH {
			return fmt.Errorf("protovcs: query %q asks for a range over %q, which is hashed into buckets and has no order",
				q.GetName(), p.GetField())
		}
		// Only the last condition may be a range. A range on an earlier
		// component leaves the later ones scattered.
		if p.GetOp() == pb.Op_OP_RANGE && i != len(q.GetBy())-1 {
			return fmt.Errorf("protovcs: query %q ranges over %q and then filters further; only the last condition may be a range",
				q.GetName(), p.GetField())
		}
	}
	return nil
}

// ItemSize is what a split thinks of one item's size.
type ItemSize int

const (
	// ItemWithinTarget is at or under split_byte_count, or the layout set none.
	ItemWithinTarget ItemSize = iota

	// ItemOverTarget is over split_byte_count and under any ceiling. The write
	// proceeds; the layout is worth a look.
	ItemOverTarget

	// ItemOverCeiling is over max_item_bytes with more than one element, so a
	// shard key component would divide it. The write is refused.
	ItemOverCeiling

	// ItemIsOneOversizedElement is over max_item_bytes and holds one element.
	// No shard key divides one element, so refusing would make the data
	// unstorable. The write proceeds and the server's max_chunk_bytes is what
	// bounds it.
	ItemIsOneOversizedElement
)

// ClassifyItem reports how one item stands against its split's two sizes.
//
// The two are different questions and were one number until they were not.
// split_byte_count asks how big items should be, which is about how much a
// reader fetches to see one shard value. max_item_bytes asks what is
// intolerable, which is about a layout having gone wrong. A store can set
// either, both, or neither.
func (c *Compiled) ClassifyItem(key ItemKey, bytes uint64, elements uint32) ItemSize {
	// The root item holds every field no rule mentions, so it has no split and
	// its own ceiling. It is fetched by every reader, which is why an unbounded
	// collection landing there is worth refusing: a layout that forgets to split
	// a growing field does not lose it, it puts it somewhere every read pays
	// for.
	if key == RootItemKey {
		if max := c.layout.GetRootMaxBytes(); max > 0 && bytes > uint64(max) {
			return ItemOverCeiling
		}
		return ItemWithinTarget
	}

	split := c.splitFor(fieldOf(key))
	if split == nil {
		return ItemWithinTarget
	}

	if max := split.maxItemBytes; max > 0 && bytes > uint64(max) {
		if elements <= 1 {
			return ItemIsOneOversizedElement
		}
		return ItemOverCeiling
	}
	if target := split.splitByteCount; target > 0 && bytes > uint64(target) {
		return ItemOverTarget
	}
	return ItemWithinTarget
}

// CheckItemSize refuses an item over its split's ceiling.
//
// Refusing rather than subdividing is forced rather than chosen. An automatic
// subdivision needs a sub-key, and there are only two kinds. One is derived from
// where an element falls among its neighbours, so groups adapt to size — and
// that breaks the rule that an element's item is a pure function of the element,
// which is what lets a writer append without reading anything. The other is
// derived from the element alone, so purity holds — but then it is exactly a
// shard key component, which a layout can already express, at a granularity
// fixed up front rather than adapting.
//
// So an item genuinely too large is a layout needing another component, and this
// says which field to add it to.
func (c *Compiled) CheckItemSize(key ItemKey, bytes uint64, elements uint32) error {
	if c.ClassifyItem(key, bytes, elements) != ItemOverCeiling {
		return nil
	}

	if key == RootItemKey {
		return fmt.Errorf(
			"%w: the root item is %d bytes and this layout refuses above %d.\n"+
				"The root item holds every field no rule mentions, and every reader fetches it.\n"+
				"A repeated field that grows is the usual cause: split it, so it is fetched by\n"+
				"the readers that want it instead of by all of them.%s",
			ErrItemTooLarge, bytes, c.layout.GetRootMaxBytes(), unsplitHint(c.unsplitCollections))
	}

	split := c.splitFor(fieldOf(key))
	if split == nil {
		return nil
	}
	return fmt.Errorf(
		"%w: item %q holds %d elements in %d bytes, and %q refuses above %d;"+
			" add a shard key component to %q so they divide further",
		ErrItemTooLarge, key, elements, bytes, split.field, split.maxItemBytes, split.field)
}

// OversizedItems reports every item over its split's target, so a caller can
// warn without the write failing.
//
// Over the target is a judgement rather than a fault: shard keys bound item size
// in the ordinary case and not in every case, and whether a bulk correction pass
// landing in one day is a layout to fix or a Tuesday is not something a library
// decides.
func (c *Compiled) OversizedItems(refs []ItemRef) []ItemRef {
	var out []ItemRef
	for _, r := range refs {
		switch c.ClassifyItem(r.Key, r.Bytes, r.Elements) {
		case ItemOverTarget, ItemIsOneOversizedElement:
			out = append(out, r)
		}
	}
	return out
}

// checkChangedItemSizes refuses any changed item over its split's ceiling.
func (c *Compiled) checkChangedItemSizes(changes map[ItemKey]*ItemRef) error {
	for key, ref := range changes {
		if ref == nil {
			continue
		}
		if err := c.CheckItemSize(key, ref.Bytes, ref.Elements); err != nil {
			return err
		}
	}
	return nil
}

// OversizedElement reports whether an item is a single element too large for its
// own ceiling, which no layout change can fix.
func (c *Compiled) OversizedElement(key ItemKey, bytes uint64, elements uint32) bool {
	return c.ClassifyItem(key, bytes, elements) == ItemIsOneOversizedElement
}

// unsplitHint names the repeated fields that are probably why a root item grew.
func unsplitHint(fields []string) string {
	if len(fields) == 0 {
		return ""
	}
	return "\nNo rule mentions: " + strings.Join(fields, ", ")
}

// fieldOf returns the split field an item key belongs to.
func fieldOf(key ItemKey) string {
	field := string(key)
	if i := strings.IndexByte(field, '/'); i >= 0 {
		field = field[:i]
	}
	return field
}

func (c *Compiled) splitFor(field string) *compiledSplit {
	for _, s := range c.splits {
		if s.field == field {
			return s
		}
	}
	return nil
}

// identityOf reads the identity annotation from a repeated field.
//
// Read from the descriptor rather than from the layout so that two layouts over
// one message cannot disagree, and so stableproto guards it like any
// other part of a stored schema.
func identityOf(fd protoreflect.FieldDescriptor) string {
	opts, ok := fd.Options().(*descriptorpb.FieldOptions)
	if !ok || opts == nil {
		return ""
	}
	v := proto.GetExtension(opts, pb.E_Identity)
	name, _ := v.(string)
	return name
}

// RequireIdentity reports the identity of a split's elements, or an error naming
// what to annotate.
//
// For an operation that needs elements told apart across versions. Nothing
// infers one: an index shifts when a neighbour is inserted, and contents cannot
// survive the edit an identity exists to track.
func (c *Compiled) RequireIdentity(field string) (string, error) {
	split := c.splitFor(field)
	if split == nil {
		return "", fmt.Errorf("protovcs: layout %q does not split %q", c.layout.GetName(), field)
	}
	if len(split.identity) == 0 {
		return "", fmt.Errorf(
			"protovcs: %q declares no identity, so its elements cannot be told apart across versions.\n"+
				"Annotate the field in the proto:\n"+
				"  repeated Elem %s = N [(nextgui.protovcs.v1.identity) = \"id\"];",
			field, field)
	}
	return fieldPathString(split.identity), nil
}

// Identity returns the field that tells one element of a split apart, or empty
// when the collection declares none and an element is identified by contents.
func (c *Compiled) Identity(field string) string {
	split := c.splitFor(field)
	if split == nil || len(split.identity) == 0 {
		return ""
	}
	return fieldPathString(split.identity)
}

func fieldPathString(path []protoreflect.FieldDescriptor) string {
	parts := make([]string, len(path))
	for i, fd := range path {
		parts[i] = string(fd.Name())
	}
	return strings.Join(parts, ".")
}

// shardValue renders one shard key component as the string that appears in an
// item key.
//
// Rendered so that sorting the strings sorts the values, because that is what
// makes a range of item keys a contiguous walk of the manifest. Dates are
// zero-padded and hashes are fixed width for the same reason.
func shardValue(k compiledShardKey, v protoreflect.Value, present bool) (string, error) {
	switch k.transform {
	case pb.Transform_TRANSFORM_IDENTITY, pb.Transform_TRANSFORM_UNSPECIFIED:
		if !present {
			return "", nil
		}
		// scalarString already produces the segment form, escaping included.
		return scalarString(k.path[len(k.path)-1], v), nil

	case pb.Transform_TRANSFORM_DAY, pb.Transform_TRANSFORM_MONTH,
		pb.Transform_TRANSFORM_QUARTER, pb.Transform_TRANSFORM_YEAR:
		if !present {
			return "", fmt.Errorf("protovcs: %s has no value, so the element belongs to no period", fieldPathString(k.path))
		}
		y, mo, d, err := dateParts(k.path[len(k.path)-1], v)
		if err != nil {
			return "", err
		}
		// Four digits is what keeps string order chronological, so a year
		// that needs more, or a sign, has no key that sorts correctly.
		if y < 0 || y > 9999 {
			return "", fmt.Errorf("protovcs: %s is in the year %d, and a date key holds only the years 0 to 9999",
				fieldPathString(k.path), y)
		}
		// Separated by the path separator rather than by dashes, so the manifest
		// nests along them. A day written as one segment would put every day of
		// a store in a single node: five years is 1,825 entries in one chunk,
		// rewritten whenever any day changes, and fetched whole to read one.
		// Written as year, month and day it is a directory per year and per
		// month, which is where the tree's cheap diffs come from.
		//
		// Ordering is unaffected: "/" sorts below every digit, so a range over
		// these segments is still contiguous.
		return DateSegment(k.transform, y, mo, d), nil

	case pb.Transform_TRANSFORM_HASH:
		var raw string
		if present {
			raw = scalarString(k.path[len(k.path)-1], v)
		}
		return HashSegment(raw, k.buckets), nil

	default:
		return "", fmt.Errorf("protovcs: transform %v is not implemented", k.transform)
	}
}

// DateSegment formats one segment of an item key from a date.
//
// Exported because generated code builds key prefixes and must produce exactly
// what the materializer produces. Two implementations of this would be two
// answers to which item an element is in.
//
// Separated by the path separator rather than by dashes so the manifest nests
// along them, and widest part first so string order matches chronological order.
// That holds for the years 0 to 9999 only, and a write refuses a date outside
// them.
func DateSegment(t pb.Transform, year, month, day int) string {
	switch t {
	case pb.Transform_TRANSFORM_YEAR:
		return fmt.Sprintf("%04d", year)
	case pb.Transform_TRANSFORM_QUARTER:
		return fmt.Sprintf("%04d/Q%d", year, (month-1)/3+1)
	case pb.Transform_TRANSFORM_MONTH:
		return fmt.Sprintf("%04d/%02d", year, month)
	default:
		return fmt.Sprintf("%04d/%02d/%02d", year, month, day)
	}
}

// HashSegment formats the bucket a value hashes into as one segment.
//
// value is the value's own segment form, as [StringSegment], [IntSegment],
// [UintSegment] or [BoolSegment] render it, and empty for a field that is not
// set. Exported for the same reason as [DateSegment]: generated code names a
// bucket and must name the one a write chooses.
func HashSegment(value string, buckets uint32) string {
	sum := sha256.Sum256([]byte(value))
	bucket := binary.BigEndian.Uint64(sum[:8]) % uint64(buckets)
	width := len(strconv.FormatUint(uint64(buckets-1), 10))
	return fmt.Sprintf("b%0*d", width, bucket)
}

// StringSegment formats a string value as one segment of an item key.
//
// Escaped, because a value containing the separator would otherwise invent a
// directory nobody declared and two different values could name one item.
func StringSegment(v string) string { return escapeSegment(v) }

// IntSegment formats a signed integer as one segment.
//
// Twenty characters, so that string order matches numeric order. A
// non-negative value is "0" and then the value in 19 digits. A negative value is
// "-" and then v + 2^63 in 19 digits: "-" sorts below "0", and adding 2^63 maps
// MinInt64..-1 onto 0..2^63-1 in the same order.
func IntSegment(v int64) string {
	if v < 0 {
		return fmt.Sprintf("-%019d", uint64(v)+(1<<63))
	}
	return fmt.Sprintf("0%019d", v)
}

// UintSegment formats an unsigned integer as one segment.
func UintSegment(v uint64) string { return fmt.Sprintf("%020d", v) }

// BoolSegment formats a boolean as one segment.
func BoolSegment(v bool) string {
	if v {
		return "true"
	}
	return "false"
}

func scalarString(fd protoreflect.FieldDescriptor, v protoreflect.Value) string {
	switch fd.Kind() {
	case protoreflect.StringKind:
		return StringSegment(v.String())
	case protoreflect.BoolKind:
		return BoolSegment(v.Bool())
	case protoreflect.EnumKind:
		return IntSegment(int64(v.Enum()))
	case protoreflect.Int32Kind, protoreflect.Int64Kind, protoreflect.Sint32Kind,
		protoreflect.Sint64Kind, protoreflect.Sfixed32Kind, protoreflect.Sfixed64Kind:
		return IntSegment(v.Int())
	case protoreflect.Uint32Kind, protoreflect.Uint64Kind, protoreflect.Fixed32Kind, protoreflect.Fixed64Kind:
		return UintSegment(v.Uint())
	case protoreflect.BytesKind:
		return StringSegment(string(v.Bytes()))
	default:
		return v.String()
	}
}

func dateParts(fd protoreflect.FieldDescriptor, v protoreflect.Value) (year, month, day int, err error) {
	m := v.Message()
	if isTimestamp(fd.Message()) {
		secondsFd := m.Descriptor().Fields().ByName("seconds")
		t := unixToDate(m.Get(secondsFd).Int())
		return t[0], t[1], t[2], nil
	}
	get := func(name string) int {
		f := m.Descriptor().Fields().ByName(protoreflect.Name(name))
		return int(m.Get(f).Int())
	}
	year, month, day = get("year"), get("month"), get("day")
	if month < 1 || month > 12 || day < 1 || day > 31 {
		return 0, 0, 0, fmt.Errorf("protovcs: %s holds %04d-%02d-%02d, which is not a date", fieldPathString([]protoreflect.FieldDescriptor{fd}), year, month, day)
	}
	return year, month, day, nil
}

// unixToDate reads a timestamp as a calendar date in UTC.
//
// UTC rather than any local zone: which item an element belongs to has to be
// the same answer on every machine that ever writes to the store, and a local
// zone would make it depend on where the writer happened to be.
func unixToDate(sec int64) [3]int {
	t := time.Unix(sec, 0).UTC()
	return [3]int{t.Year(), int(t.Month()), t.Day()}
}

// escapeSegment makes a value safe to be one segment of an item key.
//
// Item keys have path structure and the manifest nests along it, so a value
// containing a separator would create directories nobody declared. Percent
// encoding is used because it is reversible, which matters for a plaintext
// store whose filenames a person is expected to read.
func escapeSegment(s string) string {
	needs := false
	for i := 0; i < len(s); i++ {
		if s[i] == '/' || s[i] == '%' || s[i] < 0x20 || s[i] == 0x7f {
			needs = true
			break
		}
	}
	if !needs {
		return s
	}
	var b strings.Builder
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c == '/' || c == '%' || c < 0x20 || c == 0x7f {
			fmt.Fprintf(&b, "%%%02X", c)
			continue
		}
		b.WriteByte(c)
	}
	return b.String()
}
