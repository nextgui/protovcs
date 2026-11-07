package protovcs

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"

	pb "github.com/nextgui/protovcs/proto/protovcs/v1"
)

// KeyRange is a run of item keys, inclusive at both ends.
type KeyRange struct {
	From ItemKey
	To   ItemKey
}

// Scope is what a checkout fetched.
//
// A scope is a promise about what was read, not about what exists. An empty
// result inside a scope means there is nothing there; outside a scope it is a
// refusal, because the alternative is a caller quietly computing a total over
// three months of a five-year store and reporting it as the whole.
type Scope struct {
	Ranges []KeyRange
}

// Nothing is the scope covering no items at all.
//
// For a caller that only adds. Which item an element belongs to is computed
// from the element, so a write needs no prior read of the collection — see
// [Partial.PutElement], which fetches the one item it is about to change and
// nothing else.
func Nothing() Scope { return Scope{} }

// Everything is the scope covering a whole store.
func Everything() Scope {
	return Scope{Ranges: []KeyRange{{From: "", To: ItemKey("￿")}}}
}

// Field returns the scope covering one split field entirely.
//
// Only a split field. A field no layout rule mentions lives in the root item
// and is not under a key of its own, so this covers nothing for one — use
// [Compiled.ScopeForField], which knows the layout and answers correctly for
// either kind.
func Field(name string) Scope {
	return Scope{Ranges: []KeyRange{{From: ItemKey(name), To: ItemKey(name + "/￿")}}}
}

// Root returns the scope covering the root item alone.
//
// The root item holds every field no layout rule mentions: the scalars, and any
// collection the layout chose not to split. A caller that wants one of those
// has to say so, because a checkout fetches what its scope names and a scope
// that named nothing would produce a message reading as though the data had
// never been written.
//
// Not included by default. A write that only appends needs to read nothing at
// all, and that property is worth more than saving callers a Union.
func Root() Scope {
	return Scope{Ranges: []KeyRange{{From: RootItemKey, To: RootItemKey}}}
}

// Prefix returns the scope covering every item key beginning with prefix, which
// is how a caller asks for one shard value and everything under it.
func Prefix(prefix string) Scope {
	return Scope{Ranges: []KeyRange{{From: ItemKey(prefix), To: ItemKey(prefix + "￿")}}}
}

// Between returns the scope covering a range of item keys, which is how a date
// range is expressed when the date is a shard key.
func Between(from, to ItemKey) Scope {
	return Scope{Ranges: []KeyRange{{From: from, To: to}}}
}

// Union returns the scope covering everything any of these cover.
func Union(scopes ...Scope) Scope {
	var out Scope
	for _, s := range scopes {
		out.Ranges = append(out.Ranges, s.Ranges...)
	}
	return out.normalized()
}

// Covers reports whether every key in other is inside this scope.
//
// Conservative on purpose: when the answer is not clearly yes it is no, and the
// caller widens explicitly. A coverage check that wrongly says yes produces the
// silent partial read the whole mechanism exists to prevent.
func (s Scope) Covers(other Scope) bool {
	mine := s.normalized()
	for _, r := range other.normalized().Ranges {
		if !mine.coversRange(r) {
			return false
		}
	}
	return true
}

func (s Scope) coversRange(r KeyRange) bool {
	for _, m := range s.Ranges {
		if m.From <= r.From && r.To <= m.To {
			return true
		}
	}
	return false
}

// Holds reports whether one item key is inside the scope.
func (s Scope) Holds(key ItemKey) bool {
	for _, r := range s.Ranges {
		if r.From <= key && key <= r.To {
			return true
		}
	}
	return false
}

// normalized sorts and merges overlapping ranges, so coverage is a scan rather
// than a search.
func (s Scope) normalized() Scope {
	if len(s.Ranges) < 2 {
		return s
	}
	ranges := make([]KeyRange, len(s.Ranges))
	copy(ranges, s.Ranges)
	sort.Slice(ranges, func(i, j int) bool {
		if ranges[i].From != ranges[j].From {
			return ranges[i].From < ranges[j].From
		}
		return ranges[i].To < ranges[j].To
	})

	out := []KeyRange{ranges[0]}
	for _, r := range ranges[1:] {
		last := &out[len(out)-1]
		if r.From <= last.To {
			if r.To > last.To {
				last.To = r.To
			}
			continue
		}
		out = append(out, r)
	}
	return Scope{Ranges: out}
}

func (s Scope) String() string {
	parts := make([]string, 0, len(s.Ranges))
	for _, r := range s.Ranges {
		parts = append(parts, fmt.Sprintf("%q..%q", r.From, r.To))
	}
	return strings.Join(parts, ", ")
}

// Partial is part of a store, fetched and held.
//
// Reads inside its scope touch no network. Reads outside it fail, rather than
// quietly fetching more, because a query that becomes forty round trips in a
// browser is the failure this exists to prevent.
type Partial struct {
	store  *Store
	branch *Branch
	head   *pb.Commit
	state  RefState

	scope Scope
	items map[ItemKey]Item
	refs  map[ItemKey]ItemRef
}

// Checkout fetches exactly the scope named and nothing else, in one batched
// request.
func (b *Branch) Checkout(ctx context.Context, scope Scope) (*Partial, error) {
	head, state, err := b.Head(ctx)
	if err != nil && !isNoSuchRef(err) {
		return nil, err
	}

	p := &Partial{
		store:  b.store,
		branch: b,
		head:   head,
		state:  state,
		scope:  scope.normalized(),
		items:  map[ItemKey]Item{},
		refs:   map[ItemKey]ItemRef{},
	}
	if head == nil {
		return p, nil
	}
	if err := p.load(ctx, p.scope); err != nil {
		return nil, err
	}
	return p, nil
}

func (p *Partial) load(ctx context.Context, scope Scope) error {
	payload, err := p.store.openPayload(ctx, p.head)
	if err != nil {
		return err
	}
	root, err := ChunkIDFromBytes(payload.GetManifestRoot())
	if err != nil {
		return err
	}

	reader := NewManifestReader(p.store.sealer, root, p.store.fetchNode(ctx)).
		WithPrefetch(func(ids []ChunkID) error { return p.store.warm(ctx, ids) })

	var refs []ItemRef
	for _, r := range scope.Ranges {
		got, err := reader.Range(r.From, r.To)
		if err != nil {
			return err
		}
		refs = append(refs, got...)
	}

	ids := make([]ChunkID, 0, len(refs))
	for _, r := range refs {
		if _, ok := p.items[r.Key]; ok {
			continue
		}
		ids = append(ids, r.ID)
	}
	if err := p.store.warm(ctx, ids); err != nil {
		return err
	}

	for _, r := range refs {
		if _, ok := p.items[r.Key]; ok {
			continue
		}
		plaintext, err := p.store.chunk(ctx, r.ID)
		if err != nil {
			return err
		}
		frag := p.store.layout.New()
		if err := proto.Unmarshal(plaintext, frag.Interface()); err != nil {
			return fmt.Errorf("protovcs: item %q did not parse: %w", r.Key, err)
		}
		p.items[r.Key] = Item{Key: r.Key, Fragment: frag.Interface()}
		p.refs[r.Key] = r
	}
	return nil
}

// Scope returns what this handle covers.
func (p *Partial) Scope() Scope { return p.scope }

// Extend widens the scope, fetching what the new part needs.
//
// Explicit because widening costs a round trip, and a caller that does it in a
// loop should be able to see that it does.
func (p *Partial) Extend(ctx context.Context, more Scope) error {
	if p.head == nil {
		p.scope = Union(p.scope, more)
		return nil
	}
	if err := p.load(ctx, more.normalized()); err != nil {
		return err
	}
	p.scope = Union(p.scope, more)
	return nil
}

// Items returns the items this handle holds, in key order.
func (p *Partial) Items() []Item {
	keys := make([]ItemKey, 0, len(p.items))
	for k := range p.items {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })

	out := make([]Item, 0, len(keys))
	for _, k := range keys {
		out = append(out, p.items[k])
	}
	return out
}

// Materialized returns the part of the message this handle holds.
//
// The result is a complete-looking instance of the root message that is not
// complete. That is the hazard scoped types generated from a layout exist to
// remove: this method hands one out because the dynamic handle has no type to
// express the difference, so callers reaching for it should know what they have.
// Anything that is only correct over the whole store must check Scope first.
func (p *Partial) Materialized() (proto.Message, error) {
	return p.store.layout.Merge(p.Items())
}

// Require refuses unless the scope covers what a caller is about to do.
//
// The runtime form of what a generated scoped type does at compile time. One
// check at the boundary, and nothing after it has to wonder.
func (p *Partial) Require(needed Scope) error {
	if !p.scope.Covers(needed) {
		return fmt.Errorf("%w: holding %s, need %s", ErrOutOfScope, p.scope, needed)
	}
	return nil
}

// Get returns one item, or ErrOutOfScope when it was never fetched.
//
// Distinguishing "not fetched" from "not there" is the point. An item outside
// the scope returns an error; an item inside it that does not exist returns
// false, which means there is genuinely nothing there.
func (p *Partial) Get(key ItemKey) (Item, bool, error) {
	if !p.scope.Holds(key) {
		return Item{}, false, fmt.Errorf("%w: %q, holding %s", ErrOutOfScope, key, p.scope)
	}
	it, ok := p.items[key]
	return it, ok, nil
}

// Put replaces or adds one item.
//
// Allowed outside the scope, because the cost is bounded: the writer knows
// which item an element belongs to without reading anything, and carrying an
// untouched item across costs nothing. Reads are the operations that must stay
// inside the scope, because their cost is not bounded.
func (p *Partial) Put(item Item) error {
	if got := item.Fragment.ProtoReflect().Descriptor().FullName(); got != p.store.layout.Descriptor().FullName() {
		return fmt.Errorf("protovcs: item %q holds %s, not %s",
			item.Key, got, p.store.layout.Descriptor().FullName())
	}
	p.items[item.Key] = item
	p.scope = Union(p.scope, Scope{Ranges: []KeyRange{{From: item.Key, To: item.Key}}})
	return nil
}

// Commit publishes the changed items as a new version.
//
// A partial checkout can produce a complete, valid commit. Every item outside
// the scope is carried across by chunk id, without being fetched, because the
// manifest names it and the chunk is already stored. This is the same property
// a partial clone of a git repository has.
func (p *Partial) Commit(ctx context.Context, meta CommitMeta) (CommitID, error) {
	s := p.store

	// Only what this handle changed. Listing every item the parent held, to
	// rebuild a tree that is identical everywhere else, is linear in the store
	// for a constant amount of work — which is the whole cost partial checkout
	// exists to avoid, paid back on the way out.
	changes := map[ItemKey]*ItemRef{}
	pending := map[ChunkID][]byte{}

	for _, it := range p.Items() {
		canonical, err := CanonicalBytes(it.Fragment)
		if err != nil {
			return CommitID{}, fmt.Errorf("protovcs: encoding item %q: %w", it.Key, err)
		}
		if len(canonical) == 0 {
			// An item emptied by the caller is a deletion.
			changes[it.Key] = nil
			continue
		}

		id := s.sealer.ChunkID(RoleItem, string(it.Key), canonical)
		if was, ok := p.refs[it.Key]; ok && was.ID == id {
			// Checked out and not touched. Carried across by name, which is
			// what it already is in the parent's manifest.
			continue
		}
		pending[id] = canonical
		changes[it.Key] = &ItemRef{
			Key:      it.Key,
			ID:       id,
			Bytes:    uint64(len(canonical)),
			Elements: elementCount(s.layout, it.Fragment),
		}
	}

	if err := s.layout.checkChangedItemSizes(changes); err != nil {
		return CommitID{}, err
	}

	var oldRoot ChunkID
	if p.head != nil {
		payload, err := s.openPayload(ctx, p.head)
		if err != nil {
			return CommitID{}, err
		}
		if oldRoot, err = ChunkIDFromBytes(payload.GetManifestRoot()); err != nil {
			return CommitID{}, err
		}
	}

	nodes, root, err := UpdateManifest(s.sealer, oldRoot, changes, s.fetchNode(ctx))
	if err != nil {
		return CommitID{}, err
	}
	for _, n := range nodes {
		pending[n.ID] = n.Canonical
	}
	return p.branch.publishBuilt(ctx, meta, parentsOf(p.head), p.state, nodes, root, pending)
}

// PutElement stages one element into whichever item its shard key names.
//
// The item is a pure function of the element, so this needs no search. It does
// need the item itself, because appending to one that is already in the store
// without reading it would drop what is already there — so an item that was not
// checked out is fetched, which is one item and the nodes above it.
//
// That fetch is why this takes a context and Put does not. A write's cost is
// bounded by the element; this is the bound.
func (p *Partial) PutElement(ctx context.Context, field string, elem proto.Message) error {
	key, err := p.store.layout.ItemKeyFor(field, elem)
	if err != nil {
		return err
	}

	if _, held := p.items[key]; !held && p.head != nil {
		// Not resident. It may exist in the store, and appending without it
		// would lose whatever it holds.
		if err := p.load(ctx, Scope{Ranges: []KeyRange{{From: key, To: key}}}); err != nil {
			return err
		}
	}

	item, held := p.items[key]
	if !held {
		item = Item{Key: key, Fragment: p.store.layout.New().Interface()}
	}

	split := p.store.layout.splitFor(field)
	if split == nil {
		return fmt.Errorf("protovcs: layout %q does not split %q", p.store.layout.Layout().GetName(), field)
	}
	listAtOrCreate(item.Fragment.ProtoReflect(), split.path).
		Append(protoreflect.ValueOfMessage(proto.Clone(elem).ProtoReflect()))

	return p.Put(item)
}
