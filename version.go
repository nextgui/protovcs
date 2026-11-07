package protovcs

import (
	"context"
	"fmt"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"

	pb "github.com/nextgui/protovcs/proto/protovcs/v1"
)

// SchemaOf returns the layout and message a commit was written under, read from
// the store rather than from anything compiled in.
//
// This is what makes retiring a schema possible. A commit carries the layout
// and the descriptors it was written with, so deleting the generated Go for an
// old version does not make the commits written under it unreadable — it only
// means they are read as dynamic messages rather than typed ones.
func (s *Store) SchemaOf(ctx context.Context, commit *pb.Commit) (*pb.SchemaChunk, error) {
	payload, err := s.openPayloadUnchecked(ctx, commit)
	if err != nil {
		return nil, err
	}
	id, err := ChunkIDFromBytes(payload.GetSchemaChunk())
	if err != nil {
		return nil, err
	}
	plaintext, err := s.chunk(ctx, id)
	if err != nil {
		return nil, err
	}

	schema := &pb.SchemaChunk{}
	if err := proto.Unmarshal(plaintext, schema); err != nil {
		return nil, fmt.Errorf("protovcs: commit %s has an unreadable schema chunk: %w", commit.GetId(), err)
	}
	// A schema chunk that disagrees with itself is a corrupted one.
	if schema.GetLayout().GetRootMessage() != schema.GetRootMessage() {
		return nil, fmt.Errorf("protovcs: schema chunk names %s but its layout is for %s",
			schema.GetRootMessage(), schema.GetLayout().GetRootMessage())
	}

	// Different canonical rules read every chunk correctly and name none of them
	// the same way. A writer would find nothing already present and re-upload
	// the store while believing it had written a delta, so this refuses instead.
	// Zero means a commit written before the version was recorded.
	if v := schema.GetCanonicalVersion(); v != 0 && v != CanonicalVersion {
		return nil, fmt.Errorf(
			"%w: written under canonical encoding rules %d, this build has %d;"+
				" every chunk would be renamed and the whole store re-uploaded",
			ErrUnsupportedFormat, v, CanonicalVersion)
	}
	return schema, nil
}

// ReadGeneric reads a commit under the schema the commit itself carries.
//
// For a version this build has no generated code for. The result is a dynamic
// message: enough to export it, verify it, or convert it, and not enough to run
// business logic that expects a particular Go type. That distinction is the
// point — a build that no longer understands a version should not be able to
// act on it by accident.
func (s *Store) ReadGeneric(ctx context.Context, commit *pb.Commit) (proto.Message, *Compiled, error) {
	schema, err := s.SchemaOf(ctx, commit)
	if err != nil {
		return nil, nil, err
	}

	files, err := protodesc.NewFiles(schema.GetDescriptors())
	if err != nil {
		return nil, nil, fmt.Errorf("protovcs: the stored descriptors did not resolve: %w", err)
	}
	desc, err := files.FindDescriptorByName(protoreflect.FullName(schema.GetRootMessage()))
	if err != nil {
		return nil, nil, fmt.Errorf("protovcs: the stored descriptors do not define %s: %w", schema.GetRootMessage(), err)
	}
	msgDesc, ok := desc.(protoreflect.MessageDescriptor)
	if !ok {
		return nil, nil, fmt.Errorf("protovcs: %s is not a message", schema.GetRootMessage())
	}

	layout, err := CompileLayout(schema.GetLayout(), msgDesc)
	if err != nil {
		return nil, nil, fmt.Errorf("protovcs: the stored layout did not compile: %w", err)
	}

	// A second store over the same transport and key, reading through the
	// stored schema rather than this build's. The cache is shared, because a
	// chunk's name is a fingerprint and does not depend on which schema opened
	// it.
	generic := &Store{
		transport: s.transport,
		sealer:    s.sealer,
		layout:    layout,
		memo:      s.memo,
		// The cache too: it is keyed by chunk name, like the memo.
		chunkCache: s.chunkCache,
		highWater:  map[string]CommitID{},
	}
	msg, err := generic.readCommit(ctx, commit)
	if err != nil {
		return nil, nil, err
	}
	return msg, layout, nil
}

// Replace publishes a message assembled by a producer, without ever holding it
// whole.
//
// A re-normalisation produces a new instance of the entire message, and at the
// sizes protovcs exists for it does not fit in memory. The producer yields
// elements; the writer materialises one item at a time, fingerprints it, asks
// whether the store already has it, and discards it.
//
// Elements must arrive in item key order. Out of order would mean holding every
// item open at once, which is the thing being avoided, so it is reported rather
// than absorbed.
func (b *Branch) Replace(ctx context.Context, meta CommitMeta,
	produce func(yield func(field string, elem proto.Message) error) error) (CommitID, error) {

	s := b.store

	head, state, err := b.Head(ctx)
	if err != nil && !isNoSuchRef(err) {
		return CommitID{}, err
	}
	if isNoSuchRef(err) {
		head, state = nil, RefState{}
	}

	pending := map[ChunkID][]byte{}
	var refs []ItemRef

	var currentKey ItemKey
	var current proto.Message
	haveCurrent := false

	flush := func() error {
		if !haveCurrent {
			return nil
		}
		canonical, err := CanonicalBytes(current)
		if err != nil {
			return fmt.Errorf("protovcs: encoding item %q: %w", currentKey, err)
		}
		id := s.sealer.ChunkID(RoleItem, string(currentKey), canonical)
		pending[id] = canonical
		refs = append(refs, ItemRef{
			Key:      currentKey,
			ID:       id,
			Bytes:    uint64(len(canonical)),
			Elements: elementCount(s.layout, current),
		})
		haveCurrent = false
		return nil
	}

	err = produce(func(field string, elem proto.Message) error {
		key, err := s.layout.ItemKeyFor(field, elem)
		if err != nil {
			return err
		}
		if haveCurrent && key < currentKey {
			return fmt.Errorf("protovcs: Replace was given item %q after %q; elements must arrive in item key order", key, currentKey)
		}
		if haveCurrent && key != currentKey {
			if err := flush(); err != nil {
				return err
			}
		}
		if !haveCurrent {
			currentKey = key
			current = s.layout.New().Interface()
			haveCurrent = true
		}

		split := s.layout.splitFor(field)
		if split == nil {
			return fmt.Errorf("protovcs: layout %q does not split %q", s.layout.Layout().GetName(), field)
		}
		listAtOrCreate(current.ProtoReflect(), split.path).
			Append(protoreflect.ValueOfMessage(proto.Clone(elem).ProtoReflect()))
		return nil
	})
	if err != nil {
		return CommitID{}, err
	}
	if err := flush(); err != nil {
		return CommitID{}, err
	}

	// Items produced this way are already grouped, but the elements inside one
	// item arrived in whatever order the producer chose. Canonical order is
	// what the name depends on, so each item is normalised before it is named.
	for i := range refs {
		// The fragment is not kept after flush, so re-canonicalising means
		// re-materialising it from its own bytes. Cheap, and it keeps one
		// definition of canonical order rather than two.
		frag := s.layout.New()
		if err := proto.Unmarshal(pending[refs[i].ID], frag.Interface()); err != nil {
			return CommitID{}, err
		}
		items, err := s.layout.Materialize(frag.Interface())
		if err != nil {
			return CommitID{}, err
		}
		for _, it := range items {
			if it.Key != refs[i].Key {
				continue
			}
			canonical, err := CanonicalBytes(it.Fragment)
			if err != nil {
				return CommitID{}, err
			}
			id := s.sealer.ChunkID(RoleItem, string(it.Key), canonical)
			delete(pending, refs[i].ID)
			pending[id] = canonical
			refs[i].ID = id
			refs[i].Bytes = uint64(len(canonical))
		}
	}

	return b.publishRefs(ctx, meta, parentsOf(head), state, refs, pending)
}

// Relayout rewrites a branch under a different layout.
//
// Same messages, different item keys — day batching becoming month batching, or
// a shard key component being added. No application code: the elements are read
// out under the old layout and written back under the new one.
//
// It writes to the branch it is called on, so the usual shape is to relayout a
// branch created for the purpose, check the result, and then move the one that
// matters.
func (b *Branch) Relayout(ctx context.Context, meta CommitMeta, next *Compiled) (CommitID, error) {
	current, err := b.Read(ctx)
	if err != nil {
		return CommitID{}, err
	}

	rewritten := &Store{
		transport:  b.store.transport,
		sealer:     b.store.sealer,
		layout:     next,
		memo:       b.store.memo,
		chunkCache: b.store.chunkCache,
		highWater:  map[string]CommitID{},
	}
	return rewritten.Branch(b.name).Write(ctx, meta, current)
}

// Truncate makes a commit a new root, dropping its history.
//
// What makes retiring a schema final. Keeping history means old commits stay at
// their old version and remain readable only generically; truncating means no
// commit below the new version exists at all, so the code that read it can be
// deleted and the chunks it held are collected.
//
// Irreversible once collection has run. Until then the old commits are still
// stored and a ref can be pointed back at them.
func (b *Branch) Truncate(ctx context.Context, meta CommitMeta) (CommitID, error) {
	head, state, err := b.Head(ctx)
	if err != nil {
		return CommitID{}, err
	}

	current, err := b.store.readCommit(ctx, head)
	if err != nil {
		return CommitID{}, err
	}

	items, err := b.store.layout.Materialize(current)
	if err != nil {
		return CommitID{}, err
	}

	pending := map[ChunkID][]byte{}
	refs := make([]ItemRef, 0, len(items))
	for _, it := range items {
		canonical, err := CanonicalBytes(it.Fragment)
		if err != nil {
			return CommitID{}, err
		}
		id := b.store.sealer.ChunkID(RoleItem, string(it.Key), canonical)
		pending[id] = canonical
		refs = append(refs, ItemRef{
			Key:      it.Key,
			ID:       id,
			Bytes:    uint64(len(canonical)),
			Elements: elementCount(b.store.layout, it.Fragment),
		})
	}

	// No parents. Every chunk is therefore in the new commit's added set, so a
	// collector walking refs finds all of them and nothing that only the old
	// history reached.
	//
	// Forced, because the new root descends from nothing and the server refuses
	// that by default. Truncating history is exactly the case the refusal is
	// there to make deliberate.
	return b.publishRefsForced(ctx, meta, nil, state, refs, pending, true)
}
