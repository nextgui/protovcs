package protovcs

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	mathrand "math/rand/v2"
	"sync"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	pb "github.com/nextgui/protovcs/proto/protovcs/v1"
)

// Store is one versioned message on a server that cannot read it.
type Store struct {
	transport Transport
	sealer    Sealer
	layout    *Compiled

	mu sync.Mutex

	// The newest commit seen on each branch. Encryption cannot detect a server
	// replaying an older head, because the old commit is genuine, so the client
	// remembers where it has been.
	highWater map[string]CommitID

	// Chunks and commits by their own names, shared with any store derived
	// from this one. It is what makes a second full scan free, what makes a
	// retry after a lost race cost only the items the winner changed, and what
	// makes [Store.ReadAt] cost nothing over the network once the commit has
	// been seen. See memo.go.
	memo *memo

	// Attached by tests to stop or fail an operation part-way. Nil everywhere
	// else, and checked rather than compiled out — see probe.go.
	probe Probe

	// The branch whose head is hinted after a full read, or empty, and the
	// last commit hinted, so the same head is hinted once.
	hint   string
	hinted CommitID

	// Where chunks are kept between store instances, or nil. See chunkcache.go.
	chunkCache ChunkCache

	// Where highWater is kept between store instances, or nil, and what this
	// store last read from or wrote to it. See headmarks.go.
	headMarks   HeadMarks
	stored      map[string]CommitID
	loadMu      sync.Mutex
	marksLoaded bool
	saveMu      sync.Mutex
}

// StoreBuilder assembles a [Store].
type StoreBuilder struct {
	transport  Transport
	sealer     Sealer
	layout     *Compiled
	probe      Probe
	hint       string
	chunkCache ChunkCache
	headMarks  HeadMarks
}

// NewBuilder returns a builder with nothing set.
func NewBuilder() *StoreBuilder { return &StoreBuilder{} }

// WithTransport sets how the store is reached. Required.
func (b *StoreBuilder) WithTransport(t Transport) *StoreBuilder { b.transport = t; return b }

// WithSealer sets how chunks are named and protected. Required: a store must
// say whether it has a key rather than defaulting to one answer.
func (b *StoreBuilder) WithSealer(s Sealer) *StoreBuilder { b.sealer = s; return b }

// WithLayout sets how the message is decomposed. Required.
func (b *StoreBuilder) WithLayout(c *Compiled) *StoreBuilder { b.layout = c; return b }

// WithPackHints makes the store tell the server, after each full read of
// branch's head, which chunks that head reaches, so the server can pack them
// together for the next cold read.
func (b *StoreBuilder) WithPackHints(branch string) *StoreBuilder { b.hint = branch; return b }

// Build returns the store, or reports what is missing.
func (b *StoreBuilder) Build() (*Store, error) {
	switch {
	case b.transport == nil:
		return nil, fmt.Errorf("protovcs: a store needs a transport")
	case b.sealer == nil:
		return nil, fmt.Errorf("protovcs: a store needs a sealer; use PlainSealer for one with no key")
	case b.layout == nil:
		return nil, fmt.Errorf("protovcs: a store needs a layout")
	}
	return &Store{
		transport:  b.transport,
		sealer:     b.sealer,
		layout:     b.layout,
		probe:      b.probe,
		hint:       b.hint,
		chunkCache: b.chunkCache,
		highWater:  map[string]CommitID{},
		headMarks:  b.headMarks,
		stored:     map[string]CommitID{},
		memo:       newMemo(),
	}, nil
}

// Layout returns the compiled layout this store was opened with.
func (s *Store) Layout() *Compiled { return s.layout }

// Branch returns a handle on one branch. It does not check that the branch
// exists: a branch with no commits is how a store that has never been written
// to reads, and that is what makes the first write like any other.
func (s *Store) Branch(name string) *Branch {
	return &Branch{store: s, name: name, refName: s.sealer.RefName(name)}
}

// Branch is one line of history.
type Branch struct {
	store   *Store
	name    string
	refName []byte
}

// Name returns the branch's readable name.
func (b *Branch) Name() string { return b.name }

// Head returns the commit this branch points at, or ErrNoSuchRef.
//
// It refuses a head older than one this Store has already returned, or than
// the one recorded in its [HeadMarks], with ErrWentBackwards. A head on a
// different line of history passes, which is what a forced move such as
// [Branch.Truncate] produces. A Store built without HeadMarks remembers in
// memory only, so a new one accepts any head.
func (b *Branch) Head(ctx context.Context) (*pb.Commit, RefState, error) {
	if err := b.store.loadMarks(ctx); err != nil {
		return nil, RefState{}, err
	}
	refs, err := b.store.transport.ListRefs(ctx)
	if err != nil {
		return nil, RefState{}, err
	}

	var state *RefState
	for i := range refs {
		if string(refs[i].Name) == string(b.refName) {
			state = &refs[i]
			break
		}
	}
	if state == nil {
		return nil, RefState{}, fmt.Errorf("%w: %s", ErrNoSuchRef, b.name)
	}

	// A server may serve an older head than one already seen. The old commit is
	// genuine, so nothing about it fails to verify; only remembering catches it.
	b.store.mu.Lock()
	seen, ok := b.store.highWater[b.name]
	b.store.mu.Unlock()

	head, err := b.store.commitByID(ctx, state.Commit)
	if err != nil {
		return nil, RefState{}, err
	}

	if ok && seen != state.Commit {
		// Backwards means the offered head is an ancestor of one already seen.
		// The forward case — the head this client saw is an ancestor of the
		// offered one — is ordinary progress, including a retry after losing a
		// race, so asking the question the other way round would refuse every
		// normal read.
		//
		// The forward case is checked first because it is cheap: the walk from
		// the offered head stops at the one already seen, so it costs the
		// commits made since. The backwards walk from the one already seen
		// never meets a newer head and so visits the whole history. It is only
		// needed when the forward walk fails, which is a server serving an old
		// head or a branch that was forced elsewhere.
		ahead, err := b.store.isAncestor(ctx, state.Commit, seen)
		if err != nil {
			return nil, RefState{}, err
		}
		behind := false
		if !ahead {
			if behind, err = b.store.isAncestor(ctx, seen, state.Commit); err != nil {
				return nil, RefState{}, err
			}
		}
		if behind {
			return nil, RefState{}, fmt.Errorf("%w: %s is behind %s on branch %s",
				ErrWentBackwards, state.Commit, seen, b.name)
		}
	}

	if err := b.raiseHighWater(ctx, seen, ok, state.Commit); err != nil {
		return nil, RefState{}, err
	}
	b.store.saveMark(ctx, b.name)

	if err := b.store.probeAt(ctx, ProbeAfterHead); err != nil {
		return nil, RefState{}, err
	}
	return head, *state, nil
}

// raiseHighWater records head as the newest commit seen on this branch, given
// that seen (present when ok) was the newest when the read began.
//
// Another read of the same branch can record a newer head in the meantime.
// Overwriting it would move the mark backwards and let a later replay of the
// older head through, so the mark only moves to a descendant of what it holds.
// A read that lost the race still returns its head: it was the head when it
// was listed.
func (b *Branch) raiseHighWater(ctx context.Context, seen CommitID, ok bool, head CommitID) error {
	for {
		b.store.mu.Lock()
		now, has := b.store.highWater[b.name]
		if has == ok && now == seen {
			b.store.highWater[b.name] = head
			b.store.mu.Unlock()
			return nil
		}
		b.store.mu.Unlock()

		ahead, err := b.store.isAncestor(ctx, head, now)
		if err != nil {
			return err
		}
		if !ahead {
			return nil
		}
		seen, ok = now, has
	}
}

// isAncestor reports whether candidate is reachable from start.
//
// A commit missing from the walk is an error rather than a "no". The server
// never deletes a commit, so one it cannot produce means it is broken or
// hiding history, and answering "not an ancestor" would let the rollback check
// in [Branch.Head] pass a head it should refuse.
func (s *Store) isAncestor(ctx context.Context, start, candidate CommitID) (bool, error) {
	seen := map[CommitID]bool{}
	queue := []CommitID{start}

	// The usual answer is a few commits away, so the first fetch asks for one
	// and each later fetch for twice as many. A walk over n commits then costs
	// about log n round trips, and a short one does not make the server read
	// commits nobody needs.
	depth := uint32(1)

	for len(queue) > 0 {
		id := queue[0]
		queue = queue[1:]
		if seen[id] {
			continue
		}
		seen[id] = true
		if id == candidate {
			return true, nil
		}

		commit, found, fetched, err := s.historyCommit(ctx, id, depth)
		if err != nil {
			return false, err
		}
		if !found {
			return false, fmt.Errorf("%w: commit %s, while walking history", ErrNoSuchCommit, id)
		}
		if fetched {
			depth = min(depth*2, historyBatch)
		}
		for _, raw := range commit.GetParents() {
			pid, err := CommitIDFromBytes(raw)
			if err != nil {
				return false, err
			}
			queue = append(queue, pid)
		}
	}
	return false, nil
}

// historyCommit returns one commit, fetching it with up to depth-1 of its
// ancestors when it is not already held. It reports whether the server had it,
// and whether it went to the server to find out.
//
// Every commit fetched is cached under the name computed from its own
// contents, not under the name the server gave it. A commit's name covers its
// parents, so a server cannot attach a commit to a history it is not part of.
func (s *Store) historyCommit(ctx context.Context, id CommitID, depth uint32) (commit *pb.Commit, found, fetched bool, err error) {
	if held, ok := s.memo.commit(id); ok {
		return held, true, false, nil
	}

	commits, err := s.transport.GetCommits(ctx, id, depth)
	if err != nil {
		return nil, false, true, err
	}

	for _, c := range commits {
		name, err := verifiedName(c)
		if err != nil {
			return nil, false, true, err
		}
		s.memo.putCommit(name, c)
	}
	held, ok := s.memo.commit(id)
	return held, ok, true, nil
}

// Watch calls moved every time this branch points somewhere new, and blocks
// until it is done.
//
// The first call to moved carries where the branch is now, so a caller does not
// have to read the head separately and then reconcile a write that happened in
// between. A branch that does not exist yet produces no first call; moved is
// first called when somebody creates it.
//
// A branch that is deleted produces a call whose RefState is zero — Commit is
// [CommitID.IsZero] and CASToken is empty — which is how a caller tells
// deletion from a branch that has simply not moved.
//
// moved is only called for this branch. The server reports every branch and the
// filtering is here, so watching one branch is not something a caller tells the
// server. Returning an error from moved ends the watch and is what Watch
// returns.
//
// Watch reports movement and reads nothing. It does not check the head against
// what this client has already seen, so a server that moved a branch backwards
// is reported rather than refused; the refusal is in [Branch.Head], which is
// what a caller reaching for the new contents goes through.
func (b *Branch) Watch(ctx context.Context, moved func(RefState) error) error {
	var last *RefState

	return b.store.transport.WatchRefs(ctx, func(refs []RefState) error {
		var now *RefState
		for i := range refs {
			if string(refs[i].Name) == string(b.refName) {
				now = &refs[i]
				break
			}
		}

		// A branch that has never existed is not news. One that existed a
		// moment ago and does not now is, and it is reported as a zero state.
		if now == nil {
			// Reported once: a branch already reported deleted has not moved
			// again just because some other branch did.
			if last == nil || last.Commit.IsZero() {
				return nil
			}
			last = &RefState{Name: append([]byte(nil), b.refName...)}
			return moved(*last)
		}
		if last != nil && last.Commit == now.Commit && last.CASToken == now.CASToken {
			return nil
		}

		state := *now
		last = &state
		return moved(state)
	})
}

// Read returns the whole message at this branch's head.
//
// An empty message when the branch does not exist, because a store that has
// never been written to reads as one holding nothing.
func (b *Branch) Read(ctx context.Context) (proto.Message, error) {
	head, _, err := b.Head(ctx)
	if err != nil {
		if isNoSuchRef(err) {
			return b.store.layout.New().Interface(), nil
		}
		return nil, err
	}
	return b.store.readCommit(ctx, head)
}

// KnownHead is where a branch was when this store last heard, and whether it
// has heard at all.
//
// It reports what is remembered and asks nobody, so it is current only to the
// extent that something is keeping it current — a [Branch.Watch], or a writer
// recording its own commit. A caller that needs the truth calls [Branch.Head],
// which asks the server. Marks in the store's [HeadMarks] count once a read or
// write has loaded them.
func (s *Store) KnownHead(branch string) (CommitID, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	id, ok := s.highWater[branch]
	return id, ok
}

// ReadAt materialises the message as it was at one commit.
//
// Nothing here asks where a branch points, which is the difference from
// [Branch.Read] and the reason this exists: a caller that has to produce
// several figures from one state of the store reads each of them at the same
// commit, and they agree. Resolving the head separately per read cannot
// promise that, because the head can move in between.
//
// The commit and the chunks under it are both cached by their own hashes, so a
// second read at the same commit reaches the network not at all.
//
// A commit the store no longer holds is reported rather than substituted. It
// can go: garbage collection removes what no branch reaches, so a caller
// holding a commit id for a long time may name one that has been collected.
// Answering with a different state than the one asked for would be worse than
// failing, because nothing downstream could tell.
func (s *Store) ReadAt(ctx context.Context, at CommitID) (proto.Message, error) {
	commit, err := s.commitByID(ctx, at)
	if err != nil {
		return nil, err
	}
	return s.readCommit(ctx, commit)
}

// commitByID returns one commit, from the cache when it has been seen before.
func (s *Store) commitByID(ctx context.Context, id CommitID) (*pb.Commit, error) {
	if held, ok := s.memo.commit(id); ok {
		return held, nil
	}

	commits, err := s.transport.GetCommits(ctx, id, 1)
	if err != nil {
		return nil, err
	}
	if len(commits) == 0 {
		return nil, fmt.Errorf("%w: commit %s", ErrNoSuchCommit, id)
	}
	name, err := verifiedName(commits[0])
	if err != nil {
		return nil, err
	}
	if name != id {
		return nil, fmt.Errorf("%w: asked for commit %s and was sent %s", ErrTampered, id, name)
	}

	s.memo.putCommit(id, commits[0])
	return commits[0], nil
}

func (s *Store) readCommit(ctx context.Context, commit *pb.Commit) (proto.Message, error) {
	payload, err := s.openPayload(ctx, commit)
	if err != nil {
		return nil, err
	}

	root, err := ChunkIDFromBytes(payload.GetManifestRoot())
	if err != nil {
		return nil, err
	}

	reader := NewManifestReader(s.sealer, root, s.fetchNode(ctx)).
		WithPrefetch(func(ids []ChunkID) error { return s.warm(ctx, ids) })
	refs, err := reader.All()
	if err != nil {
		return nil, err
	}

	// One batched fetch for every item, rather than one per item. A full scan
	// is the most common operation on a store and doing it as a round trip per
	// item would make the whole design pointless.
	ids := make([]ChunkID, 0, len(refs))
	for _, r := range refs {
		ids = append(ids, r.ID)
	}
	if err := s.warm(ctx, ids); err != nil {
		return nil, err
	}

	items := make([]Item, 0, len(refs))
	for _, r := range refs {
		plaintext, err := s.chunk(ctx, r.ID)
		if err != nil {
			return nil, err
		}
		frag := s.layout.New()
		if err := proto.Unmarshal(plaintext, frag.Interface()); err != nil {
			return nil, fmt.Errorf("protovcs: item %q did not parse: %w", r.Key, err)
		}
		items = append(items, Item{Key: r.Key, Fragment: frag.Interface()})
	}
	merged, err := s.layout.Merge(items)
	if err != nil {
		return nil, err
	}
	s.maybeHint(ctx, commit)
	return merged, nil
}

// maybeHint sends a pack hint when commit is the head of the hinted branch and
// has not been hinted before.
//
// Every chunk it names was fetched by the read that just finished, so building
// the list costs no request, and the server learns nothing that read did not
// show it. The answer is ignored: a hint only makes later reads faster.
func (s *Store) maybeHint(ctx context.Context, commit *pb.Commit) {
	if s.hint == "" {
		return
	}
	id, err := CommitIDFromBytes(commit.GetId())
	if err != nil {
		return
	}
	// Claimed before the hint is built, so reads of the same head that finish
	// together send one hint between them rather than one each.
	s.mu.Lock()
	head, known := s.highWater[s.hint]
	if !known || head != id || s.hinted == id {
		s.mu.Unlock()
		return
	}
	previous := s.hinted
	s.hinted = id
	s.mu.Unlock()

	ids, err := s.ReachableChunks(ctx, commit)
	if err != nil {
		// Released, so a later read can try again.
		s.mu.Lock()
		if s.hinted == id {
			s.hinted = previous
		}
		s.mu.Unlock()
		return
	}
	// Kept whether or not the server took it. A server that cannot take hints
	// would otherwise be sent one on every read.
	_, _ = s.transport.PackHint(ctx, id, ids)
}

// openPayload decrypts a commit's sealed part and checks the schema is one this
// build is willing to read.
func (s *Store) openPayload(ctx context.Context, commit *pb.Commit) (*pb.CommitPayload, error) {
	// Refusing rather than reading what it can. A store read under the wrong
	// schema produces plausible wrong values rather than an obvious failure.
	if min := s.layout.Layout().GetMinReadableVersion(); min != 0 && commit.GetSchemaVersion() < min {
		return nil, fmt.Errorf("%w: commit is schema %d, this build reads %d and above",
			ErrUnsupportedSchema, commit.GetSchemaVersion(), min)
	}
	if commit.GetSchemaVersion() > s.layout.SchemaVersion() {
		return nil, fmt.Errorf("%w: commit is schema %d, this build knows %d",
			ErrUnsupportedSchema, commit.GetSchemaVersion(), s.layout.SchemaVersion())
	}
	return s.openPayloadUnchecked(ctx, commit)
}

// openPayloadUnchecked opens a commit's payload without asking whether this
// build understands its schema.
//
// Used only by the paths that deliberately read an old version: fetching the
// schema a commit was written under, and reading it generically. Everywhere
// else goes through openPayload, because reading a store under the wrong schema
// produces plausible wrong values rather than an obvious failure.
func (s *Store) openPayloadUnchecked(ctx context.Context, commit *pb.Commit) (*pb.CommitPayload, error) {
	if v := commit.GetFormatVersion(); v != 0 && v != FormatVersion {
		return nil, fmt.Errorf("%w: commit format %d", ErrUnsupportedFormat, v)
	}

	id, err := CommitIDFromBytes(commit.GetId())
	if err != nil {
		return nil, err
	}

	// A commit's payload is sealed under the commit's own name, so a payload
	// cannot be moved between commits.
	plaintext, err := s.sealer.Open(ChunkID(id), commit.GetPayload())
	if err != nil {
		return nil, err
	}

	payload := &pb.CommitPayload{}
	if err := proto.Unmarshal(plaintext, payload); err != nil {
		return nil, fmt.Errorf("protovcs: commit %s has an unreadable payload: %w", id, err)
	}
	return payload, nil
}

// fetchNode returns the manifest node reader a ManifestReader needs.
func (s *Store) fetchNode(ctx context.Context) func(string, ChunkID) (*pb.ManifestNode, error) {
	return func(prefix string, id ChunkID) (*pb.ManifestNode, error) {
		plaintext, err := s.chunk(ctx, id)
		if err != nil {
			return nil, err
		}
		node := &pb.ManifestNode{}
		if err := proto.Unmarshal(plaintext, node); err != nil {
			return nil, fmt.Errorf("protovcs: manifest node at %q did not parse: %w", prefix, err)
		}
		return node, nil
	}
}

// chunk returns a chunk's plaintext, from the cache when it is there.
func (s *Store) chunk(ctx context.Context, id ChunkID) ([]byte, error) {
	if plaintext, ok := s.memo.chunk(id); ok {
		return plaintext, nil
	}
	if err := s.warm(ctx, []ChunkID{id}); err != nil {
		return nil, err
	}
	plaintext, ok := s.memo.chunk(id)
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrNoSuchChunk, id)
	}
	return plaintext, nil
}

// warm fetches and opens every chunk not already held, in one request, and
// waits for any another read is already fetching.
func (s *Store) warm(ctx context.Context, ids []ChunkID) error {
	return s.memo.warm(ctx, ids, func(want []ChunkID) (map[ChunkID][]byte, error) {
		return s.fetchChunks(ctx, want)
	})
}

// CommitMeta is what a commit records about who made it and why.
//
// Sealed with the rest of the payload, so a server stores it and reads none of
// it. Every reader of a store's history downloads every commit's metadata, so
// this records what happened rather than copying what changed.
type CommitMeta struct {
	// One line, for a list of commits.
	Title string

	// The rest, for someone reading one commit. Usually empty.
	Description string

	// Who made the change. More than one when a person acted through an agent,
	// or triggered an import a service performed.
	Authors []Author

	// The application build that wrote it, such as "notes-ios/2.14.1".
	Client string

	// Short annotations. Small: every reader of the history downloads them.
	Labels map[string]string

	// The application's own record of the change, of the message type the
	// layout's commit_message names. Nil is always allowed, including in a store
	// that declares one: a change made by a tool with nothing structured to say
	// should not be blocked from being made.
	Details proto.Message
}

// Author is who made a change.
type Author struct {
	Name  string
	Email string

	// What this author did, when a commit has more than one: "author",
	// "committer", "agent", "service". Empty when the question does not arise.
	Role string
}

// Commit is shorthand for the metadata of a change one person made.
//
// The common case, and the one that would otherwise be four lines of struct
// literal at every call site.
func Commit(title, author, email string) CommitMeta {
	return CommitMeta{Title: title, Authors: []Author{{Name: author, Email: email}}}
}

// commitMessage builds the sealed record of why a change was made.
func (s *Store) commitMessage(meta CommitMeta) (*pb.CommitMessage, error) {
	details, err := s.detailBytes(meta)
	if err != nil {
		return nil, err
	}

	msg := &pb.CommitMessage{
		Title:       meta.Title,
		Description: meta.Description,
		Client:      meta.Client,
		Labels:      meta.Labels,
		Details:     details,
	}
	for _, a := range meta.Authors {
		msg.Authors = append(msg.Authors, &pb.Author{Name: a.Name, Email: a.Email, Role: a.Role})
	}
	return msg, nil
}

// detailBytes encodes a commit's details, refusing a message the layout did not
// declare.
//
// A store whose commits carry a type the layout does not name has a history
// only the client that wrote it can read: the descriptors travel in the
// SchemaChunk because the layout named the type, so an undeclared one arrives
// somewhere with no way to resolve it. Refusing at the write is the only point
// where that is still fixable.
func (s *Store) detailBytes(meta CommitMeta) ([]byte, error) {
	if meta.Details == nil {
		return nil, nil
	}

	want := s.layout.CommitMessage()
	if want == nil {
		return nil, fmt.Errorf("protovcs: a commit carries %s and layout %q declares no commit_message",
			meta.Details.ProtoReflect().Descriptor().FullName(), s.layout.Layout().GetName())
	}
	if got := meta.Details.ProtoReflect().Descriptor().FullName(); got != want.FullName() {
		return nil, fmt.Errorf("protovcs: a commit carries %s and layout %q declares commit_message %s",
			got, s.layout.Layout().GetName(), want.FullName())
	}

	// Canonical, so that two clients recording the same change produce the same
	// bytes. A payload is not named by its contents, so nothing breaks if they
	// do not — but a history whose records differ by encoder is one nobody can
	// compare.
	return CanonicalBytes(meta.Details)
}

// DefaultMaxAttempts is how many times [Branch.Change] rebuilds and retries
// before giving up.
//
// The figure has to be read against how many writers contend, not against how
// long anything takes. Each round of contention produces one winner and leaves
// the rest to retry, so a writer among W of them expects to spend about W
// attempts per commit it lands. A budget of 8 therefore failed at four
// concurrent writers — not occasionally, but as soon as each had several
// commits to make, because the budget was smaller than the arithmetic.
//
// This is sized for a dozen or so writers on one branch. Past that a caller
// gets an error rather than a client that never returns, which for a branch
// that busy is the honest answer: the contention is the problem.
const DefaultMaxAttempts = 24

// retryBase is the first delay after a lost race, doubling with each attempt.
//
// Retrying immediately is what a first implementation does and it is wrong with
// more than two writers: they all reread the same head, all rebuild, and all
// lose again together, so the attempt budget is spent without anybody making
// progress. Four concurrent writers exhausted five immediate attempts every
// time; with this they finish in two or three.
const retryBase = 2 * time.Millisecond

// retryCap is the longest window a delay is drawn from.
//
// It has to exceed how long an attempt takes, or backing off does not actually
// separate anybody: writers that wait 200ms between attempts that each take
// longer than that are still overlapping, and collide as reliably as if they
// had not waited. A durable commit is disk or network round trips, so the cap
// is set well above one.
const retryCap = time.Second

// backoff returns how long to wait before an attempt.
//
// Fully jittered rather than exponential-with-a-fixed-delay, because writers
// that back off by the same amount collide again at the same moment. Drawing
// uniformly from the whole window is what actually separates them.
func backoff(attempt int) time.Duration {
	window := retryBase << uint(attempt-1)
	if window > retryCap || window <= 0 {
		window = retryCap
	}
	return time.Duration(mathrand.Int64N(int64(window)) + int64(retryBase))
}

// waitBefore sleeps, and gives up if the caller's context does first.
func waitBefore(ctx context.Context, d time.Duration) error {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

// Change reads the branch, runs mutate, and publishes the result.
//
// mutate runs again when another writer commits first, so it must be safe to
// run more than once against a different starting state. Writing a computed
// value is safe. Incrementing a counter held in the caller's own memory is not.
//
// A retry is cheap: everything the winner did not touch is still in the cache,
// because a chunk's name is a fingerprint of its contents.
func (b *Branch) Change(ctx context.Context, meta CommitMeta, mutate func(context.Context, proto.Message) error) (CommitID, error) {
	var lastErr error
	for attempt := 0; attempt < DefaultMaxAttempts; attempt++ {
		if attempt > 0 {
			if err := waitBefore(ctx, backoff(attempt)); err != nil {
				return CommitID{}, err
			}
		}

		head, state, err := b.Head(ctx)
		switch {
		case err == nil:
		case isNoSuchRef(err):
			head, state = nil, RefState{}
		default:
			return CommitID{}, err
		}

		var current proto.Message
		if head == nil {
			current = b.store.layout.New().Interface()
		} else {
			if current, err = b.store.readCommit(ctx, head); err != nil {
				return CommitID{}, err
			}
		}

		if err := mutate(ctx, current); err != nil {
			return CommitID{}, err
		}

		id, err := b.publish(ctx, meta, head, state, current)
		if err == nil {
			return id, nil
		}
		if !isRaceLost(err) {
			return CommitID{}, err
		}
		lastErr = err
	}
	return CommitID{}, fmt.Errorf("protovcs: gave up after %d attempts on branch %s: %w",
		DefaultMaxAttempts, b.name, lastErr)
}

// Write publishes a message as the branch's next version, failing rather than
// retrying if somebody else wrote first.
//
// Change is what most callers want. This is for a caller that has already
// decided what the next version is and wants to know if it lost.
func (b *Branch) Write(ctx context.Context, meta CommitMeta, m proto.Message) (CommitID, error) {
	head, state, err := b.Head(ctx)
	if err != nil && !isNoSuchRef(err) {
		return CommitID{}, err
	}
	if isNoSuchRef(err) {
		head, state = nil, RefState{}
	}
	return b.publish(ctx, meta, head, state, m)
}

// publish materializes a message, uploads what the store does not already hold,
// records a commit, and moves the branch.
//
// The order matters and is the same discipline git uses. Chunks first: they are
// immutable and content-addressed, so nothing can conflict with them. The ref
// last. A crash in between leaves chunks nothing points at, which is garbage.
// The reverse order would leave a branch pointing at something incomplete,
// which is a corrupt store.
func (b *Branch) publish(ctx context.Context, meta CommitMeta, parent *pb.Commit, state RefState, m proto.Message) (CommitID, error) {
	s := b.store

	items, err := s.layout.Materialize(m)
	if err != nil {
		return CommitID{}, err
	}

	pending := map[ChunkID][]byte{}
	refs := make([]ItemRef, 0, len(items))
	for _, it := range items {
		canonical, err := CanonicalBytes(it.Fragment)
		if err != nil {
			return CommitID{}, fmt.Errorf("protovcs: encoding item %q: %w", it.Key, err)
		}
		id := s.sealer.ChunkID(RoleItem, string(it.Key), canonical)
		pending[id] = canonical
		refs = append(refs, ItemRef{
			Key:      it.Key,
			ID:       id,
			Bytes:    uint64(len(canonical)),
			Elements: elementCount(s.layout, it.Fragment),
		})
	}

	return b.publishRefs(ctx, meta, parentsOf(parent), state, refs, pending)
}

// parentsOf turns an optional single parent into the list publishRefs takes.
func parentsOf(parent *pb.Commit) []*pb.Commit {
	if parent == nil {
		return nil
	}
	return []*pb.Commit{parent}
}

// publishRefs stores what is new and records a version naming every item.
//
// Separate from publish so that a partial checkout can use it: it is handed the
// item list directly, and the items it did not change are carried across by
// chunk id without ever being fetched.
func (b *Branch) publishRefs(ctx context.Context, meta CommitMeta, parents []*pb.Commit, state RefState,
	refs []ItemRef, pending map[ChunkID][]byte) (CommitID, error) {
	return b.publishRefsForced(ctx, meta, parents, state, refs, pending, false)
}

// publishRefsForced is publishRefs with the branch allowed to leave its own
// history. Only Truncate wants that, and it says so at the call site.
func (b *Branch) publishRefsForced(ctx context.Context, meta CommitMeta, parents []*pb.Commit, state RefState,
	refs []ItemRef, pending map[ChunkID][]byte, force bool) (CommitID, error) {

	s := b.store

	// Checked before anything is uploaded, so a layout that lets an item grow
	// without bound is reported as the layout problem it is rather than as a
	// mysterious refusal from the server later.
	for _, r := range refs {
		if err := s.layout.CheckItemSize(r.Key, r.Bytes, r.Elements); err != nil {
			return CommitID{}, err
		}
	}

	nodes, err := BuildManifest(s.sealer, refs)
	if err != nil {
		return CommitID{}, err
	}
	for _, n := range nodes {
		pending[n.ID] = n.Canonical
	}
	return b.publishBuiltForced(ctx, meta, parents, state, nodes, nodes[len(nodes)-1].ID, pending, force)
}

// publishBuilt records a version whose manifest is already constructed.
//
// Separate from publishRefs so a partial checkout can hand over a tree it
// updated in place rather than one rebuilt from every item in the store.
func (b *Branch) publishBuilt(ctx context.Context, meta CommitMeta, parents []*pb.Commit, state RefState,
	nodes []ManifestChunk, rootID ChunkID, pending map[ChunkID][]byte) (CommitID, error) {
	return b.publishBuiltForced(ctx, meta, parents, state, nodes, rootID, pending, false)
}

// publishBuiltForced is publishBuilt with the branch allowed to leave its own
// history. Only Truncate wants that, and it says so at the call site.
func (b *Branch) publishBuiltForced(ctx context.Context, meta CommitMeta, parents []*pb.Commit, state RefState,
	nodes []ManifestChunk, rootID ChunkID, pending map[ChunkID][]byte, force bool) (CommitID, error) {

	s := b.store

	message, err := s.commitMessage(meta)
	if err != nil {
		return CommitID{}, err
	}
	stored, err := s.recordVersion(ctx, versionSpec{
		branch:  b.name,
		message: message,
		at:      timestamppb.New(time.Now().UTC()),
	}, parents, nodes, rootID, pending)
	if err != nil {
		return CommitID{}, err
	}

	// A complete version exists and no branch reaches it. The dangerous half of
	// the same window: the reverse order would leave a branch pointing at
	// something incomplete, which is a corrupt store rather than an unreferenced
	// one.
	if err := s.probeAt(ctx, ProbeAfterCommit); err != nil {
		return CommitID{}, err
	}
	if err := s.probeAt(ctx, ProbeBeforeRefUpdate); err != nil {
		return CommitID{}, err
	}

	if _, err := s.transport.UpdateRef(ctx, b.refName, stored, state.CASToken, force); err != nil {
		return CommitID{}, err
	}

	s.memo.putChunks(pending)
	s.mu.Lock()
	s.highWater[b.name] = stored
	s.mu.Unlock()
	s.saveMark(ctx, b.name)

	return stored, nil
}

// versionSpec is what a commit's sealed payload says beyond its tree: which
// branch it was written on, why, and when.
type versionSpec struct {
	branch  string
	message *pb.CommitMessage
	at      *timestamppb.Timestamp
}

// recordVersion uploads what a version adds and records the commit, without
// moving any branch. It returns the commit's name.
//
// Separate from publishBuiltForced so that [Store.Rewrite] can record a
// rewritten commit with the payload of the one it replaces and move the
// branches only once the whole history is rewritten.
func (s *Store) recordVersion(ctx context.Context, spec versionSpec, parents []*pb.Commit,
	nodes []ManifestChunk, rootID ChunkID, pending map[ChunkID][]byte) (CommitID, error) {

	schemaID, schemaBytes, err := s.schemaChunk()
	if err != nil {
		return CommitID{}, err
	}
	pending[schemaID] = schemaBytes

	// Which chunks this version adds, relative to the parent. The server uses
	// these to collect garbage and to answer what a client is missing, so they
	// have to be the difference rather than the whole set: repeating the whole
	// set of a large store on every commit would be tens of megabytes each.
	//
	// Computed by descending the two manifests together and stopping wherever a
	// subtree's name matches. Listing every item both versions hold and
	// subtracting would be correct and linear in the store, which is a cost that
	// only shows up on a vault nobody has yet.
	added, err := s.addedSince(ctx, nodes, rootID, parents, schemaID)
	if err != nil {
		return CommitID{}, err
	}

	// Ask what is missing first, then seal exactly that, then reserve the real
	// figure. Asking is a read and needs no reservation; sealing is local and
	// cheap. Reserving before either would mean reserving for chunks the store
	// already holds, and reserving the plaintext size would be short by the
	// envelope on every chunk.
	missing, err := s.transport.HasChunks(ctx, added)
	if err != nil {
		return CommitID{}, err
	}

	batch := make([]SealedChunk, 0, len(missing))
	var uploadBytes uint64
	for _, id := range missing {
		sealed, err := s.sealer.Seal(id, pending[id])
		if err != nil {
			return CommitID{}, err
		}
		batch = append(batch, SealedChunk{ID: id, Sealed: sealed})
		uploadBytes += uint64(len(sealed))
	}

	reservation, err := s.transport.BeginWrite(ctx, uint32(len(batch)), uploadBytes)
	if err != nil {
		return CommitID{}, err
	}

	if len(batch) > 0 {
		if err := s.transport.PutChunks(ctx, reservation, batch); err != nil {
			return CommitID{}, err
		}
		// Kept as soon as the server has them. A commit that then loses its
		// race leaves them unreferenced here as it does on the server, and
		// eviction is what removes them.
		s.keepChunks(ctx, batch)
	}

	// Everything is stored and nothing points at it yet. A failure here is the
	// crash the ordering exists for: what is left is garbage, not damage.
	if err := s.probeAt(ctx, ProbeAfterUpload); err != nil {
		return CommitID{}, err
	}

	payload := &pb.CommitPayload{
		ManifestRoot: rootID.Bytes(),
		SchemaChunk:  schemaID.Bytes(),
		TreeDigest:   nodeDigest(nodes, rootID),
		Branch:       spec.branch,
		Message:      spec.message,
		Time:         spec.at,
	}
	payloadBytes, err := CanonicalBytes(payload)
	if err != nil {
		return CommitID{}, err
	}

	nonce := make([]byte, 16)
	if _, err := rand.Read(nonce); err != nil {
		return CommitID{}, fmt.Errorf("protovcs: drawing a commit nonce: %w", err)
	}
	commit := &pb.Commit{
		FormatVersion: FormatVersion,
		SchemaVersion: s.layout.SchemaVersion(),
		Added:         chunkIDBytes(added),
		NamingNonce:   nonce,
	}
	for _, parent := range parents {
		commit.Parents = append(commit.Parents, parent.GetId())
	}

	// The payload is sealed under the commit's own name, so the name is
	// computed over everything except the payload and attached afterwards. The
	// nonce above is what keeps two commits that differ only in their payload
	// from taking the same name.
	commitID, err := CommitName(commit)
	if err != nil {
		return CommitID{}, err
	}
	if commit.Payload, err = s.sealer.Seal(ChunkID(commitID), payloadBytes); err != nil {
		return CommitID{}, err
	}
	commit.Id = commitID.Bytes()

	stored, err := s.transport.PutCommit(ctx, commit, reservation)
	if err != nil {
		return CommitID{}, err
	}
	if stored != commitID {
		return CommitID{}, fmt.Errorf("%w: wrote commit %s and the server recorded %s", ErrTampered, commitID, stored)
	}
	s.memo.putCommit(commitID, commit)
	return commitID, nil
}

// addedSince returns what this version holds that its parents did not.
//
// Only the first parent's manifest is diffed against. A merge's second parent
// can contribute chunks this diff will therefore list as added, which is
// harmless: the list is what the server must keep, and naming a chunk twice
// keeps it twice.
func (s *Store) addedSince(ctx context.Context, nodes []ManifestChunk, root ChunkID,
	parents []*pb.Commit, schemaID ChunkID) ([]ChunkID, error) {

	var oldRoot, oldSchema ChunkID
	if len(parents) > 0 && parents[0] != nil {
		payload, err := s.openPayload(ctx, parents[0])
		if err != nil {
			return nil, err
		}
		if oldRoot, err = ChunkIDFromBytes(payload.GetManifestRoot()); err != nil {
			return nil, err
		}
		if oldSchema, err = ChunkIDFromBytes(payload.GetSchemaChunk()); err != nil {
			return nil, err
		}
	}

	added, err := AddedSince(nodes, root, oldRoot, s.fetchNode(ctx))
	if err != nil {
		return nil, err
	}
	if schemaID != oldSchema {
		added = append(added, schemaID)
	}
	return added, nil
}

// schemaChunk returns the layout and descriptors this store writes under.
//
// Stored inside the store so that a client can read a commit written under a
// version whose generated code it does not have. Without it, retiring a schema
// from the codebase would make every commit written under it unreadable, which
// is the opposite of what a version control system is for.
func (s *Store) schemaChunk() (ChunkID, []byte, error) {
	set, err := descriptorSetFor(s.layout.Descriptor())
	if err != nil {
		return ChunkID{}, nil, err
	}

	schema := &pb.SchemaChunk{
		Layout:           s.layout.Layout(),
		Descriptors:      set,
		RootMessage:      string(s.layout.Descriptor().FullName()),
		SchemaVersion:    s.layout.SchemaVersion(),
		CanonicalVersion: CanonicalVersion,
	}
	canonical, err := CanonicalBytes(schema)
	if err != nil {
		return ChunkID{}, nil, err
	}
	return s.sealer.ChunkID(RoleSchema, "", canonical), canonical, nil
}

// descriptorSetFor collects a message's file and everything it imports.
func descriptorSetFor(md protoreflect.MessageDescriptor) (*descriptorpb.FileDescriptorSet, error) {
	set := &descriptorpb.FileDescriptorSet{}
	seen := map[string]bool{}

	var add func(fd protoreflect.FileDescriptor)
	add = func(fd protoreflect.FileDescriptor) {
		if seen[fd.Path()] {
			return
		}
		seen[fd.Path()] = true
		imports := fd.Imports()
		for i := 0; i < imports.Len(); i++ {
			add(imports.Get(i).FileDescriptor)
		}
		set.File = append(set.File, protodesc.ToFileDescriptorProto(fd))
	}
	add(md.ParentFile())
	return set, nil
}

// CommitName is the name of a commit: the hash of its record with the id and
// the sealed payload left out.
//
// Both the client and the server compute it this way, so a server can check
// that a commit is stored under the name it claims rather than taking the
// client's word for it.
func CommitName(commit *pb.Commit) (CommitID, error) {
	naming := proto.Clone(commit).(*pb.Commit)
	naming.Id = nil
	naming.Payload = nil

	encoded, err := CanonicalBytes(naming)
	if err != nil {
		return CommitID{}, err
	}
	return CommitID(sha256Sum(encoded)), nil
}

// verifiedName returns the name of a commit that came from a transport, and
// refuses one whose Id is not that name.
//
// The payload is opened under Id, and the name does not cover the payload. So
// a server that could pick Id freely could attach an old commit's payload to a
// record whose parents are the newest commit this client has seen, and the
// rollback check in [Branch.Head] would pass it. Every commit is checked here
// on the way in, so everything held in the memo can be trusted.
func verifiedName(commit *pb.Commit) (CommitID, error) {
	name, err := CommitName(commit)
	if err != nil {
		return CommitID{}, err
	}
	claimed, err := CommitIDFromBytes(commit.GetId())
	if err != nil {
		return CommitID{}, err
	}
	if claimed != name {
		return CommitID{}, fmt.Errorf("%w: commit %s is named %s by its contents", ErrTampered, claimed, name)
	}
	return name, nil
}

func chunkIDBytes(ids []ChunkID) [][]byte {
	out := make([][]byte, 0, len(ids))
	for _, id := range ids {
		out = append(out, id.Bytes())
	}
	return out
}

// elementCount reports how many split elements an item fragment holds, for the
// manifest to record.
func elementCount(c *Compiled, frag proto.Message) uint32 {
	var n uint32
	m := frag.ProtoReflect()
	for _, s := range c.splits {
		if list, ok := listAt(m, s.path); ok {
			n += uint32(list.Len())
		}
	}
	return n
}

func isNoSuchRef(err error) bool { return errors.Is(err, ErrNoSuchRef) }
func isRaceLost(err error) bool  { return errors.Is(err, ErrRaceLost) }

// Transport returns how this store is reached.
//
// For a caller that has to reach past the store — a maintenance job, or a test
// asserting what crossed the wire.
func (s *Store) Transport() Transport { return s.transport }

// ManifestRootOf returns the chunk naming a commit's manifest.
//
// Two versions with the same root hold the same items under the same keys,
// because the root's name is a fingerprint of its entries and each of those is a
// fingerprint of the subtree below.
func (s *Store) ManifestRootOf(ctx context.Context, commit *pb.Commit) (ChunkID, error) {
	payload, err := s.openPayload(ctx, commit)
	if err != nil {
		return ChunkID{}, err
	}
	return ChunkIDFromBytes(payload.GetManifestRoot())
}

// ReachableChunks returns every chunk a version needs: its manifest nodes, its
// items, and the schema it was written under.
//
// Linear in the store, so it is for verification and maintenance rather than for
// the write path — which is the mistake this replaced.
func (s *Store) ReachableChunks(ctx context.Context, commit *pb.Commit) ([]ChunkID, error) {
	payload, err := s.openPayload(ctx, commit)
	if err != nil {
		return nil, err
	}
	root, err := ChunkIDFromBytes(payload.GetManifestRoot())
	if err != nil {
		return nil, err
	}
	schema, err := ChunkIDFromBytes(payload.GetSchemaChunk())
	if err != nil {
		return nil, err
	}

	out := []ChunkID{root, schema}
	reader := NewManifestReader(s.sealer, root, func(prefix string, id ChunkID) (*pb.ManifestNode, error) {
		if id != root {
			out = append(out, id)
		}
		return s.fetchNode(ctx)(prefix, id)
	})
	items, err := reader.All()
	if err != nil {
		return nil, err
	}
	for _, it := range items {
		out = append(out, it.ID)
	}
	return out, nil
}

// ReadCommit returns the whole message at one commit.
//
// For reading a version other than a branch head: an older one, or one a
// maintenance job is checking. Branch.Read is what an ordinary caller wants.
func (s *Store) ReadCommit(ctx context.Context, commit *pb.Commit) (proto.Message, error) {
	return s.readCommit(ctx, commit)
}

// CommitInfo is one entry of a store's history, opened.
//
// Everything here except ID and Parents was sealed. A server holds the commit
// graph in the clear — it has to, to collect garbage — and holds who wrote
// what, when and why as bytes it cannot read.
type CommitInfo struct {
	ID      CommitID
	Parents []CommitID

	// Why the change was made and by whom, as the writer recorded them.
	Title       string
	Description string
	Authors     []Author
	Client      string
	Labels      map[string]string

	// The writer's clock at the time, which is not the server's. A history is
	// ordered by the commit graph rather than by this: two clients whose clocks
	// disagree still produce a well-ordered history, and one whose clock is
	// wrong does not reorder anybody else's work.
	Time time.Time

	// The branch it was written on, as recorded then. A branch can be deleted
	// or renamed, so this says where a commit came from rather than where it is.
	Branch string

	// What the layout says the data meant at that commit.
	SchemaVersion uint32

	// The application's own record, encoded as the message the layout's
	// commit_message names. Empty when the layout declares none or the writer
	// supplied none.
	//
	// Raw rather than opened, because who opens it differs: generated code
	// unmarshals straight into the concrete type it was built for, and a reader
	// with no such type calls [Store.OpenDetails] to get a dynamic message.
	// Opening it here would do the expensive one of those unconditionally.
	Details []byte
}

// OpenDetails returns a commit's details as a message, for a reader that has no
// generated code for the type.
//
// The descriptor comes from the schema chunk that commit was written under, not
// from this build's layout. That is the difference between a history that stays
// readable and one that does not: a commit written two schema versions ago
// names a type whose Go may since have been deleted, and its own schema chunk
// still carries the descriptor. Resolving against the current layout would work
// only for as long as nothing changed, which is not a property worth having.
//
// This is why details are bytes under a declared type rather than a
// google.protobuf.Any. An Any resolves its type URL against whatever registry
// the reader happens to have, and a retired type is not in it.
//
// One chunk fetch, because the schema chunk has to be read. Callers walking a
// whole history should expect that per distinct schema rather than per commit —
// the chunk cache is keyed by content, so commits sharing a schema share it.
func (s *Store) OpenDetails(ctx context.Context, commit *pb.Commit, info CommitInfo) (proto.Message, error) {
	if len(info.Details) == 0 {
		return nil, nil
	}

	schema, err := s.SchemaOf(ctx, commit)
	if err != nil {
		return nil, err
	}
	name := schema.GetLayout().GetCommitMessage()
	if name == "" {
		// The commit carries details and the layout it was written under
		// declares no type for them. Reported rather than dropped: history that
		// silently loses a field is worse than history that says it cannot read
		// one.
		return nil, fmt.Errorf("protovcs: commit %s carries details and the layout it was written under declares no commit_message",
			info.ID)
	}

	files, err := protodesc.NewFiles(schema.GetDescriptors())
	if err != nil {
		return nil, fmt.Errorf("protovcs: the stored descriptors did not resolve: %w", err)
	}
	desc, err := files.FindDescriptorByName(protoreflect.FullName(name))
	if err != nil {
		return nil, fmt.Errorf("protovcs: the stored descriptors do not define %s: %w", name, err)
	}
	md, ok := desc.(protoreflect.MessageDescriptor)
	if !ok {
		return nil, fmt.Errorf("protovcs: %s is not a message", name)
	}

	msg := dynamicpb.NewMessage(md)
	if err := proto.Unmarshal(info.Details, msg); err != nil {
		return nil, fmt.Errorf("protovcs: commit %s has unreadable details: %w", info.ID, err)
	}
	return msg, nil
}

// LogOptions bounds a walk through history.
type LogOptions struct {
	// How many commits to return at most. Zero means DefaultLogLimit rather than
	// unbounded: a history walk fetches every commit it returns, and a store
	// with a year of daily writes has enough of them that "all of it" should be
	// asked for rather than defaulted into.
	Limit int

	// Stop when this commit is reached, without including it. For "what has
	// happened since I last looked", where the answer is bounded by the store's
	// activity rather than by a number.
	Until CommitID
}

// DefaultLogLimit is how many commits Log returns when asked for no particular
// number.
const DefaultLogLimit = 100

// Log returns this branch's history, newest first.
//
// First parents only. A commit made by an ordinary write has one, so this is
// the whole history of a store nothing has merged; where a merge exists, this
// is the branch's own line rather than everything that ever fed into it.
//
// Each entry costs one commit fetch and one seal to open. Nothing here reads a
// chunk, so the cost is in the number of commits and not in the size of the
// store — a history of a hundred commits costs the same whether the store holds
// a megabyte or a gigabyte.
func (b *Branch) Log(ctx context.Context, opts LogOptions) ([]CommitInfo, error) {
	head, _, err := b.Head(ctx)
	if err != nil {
		// A branch that was never written has no history, which is an empty one
		// rather than a failure. A caller showing a list wants to show nothing.
		if isNoSuchRef(err) {
			return nil, nil
		}
		return nil, err
	}
	return b.store.LogFrom(ctx, head, opts)
}

// LogFrom is Log starting at a commit rather than at a branch head.
// Usage is what the server is holding for this store.
//
// Sealed bytes and objects, which is all a server can count: it holds the data
// and cannot read it. So this answers "how much room is this taking" and
// nothing about what is in it — for that, read the store.
//
// Reports [ErrNoUsage] when the transport has no server to ask, which an
// in-memory one does not. A caller showing a figure should leave it out in
// that case rather than fail: how much space a vault uses is rarely the
// question that brought anybody here.
func (s *Store) Usage(ctx context.Context) (*pb.Usage, error) {
	reporter, ok := s.transport.(UsageReporter)
	if !ok {
		return nil, ErrNoUsage
	}
	return reporter.Usage(ctx)
}

func (s *Store) LogFrom(ctx context.Context, start *pb.Commit, opts LogOptions) ([]CommitInfo, error) {
	limit := opts.Limit
	if limit <= 0 {
		limit = DefaultLogLimit
	}

	var out []CommitInfo
	commit := start
	seen := map[CommitID]bool{}

	for commit != nil && len(out) < limit {
		id, err := CommitIDFromBytes(commit.GetId())
		if err != nil {
			return nil, err
		}
		if opts.Until != (CommitID{}) && id == opts.Until {
			break
		}

		// A commit graph arrives from a server, so it is not assumed acyclic.
		// A cycle here would be an infinite walk rather than a wrong answer,
		// which is the worse failure of the two.
		if seen[id] {
			return nil, fmt.Errorf("protovcs: commit %s appears twice in its own history", id)
		}
		seen[id] = true

		info, err := s.commitInfo(ctx, commit)
		if err != nil {
			return nil, err
		}
		out = append(out, info)

		if len(info.Parents) == 0 {
			break
		}
		commits, err := s.transport.GetCommits(ctx, info.Parents[0], 1)
		if err != nil {
			return nil, err
		}
		if len(commits) == 0 {
			return nil, fmt.Errorf("%w: commit %s, the parent of %s",
				ErrNoSuchChunk, info.Parents[0], id)
		}
		name, err := verifiedName(commits[0])
		if err != nil {
			return nil, err
		}
		if name != info.Parents[0] {
			return nil, fmt.Errorf("%w: asked for commit %s and was sent %s", ErrTampered, info.Parents[0], name)
		}
		commit = commits[0]
	}
	return out, nil
}

// CommitDetails returns one commit's entry, opened.
func (s *Store) CommitDetails(ctx context.Context, commit *pb.Commit) (CommitInfo, error) {
	return s.commitInfo(ctx, commit)
}

func (s *Store) commitInfo(ctx context.Context, commit *pb.Commit) (CommitInfo, error) {
	id, err := CommitIDFromBytes(commit.GetId())
	if err != nil {
		return CommitInfo{}, err
	}

	payload, err := s.openPayload(ctx, commit)
	if err != nil {
		return CommitInfo{}, err
	}

	msg := payload.GetMessage()
	info := CommitInfo{
		ID:            id,
		Title:         msg.GetTitle(),
		Description:   msg.GetDescription(),
		Client:        msg.GetClient(),
		Labels:        msg.GetLabels(),
		Branch:        payload.GetBranch(),
		SchemaVersion: commit.GetSchemaVersion(),
	}
	for _, a := range msg.GetAuthors() {
		info.Authors = append(info.Authors, Author{Name: a.GetName(), Email: a.GetEmail(), Role: a.GetRole()})
	}
	if t := payload.GetTime(); t != nil {
		info.Time = t.AsTime()
	}
	for _, raw := range commit.GetParents() {
		pid, err := CommitIDFromBytes(raw)
		if err != nil {
			return CommitInfo{}, err
		}
		info.Parents = append(info.Parents, pid)
	}

	info.Details = msg.GetDetails()
	return info, nil
}
