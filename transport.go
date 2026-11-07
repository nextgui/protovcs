package protovcs

import (
	"context"
	"encoding/hex"
	"fmt"
	"sort"

	pb "github.com/nextgui/protovcs/proto/protovcs/v1"
)

// CommitID names one commit.
//
// The same shape as a chunk id and a different namespace. A commit is not a
// chunk: the server reads part of it, and it is never fetched as one.
type CommitID [ChunkIDLen]byte

func (c CommitID) String() string { return hex.EncodeToString(c[:]) }
func (c CommitID) IsZero() bool   { return c == CommitID{} }

// Bytes returns a copy as a slice, for putting on the wire.
func (c CommitID) Bytes() []byte {
	out := make([]byte, ChunkIDLen)
	copy(out, c[:])
	return out
}

// CommitIDFromBytes converts a wire value, refusing anything of the wrong
// length rather than padding it.
func CommitIDFromBytes(b []byte) (CommitID, error) {
	var id CommitID
	if len(b) != ChunkIDLen {
		return id, fmt.Errorf("protovcs: a commit id is %d bytes, got %d", ChunkIDLen, len(b))
	}
	copy(id[:], b)
	return id, nil
}

// RefState is a branch as the server holds it.
type RefState struct {
	// The hashed name. The real one is inside the head commit's sealed payload.
	Name []byte

	Commit CommitID

	// What a writer must return to move this ref. The only place in protovcs
	// where two writers can conflict.
	CASToken string
}

// SameRefs reports whether two ref sets describe the same branches at the same
// commits.
//
// Order-insensitive, because ListRefs promises none. The compare-and-swap token
// counts as part of the state: it moves only when somebody wrote the ref, so a
// caller holding one wants to know it is stale.
//
// Used by anything implementing [Transport.WatchRefs], which has to decide
// whether a ref set it just read is news.
func SameRefs(a, b []RefState) bool {
	if len(a) != len(b) {
		return false
	}

	x, y := sortedRefs(a), sortedRefs(b)
	for i := range x {
		if string(x[i].Name) != string(y[i].Name) ||
			x[i].Commit != y[i].Commit ||
			x[i].CASToken != y[i].CASToken {
			return false
		}
	}
	return true
}

func sortedRefs(in []RefState) []RefState {
	out := append([]RefState(nil), in...)
	sort.Slice(out, func(i, j int) bool { return string(out[i].Name) < string(out[j].Name) })
	return out
}

// SealedChunk is a chunk on its way to or from a server.
type SealedChunk struct {
	ID     ChunkID
	Sealed []byte
}

// Transport is how a client reaches a store.
//
// It is the whole server surface, and none of it requires the server to
// understand what it holds. Implementations live in subpackages so that adding
// one never edits this package.
//
// Everything except UpdateRef is either immutable or content-addressed, so it
// can be retried, duplicated, reordered or interrupted with no coordination.
// UpdateRef is the single point where concurrency has to be resolved, and it
// resolves it by refusing.
type Transport interface {
	// ListRefs returns every branch and the commit it points at.
	ListRefs(ctx context.Context) ([]RefState, error)

	// WatchRefs calls deliver with the store's branches, and again whenever one
	// of them moves, blocking until it is done.
	//
	// The first call to deliver carries the current state rather than waiting
	// for a change, so a watcher needs no separate ListRefs and cannot miss a
	// move that lands between the two calls it would otherwise have made.
	//
	// deliver receives the whole ref set every time rather than what changed.
	// A watcher that fell behind is therefore sent only the newest state
	// instead of a queue, and a repeated value is not an event: the same set
	// may arrive twice and a caller acting on the difference sees nothing.
	//
	// Returns ctx.Err() when the caller stopped it, deliver's error when
	// deliver stopped it, and the underlying failure when the watch broke.
	// Everything it holds is released before it returns.
	WatchRefs(ctx context.Context, deliver func([]RefState) error) error

	// UpdateRef moves a branch, or reports that somebody else moved it first.
	//
	// An empty expected token creates the branch and fails if it exists. A zero
	// commit deletes it.
	//
	// force allows a move to a commit that does not descend from where the
	// branch is. Without it that is refused: a branch jumping to unrelated
	// history orphans everything in between, and the chunks only that history
	// reached become collectable, which is data loss wearing the clothes of a
	// successful write.
	UpdateRef(ctx context.Context, name []byte, commit CommitID, expected string, force bool) (RefState, error)

	// GetCommits walks back from a commit, at most depth records. Depth zero
	// means one, which is what a shallow clone asks for.
	GetCommits(ctx context.Context, from CommitID, depth uint32) ([]*pb.Commit, error)

	// PutCommit records a version without making it reachable. UpdateRef does
	// that, and does it last, so a crash leaves unreferenced objects rather
	// than a branch pointing at something incomplete.
	PutCommit(ctx context.Context, commit *pb.Commit, reservation string) (CommitID, error)

	// MissingBetween returns the chunk ids a client holding `have` might not
	// hold at `want`, and whether the answer had to be the full set.
	MissingBetween(ctx context.Context, have []CommitID, want CommitID) ([]ChunkID, bool, error)

	// BeginWrite reserves quota before anything is uploaded, so a client that
	// has run out finds out before sending rather than after.
	BeginWrite(ctx context.Context, chunks uint32, bytes uint64) (string, error)

	// HasChunks returns which of these the store does not already hold.
	HasChunks(ctx context.Context, ids []ChunkID) ([]ChunkID, error)

	// PutChunks stores chunks. Idempotent, because a chunk's name determines
	// its bytes.
	PutChunks(ctx context.Context, reservation string, chunks []SealedChunk) error

	// GetChunks fetches chunks. Batched because the common operation on a store
	// is reading all of it.
	GetChunks(ctx context.Context, ids []ChunkID) ([]SealedChunk, error)

	// PackHint says which chunks a head reaches, in manifest order, so the
	// server can store them together. It reports whether the server kept the
	// hint, which it does only while commit is the head of some ref.
	PackHint(ctx context.Context, commit CommitID, ids []ChunkID) (bool, error)
}

// UsageReporter is a transport that can also say what the server is holding
// for the store.
//
// Separate from [Transport] because it is a different service. Everything in
// Transport is StoreService, which moves data; how much of it there is comes
// from StoreAdminService, and a transport that speaks only the first is a
// perfectly good transport. An in-memory one has no server to ask.
//
// [Store.Usage] reports [ErrNoUsage] when the transport does not implement
// this, so a caller showing a figure can leave it out rather than fail.
type UsageReporter interface {
	Usage(ctx context.Context) (*pb.Usage, error)
}
