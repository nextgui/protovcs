package protovcs

import (
	"encoding/hex"
	"errors"
	"fmt"
)

// ChunkIDLen is the length of a chunk name in bytes.
//
// Thirty-two, because a name is also the integrity check on what it names: a
// client asks for a name and must be certain the bytes it gets back are the
// ones that produced it. A shorter name would let a server that can find a
// collision substitute one item for another, and a store is expected to outlive
// several rounds of hardware getting faster.
const ChunkIDLen = 32

// ChunkID names one chunk. It is a fingerprint of the chunk's contents under a
// key only the client holds.
type ChunkID [ChunkIDLen]byte

// String returns the hexadecimal form, which is what appears in errors and in
// a path-filing backend's filenames.
func (c ChunkID) String() string { return hex.EncodeToString(c[:]) }

// Bytes returns a copy as a slice, for putting on the wire.
func (c ChunkID) Bytes() []byte {
	out := make([]byte, ChunkIDLen)
	copy(out, c[:])
	return out
}

// IsZero reports whether this is the empty id, which no chunk has.
func (c ChunkID) IsZero() bool { return c == ChunkID{} }

// ChunkIDFromBytes converts a wire value, refusing anything of the wrong
// length rather than padding or truncating it.
func ChunkIDFromBytes(b []byte) (ChunkID, error) {
	var id ChunkID
	if len(b) != ChunkIDLen {
		return id, fmt.Errorf("protovcs: a chunk id is %d bytes, got %d", ChunkIDLen, len(b))
	}
	copy(id[:], b)
	return id, nil
}

// Role separates the kinds of thing stored as chunks.
//
// It goes into the fingerprint, so an item and a manifest node holding the same
// bytes have different names. Without that a server could return a manifest
// node where an item was asked for, and both would open correctly because both
// were sealed under the same key.
type Role string

const (
	RoleItem     Role = "item"
	RoleManifest Role = "manifest"
	RoleSchema   Role = "schema"
)

// ItemKey identifies one item within a version, as a path: "entries/2026/08/14".
//
// Item keys sort, and sorting them is what makes a range of them a contiguous
// walk of the manifest rather than a lookup for each one.
type ItemKey string

// RootItemKey is the item holding every field no layout rule mentions.
//
// The empty string, so it sorts before every other key and a reader walking in
// order meets the scalars before the collections that depend on them.
const RootItemKey ItemKey = ""

// Three versions travel with a store, and they answer different questions.
//
//	FormatVersion     how the bytes are framed and parsed
//	CanonicalVersion  what a chunk is called
//	schema_version    what the data means  (on the layout, declared by hand)
//
// They are separate because they fail differently. A format this build does not
// know cannot be parsed at all. Canonical rules it does not know can be parsed
// perfectly and merely produce different names. A schema it does not know parses
// and names correctly and means something else.
//
// The Go packages are versioned to match, at lib/protovcs/v1, so a second
// implementation can be linked beside this one while stores move between them.
const (
	// FormatVersion is the version of the protovcs formats themselves: the
	// chunk envelope, the manifest node, and the commit record.
	//
	// A build that meets a format it does not recognise refuses before reading
	// anything else, so an old client cannot take a newer envelope's bytes and
	// interpret them as the fields it expects. One number covers all three
	// deliberately: they are written and read together, and a client that can
	// parse two of the three can do nothing useful.
	FormatVersion = 1

	// CanonicalVersion is the version of the encoding rules that decide a
	// chunk's name: field order, packing, map entry order, and where unknown
	// fields go.
	//
	// Changing these does not stop anything being read — the bytes are ordinary
	// protobuf either way. It changes what every chunk is called, so a client
	// on different rules would find nothing already present, re-upload the whole
	// store, and believe it had written a delta. Recorded on every commit so
	// that is a detected condition rather than a bill.
	CanonicalVersion = 1
)

var (
	// ErrNoSuchChunk is returned when a store does not hold a chunk. Distinct
	// from a transport failure, because a missing chunk in a commit that claims
	// to reference it means the store is damaged rather than unreachable.
	ErrNoSuchChunk = errors.New("protovcs: no such chunk")

	// ErrNoSuchCommit is returned for a commit the store does not hold.
	//
	// Distinct from a missing chunk: a commit goes when garbage collection
	// removes what no branch reaches, so a caller that has held an id for a
	// while can name one legitimately and needs to tell that from damage.
	ErrNoSuchCommit = errors.New("protovcs: no such commit")

	// ErrNoSuchRef is returned for a branch that does not exist.
	ErrNoSuchRef = errors.New("protovcs: no such ref")

	// ErrNoUsage is returned by [Store.Usage] when the transport cannot say.
	// An in-memory store has no server to ask, so this is a fact about the
	// transport rather than a failure, and a caller showing a figure should
	// leave it out rather than give up.
	ErrNoUsage = errors.New("protovcs: this transport cannot report usage")

	// ErrRaceLost is returned when a ref moved between reading it and writing
	// it. Not a failure: the caller rebuilds against the new state and retries,
	// which is what Change does.
	ErrRaceLost = errors.New("protovcs: another writer got there first")

	// ErrTokenExpired is returned when the server ended a call because the
	// store token it was made with expired. The caller gets a new token and
	// tries again.
	ErrTokenExpired = errors.New("protovcs: the store token expired")

	// ErrDuplicateIdentity is returned when two elements of one item share the
	// identity their collection declared.
	//
	// Almost always a double append or a deduplication that missed. The store
	// would read without complaint and hold one record twice, so it is refused
	// where the two are still distinguishable from a legitimate pair.
	ErrDuplicateIdentity = errors.New("protovcs: two elements of one item share an identity")

	// ErrItemTooLarge is returned when an item outgrew the size its split
	// allows and holds more than one element, so a shard key component would
	// divide it.
	//
	// Not returned for a single element that is larger than the limit on its
	// own. No shard key divides one element, so refusing it would make the data
	// unstorable; the server's max_chunk_bytes is what bounds that case.
	ErrItemTooLarge = errors.New("protovcs: an item is larger than its split allows")

	// ErrNotFastForward is returned when a branch is asked to move to a commit
	// that does not descend from where it is, without saying so explicitly.
	//
	// The server refuses it because the accident it prevents is silent: the
	// history in between is orphaned, and the chunks only that history reached
	// become collectable. Nothing about the write fails.
	ErrNotFastForward = errors.New("protovcs: that commit does not descend from the branch head")

	// ErrTampered is returned when a chunk does not open under the name it was
	// asked for. It is never returned for a chunk that is merely damaged in
	// transit, which the transport reports separately.
	ErrTampered = errors.New("protovcs: a chunk does not match the name it was fetched under")

	// ErrUnsupportedFormat is returned for a chunk or commit written by a newer
	// build. Distinct from ErrTampered on purpose: a user running an old client
	// must not be told their data was interfered with.
	ErrUnsupportedFormat = errors.New("protovcs: written in a format this build does not know")

	// ErrUnsupportedSchema is returned for a commit whose schema_version this
	// build does not have code for and is not willing to read generically.
	ErrUnsupportedSchema = errors.New("protovcs: written under a schema version this build does not support")

	// ErrOutOfScope is returned by the dynamic partial handle when a caller
	// reads outside what it checked out. Generated scoped types make the same
	// mistake a compile error instead.
	ErrOutOfScope = errors.New("protovcs: outside the checked-out scope")

	// ErrWentBackwards is returned when a server offers a head older than one
	// this client has already seen. Encryption cannot detect a replayed old
	// version, so the client remembers where it has been.
	ErrWentBackwards = errors.New("protovcs: the server offered an older version than one already seen")
)
