// Package server holds the rules a protovcs server enforces.
//
// None of them require understanding what is stored. The server checks that a
// commit does not name a chunk it does not hold, that a branch moves only
// against the token its writer read, that a store stays inside its limits, and
// that nothing reachable is collected. Everything else is opaque bytes.
//
// The rules live here rather than in the service handler so they can be tested
// without a server, and so a second transport does not get a second copy of
// them.
package server

import (
	"context"
	"errors"

	protovcs "github.com/nextgui/protovcs"
	pb "github.com/nextgui/protovcs/proto/protovcs/v1"
)

// ErrNoSuchStore is returned for a store that was never created, or has been
// deleted.
var ErrNoSuchStore = errors.New("protovcs: no such store")

// ErrQuotaExceeded is returned when an operation would put a store over one of
// its limits. It names the limit, the current usage and the ceiling, because
// "quota exceeded" is not something a caller can act on.
var ErrQuotaExceeded = errors.New("protovcs: store is at its limit")

// ErrNoReservation is returned when chunks are uploaded without a live
// reservation, or against one that has expired.
var ErrNoReservation = errors.New("protovcs: no live reservation")

// ErrSchemaTooOld is returned when a commit would write a schema version older
// than the store already holds, or older than an operator's floor.
//
// A client is meant to refuse this itself. This is the backstop for when the
// client running is not the client anybody expected — an old build left on a
// laptop for a year, writing over a store that has since migrated.
var ErrSchemaTooOld = errors.New("protovcs: that schema version is older than this store accepts")

// Storage is where a server keeps what it holds.
//
// Two kinds of thing behind one interface, because they are always used
// together and separating them would put a distributed transaction between
// them. In production the metadata is a transactional store and the chunks are
// object storage; the interface says nothing about that.
//
// Chunk operations are idempotent and content-addressed, so an implementation
// needs no locking for them. Everything touching a store's record does need to
// be atomic, which is what the Update method is for.
type Storage interface {
	// CreateStore records a new store. Creating one that exists returns the
	// existing record rather than an error, so a retry after an unknown outcome
	// does not leave a second store nobody knows about.
	CreateStore(ctx context.Context, store *pb.Store) (*pb.Store, error)

	// GetStore returns a store's record, or ErrNoSuchStore.
	GetStore(ctx context.Context, storeID string) (*pb.Store, error)

	// ListStores returns records matching a filter.
	ListStores(ctx context.Context, req *pb.ListStoresRequest) ([]*pb.Store, error)

	// Update applies a change to a store's record atomically.
	//
	// The usage counters move inside this, in the same transaction as the
	// commit that caused them to move, so they cannot drift from what is
	// actually stored by a process dying between two writes.
	Update(ctx context.Context, storeID string, fn func(*pb.Store) error) (*pb.Store, error)

	// StoreGeneration returns a number that changes whenever the store's
	// record is written, or ErrNoSuchStore.
	//
	// A ref watch reads it on every tick and lists refs only when it moved.
	// On a bucket it is one metadata read, where a listing is billed as a
	// write. It is enough because [Server.UpdateRef] writes the record after
	// every move it makes.
	StoreGeneration(ctx context.Context, storeID string) (int64, error)

	PutChunk(ctx context.Context, storeID string, id protovcs.ChunkID, sealed []byte) error
	GetChunk(ctx context.Context, storeID string, id protovcs.ChunkID) ([]byte, error)
	HasChunk(ctx context.Context, storeID string, id protovcs.ChunkID) (bool, error)
	DeleteChunk(ctx context.Context, storeID string, id protovcs.ChunkID) error

	// ListChunks returns every chunk a store holds, for collection and for
	// reconciling the counters against reality.
	ListChunks(ctx context.Context, storeID string) ([]protovcs.ChunkID, error)

	// ChunkSize returns a chunk's stored size without reading it.
	ChunkSize(ctx context.Context, storeID string, id protovcs.ChunkID) (uint64, error)

	PutCommit(ctx context.Context, storeID string, id protovcs.CommitID, commit *pb.Commit) error
	GetCommit(ctx context.Context, storeID string, id protovcs.CommitID) (*pb.Commit, error)
	ListCommits(ctx context.Context, storeID string) ([]protovcs.CommitID, error)

	ListRefs(ctx context.Context, storeID string) ([]protovcs.RefState, error)

	// UpdateRef moves a branch only if its token still matches, and returns
	// protovcs.ErrRaceLost when it does not.
	//
	// The token is checked before anything else, so a writer that simply lost a
	// race is told exactly that. Without force it then also refuses a commit
	// that does not descend from where the branch is, which is a different
	// mistake: losing a race is ordinary and retried, while leaving a branch's
	// own history orphans everything in between.
	UpdateRef(ctx context.Context, storeID string, name []byte, commit protovcs.CommitID, expected string, force bool) (protovcs.RefState, error)

	// Reservations. A reservation holds quota between a client being told it
	// may write and the commit that spends it.
	PutReservation(ctx context.Context, storeID string, r *Reservation) error
	GetReservation(ctx context.Context, storeID, id string) (*Reservation, error)

	// UpdateReservation applies fn to a reservation atomically, retrying on a
	// concurrent change, and returns the result. An error from fn leaves the
	// reservation unchanged and is returned as is. A missing reservation is
	// ErrNoReservation.
	UpdateReservation(ctx context.Context, storeID, id string, fn func(*Reservation) error) (*Reservation, error)
	DeleteReservation(ctx context.Context, storeID, id string) error
	ListReservations(ctx context.Context, storeID string) ([]*Reservation, error)

	// GetCondemned returns the chunks collection has marked for deletion. A
	// store with none returns an empty set.
	GetCondemned(ctx context.Context, storeID string) (*Condemned, error)

	// UpdateCondemned applies fn to the marked chunks and writes the result,
	// conditionally on the set not having changed since it was read.
	UpdateCondemned(ctx context.Context, storeID string, fn func(*Condemned) error) (*Condemned, error)
}

// Reservation is quota held for a write that has not finished.
//
// Limits are checked here rather than at commit time. Checking at commit would
// be too late: the chunks are already stored, and a commit that never arrives
// leaves them there with nothing accounting for them.
type Reservation struct {
	ID string

	// What the client said it would upload. An estimate, and the ceiling on
	// what it may actually send.
	Chunks uint32
	Bytes  uint64

	// What it has sent so far.
	UsedChunks uint32
	UsedBytes  uint64

	// When an abandoned reservation is released and the chunks it covered
	// become eligible for collection.
	ExpiresUnix int64
}

// Condemned is the set of chunks one collection run found unreachable, for a
// later run to delete.
//
// A chunk is deleted only after it has been marked for a full
// [CollectionGrace]. While it is marked, HasChunks reports it missing and
// PutCommit refuses a commit that adds it, so no new commit can come to depend
// on it unnoticed.
type Condemned struct {
	Chunks map[protovcs.ChunkID]CondemnedChunk
}

// CondemnedChunk is one marked chunk.
type CondemnedChunk struct {
	// When collection marked it.
	MarkedUnix int64

	// A run has claimed it and is deleting it. A writer can no longer take it
	// back, and must wait for the delete and upload it again.
	Deleting bool
}

// NewCondemned returns an empty set.
func NewCondemned() *Condemned {
	return &Condemned{Chunks: map[protovcs.ChunkID]CondemnedChunk{}}
}

// Clone returns a copy that shares nothing with c.
func (c *Condemned) Clone() *Condemned {
	out := NewCondemned()
	for id, e := range c.Chunks {
		out.Chunks[id] = e
	}
	return out
}
