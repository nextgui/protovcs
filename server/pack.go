package server

import (
	"context"
	"errors"
	"fmt"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	protovcs "github.com/nextgui/protovcs"
	pb "github.com/nextgui/protovcs/proto/protovcs/v1"
)

// MaxPackHintChunks is the most chunk ids one pack hint may name. A store holding
// ten years of a household's finances reaches about 3,800.
const MaxPackHintChunks = 100_000

// ErrHintTooLarge is returned for a pack hint naming more than
// MaxPackHintChunks chunks.
var ErrHintTooLarge = errors.New("protovcs: a pack hint names more chunks than one hint may")

// ChunkBatchReader is storage that can read many chunks in fewer requests
// than one each. GetChunks uses it when the storage offers it.
type ChunkBatchReader interface {
	// GetChunks returns sealed chunks in the order asked for.
	GetChunks(ctx context.Context, storeID string, ids []protovcs.ChunkID) ([][]byte, error)
}

// PackScheduler starts a pack pass for one store soon, outside the request
// that asked for it. A deployment backs it with a message queue whose
// deliveries call [Server.PackNow].
type PackScheduler interface {
	SchedulePack(ctx context.Context, storeID string) error
}

// PackRequestInterval is the least time between two pack passes a hint asks
// for on one store. A hint inside it is kept and waits for the next request or
// the sweep, so a burst of reads asks for one pass.
const PackRequestInterval = 10 * time.Minute

// WithPackScheduler makes a kept hint ask for a pack pass, at most once per
// PackRequestInterval per store, instead of waiting for the sweep.
func (s *Server) WithPackScheduler(p PackScheduler) *Server {
	s.packScheduler = p
	return s
}

// Packer is storage that can keep packs: a second copy of the chunks a head
// reaches, a few megabytes to an object.
//
// Packs are a cache. Every packed chunk is also stored loose, so nothing else
// in this package needs to know packs exist.
type Packer interface {
	// PutPackHint stores a store's latest hint, replacing any earlier one.
	PutPackHint(ctx context.Context, storeID string, hint *pb.PackHintRecord) error

	// Pack runs one pass over a store's packs under the policy given.
	Pack(ctx context.Context, storeID string, policy PackPolicy) (PackResult, error)

	// DropPacks deletes every pack a store has, its index and its hint.
	DropPacks(ctx context.Context, storeID string) error

	// ClaimPackRequest records that a pass was asked for at now, unless one
	// was asked for less than interval before. It reports whether this caller
	// made the claim; of several racing, one does.
	ClaimPackRequest(ctx context.Context, storeID string, now time.Time, interval time.Duration) (bool, error)
}

// PackPolicy is what one pass may do.
type PackPolicy struct {
	// A delta is built when the new chunks number at least MinNewChunks or
	// total at least MinNewBytes.
	MinNewChunks int
	MinNewBytes  uint64

	// A pack is rewritten when its dead bytes are at least this fraction of it.
	DeadFraction float64

	// While a store has more packs than this, the smallest are merged.
	MaxPacks int

	// Where a pack is closed and the next one started.
	PackBytes uint64

	// The most bytes a store's packs may occupy.
	MaxTotalBytes uint64

	// How long a retired or unnamed pack is kept before it is deleted.
	Grace time.Duration

	Now time.Time
}

// PackResult is what one pass did to one store.
type PackResult struct {
	// Built is how many packs the pass wrote, and Copied how many chunks went
	// into them.
	Built  int
	Copied int

	// Retired is how many packs stopped being used for reads.
	Retired int

	// Deleted is how many pack objects were removed.
	Deleted int

	// Skipped says why the pass built nothing when it had a hint, or is empty.
	Skipped string
}

// Policy values, accepted 2026-10-29 without a real sync history to measure
// them against.
const (
	packMinNewChunks = 64
	packMinNewBytes  = 256 << 10
	packDeadFraction = 0.5
	packMaxPacks     = 16
	packBytes        = 4 << 20
)

// packPolicy is the policy for one store, capped by what it may store.
func (s *Server) packPolicy(store *pb.Store) PackPolicy {
	limit := store.GetQuota().GetMaxBytes()
	if limit == 0 {
		limit = store.GetUsage().GetBytes()
	}
	return PackPolicy{
		MinNewChunks:  packMinNewChunks,
		MinNewBytes:   packMinNewBytes,
		DeadFraction:  packDeadFraction,
		MaxPacks:      packMaxPacks,
		PackBytes:     packBytes,
		MaxTotalBytes: limit,
		Grace:         CollectionGrace,
		Now:           s.now(),
	}
}

// PackHint keeps a hint naming the chunks a head reaches, and reports whether
// it was kept.
//
// It is kept only while commit is the head of some ref, because a hint about a
// commit that has already moved on describes chunks the next read will not
// all want. Storage that keeps no packs accepts nothing, which is not an error.
func (s *Server) PackHint(ctx context.Context, storeID string, commit protovcs.CommitID, ids []protovcs.ChunkID) (bool, error) {
	store, err := s.storage.GetStore(ctx, storeID)
	if err != nil {
		return false, err
	}
	if len(ids) > MaxPackHintChunks {
		return false, fmt.Errorf("%w: %d, and the limit is %d", ErrHintTooLarge, len(ids), MaxPackHintChunks)
	}
	packer, ok := s.storage.(Packer)
	if !ok || store.GetState() != pb.Store_STATE_ACTIVE {
		return false, nil
	}

	refs, err := s.storage.ListRefs(ctx, storeID)
	if err != nil {
		return false, err
	}
	head := false
	for _, r := range refs {
		if r.Commit == commit {
			head = true
			break
		}
	}
	if !head {
		return false, nil
	}

	raw := make([][]byte, 0, len(ids))
	for _, id := range ids {
		raw = append(raw, id.Bytes())
	}
	if err := packer.PutPackHint(ctx, storeID, &pb.PackHintRecord{
		Commit:     commit.Bytes(),
		ChunkIds:   raw,
		ReceivedAt: timestamppb.New(s.now()),
	}); err != nil {
		return false, err
	}

	// The hint is stored before the pass is asked for, so the pass reads it.
	// A failure to ask is not the caller's problem: the hint is kept, and the
	// sweep packs it if nothing sooner does.
	if s.packScheduler != nil {
		claimed, err := packer.ClaimPackRequest(ctx, storeID, s.now(), PackRequestInterval)
		if err == nil && claimed {
			_ = s.packScheduler.SchedulePack(ctx, storeID)
		}
	}
	return true, nil
}

// PackNow runs a pack pass over one store, the same pass the sweep runs. It is
// what a delivery from the PackScheduler's queue calls.
func (s *Server) PackNow(ctx context.Context, storeID string) (PackResult, error) {
	return s.packStore(ctx, storeID)
}

// packStore runs the packing step of a sweep over one store.
func (s *Server) packStore(ctx context.Context, storeID string) (PackResult, error) {
	packer, ok := s.storage.(Packer)
	if !ok {
		return PackResult{}, nil
	}
	store, err := s.storage.GetStore(ctx, storeID)
	if err != nil {
		return PackResult{}, err
	}
	if s := store.GetState(); s == pb.Store_STATE_DELETING || s == pb.Store_STATE_PURGING {
		return PackResult{}, packer.DropPacks(ctx, storeID)
	}
	return packer.Pack(ctx, storeID, s.packPolicy(store))
}
