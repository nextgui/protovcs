package server

import (
	"context"
	"fmt"

	pb "github.com/nextgui/protovcs/proto/protovcs/v1"
)

// StoreSweep is what one maintenance pass did to one store.
//
// Err holds the first failure, and the fields before it hold whatever ran
// before that failure. A store whose collection failed still reports the
// reservations the same pass released.
type StoreSweep struct {
	// StoreID is the store this record is about.
	StoreID string

	// ReservationsReleased is how many abandoned writes gave their quota back.
	ReservationsReleased int

	// Collected is what collection deleted. Zero when collection did not run.
	Collected CollectResult

	// UsageBefore and UsageAfter are the store's counters either side of
	// reconciliation. They differ when the counters had drifted from what is
	// actually stored, which is not expected and is worth reporting.
	UsageBefore *pb.Usage
	UsageAfter  *pb.Usage

	// Packed is what the packing step did. Zero when storage keeps no packs.
	Packed PackResult

	// Err is why this store's pass stopped, or nil.
	Err error
}

// UsageDrifted reports whether reconciliation had to correct this store's
// counters.
func (s StoreSweep) UsageDrifted() bool {
	return s.UsageBefore.GetObjects() != s.UsageAfter.GetObjects() ||
		s.UsageBefore.GetBytes() != s.UsageAfter.GetBytes()
}

// SweepResult is what one maintenance pass did.
type SweepResult struct {
	// Stores is one record per store the pass visited, in the order storage
	// listed them.
	Stores []StoreSweep
}

// Failed returns the stores whose pass stopped on an error.
func (r SweepResult) Failed() []StoreSweep {
	var out []StoreSweep
	for _, s := range r.Stores {
		if s.Err != nil {
			out = append(out, s)
		}
	}
	return out
}

// Sweep runs the maintenance jobs once over every store.
//
// Each store gets its expired reservations released, its unreachable chunks
// collected, its usage counters recounted from what is actually stored, and
// its packs brought up to date with its latest pack hint.
//
// A store whose pass fails does not stop the pass. Collection deletes things,
// so it fails closed and leaves the store exactly as it was; the next sweep
// tries again. Stopping at the first failure would mean one broken store
// suspends collection for every store listed after it.
//
// The returned error means no store was visited, because the store list could
// not be read. A failure on an individual store is in that store's [StoreSweep]
// and in [SweepResult.Failed], so a caller can report both what ran and what
// did not.
//
// Nothing here holds a lock, because collection does not. What keeps a
// concurrent writer's chunks safe is the grace period on a chunk's write time
// and the check for a live reservation, both in [Server.Collect]. Two sweeps
// running at once therefore delete nothing they should not, but each subtracts
// what it deleted from the usage counters, so overlapping sweeps can leave
// those counters reading low until the next pass reconciles them.
func (s *Server) Sweep(ctx context.Context) (SweepResult, error) {
	var out SweepResult

	stores, err := s.ListStores(ctx, &pb.ListStoresRequest{})
	if err != nil {
		return out, fmt.Errorf("protovcs: sweeping: listing stores: %w", err)
	}

	for _, store := range stores {
		out.Stores = append(out.Stores, s.sweepStore(ctx, store.GetStoreId()))
	}
	return out, nil
}

// sweepStore runs the four jobs over one store.
func (s *Server) sweepStore(ctx context.Context, storeID string) StoreSweep {
	out := StoreSweep{StoreID: storeID}

	released, err := s.ExpireReservations(ctx, storeID)
	out.ReservationsReleased = released
	if err != nil {
		out.Err = fmt.Errorf("expiring reservations: %w", err)
		return out
	}

	collected, err := s.Collect(ctx, storeID)
	out.Collected = collected
	if err != nil {
		out.Err = fmt.Errorf("collecting: %w", err)
		return out
	}

	// Drift is not expected, because usage moves in the same operation as the
	// write that moves it. Reconciling every pass is what makes drift a
	// reported condition rather than one found while investigating something
	// else.
	before, after, err := s.Reconcile(ctx, storeID)
	out.UsageBefore, out.UsageAfter = before, after
	if err != nil {
		out.Err = fmt.Errorf("reconciling: %w", err)
		return out
	}

	// Last, because packs are a cache: a store whose pass failed earlier is
	// still readable, only slower.
	packed, err := s.packStore(ctx, storeID)
	out.Packed = packed
	if err != nil {
		out.Err = fmt.Errorf("packing: %w", err)
	}
	return out
}
