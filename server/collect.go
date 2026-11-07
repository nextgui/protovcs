package server

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"google.golang.org/protobuf/proto"

	protovcs "github.com/nextgui/protovcs"
	pb "github.com/nextgui/protovcs/proto/protovcs/v1"
)

// collectParallelism bounds how many chunks one collection deletes at once.
const collectParallelism = 16

// ChunkInfo is what a listing says about one chunk.
type ChunkInfo struct {
	ID   protovcs.ChunkID
	Size uint64

	// When the chunk was written. Zero when the backend does not record it.
	Written time.Time
}

// ChunkInfoLister is storage whose chunk listing carries each chunk's size
// and write time, so a pass over a store does not ask about every chunk again.
// Object storage lists both for free; asking costs a request per chunk.
type ChunkInfoLister interface {
	ListChunkInfo(ctx context.Context, storeID string) ([]ChunkInfo, error)
}

// listChunkInfo lists a store's chunks with their sizes, and reports whether
// their write times are known. Storage that cannot list sizes is asked for
// each chunk's.
func (s *Server) listChunkInfo(ctx context.Context, storeID string) ([]ChunkInfo, bool, error) {
	if l, ok := s.storage.(ChunkInfoLister); ok {
		infos, err := l.ListChunkInfo(ctx, storeID)
		return infos, true, err
	}
	ids, err := s.storage.ListChunks(ctx, storeID)
	if err != nil {
		return nil, false, err
	}
	out := make([]ChunkInfo, 0, len(ids))
	for _, id := range ids {
		size, err := s.storage.ChunkSize(ctx, storeID, id)
		if err != nil {
			return nil, false, err
		}
		out = append(out, ChunkInfo{ID: id, Size: size})
	}
	return out, false, nil
}

// CommitInfo is one stored commit's size and write time.
type CommitInfo struct {
	ID   protovcs.CommitID
	Size uint64

	// When the commit was written. Zero when the backend does not record it.
	Written time.Time
}

// CommitInfoLister is storage whose commit listing carries each commit's size
// and write time.
type CommitInfoLister interface {
	ListCommitInfo(ctx context.Context, storeID string) ([]CommitInfo, error)
}

// listCommitInfo lists a store's commits with their sizes. Storage that cannot
// list them is asked for each commit, and its write times are left zero.
func (s *Server) listCommitInfo(ctx context.Context, storeID string) ([]CommitInfo, error) {
	if l, ok := s.storage.(CommitInfoLister); ok {
		return l.ListCommitInfo(ctx, storeID)
	}
	ids, err := s.storage.ListCommits(ctx, storeID)
	if err != nil {
		return nil, err
	}
	out := make([]CommitInfo, 0, len(ids))
	for _, id := range ids {
		c, err := s.storage.GetCommit(ctx, storeID, id)
		if err != nil {
			return nil, err
		}
		out = append(out, CommitInfo{ID: id, Size: uint64(proto.Size(c))})
	}
	return out, nil
}

// CollectResult is what one collection run did.
type CollectResult struct {
	Reachable    int
	Deleted      int
	SpannedGrace int
	FreedBytes   uint64

	// Chunks found unreachable for the first time, and marked for a later run
	// to delete.
	Marked int

	// A commit was written while the run was working out what is reachable,
	// so it deleted nothing. The next run collects what this one would have.
	Interrupted bool
}

// Collect deletes chunks no commit reaches.
//
// The server can do this without reading anything, because a commit's parents
// and the chunk ids it adds are in the clear. Reachability is the union of the
// added sets over every commit reachable from any branch.
//
// Five things stop it destroying live data. A chunk younger than the grace
// period is left alone, because a writer uploads chunks before the commit that
// references them and during that window its chunks look exactly like garbage.
// A chunk covered by a live reservation is left alone for the same reason,
// more precisely. A commit younger than the grace period counts as a branch
// head, because a writer stores its commit before moving the branch to it, and
// the commit may reuse an old chunk that nothing else reaches. A run that
// sees a commit written while it worked deletes nothing, because that commit
// can reuse an old chunk too, and the run computed reachability without it.
//
// And deletion takes two runs. The first marks an unreachable chunk. A later
// run, at least [CollectionGrace] afterwards, deletes it if it is still
// unreachable and still marked. While a chunk is marked, HasChunks reports it
// missing, PutChunks takes it back, and PutCommit refuses a commit that adds
// it. So a commit that reuses an old chunk either takes it back first, or was
// written before the mark and is a young commit the later run counts. Without
// this, a commit recorded between a run's last check and its deletes could
// lose a chunk.
//
// This is the one operation in protovcs that deletes anything, so it fails
// closed: any error while computing what is reachable abandons the run rather
// than deleting what it managed to prove.
func (s *Server) Collect(ctx context.Context, storeID string) (CollectResult, error) {
	var out CollectResult

	store, err := s.storage.GetStore(ctx, storeID)
	if err != nil {
		return out, err
	}

	commitsBefore := store.GetUsage().GetCommits()
	reachable, err := s.reachable(ctx, storeID, s.now().Add(-CollectionGrace))
	if err != nil {
		return out, fmt.Errorf("protovcs: not collecting, because reachability could not be computed: %w", err)
	}
	out.Reachable = len(reachable)

	// A store being deleted has no reachable set worth keeping. Everything goes
	// once the retention window has passed.
	deleting := store.GetState() == pb.Store_STATE_DELETING

	all, timed, err := s.listChunkInfo(ctx, storeID)
	if err != nil {
		return out, err
	}

	cutoff := s.now().Add(-CollectionGrace)
	live, err := s.reservedChunkBudget(ctx, storeID)
	if err != nil {
		return out, err
	}

	var doomed []ChunkInfo
	for _, info := range all {
		if !deleting && reachable[info.ID] {
			continue
		}

		// A live reservation means somebody is mid-write. Their chunks are
		// unreachable by construction until their commit lands.
		if live && !deleting {
			out.SpannedGrace++
			continue
		}

		if !deleting {
			young := info.Written.After(cutoff)
			if !timed {
				young = s.chunkIsYoungerThan(ctx, storeID, info.ID, cutoff)
			}
			if young {
				out.SpannedGrace++
				continue
			}
		}
		doomed = append(doomed, info)
	}

	if !deleting {
		// Marks are left as they are while a write is in progress, because the
		// run did not look at the chunks it may be reusing.
		if live {
			return out, nil
		}
		if len(doomed) > 0 {
			now, err := s.storage.GetStore(ctx, storeID)
			if err != nil {
				return out, err
			}
			if now.GetUsage().GetCommits() != commitsBefore {
				out.Interrupted = true
				return out, nil
			}
		}
		doomed, err = s.markAndClaim(ctx, storeID, doomed, &out)
		if err != nil {
			return out, err
		}
	}

	// Deleted in parallel. A store being deleted is collected while its account
	// is deleted, and one request per chunk in sequence would take minutes on a
	// large one. What was deleted before a failure is still subtracted from the
	// counters below.
	var mu sync.Mutex
	var firstErr error
	var gone []protovcs.ChunkID
	sem := make(chan struct{}, collectParallelism)
	var wg sync.WaitGroup
	for _, info := range doomed {
		wg.Add(1)
		sem <- struct{}{}
		go func() {
			defer wg.Done()
			defer func() { <-sem }()
			err := s.storage.DeleteChunk(ctx, storeID, info.ID)
			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				if firstErr == nil {
					firstErr = err
				}
				return
			}
			out.Deleted++
			out.FreedBytes += info.Size
			gone = append(gone, info.ID)
		}()
	}
	wg.Wait()

	// A chunk whose delete failed stays claimed, and the next run tries again.
	if !deleting && len(gone) > 0 {
		if _, err := s.storage.UpdateCondemned(ctx, storeID, func(c *Condemned) error {
			for _, id := range gone {
				delete(c.Chunks, id)
			}
			return nil
		}); err != nil {
			firstErr = errors.Join(firstErr, err)
		}
	}

	if out.Deleted > 0 {
		freed := out.FreedBytes
		deleted := uint64(out.Deleted)
		if _, err := s.storage.Update(ctx, storeID, func(st *pb.Store) error {
			if st.Usage == nil {
				st.Usage = &pb.Usage{}
			}
			st.Usage.Objects = saturatingSub(st.Usage.Objects, deleted)
			st.Usage.Bytes = saturatingSub(st.Usage.Bytes, freed)
			return nil
		}); err != nil {
			return out, err
		}
	}
	return out, firstErr
}

// markAndClaim records which chunks are unreachable and returns the ones a
// run may delete now: those marked at least [CollectionGrace] ago, or claimed
// by a run that did not finish. They are claimed in the same update, after
// which a writer can no longer take them back.
//
// A mark on a chunk that is no longer unreachable is dropped. It was taken
// back, became reachable through a young commit, or is already gone.
func (s *Server) markAndClaim(ctx context.Context, storeID string, unreachable []ChunkInfo, out *CollectResult) ([]ChunkInfo, error) {
	now := s.now()
	cutoff := now.Add(-CollectionGrace).Unix()
	garbage := make(map[protovcs.ChunkID]bool, len(unreachable))
	for _, info := range unreachable {
		garbage[info.ID] = true
	}

	var claimed []ChunkInfo
	var marked int
	_, err := s.storage.UpdateCondemned(ctx, storeID, func(c *Condemned) error {
		claimed, marked = claimed[:0], 0
		for id := range c.Chunks {
			if !garbage[id] {
				delete(c.Chunks, id)
			}
		}
		for _, info := range unreachable {
			e, ok := c.Chunks[info.ID]
			switch {
			case !ok:
				c.Chunks[info.ID] = CondemnedChunk{MarkedUnix: now.Unix()}
				marked++
			case e.Deleting || e.MarkedUnix <= cutoff:
				e.Deleting = true
				c.Chunks[info.ID] = e
				claimed = append(claimed, info)
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	out.Marked = marked
	out.SpannedGrace += len(unreachable) - len(claimed) - marked
	return claimed, nil
}

// reachable returns every chunk id reachable from any branch, or from any
// commit written after youngSince. A commit with no recorded write time counts
// as young, so storage that cannot say when a commit was written keeps
// everything any commit reaches.
func (s *Server) reachable(ctx context.Context, storeID string, youngSince time.Time) (map[protovcs.ChunkID]bool, error) {
	refs, err := s.storage.ListRefs(ctx, storeID)
	if err != nil {
		return nil, err
	}
	commits, err := s.listCommitInfo(ctx, storeID)
	if err != nil {
		return nil, err
	}

	out := map[protovcs.ChunkID]bool{}
	seen := map[protovcs.CommitID]bool{}
	var queue []protovcs.CommitID
	for _, r := range refs {
		queue = append(queue, r.Commit)
	}
	for _, c := range commits {
		if c.Written.IsZero() || c.Written.After(youngSince) {
			queue = append(queue, c.ID)
		}
	}

	for len(queue) > 0 {
		id := queue[0]
		queue = queue[1:]
		if seen[id] {
			continue
		}
		seen[id] = true

		c, err := s.storage.GetCommit(ctx, storeID, id)
		if err != nil {
			return nil, err
		}
		for _, raw := range c.GetAdded() {
			cid, err := protovcs.ChunkIDFromBytes(raw)
			if err != nil {
				return nil, err
			}
			out[cid] = true
		}
		for _, raw := range c.GetParents() {
			pid, err := protovcs.CommitIDFromBytes(raw)
			if err != nil {
				return nil, err
			}
			queue = append(queue, pid)
		}
	}
	return out, nil
}

// reservedChunkBudget reports whether any reservation is still live.
func (s *Server) reservedChunkBudget(ctx context.Context, storeID string) (bool, error) {
	reservations, err := s.storage.ListReservations(ctx, storeID)
	if err != nil {
		return false, err
	}
	now := s.now().Unix()
	for _, r := range reservations {
		if r.ExpiresUnix >= now {
			return true, nil
		}
	}
	return false, nil
}

// chunkIsYoungerThan is a hook for backends that record write times. The
// in-memory one does; object storage gives it for free.
func (s *Server) chunkIsYoungerThan(ctx context.Context, storeID string, id protovcs.ChunkID, cutoff time.Time) bool {
	type aged interface {
		ChunkWrittenAt(ctx context.Context, storeID string, id protovcs.ChunkID) (time.Time, error)
	}
	a, ok := s.storage.(aged)
	if !ok {
		// A backend that cannot say how old a chunk is cannot distinguish an
		// abandoned write from a live one, so nothing outside the reachable set
		// is ever collected. Refusing to guess is the only safe answer.
		return true
	}
	at, err := a.ChunkWrittenAt(ctx, storeID, id)
	if err != nil {
		return true
	}
	return at.After(cutoff)
}

// ExpireReservations releases quota held by writes that never finished.
func (s *Server) ExpireReservations(ctx context.Context, storeID string) (int, error) {
	reservations, err := s.storage.ListReservations(ctx, storeID)
	if err != nil {
		return 0, err
	}

	now := s.now().Unix()
	released := 0
	for _, r := range reservations {
		if r.ExpiresUnix >= now {
			continue
		}
		if err := s.releaseReservation(ctx, storeID, r.ID); err != nil {
			return released, err
		}
		released++
	}
	return released, nil
}

// Reconcile recounts usage from what is actually stored and corrects drift.
//
// Drift is not expected. The counters move in the same transaction as the write
// that causes them to move. This exists so that drift is a detected condition
// rather than a surprise found while investigating something else, and it
// returns what it corrected so a caller can alarm on it being non-zero.
func (s *Server) Reconcile(ctx context.Context, storeID string) (before, after *pb.Usage, err error) {
	store, err := s.storage.GetStore(ctx, storeID)
	if err != nil {
		return nil, nil, err
	}
	before = store.GetUsage()

	chunks, _, err := s.listChunkInfo(ctx, storeID)
	if err != nil {
		return nil, nil, err
	}
	var bytes uint64
	for _, c := range chunks {
		bytes += c.Size
	}
	// Commits are charged by their size, as PutCommit charges them.
	commits, err := s.listCommitInfo(ctx, storeID)
	if err != nil {
		return nil, nil, err
	}
	for _, c := range commits {
		bytes += c.Size
	}
	refs, err := s.storage.ListRefs(ctx, storeID)
	if err != nil {
		return nil, nil, err
	}

	updated, err := s.storage.Update(ctx, storeID, func(st *pb.Store) error {
		if st.Usage == nil {
			st.Usage = &pb.Usage{}
		}
		st.Usage.Objects = uint64(len(chunks))
		st.Usage.Bytes = bytes
		st.Usage.Commits = uint64(len(commits))
		st.Usage.Refs = uint32(len(refs))
		return nil
	})
	if err != nil {
		return before, nil, err
	}
	return before, updated.GetUsage(), nil
}
