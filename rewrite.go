package protovcs

import (
	"context"
	"errors"
	"fmt"
	"maps"

	"google.golang.org/protobuf/proto"

	pb "github.com/nextgui/protovcs/proto/protovcs/v1"
)

// RewriteFunc changes one version of a store in place. It is called once per
// commit, parents before children, with that commit's message.
//
// It must be a pure function of the message: the same input must give the
// same output, because a rewrite that loses a race on a branch rewrites the
// commits that landed meanwhile with the same function.
type RewriteFunc func(ctx context.Context, m proto.Message) error

// RewriteResult says what [Store.Rewrite] did.
type RewriteResult struct {
	rewritten map[CommitID]CommitID
	moved     int
}

// Rewritten maps each commit that was replaced to the commit replacing it.
// A commit the function did not change, and whose parents were not replaced,
// is kept and is absent from the map.
func (r RewriteResult) Rewritten() map[CommitID]CommitID {
	return maps.Clone(r.rewritten)
}

// BranchesMoved is how many branches now point at a rewritten commit.
func (r RewriteResult) BranchesMoved() int { return r.moved }

// historyBatch is how many commits one GetCommits call asks for.
const historyBatch = 1000

// Rewrite replaces every commit reachable from any branch with one whose
// message has been passed through rewrite, and then moves every branch to the
// rewritten history.
//
// The shape of the history is kept: a rewritten commit has the rewritten
// commits of its parents as parents, in the same order, and keeps the branch
// name, message and time of the commit it replaces. A commit rewrite leaves
// unchanged, whose parents were not replaced either, is kept as it is, so a
// history whose early commits hold nothing to change keeps those commits.
//
// Branches are moved last, each by compare-and-swap with force, because a
// rewritten commit does not descend from the one it replaces. When another
// writer moves a branch first, the commits it added are rewritten too and the
// move is tried again. Branches already moved stay moved; a rewrite that gives
// up partway leaves some branches on rewritten history and the rest on the
// old, and running it again finishes the job.
//
// The replaced commits stay in the store until nothing reaches them. A
// server's collection then deletes every chunk only they reached, once the
// grace period has passed.
func (s *Store) Rewrite(ctx context.Context, rewrite RewriteFunc) (RewriteResult, error) {
	mapped := map[CommitID]CommitID{}
	result := RewriteResult{rewritten: map[CommitID]CommitID{}}

	// Loaded first, so the marks it holds follow the rewrite too.
	if err := s.loadMarks(ctx); err != nil {
		return result, err
	}

	var lastErr error
	for attempt := 0; attempt < DefaultMaxAttempts; attempt++ {
		if attempt > 0 {
			if err := waitBefore(ctx, backoff(attempt)); err != nil {
				return result, err
			}
		}
		refs, err := s.transport.ListRefs(ctx)
		if err != nil {
			return result, err
		}
		if err := s.rewriteReachable(ctx, refs, mapped, rewrite, &result); err != nil {
			return result, err
		}

		lost, err := s.moveRewrittenRefs(ctx, refs, mapped, &result)
		if err != nil {
			return result, err
		}
		if lost == nil {
			s.followRewrite(ctx, mapped)
			return result, nil
		}
		lastErr = lost
	}
	return result, fmt.Errorf("protovcs: gave up rewriting after %d attempts: %w", DefaultMaxAttempts, lastErr)
}

// rewriteReachable rewrites every commit reachable from refs that is not yet
// in mapped, parents first, and records each in mapped.
func (s *Store) rewriteReachable(ctx context.Context, refs []RefState, mapped map[CommitID]CommitID,
	rewrite RewriteFunc, result *RewriteResult) error {

	var heads []CommitID
	for _, r := range refs {
		heads = append(heads, r.Commit)
	}
	order, err := s.historyOf(ctx, heads, mapped)
	if err != nil {
		return err
	}
	for _, commit := range order {
		id, err := CommitIDFromBytes(commit.GetId())
		if err != nil {
			return err
		}
		next, err := s.rewriteCommit(ctx, commit, mapped, rewrite)
		if err != nil {
			return fmt.Errorf("protovcs: rewriting commit %s: %w", id, err)
		}
		mapped[id] = next
		if next != id {
			mapped[next] = next
			result.rewritten[id] = next
		}
	}
	return nil
}

// moveRewrittenRefs points each ref at the rewrite of its commit. It returns
// a non-nil lost error when another writer moved a ref first, and an error
// for anything else.
func (s *Store) moveRewrittenRefs(ctx context.Context, refs []RefState, mapped map[CommitID]CommitID,
	result *RewriteResult) (lost, err error) {

	for _, r := range refs {
		next, ok := mapped[r.Commit]
		if !ok {
			return nil, fmt.Errorf("protovcs: branch head %s was not rewritten", r.Commit)
		}
		if next == r.Commit {
			continue
		}
		if _, err := s.transport.UpdateRef(ctx, r.Name, next, r.CASToken, true); err != nil {
			if errors.Is(err, ErrRaceLost) {
				return err, nil
			}
			return nil, err
		}
		result.moved++
	}
	return nil, nil
}

// followRewrite moves what this client remembers of each branch onto the
// rewritten history, so its next read does not compare against a commit that
// no branch reaches. The marks in its HeadMarks move with it.
func (s *Store) followRewrite(ctx context.Context, mapped map[CommitID]CommitID) {
	var moved []string
	s.mu.Lock()
	for name, seen := range s.highWater {
		if next, ok := mapped[seen]; ok {
			s.highWater[name] = next
			moved = append(moved, name)
		}
	}
	s.mu.Unlock()
	for _, name := range moved {
		s.saveMark(ctx, name)
	}
}

// rewriteCommit returns the commit replacing one: the same commit when
// rewrite changes nothing and no parent was replaced, a new one otherwise.
func (s *Store) rewriteCommit(ctx context.Context, commit *pb.Commit, mapped map[CommitID]CommitID,
	rewrite RewriteFunc) (CommitID, error) {

	id, err := CommitIDFromBytes(commit.GetId())
	if err != nil {
		return CommitID{}, err
	}
	payload, err := s.openPayload(ctx, commit)
	if err != nil {
		return CommitID{}, err
	}
	message, err := s.readCommit(ctx, commit)
	if err != nil {
		return CommitID{}, err
	}
	before := proto.Clone(message)
	if err := rewrite(ctx, message); err != nil {
		return CommitID{}, err
	}

	parents := make([]*pb.Commit, 0, len(commit.GetParents()))
	parentsMoved := false
	for _, raw := range commit.GetParents() {
		pid, err := CommitIDFromBytes(raw)
		if err != nil {
			return CommitID{}, err
		}
		next, ok := mapped[pid]
		if !ok {
			return CommitID{}, fmt.Errorf("protovcs: parent %s was not rewritten before its child", pid)
		}
		if next != pid {
			parentsMoved = true
		}
		parent, err := s.commitByID(ctx, next)
		if err != nil {
			return CommitID{}, err
		}
		parents = append(parents, parent)
	}
	if !parentsMoved && proto.Equal(before, message) {
		return id, nil
	}

	refs, pending, err := s.itemRefs(message)
	if err != nil {
		return CommitID{}, err
	}
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

	next, err := s.recordVersion(ctx, versionSpec{
		branch:  payload.GetBranch(),
		message: payload.GetMessage(),
		at:      payload.GetTime(),
	}, parents, nodes, nodes[len(nodes)-1].ID, pending)
	if err != nil {
		return CommitID{}, err
	}

	s.memo.putChunks(pending)
	return next, nil
}

// itemRefs materializes a message into the items a version names, and the
// plaintext of each by chunk id.
func (s *Store) itemRefs(m proto.Message) ([]ItemRef, map[ChunkID][]byte, error) {
	items, err := s.layout.Materialize(m)
	if err != nil {
		return nil, nil, err
	}
	pending := map[ChunkID][]byte{}
	refs := make([]ItemRef, 0, len(items))
	for _, it := range items {
		canonical, err := CanonicalBytes(it.Fragment)
		if err != nil {
			return nil, nil, fmt.Errorf("protovcs: encoding item %q: %w", it.Key, err)
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
	return refs, pending, nil
}

// historyOf returns every commit reachable from heads that mapped does not
// hold, parents before children. The walk stops at a commit mapped holds.
func (s *Store) historyOf(ctx context.Context, heads []CommitID, mapped map[CommitID]CommitID) ([]*pb.Commit, error) {
	known := map[CommitID]*pb.Commit{}
	queue := append([]CommitID(nil), heads...)
	for len(queue) > 0 {
		id := queue[0]
		queue = queue[1:]
		if _, done := mapped[id]; done {
			continue
		}
		if _, ok := known[id]; ok {
			continue
		}
		commits, err := s.transport.GetCommits(ctx, id, historyBatch)
		if err != nil {
			return nil, err
		}
		if len(commits) == 0 {
			return nil, fmt.Errorf("%w: commit %s", ErrNoSuchCommit, id)
		}
		for _, c := range commits {
			cid, err := verifiedName(c)
			if err != nil {
				return nil, err
			}
			if _, done := mapped[cid]; done {
				continue
			}
			known[cid] = c
			s.memo.putCommit(cid, c)
			for _, raw := range c.GetParents() {
				pid, err := CommitIDFromBytes(raw)
				if err != nil {
					return nil, err
				}
				queue = append(queue, pid)
			}
		}
	}
	return parentsFirst(heads, known)
}

// parentsFirst orders commits so that every commit comes after its parents.
// Iterative, so a long history does not need a deep stack.
func parentsFirst(heads []CommitID, known map[CommitID]*pb.Commit) ([]*pb.Commit, error) {
	type frame struct {
		id       CommitID
		expanded bool
	}
	var out []*pb.Commit
	state := map[CommitID]int{} // 1 visiting, 2 done
	for _, head := range heads {
		stack := []frame{{id: head}}
		for len(stack) > 0 {
			top := stack[len(stack)-1]
			stack = stack[:len(stack)-1]
			c, ok := known[top.id]
			if !ok || state[top.id] == 2 {
				continue
			}
			if top.expanded {
				state[top.id] = 2
				out = append(out, c)
				continue
			}
			if state[top.id] == 1 {
				return nil, fmt.Errorf("protovcs: commit %s is its own ancestor", top.id)
			}
			state[top.id] = 1
			stack = append(stack, frame{id: top.id, expanded: true})
			for _, raw := range c.GetParents() {
				pid, err := CommitIDFromBytes(raw)
				if err != nil {
					return nil, err
				}
				if state[pid] == 0 {
					stack = append(stack, frame{id: pid})
				}
			}
		}
	}
	return out, nil
}
