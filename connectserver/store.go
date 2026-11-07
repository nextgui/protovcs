// Package connectserver serves protovcs's StoreService and StoreAdminService
// over Connect.
//
// A translation and nothing else. Every decision about what a store holds, such
// as what a quota allows, whether a commit may name a chunk and whether a branch
// may move, is made by [server.Server]. Who may call which method is decided by
// an [Authenticator] and the access level each method needs.
//
// [NewStoreHandler] and [NewAdminHandler] are complete Connect handlers that
// check access themselves. [Store] and [Admin] are the same methods without
// the check, for a host that checks access its own way before calling them.
package connectserver

import (
	"context"
	"errors"
	"time"

	"connectrpc.com/connect"

	protovcs "github.com/nextgui/protovcs"
	pb "github.com/nextgui/protovcs/proto/protovcs/v1"
	"github.com/nextgui/protovcs/server"
	"github.com/nextgui/protovcs/storetoken"
)

// Store is StoreService's methods over a server, with no access check.
type Store struct {
	server *server.Server
	now    func() time.Time
}

// NewStore returns the methods over srv. now decides when a watch's token has
// expired; nil means [time.Now].
func NewStore(srv *server.Server, now func() time.Time) (*Store, error) {
	if srv == nil {
		return nil, errors.New("connectserver: built without a store server")
	}
	if now == nil {
		now = time.Now
	}
	return &Store{server: srv, now: now}, nil
}

// asConnect maps a rule's refusal to the code a client should see.
//
// The distinctions matter to a caller rather than to a log. Losing a race is
// ordinary and retried; being out of quota is not retried and needs the message
// to reach a person; a branch that would leave its own history is a mistake to
// report rather than to work around.
func asConnect(err error) error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, server.ErrNoSuchStore):
		return connect.NewError(connect.CodeNotFound, err)
	case errors.Is(err, server.ErrHintTooLarge):
		return connect.NewError(connect.CodeInvalidArgument, err)
	case errors.Is(err, server.ErrQuotaExceeded):
		return connect.NewError(connect.CodeResourceExhausted, err)
	case errors.Is(err, server.ErrNoReservation):
		return connect.NewError(connect.CodeFailedPrecondition, err)
	case errors.Is(err, server.ErrSchemaTooOld):
		return connect.NewError(connect.CodeFailedPrecondition, err)
	case errors.Is(err, server.ErrNotRestorable):
		return connect.NewError(connect.CodeFailedPrecondition, err)
	case errors.Is(err, protovcs.ErrRaceLost):
		return connect.NewError(connect.CodeAborted, err)
	case errors.Is(err, protovcs.ErrNotFastForward):
		return connect.NewError(connect.CodeFailedPrecondition, err)
	case errors.Is(err, protovcs.ErrNoSuchChunk):
		return connect.NewError(connect.CodeNotFound, err)
	case errors.Is(err, protovcs.ErrNoSuchCommit):
		// Not found rather than internal. A client holding a commit id for a
		// while can ask for one the server no longer has, and it needs to tell
		// that from a server that is broken.
		return connect.NewError(connect.CodeNotFound, err)
	default:
		return connect.NewError(connect.CodeInternal, err)
	}
}

func chunkIDs(raw [][]byte) ([]protovcs.ChunkID, error) {
	out := make([]protovcs.ChunkID, 0, len(raw))
	for _, b := range raw {
		id, err := protovcs.ChunkIDFromBytes(b)
		if err != nil {
			return nil, err
		}
		out = append(out, id)
	}
	return out, nil
}

func chunkIDBytes(ids []protovcs.ChunkID) [][]byte {
	out := make([][]byte, 0, len(ids))
	for _, id := range ids {
		out = append(out, id.Bytes())
	}
	return out
}

func refsOf(refs []protovcs.RefState) []*pb.Ref {
	out := make([]*pb.Ref, 0, len(refs))
	for _, r := range refs {
		out = append(out, &pb.Ref{Name: r.Name, Commit: r.Commit.Bytes(), CasToken: r.CASToken})
	}
	return out
}

// ListRefs returns every branch and the commit it points at.
func (s *Store) ListRefs(ctx context.Context, req *pb.ListRefsRequest) (*pb.ListRefsResponse, error) {
	refs, err := s.server.ListRefs(ctx, req.GetStoreId())
	if err != nil {
		return nil, asConnect(err)
	}
	return &pb.ListRefsResponse{Refs: refsOf(refs)}, nil
}

// WatchRefs streams the store's branches, once at the start and again every
// time one of them moves.
//
// It returns when ctx ends, and the server releases the subscription behind it
// before this returns.
//
// It also returns when expires passes, if it is not zero. Access is checked
// once, when the stream opens, so without this a watch would keep reporting
// branch moves to a caller whose token is no longer valid. The stream then ends
// with CodeUnauthenticated wrapping [storetoken.ErrExpired], and a client with a
// fresh token opens a new one.
func (s *Store) WatchRefs(ctx context.Context, req *pb.WatchRefsRequest, expires time.Time, send func(*pb.WatchRefsResponse) error) error {
	if !expires.IsZero() && !s.now().Before(expires) {
		return connect.NewError(connect.CodeUnauthenticated, storetoken.ErrExpired)
	}
	ctx, cancel := s.untilExpiry(ctx, expires)
	defer cancel()

	err := s.server.WatchRefs(ctx, req.GetStoreId(), func(refs []protovcs.RefState) error {
		return send(&pb.WatchRefsResponse{Refs: refsOf(refs)})
	})

	if errors.Is(context.Cause(ctx), errTokenExpired) {
		return connect.NewError(connect.CodeUnauthenticated, storetoken.ErrExpired)
	}
	// A watch has no natural end, so the caller leaving is how it always
	// finishes. Reporting that as a failure would put a cancellation in every
	// server log.
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return nil
	}
	return asConnect(err)
}

// errTokenExpired is the cause of a watch context ended by its token's expiry.
var errTokenExpired = errors.New("connectserver: the watch's token expired")

func (s *Store) untilExpiry(ctx context.Context, expires time.Time) (context.Context, context.CancelFunc) {
	if expires.IsZero() {
		return context.WithCancel(ctx)
	}
	return context.WithTimeoutCause(ctx, expires.Sub(s.now()), errTokenExpired)
}

// UpdateRef moves a branch, or reports that somebody else moved it first.
func (s *Store) UpdateRef(ctx context.Context, req *pb.UpdateRefRequest) (*pb.UpdateRefResponse, error) {
	var commit protovcs.CommitID
	if !req.GetDelete() {
		var err error
		if commit, err = protovcs.CommitIDFromBytes(req.GetCommit()); err != nil {
			return nil, connect.NewError(connect.CodeInvalidArgument, err)
		}
	}

	state, err := s.server.UpdateRef(ctx, req.GetStoreId(), req.GetName(),
		commit, req.GetExpectedCasToken(), req.GetForce())

	// Losing a race is not an error on the wire. It is the answer to a question
	// the caller asked, and the caller's next move is to rebuild and try again,
	// which needs the current state, so it is returned rather than dropped.
	if errors.Is(err, protovcs.ErrRaceLost) {
		return &pb.UpdateRefResponse{
			Updated: false,
			Current: &pb.Ref{Name: state.Name, Commit: state.Commit.Bytes(), CasToken: state.CASToken},
		}, nil
	}
	if err != nil {
		return nil, asConnect(err)
	}
	return &pb.UpdateRefResponse{
		Updated: true,
		Current: &pb.Ref{Name: state.Name, Commit: state.Commit.Bytes(), CasToken: state.CASToken},
	}, nil
}

// GetCommits walks back from a commit.
func (s *Store) GetCommits(ctx context.Context, req *pb.GetCommitsRequest) (*pb.GetCommitsResponse, error) {
	from, err := protovcs.CommitIDFromBytes(req.GetFrom())
	if err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, err)
	}
	commits, err := s.server.GetCommits(ctx, req.GetStoreId(), from, req.GetDepth())
	if err != nil {
		return nil, asConnect(err)
	}
	return &pb.GetCommitsResponse{Commits: commits}, nil
}

// PutCommit records a version without making it reachable.
func (s *Store) PutCommit(ctx context.Context, req *pb.PutCommitRequest) (*pb.PutCommitResponse, error) {
	id, err := s.server.PutCommit(ctx, req.GetStoreId(), req.GetReservation(), req.GetCommit())
	if err != nil {
		return nil, asConnect(err)
	}
	return &pb.PutCommitResponse{Id: id.Bytes()}, nil
}

// MissingBetween returns what a client holding one commit is short of at
// another.
func (s *Store) MissingBetween(ctx context.Context, req *pb.MissingBetweenRequest) (*pb.MissingBetweenResponse, error) {
	have := make([]protovcs.CommitID, 0, len(req.GetHave()))
	for _, raw := range req.GetHave() {
		id, err := protovcs.CommitIDFromBytes(raw)
		if err != nil {
			return nil, connect.NewError(connect.CodeInvalidArgument, err)
		}
		have = append(have, id)
	}
	want, err := protovcs.CommitIDFromBytes(req.GetWant())
	if err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, err)
	}

	ids, full, err := s.server.MissingBetween(ctx, req.GetStoreId(), have, want)
	if err != nil {
		return nil, asConnect(err)
	}
	return &pb.MissingBetweenResponse{ChunkIds: chunkIDBytes(ids), Full: full}, nil
}

// BeginWrite reserves quota before anything is uploaded.
func (s *Store) BeginWrite(ctx context.Context, req *pb.BeginWriteRequest) (*pb.BeginWriteResponse, error) {
	r, err := s.server.BeginWrite(ctx, req.GetStoreId(), req.GetEstimatedChunks(), req.GetEstimatedBytes())
	if err != nil {
		return nil, asConnect(err)
	}
	return &pb.BeginWriteResponse{Reservation: r.ID, ExpiresUnix: r.ExpiresUnix}, nil
}

// HasChunks returns which ids the store does not hold.
func (s *Store) HasChunks(ctx context.Context, req *pb.HasChunksRequest) (*pb.HasChunksResponse, error) {
	ids, err := chunkIDs(req.GetChunkIds())
	if err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, err)
	}
	missing, err := s.server.HasChunks(ctx, req.GetStoreId(), ids)
	if err != nil {
		return nil, asConnect(err)
	}
	return &pb.HasChunksResponse{Missing: chunkIDBytes(missing)}, nil
}

// PutChunks stores a batch.
func (s *Store) PutChunks(ctx context.Context, req *pb.PutChunksRequest) (*pb.PutChunksResponse, error) {
	chunks := make([]protovcs.SealedChunk, 0, len(req.GetChunks()))
	ids := make([]protovcs.ChunkID, 0, len(req.GetChunks()))
	seen := make(map[protovcs.ChunkID]bool, len(req.GetChunks()))
	for _, c := range req.GetChunks() {
		id, err := protovcs.ChunkIDFromBytes(c.GetChunkId())
		if err != nil {
			return nil, connect.NewError(connect.CodeInvalidArgument, err)
		}
		chunks = append(chunks, protovcs.SealedChunk{ID: id, Sealed: c.GetSealed()})
		if !seen[id] {
			seen[id] = true
			ids = append(ids, id)
		}
	}

	before, err := s.server.HasChunks(ctx, req.GetStoreId(), ids)
	if err != nil {
		return nil, asConnect(err)
	}
	if err := s.server.PutChunks(ctx, req.GetStoreId(), req.GetReservation(), chunks); err != nil {
		return nil, asConnect(err)
	}
	return &pb.PutChunksResponse{
		Stored:         uint32(len(before)),
		AlreadyPresent: uint32(len(chunks) - len(before)),
	}, nil
}

// PackHint records which chunks a head reaches.
func (s *Store) PackHint(ctx context.Context, req *pb.PackHintRequest) (*pb.PackHintResponse, error) {
	commit, err := protovcs.CommitIDFromBytes(req.GetCommit())
	if err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, err)
	}
	ids, err := chunkIDs(req.GetChunkIds())
	if err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, err)
	}
	accepted, err := s.server.PackHint(ctx, req.GetStoreId(), commit, ids)
	if err != nil {
		return nil, asConnect(err)
	}
	return &pb.PackHintResponse{Accepted: accepted}, nil
}

// GetChunks streams chunks back.
//
// Streamed rather than batched into one response because the common operation
// on a store is reading all of it, and a client can start opening chunks before
// the last one has arrived.
func (s *Store) GetChunks(ctx context.Context, req *pb.GetChunksRequest, send func(*pb.GetChunksResponse) error) error {
	ids, err := chunkIDs(req.GetChunkIds())
	if err != nil {
		return connect.NewError(connect.CodeInvalidArgument, err)
	}
	chunks, err := s.server.GetChunks(ctx, req.GetStoreId(), ids)
	if err != nil {
		return asConnect(err)
	}
	for _, chunk := range chunks {
		if err := send(&pb.GetChunksResponse{ChunkId: chunk.ID.Bytes(), Sealed: chunk.Sealed}); err != nil {
			return err
		}
	}
	return nil
}
