// Package connecttransport reaches a protovcs store over the network.
//
// The only transport that crosses a machine boundary. It translates and decides
// nothing: every refusal comes back from the server, and this turns it into the
// error a caller can act on — losing a race must be distinguishable from being
// out of quota, because the first is retried and the second is not.
package connecttransport

import (
	"context"
	"errors"
	"fmt"
	"net/http"

	"connectrpc.com/connect"

	protovcs "github.com/nextgui/protovcs"
	pb "github.com/nextgui/protovcs/proto/protovcs/v1"
)

// Transport is a protovcs store reached over Connect.
type Transport struct {
	client  pb.StoreServiceClient
	storeID string

	// The admin service, for the one question on it a store's own holder may
	// ask: how much room the store is taking. Nil when the transport was built
	// from an existing StoreService client, which says nothing about where the
	// admin service is.
	admin pb.StoreAdminServiceClient
}

// New returns a transport for one store.
//
// storeID is fixed at construction rather than passed per call: a client holds
// one store's key, and a handle that could address another store would be one
// that could be pointed at the wrong one.
func New(httpClient connect.HTTPClient, baseURL, storeID string, opts ...connect.ClientOption) *Transport {
	return &Transport{
		client:  pb.NewStoreServiceClient(httpClient, baseURL, opts...),
		admin:   pb.NewStoreAdminServiceClient(httpClient, baseURL, opts...),
		storeID: storeID,
	}
}

// NewWithClient wraps an existing generated client, for a caller that has one
// already configured.
func NewWithClient(client pb.StoreServiceClient, storeID string) *Transport {
	return &Transport{client: client, storeID: storeID}
}

// WithToken attaches a store token to every request the client makes.
//
// A store id is not a secret — it is in a URL path and in logs — so a server
// reachable by more than one person needs proof that a request may touch the
// store it names. This is how a client supplies it.
//
// A function rather than a string, because a token expires and a client that
// outlives one should remint rather than start failing. It is called once per
// request; a caller with a fixed token returns it unchanged.
//
// An interceptor rather than a header set at each call, so that a method added
// later carries the token without anybody remembering to add it. Forgetting is
// the failure worth designing out: it is not caught by any test that only
// exercises the methods that exist today.
func WithToken(token func(context.Context) (string, error)) connect.ClientOption {
	return connect.WithInterceptors(tokenInterceptor{token: token})
}

type tokenInterceptor struct {
	token func(context.Context) (string, error)
}

func (i tokenInterceptor) WrapUnary(next connect.UnaryFunc) connect.UnaryFunc {
	return func(ctx context.Context, req connect.AnyRequest) (connect.AnyResponse, error) {
		if err := i.set(ctx, req.Header()); err != nil {
			return nil, err
		}
		return next(ctx, req)
	}
}

func (i tokenInterceptor) WrapStreamingClient(next connect.StreamingClientFunc) connect.StreamingClientFunc {
	return func(ctx context.Context, spec connect.Spec) connect.StreamingClientConn {
		conn := next(ctx, spec)
		// A failure to mint shows up when the stream is used rather than here,
		// because this returns no error. Setting nothing leaves the request
		// unauthenticated, which the server refuses — the same outcome as a bad
		// token, reported by the side that decides.
		_ = i.set(ctx, conn.RequestHeader())
		return conn
	}
}

// WrapStreamingHandler is required by the interface and does nothing: this is a
// client interceptor, and a handler here would be attaching a credential to a
// response.
func (i tokenInterceptor) WrapStreamingHandler(next connect.StreamingHandlerFunc) connect.StreamingHandlerFunc {
	return next
}

func (i tokenInterceptor) set(ctx context.Context, header http.Header) error {
	if i.token == nil {
		return nil
	}
	token, err := i.token(ctx)
	if err != nil {
		return fmt.Errorf("protovcs: getting a store token: %w", err)
	}
	if token != "" {
		header.Set("Authorization", "Bearer "+token)
	}
	return nil
}

// fromConnect turns a server's refusal back into the error a caller matches on.
//
// The codes are the wire's vocabulary and the errors are the library's, and a
// caller retries on one of them and not the others — so this mapping is what
// makes Change's retry loop work over a network rather than only in process.
func fromConnect(err error) error {
	if err == nil {
		return nil
	}
	var cerr *connect.Error
	if !errors.As(err, &cerr) {
		return err
	}
	switch cerr.Code() {
	case connect.CodeAborted:
		return fmt.Errorf("%w: %s", protovcs.ErrRaceLost, cerr.Message())
	case connect.CodeNotFound:
		return fmt.Errorf("%w: %s", protovcs.ErrNoSuchChunk, cerr.Message())
	case connect.CodeUnauthenticated:
		return fmt.Errorf("%w: %s", protovcs.ErrTokenExpired, cerr.Message())
	default:
		return err
	}
}

// ListRefs returns every branch and the commit it points at.
func (t *Transport) ListRefs(ctx context.Context) ([]protovcs.RefState, error) {
	resp, err := t.client.ListRefs(ctx, connect.NewRequest(&pb.ListRefsRequest{StoreId: t.storeID}))
	if err != nil {
		return nil, fromConnect(err)
	}

	out := make([]protovcs.RefState, 0, len(resp.Msg.GetRefs()))
	for _, r := range resp.Msg.GetRefs() {
		commit, err := protovcs.CommitIDFromBytes(r.GetCommit())
		if err != nil {
			return nil, err
		}
		out = append(out, protovcs.RefState{Name: r.GetName(), Commit: commit, CASToken: r.GetCasToken()})
	}
	return out, nil
}

// WatchRefs calls deliver with the store's branches and again whenever one
// moves, reading the stream until it ends.
//
// The stream is closed before this returns, however it ends — the caller
// cancelling, deliver refusing, or the connection breaking — so a watcher that
// walked away leaves no connection behind.
//
// The server ends a watch when the token it was opened with expires. This then
// opens a new stream, which asks the token source for a token again. The new
// stream's first delivery is the current state, so nothing that moved in
// between is missed. A stream that ends that way before delivering anything is
// not reopened, so a token source that keeps returning an expired token gets
// [protovcs.ErrTokenExpired] rather than a loop.
//
// A stream that carries nothing for a long time is a stream an intermediary may
// close. Nothing here sends a keepalive, so an idle watch behind a proxy with an
// idle timeout ends with that proxy's error, and the caller's answer is to call
// this again: the first delivery is the current state, so a watch reopened after
// a gap reports anything that moved during it.
func (t *Transport) WatchRefs(ctx context.Context, deliver func([]protovcs.RefState) error) error {
	for {
		delivered, err := t.watchOnce(ctx, deliver)
		if errors.Is(err, protovcs.ErrTokenExpired) && delivered && ctx.Err() == nil {
			continue
		}
		return err
	}
}

// watchOnce reads one WatchRefs stream until it ends, and reports whether it
// delivered anything.
func (t *Transport) watchOnce(ctx context.Context, deliver func([]protovcs.RefState) error) (bool, error) {
	// Cancelled on every return, including deliver's refusal. Connect unblocks
	// a stalled Receive on the request context and not on Close, so a watch
	// waiting for a change that never comes needs this to be able to stop.
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	stream, err := t.client.WatchRefs(ctx, connect.NewRequest(&pb.WatchRefsRequest{StoreId: t.storeID}))
	if err != nil {
		return false, fromConnect(err)
	}
	defer stream.Close()

	delivered := false
	for stream.Receive() {
		refs := make([]protovcs.RefState, 0, len(stream.Msg().GetRefs()))
		for _, r := range stream.Msg().GetRefs() {
			commit, err := protovcs.CommitIDFromBytes(r.GetCommit())
			if err != nil {
				return delivered, err
			}
			refs = append(refs, protovcs.RefState{Name: r.GetName(), Commit: commit, CASToken: r.GetCasToken()})
		}
		if err := deliver(refs); err != nil {
			return delivered, err
		}
		delivered = true
	}
	if err := stream.Err(); err != nil {
		return delivered, fromConnect(err)
	}

	// A watch has no natural end, so reaching here means it stopped. When the
	// caller stopped it that is the context's error, which is what a caller
	// checks for to tell its own cancellation from a failure. Otherwise the
	// server closed the stream, which is reported rather than returned as nil:
	// a nil would read as the watch having finished its work.
	if ctx.Err() != nil {
		return delivered, ctx.Err()
	}
	return delivered, fmt.Errorf("protovcs: the server ended the ref watch")
}

// updateRefError turns a server's refusal on UpdateRef into the error a caller
// matches on. FailedPrecondition on UpdateRef means the target commit does not
// descend from the branch head and force was not set.
func updateRefError(err error) error {
	var cerr *connect.Error
	if errors.As(err, &cerr) && cerr.Code() == connect.CodeFailedPrecondition {
		return fmt.Errorf("%w: %s", protovcs.ErrNotFastForward, cerr.Message())
	}
	return fromConnect(err)
}

// UpdateRef moves a branch, or reports that somebody else moved it first.
func (t *Transport) UpdateRef(ctx context.Context, name []byte, commit protovcs.CommitID, expected string, force bool) (protovcs.RefState, error) {
	resp, err := t.client.UpdateRef(ctx, connect.NewRequest(&pb.UpdateRefRequest{
		StoreId:          t.storeID,
		Name:             name,
		Commit:           commit.Bytes(),
		ExpectedCasToken: expected,
		Delete:           commit.IsZero(),
		Force:            force,
	}))
	if err != nil {
		return protovcs.RefState{}, updateRefError(err)
	}

	current := protovcs.RefState{}
	if r := resp.Msg.GetCurrent(); r != nil {
		got, err := protovcs.CommitIDFromBytes(r.GetCommit())
		if err != nil {
			return protovcs.RefState{}, err
		}
		current = protovcs.RefState{Name: r.GetName(), Commit: got, CASToken: r.GetCasToken()}
	}

	// A refused move comes back as a successful call reporting failure, because
	// the caller needs the current state to rebuild against. Turning it into an
	// error here is what lets Change treat a network race exactly as it treats
	// an in-process one.
	if !resp.Msg.GetUpdated() {
		return current, protovcs.ErrRaceLost
	}
	return current, nil
}

// GetCommits walks back from a commit.
// getCommitsError turns a server's refusal into the error a caller matches on.
//
// NotFound means the commit is not there, which is different from a chunk that
// is not there: a commit goes when garbage collection removes what no branch
// reaches, so a caller holding an id for a while can name one legitimately.
// fromConnect maps NotFound to ErrNoSuchChunk, which is right for everything
// else on this transport and wrong here.
func getCommitsError(err error) error {
	var cerr *connect.Error
	if errors.As(err, &cerr) && cerr.Code() == connect.CodeNotFound {
		return fmt.Errorf("%w: %s", protovcs.ErrNoSuchCommit, cerr.Message())
	}
	return fromConnect(err)
}

func (t *Transport) GetCommits(ctx context.Context, from protovcs.CommitID, depth uint32) ([]*pb.Commit, error) {
	resp, err := t.client.GetCommits(ctx, connect.NewRequest(&pb.GetCommitsRequest{
		StoreId: t.storeID,
		From:    from.Bytes(),
		Depth:   depth,
	}))
	if err != nil {
		return nil, getCommitsError(err)
	}
	return resp.Msg.GetCommits(), nil
}

// PutCommit records a version without making it reachable.
func (t *Transport) PutCommit(ctx context.Context, commit *pb.Commit, reservation string) (protovcs.CommitID, error) {
	resp, err := t.client.PutCommit(ctx, connect.NewRequest(&pb.PutCommitRequest{
		StoreId:     t.storeID,
		Commit:      commit,
		Reservation: reservation,
	}))
	if err != nil {
		return protovcs.CommitID{}, fromConnect(err)
	}
	return protovcs.CommitIDFromBytes(resp.Msg.GetId())
}

// PackHint says which chunks a head reaches.
func (t *Transport) PackHint(ctx context.Context, commit protovcs.CommitID, ids []protovcs.ChunkID) (bool, error) {
	raw := make([][]byte, 0, len(ids))
	for _, id := range ids {
		raw = append(raw, id.Bytes())
	}
	resp, err := t.client.PackHint(ctx, connect.NewRequest(&pb.PackHintRequest{
		StoreId:  t.storeID,
		Commit:   commit.Bytes(),
		ChunkIds: raw,
	}))
	if err != nil {
		return false, fromConnect(err)
	}
	return resp.Msg.GetAccepted(), nil
}

// MissingBetween returns what a client holding one commit is short of at
// another.
func (t *Transport) MissingBetween(ctx context.Context, have []protovcs.CommitID, want protovcs.CommitID) ([]protovcs.ChunkID, bool, error) {
	raw := make([][]byte, 0, len(have))
	for _, h := range have {
		raw = append(raw, h.Bytes())
	}

	resp, err := t.client.MissingBetween(ctx, connect.NewRequest(&pb.MissingBetweenRequest{
		StoreId: t.storeID,
		Have:    raw,
		Want:    want.Bytes(),
	}))
	if err != nil {
		return nil, false, fromConnect(err)
	}

	out := make([]protovcs.ChunkID, 0, len(resp.Msg.GetChunkIds()))
	for _, b := range resp.Msg.GetChunkIds() {
		id, err := protovcs.ChunkIDFromBytes(b)
		if err != nil {
			return nil, false, err
		}
		out = append(out, id)
	}
	return out, resp.Msg.GetFull(), nil
}

// BeginWrite reserves quota before anything is uploaded.
func (t *Transport) BeginWrite(ctx context.Context, chunks uint32, bytes uint64) (string, error) {
	resp, err := t.client.BeginWrite(ctx, connect.NewRequest(&pb.BeginWriteRequest{
		StoreId:         t.storeID,
		EstimatedChunks: chunks,
		EstimatedBytes:  bytes,
	}))
	if err != nil {
		return "", fromConnect(err)
	}
	return resp.Msg.GetReservation(), nil
}

// HasChunks returns which ids the store does not hold.
func (t *Transport) HasChunks(ctx context.Context, ids []protovcs.ChunkID) ([]protovcs.ChunkID, error) {
	raw := make([][]byte, 0, len(ids))
	for _, id := range ids {
		raw = append(raw, id.Bytes())
	}

	resp, err := t.client.HasChunks(ctx, connect.NewRequest(&pb.HasChunksRequest{
		StoreId:  t.storeID,
		ChunkIds: raw,
	}))
	if err != nil {
		return nil, fromConnect(err)
	}

	out := make([]protovcs.ChunkID, 0, len(resp.Msg.GetMissing()))
	for _, b := range resp.Msg.GetMissing() {
		id, err := protovcs.ChunkIDFromBytes(b)
		if err != nil {
			return nil, err
		}
		out = append(out, id)
	}
	return out, nil
}

// MaxBatchBytes is how much one PutChunks request carries.
//
// A store's items are small and its manifest nodes smaller, so a commit's whole
// upload usually fits in one request. This bounds the one that does not, and it
// costs nothing to split: a chunk's name determines its bytes, so a batch that
// half arrived is a batch that can simply be sent again.
const MaxBatchBytes = 4 << 20

// PutChunks stores chunks, splitting into requests that fit.
func (t *Transport) PutChunks(ctx context.Context, reservation string, chunks []protovcs.SealedChunk) error {
	var batch []*pb.SealedChunk
	var size int

	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		_, err := t.client.PutChunks(ctx, connect.NewRequest(&pb.PutChunksRequest{
			StoreId:     t.storeID,
			Reservation: reservation,
			Chunks:      batch,
		}))
		batch, size = nil, 0
		return fromConnect(err)
	}

	for _, c := range chunks {
		if size+len(c.Sealed) > MaxBatchBytes && len(batch) > 0 {
			if err := flush(); err != nil {
				return err
			}
		}
		batch = append(batch, &pb.SealedChunk{ChunkId: c.ID.Bytes(), Sealed: c.Sealed})
		size += len(c.Sealed)
	}
	return flush()
}

// GetChunks fetches chunks, reading the stream to the end.
func (t *Transport) GetChunks(ctx context.Context, ids []protovcs.ChunkID) ([]protovcs.SealedChunk, error) {
	raw := make([][]byte, 0, len(ids))
	for _, id := range ids {
		raw = append(raw, id.Bytes())
	}

	stream, err := t.client.GetChunks(ctx, connect.NewRequest(&pb.GetChunksRequest{
		StoreId:  t.storeID,
		ChunkIds: raw,
	}))
	if err != nil {
		return nil, fromConnect(err)
	}
	defer stream.Close()

	out := make([]protovcs.SealedChunk, 0, len(ids))
	for stream.Receive() {
		msg := stream.Msg()
		id, err := protovcs.ChunkIDFromBytes(msg.GetChunkId())
		if err != nil {
			return nil, err
		}
		out = append(out, protovcs.SealedChunk{ID: id, Sealed: msg.GetSealed()})
	}
	if err := stream.Err(); err != nil {
		return nil, fromConnect(err)
	}

	// A short answer means the store does not hold something a commit says it
	// does, which is damage rather than absence and must not read as an empty
	// result.
	if len(out) != len(ids) {
		return nil, fmt.Errorf("%w: asked for %d chunks and received %d",
			protovcs.ErrNoSuchChunk, len(ids), len(out))
	}
	return out, nil
}

// Usage asks what the server is holding for this store.
//
// On StoreAdminService rather than StoreService, and the only method there a
// store's own holder may call: everything else on it creates a store or
// destroys one and needs admin, while this reads a number about a store the
// caller already has a token for.
func (t *Transport) Usage(ctx context.Context) (*pb.Usage, error) {
	if t.admin == nil {
		return nil, fmt.Errorf("%w: this transport was built from a StoreService client alone",
			protovcs.ErrNoUsage)
	}
	resp, err := t.admin.GetUsage(ctx, connect.NewRequest(&pb.GetUsageRequest{StoreId: t.storeID}))
	if err != nil {
		return nil, fromConnect(err)
	}
	return resp.Msg.GetUsage(), nil
}

// Ensure the transport satisfies the interface at compile time rather than at
// the first call that uses a method nobody implemented.
var (
	_ protovcs.Transport     = (*Transport)(nil)
	_ protovcs.UsageReporter = (*Transport)(nil)
)

// HTTPClient is the subset of http.Client this needs, restated so a caller can
// pass something else.
type HTTPClient = connect.HTTPClient

var _ = http.DefaultClient
