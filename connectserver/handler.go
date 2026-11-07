package connectserver

import (
	"compress/gzip"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"connectrpc.com/connect"

	pb "github.com/nextgui/protovcs/proto/protovcs/v1"
	"github.com/nextgui/protovcs/server"
	"github.com/nextgui/protovcs/storetoken"
)

// DefaultMaxRequestBytes is the largest request message a handler reads, for
// every method but PutChunks.
//
// Connect reads a request before any access check, because the check reads the
// store id out of the message. Without a limit an unauthenticated caller
// decides how much memory one request takes.
const DefaultMaxRequestBytes = 8 << 20

// MaxPutChunksBytes is PutChunks's limit. A client batches chunks into smaller
// requests, but one chunk larger than a batch goes alone, and nothing caps a
// chunk unless the store's quota sets max_chunk_bytes.
const MaxPutChunksBytes = 32 << 20

// readLimits sets each method's request limit, before and after
// decompression.
//
// Connect's own limit stops at the limit, then inflates the rest of the
// message to report its size. Gzip expands about a thousand to one, so a small
// request could cost a thousand times the limit in CPU before any access
// check. The gzip decompressor registered here fails at the limit instead.
func readLimits() connect.HandlerOption {
	return connect.WithConditionalHandlerOptions(func(spec connect.Spec) []connect.HandlerOption {
		max := DefaultMaxRequestBytes
		if spec.Procedure == pb.StoreServicePutChunksProcedure {
			max = MaxPutChunksBytes
		}
		return []connect.HandlerOption{
			connect.WithReadMaxBytes(max),
			// Unregistered first, because registering a name again lists it
			// twice in what the handler says it accepts.
			connect.WithCompression("gzip", nil, nil),
			connect.WithCompression("gzip",
				func() connect.Decompressor { return &limitedGzip{procedure: spec.Procedure, max: int64(max)} },
				func() connect.Compressor { return gzip.NewWriter(io.Discard) },
			),
		}
	})
}

// limitedGzip is a gzip decompressor that fails once a message inflates past
// max bytes.
type limitedGzip struct {
	procedure string
	max       int64
	read      int64
	r         *gzip.Reader
}

func (d *limitedGzip) Read(p []byte) (int, error) {
	if d.r == nil {
		return 0, io.EOF
	}
	n, err := d.r.Read(p)
	d.read += int64(n)
	if d.read > d.max {
		return n, connect.NewError(connect.CodeResourceExhausted,
			fmt.Errorf("a request message to %s inflates past its %d byte limit", d.procedure, d.max))
	}
	return n, err
}

func (d *limitedGzip) Close() error {
	if d.r == nil {
		return nil
	}
	return d.r.Close()
}

func (d *limitedGzip) Reset(r io.Reader) error {
	d.read = 0
	if d.r == nil {
		g, err := gzip.NewReader(r)
		if err != nil {
			return err
		}
		d.r = g
		return nil
	}
	return d.r.Reset(r)
}

// Mount serves both services on mux, checking each request with auth.
func Mount(mux *http.ServeMux, srv *server.Server, auth Authenticator, opts ...connect.HandlerOption) error {
	store, err := NewStore(srv, nil)
	if err != nil {
		return err
	}
	admin, err := NewAdmin(srv)
	if err != nil {
		return err
	}
	opts = append([]connect.HandlerOption{readLimits()}, opts...)
	path, storeHandler := pb.NewStoreServiceHandler(NewStoreHandler(store, auth), opts...)
	mux.Handle(path, withResponseController(storeHandler))
	mux.Handle(pb.NewStoreAdminServiceHandler(NewAdminHandler(admin, auth), opts...))
	return nil
}

type responseControllerKey struct{}

func withResponseController(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := context.WithValue(r.Context(), responseControllerKey{}, http.NewResponseController(w))
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

func clearReadDeadline(ctx context.Context) {
	if rc, ok := ctx.Value(responseControllerKey{}).(*http.ResponseController); ok {
		_ = rc.SetReadDeadline(time.Time{})
	}
}

// denied is the response to a refused access check. Permission denied for a
// missing token as well as a wrong one.
func denied(err error) error {
	return connect.NewError(connect.CodePermissionDenied, err)
}

// claimsOf authenticates a request. An expired token is reported as
// unauthenticated so a client knows to remint; any other failure gets empty
// claims, which [Allow] refuses.
func claimsOf(auth Authenticator, h http.Header) (storetoken.Claims, error) {
	c, err := auth.Authenticate(h)
	if err != nil {
		if errors.Is(err, storetoken.ErrExpired) {
			return storetoken.Claims{}, connect.NewError(connect.CodeUnauthenticated, err)
		}
		return storetoken.Claims{}, nil
	}
	return c, nil
}

// StoreHandler serves StoreService, checking each method's access first.
type StoreHandler struct {
	store *Store
	auth  Authenticator
}

var _ pb.StoreServiceHandler = (*StoreHandler)(nil)

// NewStoreHandler returns StoreService over store, checking access with auth.
func NewStoreHandler(store *Store, auth Authenticator) *StoreHandler {
	return &StoreHandler{store: store, auth: auth}
}

// StoreAccess is the access each StoreService method needs, by method name.
//
// Reading refs and moving them are both in StoreService, and one of them
// destroys work, so levels are per method. PackHint needs write because a hint
// makes the server copy bytes, which costs the store something.
var StoreAccess = map[string]pb.Access{
	"ListRefs":       pb.Access_ACCESS_READ,
	"WatchRefs":      pb.Access_ACCESS_READ,
	"GetCommits":     pb.Access_ACCESS_READ,
	"MissingBetween": pb.Access_ACCESS_READ,
	"HasChunks":      pb.Access_ACCESS_READ,
	"GetChunks":      pb.Access_ACCESS_READ,
	"UpdateRef":      pb.Access_ACCESS_WRITE,
	"PutCommit":      pb.Access_ACCESS_WRITE,
	"BeginWrite":     pb.Access_ACCESS_WRITE,
	"PutChunks":      pb.Access_ACCESS_WRITE,
	"PackHint":       pb.Access_ACCESS_WRITE,
}

// allow checks a request against the access its method needs, and returns the
// claims it carried.
func allow(auth Authenticator, h http.Header, procedure, storeID string, table map[string]pb.Access) (storetoken.Claims, error) {
	c, err := claimsOf(auth, h)
	if err != nil {
		return storetoken.Claims{}, err
	}
	need, ok := table[procedure[strings.LastIndex(procedure, "/")+1:]]
	if !ok {
		// A method nobody gave a level to is served to nobody.
		return storetoken.Claims{}, denied(ErrDenied)
	}
	if err := Allow(c, storeID, need); err != nil {
		return storetoken.Claims{}, denied(err)
	}
	return c, nil
}

func unary[Req, Resp any](h *StoreHandler, ctx context.Context, req *connect.Request[Req], storeID string,
	call func(context.Context, *Req) (*Resp, error)) (*connect.Response[Resp], error) {
	if _, err := allow(h.auth, req.Header(), req.Spec().Procedure, storeID, StoreAccess); err != nil {
		return nil, err
	}
	resp, err := call(ctx, req.Msg)
	if err != nil {
		return nil, err
	}
	return connect.NewResponse(resp), nil
}

func (h *StoreHandler) ListRefs(ctx context.Context, req *connect.Request[pb.ListRefsRequest]) (*connect.Response[pb.ListRefsResponse], error) {
	return unary(h, ctx, req, req.Msg.GetStoreId(), h.store.ListRefs)
}

func (h *StoreHandler) WatchRefs(ctx context.Context, req *connect.Request[pb.WatchRefsRequest], stream *connect.ServerStream[pb.WatchRefsResponse]) error {
	c, err := allow(h.auth, req.Header(), req.Spec().Procedure, req.Msg.GetStoreId(), StoreAccess)
	if err != nil {
		return err
	}
	// The request message has been read, and a watch is a response that never
	// ends, so the server's ReadTimeout must not stay armed while the stream
	// is open.
	clearReadDeadline(ctx)
	return h.store.WatchRefs(ctx, req.Msg, c.Expires, stream.Send)
}

func (h *StoreHandler) UpdateRef(ctx context.Context, req *connect.Request[pb.UpdateRefRequest]) (*connect.Response[pb.UpdateRefResponse], error) {
	return unary(h, ctx, req, req.Msg.GetStoreId(), h.store.UpdateRef)
}

func (h *StoreHandler) GetCommits(ctx context.Context, req *connect.Request[pb.GetCommitsRequest]) (*connect.Response[pb.GetCommitsResponse], error) {
	return unary(h, ctx, req, req.Msg.GetStoreId(), h.store.GetCommits)
}

func (h *StoreHandler) PutCommit(ctx context.Context, req *connect.Request[pb.PutCommitRequest]) (*connect.Response[pb.PutCommitResponse], error) {
	return unary(h, ctx, req, req.Msg.GetStoreId(), h.store.PutCommit)
}

func (h *StoreHandler) MissingBetween(ctx context.Context, req *connect.Request[pb.MissingBetweenRequest]) (*connect.Response[pb.MissingBetweenResponse], error) {
	return unary(h, ctx, req, req.Msg.GetStoreId(), h.store.MissingBetween)
}

func (h *StoreHandler) BeginWrite(ctx context.Context, req *connect.Request[pb.BeginWriteRequest]) (*connect.Response[pb.BeginWriteResponse], error) {
	return unary(h, ctx, req, req.Msg.GetStoreId(), h.store.BeginWrite)
}

func (h *StoreHandler) HasChunks(ctx context.Context, req *connect.Request[pb.HasChunksRequest]) (*connect.Response[pb.HasChunksResponse], error) {
	return unary(h, ctx, req, req.Msg.GetStoreId(), h.store.HasChunks)
}

func (h *StoreHandler) PutChunks(ctx context.Context, req *connect.Request[pb.PutChunksRequest]) (*connect.Response[pb.PutChunksResponse], error) {
	return unary(h, ctx, req, req.Msg.GetStoreId(), h.store.PutChunks)
}

func (h *StoreHandler) PackHint(ctx context.Context, req *connect.Request[pb.PackHintRequest]) (*connect.Response[pb.PackHintResponse], error) {
	return unary(h, ctx, req, req.Msg.GetStoreId(), h.store.PackHint)
}

func (h *StoreHandler) GetChunks(ctx context.Context, req *connect.Request[pb.GetChunksRequest], stream *connect.ServerStream[pb.GetChunksResponse]) error {
	if _, err := allow(h.auth, req.Header(), req.Spec().Procedure, req.Msg.GetStoreId(), StoreAccess); err != nil {
		return err
	}
	return h.store.GetChunks(ctx, req.Msg, stream.Send)
}

// AdminHandler serves StoreAdminService, checking each method's access first.
type AdminHandler struct {
	admin *Admin
	auth  Authenticator
}

var _ pb.StoreAdminServiceHandler = (*AdminHandler)(nil)

// NewAdminHandler returns StoreAdminService over admin, checking access with
// auth.
func NewAdminHandler(admin *Admin, auth Authenticator) *AdminHandler {
	return &AdminHandler{admin: admin, auth: auth}
}

// AdminAccess is the access each StoreAdminService method needs, by method
// name. ListStores names no store and is checked by [AllowEveryStore] instead.
//
// Everything that creates or destroys a store needs admin. GetUsage needs read:
// how much space a store uses is a thing its owner may see.
var AdminAccess = map[string]pb.Access{
	"CreateStore":        pb.Access_ACCESS_ADMIN,
	"GetStore":           pb.Access_ACCESS_ADMIN,
	"SetQuota":           pb.Access_ACCESS_ADMIN,
	"GetUsage":           pb.Access_ACCESS_READ,
	"DeleteStore":        pb.Access_ACCESS_ADMIN,
	"RestoreStore":       pb.Access_ACCESS_ADMIN,
	"TriggerMaintenance": pb.Access_ACCESS_ADMIN,
}

func adminUnary[Req, Resp any](h *AdminHandler, ctx context.Context, req *connect.Request[Req], storeID string,
	call func(context.Context, *Req) (*Resp, error)) (*connect.Response[Resp], error) {
	if _, err := allow(h.auth, req.Header(), req.Spec().Procedure, storeID, AdminAccess); err != nil {
		return nil, err
	}
	resp, err := call(ctx, req.Msg)
	if err != nil {
		return nil, err
	}
	return connect.NewResponse(resp), nil
}

func (h *AdminHandler) CreateStore(ctx context.Context, req *connect.Request[pb.CreateStoreRequest]) (*connect.Response[pb.CreateStoreResponse], error) {
	return adminUnary(h, ctx, req, req.Msg.GetStoreId(), h.admin.CreateStore)
}

func (h *AdminHandler) GetStore(ctx context.Context, req *connect.Request[pb.GetStoreRequest]) (*connect.Response[pb.GetStoreResponse], error) {
	return adminUnary(h, ctx, req, req.Msg.GetStoreId(), h.admin.GetStore)
}

func (h *AdminHandler) ListStores(ctx context.Context, req *connect.Request[pb.ListStoresRequest]) (*connect.Response[pb.ListStoresResponse], error) {
	c, err := claimsOf(h.auth, req.Header())
	if err != nil {
		return nil, err
	}
	if err := AllowEveryStore(c); err != nil {
		return nil, denied(err)
	}
	resp, err := h.admin.ListStores(ctx, req.Msg)
	if err != nil {
		return nil, err
	}
	return connect.NewResponse(resp), nil
}

func (h *AdminHandler) SetQuota(ctx context.Context, req *connect.Request[pb.SetQuotaRequest]) (*connect.Response[pb.SetQuotaResponse], error) {
	return adminUnary(h, ctx, req, req.Msg.GetStoreId(), h.admin.SetQuota)
}

func (h *AdminHandler) GetUsage(ctx context.Context, req *connect.Request[pb.GetUsageRequest]) (*connect.Response[pb.GetUsageResponse], error) {
	return adminUnary(h, ctx, req, req.Msg.GetStoreId(), h.admin.GetUsage)
}

func (h *AdminHandler) DeleteStore(ctx context.Context, req *connect.Request[pb.DeleteStoreRequest]) (*connect.Response[pb.DeleteStoreResponse], error) {
	return adminUnary(h, ctx, req, req.Msg.GetStoreId(), h.admin.DeleteStore)
}

func (h *AdminHandler) RestoreStore(ctx context.Context, req *connect.Request[pb.RestoreStoreRequest]) (*connect.Response[pb.RestoreStoreResponse], error) {
	return adminUnary(h, ctx, req, req.Msg.GetStoreId(), h.admin.RestoreStore)
}

func (h *AdminHandler) TriggerMaintenance(ctx context.Context, req *connect.Request[pb.TriggerMaintenanceRequest]) (*connect.Response[pb.TriggerMaintenanceResponse], error) {
	return adminUnary(h, ctx, req, req.Msg.GetStoreId(), h.admin.TriggerMaintenance)
}

// Sweep runs [server.Server.Sweep] every interval until ctx ends, logging what
// each pass did through report. It is what a server without a scheduler runs
// instead of a sweep job.
func Sweep(ctx context.Context, srv *server.Server, interval time.Duration, report func(server.SweepResult, error)) {
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		res, err := srv.Sweep(ctx)
		if report != nil {
			report(res, err)
		}
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}
