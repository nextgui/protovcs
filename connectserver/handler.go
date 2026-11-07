package connectserver

import (
	"context"
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

// readLimits sets each method's request limit.
func readLimits() connect.HandlerOption {
	return connect.WithConditionalHandlerOptions(func(spec connect.Spec) []connect.HandlerOption {
		max := DefaultMaxRequestBytes
		if spec.Procedure == pb.StoreServicePutChunksProcedure {
			max = MaxPutChunksBytes
		}
		return []connect.HandlerOption{connect.WithReadMaxBytes(max)}
	})
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
	mux.Handle(pb.NewStoreServiceHandler(NewStoreHandler(store, auth), opts...))
	mux.Handle(pb.NewStoreAdminServiceHandler(NewAdminHandler(admin, auth), opts...))
	return nil
}

// denied is the response to a refused access check. Permission denied for a
// missing token as well as a wrong one.
func denied(err error) error {
	return connect.NewError(connect.CodePermissionDenied, err)
}

// claimsOf authenticates a request. A request that does not authenticate gets
// empty claims, which [Allow] refuses.
func claimsOf(auth Authenticator, h http.Header) storetoken.Claims {
	c, err := auth.Authenticate(h)
	if err != nil {
		return storetoken.Claims{}
	}
	return c
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
	c := claimsOf(auth, h)
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
	if err := AllowEveryStore(claimsOf(h.auth, req.Header())); err != nil {
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
