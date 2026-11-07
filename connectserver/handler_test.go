package connectserver_test

import (
	"context"
	"crypto/rand"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/reflect/protoreflect"

	protovcs "github.com/nextgui/protovcs"
	"github.com/nextgui/protovcs/connectserver"
	"github.com/nextgui/protovcs/connecttransport"
	pb "github.com/nextgui/protovcs/proto/protovcs/v1"
	"github.com/nextgui/protovcs/server"
	"github.com/nextgui/protovcs/storetoken"
)

const storeID = "s1"

type fixture struct {
	url    string
	srv    *server.Server
	issuer *storetoken.Issuer
}

func newFixture(t *testing.T) fixture {
	t.Helper()
	return newFixtureWithServer(t, func(h http.Handler) *httptest.Server {
		return httptest.NewServer(h)
	})
}

func newFixtureWithServer(t *testing.T, start func(http.Handler) *httptest.Server) fixture {
	t.Helper()
	seed := make([]byte, 32)
	if _, err := rand.Read(seed); err != nil {
		t.Fatal(err)
	}
	issuer, err := storetoken.NewIssuer(seed)
	if err != nil {
		t.Fatalf("NewIssuer: %v", err)
	}
	verifier, err := storetoken.NewVerifier(issuer.PublicKey())
	if err != nil {
		t.Fatalf("NewVerifier: %v", err)
	}

	srv := server.New(server.NewMemStorage())
	if _, err := srv.CreateStore(context.Background(), &pb.CreateStoreRequest{StoreId: storeID, Quota: &pb.Quota{MaxBytes: 64 << 20}}); err != nil {
		t.Fatalf("CreateStore: %v", err)
	}
	mux := http.NewServeMux()
	if err := connectserver.Mount(mux, srv, connectserver.Tokens(verifier)); err != nil {
		t.Fatalf("Mount: %v", err)
	}
	hs := start(mux)
	t.Cleanup(hs.Close)
	return fixture{url: hs.URL, srv: srv, issuer: issuer}
}

func (f fixture) token(t *testing.T, store string, access pb.Access) string {
	t.Helper()
	tok, err := f.issuer.Issue(store, access, time.Hour)
	if err != nil {
		t.Fatalf("Issue: %v", err)
	}
	return tok
}

func withToken[T any](msg *T, token string) *connect.Request[T] {
	req := connect.NewRequest(msg)
	if token != "" {
		req.Header().Set("Authorization", "Bearer "+token)
	}
	return req
}

func codeOf(err error) connect.Code {
	var cerr *connect.Error
	if errors.As(err, &cerr) {
		return cerr.Code()
	}
	return connect.CodeUnknown
}

func TestEachMethodNeedsItsAccess(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	store := pb.NewStoreServiceClient(http.DefaultClient, f.url)
	admin := pb.NewStoreAdminServiceClient(http.DefaultClient, f.url)

	read := f.token(t, storeID, pb.Access_ACCESS_READ)
	write := f.token(t, storeID, pb.Access_ACCESS_WRITE)
	adminTok := f.token(t, storeID, pb.Access_ACCESS_ADMIN)
	other := f.token(t, "s2", pb.Access_ACCESS_ADMIN)

	refs := func(tok string) error {
		_, err := store.ListRefs(ctx, withToken(&pb.ListRefsRequest{StoreId: storeID}, tok))
		return err
	}
	begin := func(tok string) error {
		_, err := store.BeginWrite(ctx, withToken(&pb.BeginWriteRequest{StoreId: storeID, EstimatedChunks: 1, EstimatedBytes: 10}, tok))
		return err
	}
	quota := func(tok string) error {
		_, err := admin.SetQuota(ctx, withToken(&pb.SetQuotaRequest{StoreId: storeID, Quota: &pb.Quota{MaxBytes: 64 << 20}}, tok))
		return err
	}
	usage := func(tok string) error {
		_, err := admin.GetUsage(ctx, withToken(&pb.GetUsageRequest{StoreId: storeID}, tok))
		return err
	}
	list := func(tok string) error {
		_, err := admin.ListStores(ctx, withToken(&pb.ListStoresRequest{}, tok))
		return err
	}

	for _, c := range []struct {
		name  string
		call  func(string) error
		token string
		ok    bool
	}{
		{"ListRefs with no token", refs, "", false},
		{"ListRefs with read", refs, read, true},
		{"ListRefs for another store", refs, other, false},
		{"BeginWrite with read", begin, read, false},
		{"BeginWrite with write", begin, write, true},
		{"SetQuota with write", quota, write, false},
		{"SetQuota with admin", quota, adminTok, true},
		{"GetUsage with read", usage, read, true},
		{"ListStores with admin", list, adminTok, false},
	} {
		err := c.call(c.token)
		switch {
		case c.ok && err != nil:
			t.Errorf("%s: %v, want it allowed", c.name, err)
		case !c.ok && codeOf(err) != connect.CodePermissionDenied:
			t.Errorf("%s: %v, want permission_denied", c.name, err)
		}
	}
}

// Only PutChunks reads requests larger than the default.
func TestRequestLimitsArePerMethod(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	store := pb.NewStoreServiceClient(http.DefaultClient, f.url)
	write := f.token(t, storeID, pb.Access_ACCESS_WRITE)

	big := make([]byte, connectserver.DefaultMaxRequestBytes+1024)
	_, err := store.HasChunks(ctx, withToken(&pb.HasChunksRequest{StoreId: storeID, ChunkIds: [][]byte{big}}, write))
	if codeOf(err) != connect.CodeResourceExhausted {
		t.Fatalf("HasChunks over the default limit returned %v, want resource_exhausted", err)
	}

	r, err := store.BeginWrite(ctx, withToken(&pb.BeginWriteRequest{StoreId: storeID, EstimatedChunks: 1, EstimatedBytes: uint64(len(big)) + 1024}, write))
	if err != nil {
		t.Fatalf("BeginWrite: %v", err)
	}
	id := make([]byte, 32)
	id[0] = 1
	_, err = store.PutChunks(ctx, withToken(&pb.PutChunksRequest{
		StoreId:     storeID,
		Reservation: r.Msg.GetReservation(),
		Chunks:      []*pb.SealedChunk{{ChunkId: id, Sealed: big}},
	}, write))
	if codeOf(err) == connect.CodeResourceExhausted {
		t.Fatalf("PutChunks under its own limit was refused for size: %v", err)
	}
}

// Every method either service declares has a level, so none is served by
// falling through to a default.
func TestEveryMethodHasAnAccessLevel(t *testing.T) {
	file := (&pb.ListRefsRequest{}).ProtoReflect().Descriptor().ParentFile()
	adminFile := (&pb.CreateStoreRequest{}).ProtoReflect().Descriptor().ParentFile()
	for _, c := range []struct {
		service protoreflect.ServiceDescriptor
		table   map[string]pb.Access
		exempt  string
	}{
		{file.Services().ByName("StoreService"), connectserver.StoreAccess, ""},
		{adminFile.Services().ByName("StoreAdminService"), connectserver.AdminAccess, "ListStores"},
	} {
		methods := c.service.Methods()
		for i := 0; i < methods.Len(); i++ {
			name := string(methods.Get(i).Name())
			if _, ok := c.table[name]; !ok && name != c.exempt {
				t.Errorf("%s.%s has no access level", c.service.Name(), name)
			}
		}
	}
}

func TestAnExpiredTokenIsReportedAsUnauthenticated(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	f.issuer.SetClock(func() time.Time { return time.Now().Add(-2 * time.Hour) })
	expired, err := f.issuer.Issue(storeID, pb.Access_ACCESS_ADMIN, time.Hour)
	if err != nil {
		t.Fatalf("Issue: %v", err)
	}

	tr := connecttransport.New(http.DefaultClient, f.url, storeID,
		connecttransport.WithToken(func(context.Context) (string, error) { return expired, nil }))

	if _, err := tr.ListRefs(ctx); !errors.Is(err, protovcs.ErrTokenExpired) {
		t.Fatalf("ListRefs with expired token returned %v, want ErrTokenExpired", err)
	}
	if _, err := tr.Usage(ctx); !errors.Is(err, protovcs.ErrTokenExpired) {
		t.Fatalf("Usage with expired token returned %v, want ErrTokenExpired", err)
	}
	if err := tr.WatchRefs(ctx, func([]protovcs.RefState) error { return nil }); !errors.Is(err, protovcs.ErrTokenExpired) {
		t.Fatalf("WatchRefs with expired token returned %v, want ErrTokenExpired", err)
	}
}

func TestAWatchOutlivesTheServerReadTimeout(t *testing.T) {
	const readTimeout = 40 * time.Millisecond
	f := newFixtureWithServer(t, func(h http.Handler) *httptest.Server {
		hs := httptest.NewUnstartedServer(h)
		hs.Config.ReadTimeout = readTimeout
		hs.Start()
		return hs
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	write := f.token(t, storeID, pb.Access_ACCESS_WRITE)
	tr := connecttransport.New(http.DefaultClient, f.url, storeID,
		connecttransport.WithToken(func(context.Context) (string, error) { return write, nil }))

	delivered := make(chan []protovcs.RefState, 8)
	done := make(chan error, 1)
	go func() {
		done <- tr.WatchRefs(ctx, func(refs []protovcs.RefState) error {
			delivered <- refs
			return nil
		})
	}()

	select {
	case <-delivered:
	case err := <-done:
		t.Fatalf("watch ended before first delivery: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for first watch delivery")
	}

	// Wait well past the server's ReadTimeout; the open watch stream must stay alive.
	time.Sleep(3 * readTimeout)

	cid, err := f.srv.PutCommit(ctx, storeID, "", &pb.Commit{
		FormatVersion: protovcs.FormatVersion,
		NamingNonce:   []byte{1},
	})
	if err != nil {
		t.Fatalf("PutCommit: %v", err)
	}
	if _, err := tr.UpdateRef(ctx, []byte("main"), cid, "", false); err != nil {
		t.Fatalf("UpdateRef: %v", err)
	}

	select {
	case refs := <-delivered:
		if len(refs) != 1 || refs[0].Commit != cid {
			t.Fatalf("watch delivered %+v, want main at %s", refs, cid)
		}
	case err := <-done:
		t.Fatalf("watch was killed by ReadTimeout: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for watch delivery after ReadTimeout")
	}
}

func TestNonFastForwardMoveOverConnectReturnsErrNotFastForward(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	write := f.token(t, storeID, pb.Access_ACCESS_WRITE)
	tr := connecttransport.New(http.DefaultClient, f.url, storeID,
		connecttransport.WithToken(func(context.Context) (string, error) { return write, nil }))

	c1, err := f.srv.PutCommit(ctx, storeID, "", &pb.Commit{
		FormatVersion: protovcs.FormatVersion,
		NamingNonce:   []byte{1},
	})
	if err != nil {
		t.Fatalf("PutCommit(c1): %v", err)
	}
	c2, err := f.srv.PutCommit(ctx, storeID, "", &pb.Commit{
		FormatVersion: protovcs.FormatVersion,
		NamingNonce:   []byte{2},
	})
	if err != nil {
		t.Fatalf("PutCommit(c2): %v", err)
	}

	state, err := tr.UpdateRef(ctx, []byte("main"), c1, "", false)
	if err != nil {
		t.Fatalf("initial UpdateRef: %v", err)
	}
	if _, err := tr.UpdateRef(ctx, []byte("main"), c2, state.CASToken, false); !errors.Is(err, protovcs.ErrNotFastForward) {
		t.Fatalf("non-fast-forward UpdateRef over Connect returned %v, want ErrNotFastForward", err)
	}
}

func TestPutChunksWithDuplicateIDsCountsUniqueStoredChunks(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	store := pb.NewStoreServiceClient(http.DefaultClient, f.url)
	write := f.token(t, storeID, pb.Access_ACCESS_WRITE)

	r, err := store.BeginWrite(ctx, withToken(&pb.BeginWriteRequest{
		StoreId:         storeID,
		EstimatedChunks: 2,
		EstimatedBytes:  1024,
	}, write))
	if err != nil {
		t.Fatalf("BeginWrite: %v", err)
	}

	id := make([]byte, 32)
	id[0] = 0x42
	chunk := &pb.SealedChunk{ChunkId: id, Sealed: []byte("sealed-bytes")}
	resp, err := store.PutChunks(ctx, withToken(&pb.PutChunksRequest{
		StoreId:     storeID,
		Reservation: r.Msg.GetReservation(),
		Chunks:      []*pb.SealedChunk{chunk, chunk},
	}, write))
	if err != nil {
		t.Fatalf("PutChunks: %v", err)
	}
	if resp.Msg.GetStored() != 1 || resp.Msg.GetAlreadyPresent() != 1 {
		t.Fatalf("PutChunks with duplicate chunk reported stored=%d already_present=%d, want 1 and 1",
			resp.Msg.GetStored(), resp.Msg.GetAlreadyPresent())
	}
}
