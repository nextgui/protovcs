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

	"github.com/nextgui/protovcs/connectserver"
	pb "github.com/nextgui/protovcs/proto/protovcs/v1"
	"github.com/nextgui/protovcs/server"
	"github.com/nextgui/protovcs/storetoken"
)

const storeID = "s1"

type fixture struct {
	url    string
	issuer *storetoken.Issuer
}

func newFixture(t *testing.T) fixture {
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
	hs := httptest.NewServer(mux)
	t.Cleanup(hs.Close)
	return fixture{url: hs.URL, issuer: issuer}
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
