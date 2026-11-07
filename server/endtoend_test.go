package server_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	protovcs "github.com/nextgui/protovcs"
	testpb "github.com/nextgui/protovcs/proto/protovcs/testpb"
	pb "github.com/nextgui/protovcs/proto/protovcs/v1"
	"github.com/nextgui/protovcs/server"
)

// These exercise a real client against the real server rules. Everything the
// client does here goes through quota checks, reservations and the same commit
// validation production will run.

func clientStore(t *testing.T, s *server.Server) *protovcs.Store {
	t.Helper()

	l := &pb.Layout{
		Name:          "teststore",
		RootMessage:   "nextgui.protovcs.testpb.Store",
		SchemaVersion: 1,
		Splits: []*pb.Split{{
			Field: "records",
			By: []*pb.ShardKey{
				{Field: "group", Transform: pb.Transform_TRANSFORM_IDENTITY},
				{Field: "date", Transform: pb.Transform_TRANSFORM_MONTH},
			},
			Order: []string{"id"},
		}},
	}
	c, err := protovcs.CompileLayout(l, (&testpb.Store{}).ProtoReflect().Descriptor())
	if err != nil {
		t.Fatalf("CompileLayout: %v", err)
	}

	key := make([]byte, protovcs.RootKeyLen)
	for i := range key {
		key[i] = byte(i)
	}
	sealer, err := protovcs.NewKeyring(key)
	if err != nil {
		t.Fatalf("NewKeyring: %v", err)
	}

	store, err := protovcs.NewBuilder().
		WithTransport(s.Transport(storeID)).
		WithSealer(sealer).
		WithLayout(c).
		Build()
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	return store
}

func sampleRecords(n int) []*testpb.Record {
	groups := []string{"alpha", "beta"}
	out := make([]*testpb.Record, 0, n)
	for i := 0; i < n; i++ {
		out = append(out, &testpb.Record{
			Id:     fmt.Sprintf("rec-%04d", i),
			Group:  groups[i%len(groups)],
			Date:   &testpb.Date{Year: 2026, Month: int32(1 + i%12), Day: 1},
			Amount: int64(i),
		})
	}
	return out
}

func TestAClientWritesAndReadsThroughTheServer(t *testing.T) {
	ctx := context.Background()
	s, _ := newServer(t, &pb.Quota{MaxBytes: 10 << 20, MaxObjects: 10000})
	client := clientStore(t, s)

	want := &testpb.Store{
		Manifest: &testpb.Manifest{SchemaVersion: 1},
		Name:     "through the server",
		Records:  sampleRecords(80),
	}
	if _, err := client.Branch("main").Write(ctx, protovcs.CommitMeta{Title: "first"}, want); err != nil {
		t.Fatalf("Write: %v", err)
	}

	got, err := client.Branch("main").Read(ctx)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	gs := got.(*testpb.Store)
	if gs.GetName() != want.GetName() || len(gs.GetRecords()) != len(want.GetRecords()) {
		t.Fatalf("round trip through the server changed the store")
	}

	store, err := s.GetStore(ctx, storeID)
	if err != nil {
		t.Fatalf("GetStore: %v", err)
	}
	if store.GetUsage().GetObjects() == 0 || store.GetUsage().GetCommits() != 1 {
		t.Fatalf("usage after one write: %v", store.GetUsage())
	}
	if store.GetUsage().GetReservedBytes() != 0 {
		t.Fatalf("a finished write still holds %d reserved bytes", store.GetUsage().GetReservedBytes())
	}
}

func TestAClientHittingItsQuotaIsToldBeforeUploading(t *testing.T) {
	ctx := context.Background()
	s, storage := newServer(t, &pb.Quota{MaxBytes: 2000, MaxObjects: 10000})
	client := clientStore(t, s)

	_, err := client.Branch("main").Write(ctx, protovcs.CommitMeta{Title: "too big"}, &testpb.Store{
		Manifest: &testpb.Manifest{SchemaVersion: 1},
		Records:  sampleRecords(500),
	})
	if !errors.Is(err, server.ErrQuotaExceeded) {
		t.Fatalf("writing past the limit returned %v, want ErrQuotaExceeded", err)
	}

	// Nothing landed. The point of reserving before uploading is that a refused
	// write does not leave the store holding what it refused.
	chunks, err := storage.ListChunks(ctx, storeID)
	if err != nil {
		t.Fatalf("ListChunks: %v", err)
	}
	if len(chunks) != 0 {
		t.Fatalf("a refused write left %d chunks behind", len(chunks))
	}
}

func TestCollectionAfterAFailedWriteFreesTheOrphans(t *testing.T) {
	ctx := context.Background()
	s, storage := newServer(t, &pb.Quota{MaxBytes: 10 << 20, MaxObjects: 10000})
	client := clientStore(t, s)

	if _, err := client.Branch("main").Write(ctx, protovcs.CommitMeta{Title: "first"}, &testpb.Store{
		Manifest: &testpb.Manifest{SchemaVersion: 1},
		Records:  sampleRecords(40),
	}); err != nil {
		t.Fatalf("Write: %v", err)
	}

	// A second write's chunks land, and its commit never does.
	r, err := s.BeginWrite(ctx, storeID, 4, 4000)
	if err != nil {
		t.Fatalf("BeginWrite: %v", err)
	}
	orphan := chunk(0xAB, 64)
	if err := s.PutChunks(ctx, storeID, r.ID, []protovcs.SealedChunk{orphan}); err != nil {
		t.Fatalf("PutChunks: %v", err)
	}

	before, err := storage.ListChunks(ctx, storeID)
	if err != nil {
		t.Fatalf("ListChunks: %v", err)
	}

	// Nothing goes while the reservation is live.
	res, err := s.Collect(ctx, storeID)
	if err != nil {
		t.Fatalf("Collect: %v", err)
	}
	if res.Deleted != 0 {
		t.Fatalf("collection deleted %d chunks while a write was reserved", res.Deleted)
	}

	// After the reservation expires and the grace period passes, the orphan
	// goes and the store still reads.
	s2 := server.New(storage).WithClock(func() time.Time { return farFuture() })
	if _, err := s2.ExpireReservations(ctx, storeID); err != nil {
		t.Fatalf("ExpireReservations: %v", err)
	}
	if res, err := s2.Collect(ctx, storeID); err != nil || res.Deleted != 0 || res.Marked != 1 {
		t.Fatalf("the first collection past the grace period returned %+v, %v, want the orphan marked and nothing deleted", res, err)
	}
	s3 := server.New(storage).WithClock(func() time.Time { return farFuture().Add(2 * server.CollectionGrace) })
	res, err = s3.Collect(ctx, storeID)
	if err != nil {
		t.Fatalf("Collect: %v", err)
	}
	if res.Deleted != 1 {
		t.Fatalf("collection deleted %d chunks, want 1", res.Deleted)
	}
	if len(before)-1 != countChunks(t, ctx, storage) {
		t.Fatal("collection removed more than the orphan")
	}

	got, err := client.Branch("main").Read(ctx)
	if err != nil {
		t.Fatalf("reading after collection: %v", err)
	}
	if len(got.(*testpb.Store).GetRecords()) != 40 {
		t.Fatal("collection removed data the store still needed")
	}
}

func TestTwoClientsRacingThroughTheServer(t *testing.T) {
	ctx := context.Background()
	s, _ := newServer(t, &pb.Quota{MaxBytes: 10 << 20, MaxObjects: 10000})

	a := clientStore(t, s)
	b := clientStore(t, s)

	if _, err := a.Branch("main").Write(ctx, protovcs.CommitMeta{Title: "base"}, &testpb.Store{
		Manifest: &testpb.Manifest{SchemaVersion: 1},
		Records:  sampleRecords(10),
	}); err != nil {
		t.Fatalf("Write: %v", err)
	}

	raced := false
	if _, err := a.Branch("main").Change(ctx, protovcs.CommitMeta{Title: "from a"},
		func(ctx context.Context, m proto.Message) error {
			if !raced {
				raced = true
				if _, err := b.Branch("main").Change(ctx, protovcs.CommitMeta{Title: "from b"},
					func(_ context.Context, m proto.Message) error {
						st := m.(*testpb.Store)
						st.Records = append(st.Records, &testpb.Record{
							Id: "from-b", Group: "beta", Date: &testpb.Date{Year: 2026, Month: 7, Day: 1},
						})
						return nil
					}); err != nil {
					return err
				}
			}
			st := m.(*testpb.Store)
			st.Records = append(st.Records, &testpb.Record{
				Id: "from-a", Group: "alpha", Date: &testpb.Date{Year: 2026, Month: 8, Day: 1},
			})
			return nil
		}); err != nil {
		t.Fatalf("Change: %v", err)
	}

	got, err := a.Branch("main").Read(ctx)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	ids := map[string]bool{}
	for _, r := range got.(*testpb.Store).GetRecords() {
		ids[r.GetId()] = true
	}
	if !ids["from-a"] || !ids["from-b"] {
		t.Fatal("a concurrent write was lost through the server")
	}
}

// farFuture is past every reservation and grace period this package uses.
func farFuture() time.Time {
	return time.Now().UTC().Add(24 * time.Hour)
}

func countChunks(t *testing.T, ctx context.Context, storage *server.MemStorage) int {
	t.Helper()
	chunks, err := storage.ListChunks(ctx, storeID)
	if err != nil {
		t.Fatalf("ListChunks: %v", err)
	}
	return len(chunks)
}
