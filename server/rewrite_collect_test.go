package server_test

import (
	"context"
	"strings"
	"testing"

	"google.golang.org/protobuf/proto"

	protovcs "github.com/nextgui/protovcs"
	testpb "github.com/nextgui/protovcs/proto/protovcs/testpb"
	"github.com/nextgui/protovcs/server"
)

// After a history rewrite, collection deletes the chunks only the old history
// reached, once the grace period has passed, and keeps everything the new
// history reaches. This is what makes erasing records from a store remove
// them from storage and not only from what a branch shows.
func TestCollectionDeletesWhatARewriteLeftBehind(t *testing.T) {
	ctx := context.Background()
	f := newSweepFixture(t)
	f.createStore(t, storeID)
	client := clientStore(t, f.server)
	main := client.Branch("main")

	recs := sampleRecords(30)
	recs = append(recs, &testpb.Record{Id: "secret-1", Group: "gamma",
		Date: &testpb.Date{Year: 2026, Month: 2, Day: 1}, Amount: 99})
	if _, err := main.Write(ctx, protovcs.Commit("seed", "t", "t@example.com"), &testpb.Store{Records: recs}); err != nil {
		t.Fatalf("Write: %v", err)
	}
	oldHead, err := main.Change(ctx, protovcs.Commit("more", "t", "t@example.com"), func(_ context.Context, m proto.Message) error {
		m.(*testpb.Store).Records = append(m.(*testpb.Store).Records, &testpb.Record{
			Id: "plain-1", Group: "alpha", Date: &testpb.Date{Year: 2026, Month: 3, Day: 1}})
		return nil
	})
	if err != nil {
		t.Fatalf("Change: %v", err)
	}
	before, err := f.storage.ListChunks(ctx, storeID)
	if err != nil {
		t.Fatal(err)
	}

	if _, err := client.Rewrite(ctx, func(_ context.Context, m proto.Message) error {
		st := m.(*testpb.Store)
		kept := st.Records[:0]
		for _, r := range st.GetRecords() {
			if !strings.HasPrefix(r.GetId(), "secret") {
				kept = append(kept, r)
			}
		}
		st.Records = kept
		return nil
	}); err != nil {
		t.Fatalf("Rewrite: %v", err)
	}

	// Inside the grace period nothing goes.
	res, err := f.server.Collect(ctx, storeID)
	if err != nil {
		t.Fatalf("Collect: %v", err)
	}
	if res.Deleted != 0 {
		t.Fatalf("collection deleted %d chunks inside the grace period", res.Deleted)
	}

	f.now = f.now.Add(2 * server.CollectionGrace)
	if _, err := f.server.Collect(ctx, storeID); err != nil {
		t.Fatalf("Collect: %v", err)
	}
	f.now = f.now.Add(2 * server.CollectionGrace)
	res, err = f.server.Collect(ctx, storeID)
	if err != nil {
		t.Fatalf("Collect: %v", err)
	}
	if res.Deleted == 0 {
		t.Fatalf("collection deleted nothing after a rewrite; %d chunks were stored before it", len(before))
	}

	// The old head can no longer be read: chunks only it reached are gone.
	fresh := clientStore(t, f.server)
	if _, err := fresh.ReadAt(ctx, oldHead); err == nil {
		t.Error("the replaced head still reads, so the chunks holding the secret were kept")
	}

	// The new history reads in full, without the secret.
	got, err := fresh.Branch("main").Read(ctx)
	if err != nil {
		t.Fatalf("Read after collection: %v", err)
	}
	st := got.(*testpb.Store)
	if len(st.GetRecords()) != 31 {
		t.Fatalf("main holds %d records after collection, want 31", len(st.GetRecords()))
	}
	for _, r := range st.GetRecords() {
		if strings.HasPrefix(r.GetId(), "secret") {
			t.Fatalf("main still holds %s", r.GetId())
		}
	}
}
