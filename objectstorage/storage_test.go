package objectstorage_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	protovcs "github.com/nextgui/protovcs"
	"github.com/nextgui/protovcs/objectstorage"
	testpb "github.com/nextgui/protovcs/proto/protovcs/testpb"
	gen "github.com/nextgui/protovcs/proto/protovcs/testpb/teststorelayout"
	pb "github.com/nextgui/protovcs/proto/protovcs/v1"
	"github.com/nextgui/protovcs/server"
)

// The same store, on every backend. A protovcs store must behave identically
// whichever objects hold it, and the only way to know is to run it on each.

const storeID = "store-1"

func newServer(t *testing.T, b objectstorage.Bucket, quota *pb.Quota) (*server.Server, *objectstorage.Storage) {
	t.Helper()

	st := objectstorage.New(b)
	srv := server.New(st)
	if _, err := srv.CreateStore(context.Background(), &pb.CreateStoreRequest{
		StoreId: storeID, Owner: "someone", Quota: quota,
	}); err != nil {
		t.Fatalf("CreateStore: %v", err)
	}
	return srv, st
}

func client(t *testing.T, srv *server.Server) *gen.Store {
	t.Helper()

	key := make([]byte, protovcs.RootKeyLen)
	for i := range key {
		key[i] = byte(i + 9)
	}
	sealer, err := protovcs.NewKeyring(key)
	if err != nil {
		t.Fatalf("NewKeyring: %v", err)
	}
	s, err := gen.Open(protovcs.NewBuilder().WithTransport(srv.Transport(storeID)).WithSealer(sealer))
	if err != nil {
		t.Fatalf("gen.Open: %v", err)
	}
	return s
}

func records(n int) []*testpb.Record {
	out := make([]*testpb.Record, 0, n)
	for i := 0; i < n; i++ {
		out = append(out, &testpb.Record{
			Id:    fmt.Sprintf("rec-%05d", i),
			Group: fmt.Sprintf("g%d", i%4),
			Date:  &testpb.Date{Year: 2026, Month: int32(1 + i%12), Day: int32(1 + i%28)},
		})
	}
	return out
}

func meta(msg string) gen.Meta {
	return gen.Commit(msg, "t", "t@example.com")
}

func TestAStoreRoundTripsOnEveryBucket(t *testing.T) {
	eachBucket(t, func(t *testing.T, b objectstorage.Bucket) {
		ctx := context.Background()
		srv, _ := newServer(t, b, &pb.Quota{MaxBytes: 10 << 20, MaxObjects: 100000})

		want := &testpb.Store{
			Manifest: &testpb.Manifest{SchemaVersion: 1},
			Name:     "durable",
			Records:  records(120),
		}
		if _, err := client(t, srv).Branch("main").Write(ctx, meta("first"), want); err != nil {
			t.Fatalf("Write: %v", err)
		}

		got, err := client(t, srv).Branch("main").Read(ctx)
		if err != nil {
			t.Fatalf("Read: %v", err)
		}
		if got.GetName() != "durable" || len(got.GetRecords()) != 120 {
			t.Fatalf("round trip changed the store: %q with %d records", got.GetName(), len(got.GetRecords()))
		}
	})
}

// TestAStoreSurvivesANewProcess is the property MemStorage cannot have. The
// second server shares nothing with the first but the objects.
func TestAStoreSurvivesANewProcess(t *testing.T) {
	eachBucket(t, func(t *testing.T, b objectstorage.Bucket) {
		ctx := context.Background()
		srv, _ := newServer(t, b, &pb.Quota{MaxBytes: 10 << 20, MaxObjects: 100000})

		if _, err := client(t, srv).Branch("main").Write(ctx, meta("first"), &testpb.Store{
			Manifest: &testpb.Manifest{SchemaVersion: 1},
			Records:  records(60),
		}); err != nil {
			t.Fatalf("Write: %v", err)
		}

		// A server built fresh over the same objects.
		second := server.New(objectstorage.New(b))
		got, err := client(t, second).Branch("main").Read(ctx)
		if err != nil {
			t.Fatalf("reading from a second server: %v", err)
		}
		if len(got.GetRecords()) != 60 {
			t.Fatalf("a second server sees %d records, want 60", len(got.GetRecords()))
		}

		// And can still write, so the compare-and-swap token survived rather
		// than only the data.
		if _, err := client(t, second).Branch("main").Change(ctx, meta("continued"),
			func(_ context.Context, st *testpb.Store) error {
				st.Records = append(st.Records, &testpb.Record{
					Id: "added", Group: "g0", Date: &testpb.Date{Year: 2026, Month: 3, Day: 3},
				})
				return nil
			}); err != nil {
			t.Fatalf("writing from a second server: %v", err)
		}
	})
}

func TestUsageSurvivesOnEveryBucket(t *testing.T) {
	eachBucket(t, func(t *testing.T, b objectstorage.Bucket) {
		ctx := context.Background()
		srv, st := newServer(t, b, &pb.Quota{MaxBytes: 10 << 20, MaxObjects: 100000})

		if _, err := client(t, srv).Branch("main").Write(ctx, meta("first"), &testpb.Store{
			Manifest: &testpb.Manifest{SchemaVersion: 1},
			Records:  records(40),
		}); err != nil {
			t.Fatalf("Write: %v", err)
		}

		store, err := server.New(st).GetStore(ctx, storeID)
		if err != nil {
			t.Fatalf("GetStore: %v", err)
		}
		if store.GetUsage().GetObjects() == 0 || store.GetUsage().GetCommits() != 1 {
			t.Fatalf("usage did not survive: %v", store.GetUsage())
		}

		chunks, err := st.ListChunks(ctx, storeID)
		if err != nil {
			t.Fatalf("ListChunks: %v", err)
		}
		if int(store.GetUsage().GetObjects()) != len(chunks) {
			t.Fatalf("usage counts %d objects and the bucket holds %d",
				store.GetUsage().GetObjects(), len(chunks))
		}
	})
}

// TestABranchMoveChangesTheStoreGenerationOnEveryBucket is what lets a ref
// watch read one generation per tick instead of listing the refs: every move a
// server makes is followed by a new generation of the store's record.
func TestABranchMoveChangesTheStoreGenerationOnEveryBucket(t *testing.T) {
	eachBucket(t, func(t *testing.T, b objectstorage.Bucket) {
		ctx := context.Background()
		srv, st := newServer(t, b, &pb.Quota{MaxBytes: 10 << 20, MaxObjects: 100000})

		before, err := st.StoreGeneration(ctx, storeID)
		if err != nil {
			t.Fatalf("StoreGeneration: %v", err)
		}
		if _, err := client(t, srv).Branch("main").Write(ctx, meta("first"), &testpb.Store{
			Manifest: &testpb.Manifest{SchemaVersion: 1},
			Records:  records(3),
		}); err != nil {
			t.Fatalf("Write: %v", err)
		}
		after, err := st.StoreGeneration(ctx, storeID)
		if err != nil {
			t.Fatalf("StoreGeneration: %v", err)
		}
		if after == before {
			t.Fatalf("the store's generation stayed %d across a branch move", before)
		}

		if _, err := st.StoreGeneration(ctx, "no-such-store"); !errors.Is(err, server.ErrNoSuchStore) {
			t.Fatalf("StoreGeneration of a missing store returned %v, want ErrNoSuchStore", err)
		}
	})
}

func TestAStaleTokenIsRefusedOnEveryBucket(t *testing.T) {
	eachBucket(t, func(t *testing.T, b objectstorage.Bucket) {
		ctx := context.Background()
		srv, _ := newServer(t, b, &pb.Quota{MaxBytes: 10 << 20, MaxObjects: 100000})

		if _, err := client(t, srv).Branch("main").Write(ctx, meta("first"), &testpb.Store{
			Manifest: &testpb.Manifest{SchemaVersion: 1},
			Records:  records(10),
		}); err != nil {
			t.Fatalf("Write: %v", err)
		}

		refs, err := srv.ListRefs(ctx, storeID)
		if err != nil {
			t.Fatalf("ListRefs: %v", err)
		}
		stale := refs[0].CASToken

		if _, err := client(t, srv).Branch("main").Change(ctx, meta("second"),
			func(_ context.Context, st *testpb.Store) error {
				st.Records = append(st.Records, &testpb.Record{
					Id: "x", Group: "g0", Date: &testpb.Date{Year: 2026, Month: 2, Day: 2},
				})
				return nil
			}); err != nil {
			t.Fatalf("Change: %v", err)
		}

		head, err := srv.ListRefs(ctx, storeID)
		if err != nil {
			t.Fatalf("ListRefs: %v", err)
		}
		if _, err := srv.UpdateRef(ctx, storeID, refs[0].Name, head[0].Commit, stale, false); !errors.Is(err, protovcs.ErrRaceLost) {
			t.Fatalf("a stale token returned %v, want ErrRaceLost", err)
		}
	})
}

func TestAStoreIdCannotNameALocation(t *testing.T) {
	eachBucket(t, func(t *testing.T, b objectstorage.Bucket) {
		ctx := context.Background()
		st := objectstorage.New(b)
		srv := server.New(st)

		// A caller supplies the id, so it must not be able to reach anywhere by
		// choosing one.
		nasty := "../../escaped"
		if _, err := srv.CreateStore(ctx, &pb.CreateStoreRequest{
			StoreId: nasty, Quota: &pb.Quota{MaxBytes: 1 << 20},
		}); err != nil {
			t.Fatalf("CreateStore: %v", err)
		}
		if _, err := srv.GetStore(ctx, nasty); err != nil {
			t.Fatalf("a store with an awkward id is not readable: %v", err)
		}

		objects, err := b.List(ctx, "")
		if err != nil {
			t.Fatalf("List: %v", err)
		}
		for _, o := range objects {
			if len(o.Key) > 1 && o.Key[:2] == ".." {
				t.Fatalf("a store id escaped into the key space: %q", o.Key)
			}
		}
	})
}

// TestCollectionOnEveryBucket is the operation that deletes things, run against
// each backend rather than against whichever one somebody happened to test.
//
// Ages the orphan by moving the bucket's clock rather than by waiting, which is
// what the emulator's SetClock and a filesystem's Chtimes are both for.
func TestCollectionOnEveryBucket(t *testing.T) {
	eachAgeableBucket(t, func(t *testing.T, b objectstorage.Bucket, age func(key string)) {
		ctx := context.Background()
		srv, st := newServer(t, b, &pb.Quota{MaxBytes: 10 << 20, MaxObjects: 100000})

		if _, err := client(t, srv).Branch("main").Write(ctx, meta("first"), &testpb.Store{
			Manifest: &testpb.Manifest{SchemaVersion: 1},
			Records:  records(20),
		}); err != nil {
			t.Fatalf("Write: %v", err)
		}
		kept, err := st.ListChunks(ctx, storeID)
		if err != nil {
			t.Fatalf("ListChunks: %v", err)
		}

		// An orphan: uploaded and never referenced by a commit.
		r, err := srv.BeginWrite(ctx, storeID, 4, 4000)
		if err != nil {
			t.Fatalf("BeginWrite: %v", err)
		}
		var orphan protovcs.ChunkID
		orphan[0] = 0xAB
		if err := srv.PutChunks(ctx, storeID, r.ID, []protovcs.SealedChunk{
			{ID: orphan, Sealed: make([]byte, 64)},
		}); err != nil {
			t.Fatalf("PutChunks: %v", err)
		}

		// Inside the grace period nothing goes, because a chunk this young may
		// belong to a write still in flight.
		res, err := srv.Collect(ctx, storeID)
		if err != nil {
			t.Fatalf("Collect: %v", err)
		}
		if res.Deleted != 0 {
			t.Fatalf("collection deleted %d chunks inside the grace period", res.Deleted)
		}

		age(orphanKey(orphan))
		if err := st.DeleteReservation(ctx, storeID, r.ID); err != nil {
			t.Fatalf("DeleteReservation: %v", err)
		}

		// The first run past the grace period marks the orphan. A run a grace
		// period after that deletes it.
		if res, err := srv.Collect(ctx, storeID); err != nil || res.Deleted != 0 || res.Marked != 1 {
			t.Fatalf("the first collection returned %+v, %v, want the orphan marked and nothing deleted", res, err)
		}
		srv.WithClock(func() time.Time { return time.Now().Add(2 * server.CollectionGrace) })
		res, err = srv.Collect(ctx, storeID)
		if err != nil {
			t.Fatalf("Collect: %v", err)
		}
		if res.Deleted != 1 {
			t.Fatalf("collection deleted %d chunks, want the one orphan", res.Deleted)
		}

		// Everything a commit reaches is still there, and the store still reads.
		after, err := st.ListChunks(ctx, storeID)
		if err != nil {
			t.Fatalf("ListChunks: %v", err)
		}
		if len(after) != len(kept) {
			t.Fatalf("collection left %d chunks, want the %d the commit reaches", len(after), len(kept))
		}
		got, err := client(t, srv).Branch("main").Read(ctx)
		if err != nil {
			t.Fatalf("reading after collection: %v", err)
		}
		if len(got.GetRecords()) != 20 {
			t.Fatalf("collection removed data the store needed: %d records left", len(got.GetRecords()))
		}
	})
}

func orphanKey(id protovcs.ChunkID) string {
	name := id.String()
	return "stores/" + hexOf(storeID) + "/chunks/" + name[:2] + "/" + name
}

func hexOf(s string) string {
	const digits = "0123456789abcdef"
	out := make([]byte, 0, len(s)*2)
	for _, c := range []byte(s) {
		out = append(out, digits[c>>4], digits[c&0x0f])
	}
	return string(out)
}

// TestConcurrentCommitsLoseNothingOnEveryBucket is the durable version of the
// race that lib/protovcs tests in memory.
//
// Four clients commit to one branch at once, each retrying when it loses. The
// property is that every commit that reported success is in the final store:
// a writer that is told its change was saved and then silently replaced is the
// exact failure compare-and-swap exists to prevent, and nothing else reports it.
func TestConcurrentCommitsLoseNothingOnEveryBucket(t *testing.T) {
	eachBucket(t, func(t *testing.T, b objectstorage.Bucket) {
		ctx := context.Background()
		srv, _ := newServer(t, b, &pb.Quota{MaxBytes: 50 << 20, MaxObjects: 100000})

		if _, err := client(t, srv).Branch("main").Write(ctx, meta("base"), &testpb.Store{
			Manifest: &testpb.Manifest{SchemaVersion: 1},
		}); err != nil {
			t.Fatalf("Write: %v", err)
		}

		const writers, each = 4, 3
		var wg sync.WaitGroup
		errs := make([]error, writers)
		for w := 0; w < writers; w++ {
			wg.Add(1)
			go func(w int) {
				defer wg.Done()
				// Its own client, because two clients sharing nothing but the
				// objects is what two processes are.
				c := client(t, srv).Branch("main")
				for i := 0; i < each; i++ {
					id := fmt.Sprintf("w%d-%d", w, i)
					if _, err := c.Change(ctx, meta(id), func(_ context.Context, st *testpb.Store) error {
						st.Records = append(st.Records, &testpb.Record{
							Id: id, Group: "g0", Date: &testpb.Date{Year: 2026, Month: 5, Day: 5},
						})
						return nil
					}); err != nil {
						errs[w] = fmt.Errorf("%s: %w", id, err)
						return
					}
				}
			}(w)
		}
		wg.Wait()

		for _, err := range errs {
			if err != nil {
				t.Fatalf("a writer gave up: %v", err)
			}
		}

		got, err := client(t, srv).Branch("main").Read(ctx)
		if err != nil {
			t.Fatalf("Read: %v", err)
		}
		have := map[string]bool{}
		for _, r := range got.GetRecords() {
			if have[r.GetId()] {
				t.Fatalf("%s is in the store twice", r.GetId())
			}
			have[r.GetId()] = true
		}
		for w := 0; w < writers; w++ {
			for i := 0; i < each; i++ {
				if id := fmt.Sprintf("w%d-%d", w, i); !have[id] {
					t.Fatalf("%s reported success and is not in the store", id)
				}
			}
		}
	})
}

// The reservation id is the one part of a key that arrives from a request as
// text. On a directory, one that climbed out of the store would read and write
// other stores' files.
func TestAReservationIDCannotNameAPath(t *testing.T) {
	ctx := context.Background()
	b, err := objectstorage.OpenDir(t.TempDir())
	if err != nil {
		t.Fatalf("OpenDir: %v", err)
	}
	defer b.Close()
	storage := objectstorage.New(b)

	for _, id := range []string{"../../other/reservations/0123", "a/b", ""} {
		if _, err := storage.GetReservation(ctx, "store", id); !errors.Is(err, server.ErrNoReservation) {
			t.Errorf("GetReservation(%q) returned %v, want ErrNoReservation", id, err)
		}
		if err := storage.PutReservation(ctx, "store", &server.Reservation{ID: id}); !errors.Is(err, server.ErrNoReservation) {
			t.Errorf("PutReservation(%q) returned %v, want ErrNoReservation", id, err)
		}
	}
}
