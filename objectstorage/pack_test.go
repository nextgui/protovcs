package objectstorage_test

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"math/rand"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	protovcs "github.com/nextgui/protovcs"
	"github.com/nextgui/protovcs/objectstorage"
	testpb "github.com/nextgui/protovcs/proto/protovcs/testpb"
	pb "github.com/nextgui/protovcs/proto/protovcs/v1"
	"github.com/nextgui/protovcs/server"
)

// slowBucket adds a fixed delay to every object read and write, and counts
// them, so a test can see how a cold read behaves against a bucket whose
// requests each take a round trip.
type slowBucket struct {
	objectstorage.Bucket
	delay time.Duration
	ops   atomic.Int64
}

func (b *slowBucket) wait() { b.ops.Add(1); time.Sleep(b.delay) }

func (b *slowBucket) Get(ctx context.Context, key string) ([]byte, int64, error) {
	b.wait()
	return b.Bucket.Get(ctx, key)
}

func (b *slowBucket) GetRange(ctx context.Context, key string, off, n int64) ([]byte, error) {
	b.wait()
	return b.Bucket.GetRange(ctx, key, off, n)
}

func (b *slowBucket) Stat(ctx context.Context, key string) (objectstorage.ObjectInfo, error) {
	b.wait()
	return b.Bucket.Stat(ctx, key)
}

func (b *slowBucket) Put(ctx context.Context, key string, data []byte) error {
	b.wait()
	return b.Bucket.Put(ctx, key, data)
}

// sequentialStorage hides GetChunks and packing, so the server reads one chunk
// at a time the way it does over storage that keeps no packs.
type sequentialStorage struct{ server.Storage }

// countingTransport counts client requests that fetch chunks or commits.
type countingTransport struct {
	protovcs.Transport
	calls *atomic.Int64
}

func (c countingTransport) GetChunks(ctx context.Context, ids []protovcs.ChunkID) ([]protovcs.SealedChunk, error) {
	c.calls.Add(1)
	return c.Transport.GetChunks(ctx, ids)
}

func (c countingTransport) GetCommits(ctx context.Context, from protovcs.CommitID, depth uint32) ([]*pb.Commit, error) {
	c.calls.Add(1)
	return c.Transport.GetCommits(ctx, from, depth)
}

func dayLayout(t *testing.T) *protovcs.Compiled {
	t.Helper()
	c, err := protovcs.CompileLayout(&pb.Layout{
		Name:          "daily",
		RootMessage:   "nextgui.protovcs.testpb.Store",
		SchemaVersion: 1,
		Splits: []*pb.Split{{
			Field: "records",
			By:    []*pb.ShardKey{{Field: "date", Transform: pb.Transform_TRANSFORM_DAY}},
			Order: []string{"id"},
		}},
	}, (&testpb.Store{}).ProtoReflect().Descriptor())
	if err != nil {
		t.Fatalf("CompileLayout: %v", err)
	}
	return c
}

// dayClient opens a client that hints for main, over a transport that counts
// its requests into calls.
func dayClient(t *testing.T, srv *server.Server, calls *atomic.Int64) *protovcs.Store {
	t.Helper()
	key := make([]byte, protovcs.RootKeyLen)
	for i := range key {
		key[i] = byte(i + 3)
	}
	sealer, err := protovcs.NewKeyring(key)
	if err != nil {
		t.Fatalf("NewKeyring: %v", err)
	}
	if calls == nil {
		calls = &atomic.Int64{}
	}
	s, err := protovcs.NewBuilder().
		WithTransport(countingTransport{srv.Transport(storeID), calls}).
		WithSealer(sealer).WithLayout(dayLayout(t)).WithPackHints("main").Build()
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	return s
}

var packStart = time.Date(2016, 1, 1, 0, 0, 0, 0, time.UTC)

func dayRecord(d, i int, group string) *testpb.Record {
	day := packStart.AddDate(0, 0, d)
	return &testpb.Record{
		Id:    fmt.Sprintf("rec-%05d-%02d", d, i),
		Group: fmt.Sprintf("%s MERCHANT %05d-%02d SEATTLE WA", group, d, i),
		Date:  &testpb.Date{Year: int32(day.Year()), Month: int32(day.Month()), Day: int32(day.Day())},
	}
}

// packFixture is a server over a directory bucket holding days of records,
// written by one client.
type packFixture struct {
	bucket *slowBucket
	st     *objectstorage.Storage
	srv    *server.Server
	writer *protovcs.Store
	now    time.Time
}

func newPackFixture(t *testing.T, days, perDay int) *packFixture {
	t.Helper()
	ctx := context.Background()
	dir, err := objectstorage.OpenDir(t.TempDir())
	if err != nil {
		t.Fatalf("OpenDir: %v", err)
	}
	f := &packFixture{bucket: &slowBucket{Bucket: dir}, now: time.Now()}
	f.st = objectstorage.New(f.bucket)
	f.srv = server.New(f.st)
	if _, err := f.srv.CreateStore(ctx, &pb.CreateStoreRequest{StoreId: storeID, Owner: "someone"}); err != nil {
		t.Fatalf("CreateStore: %v", err)
	}
	v := &testpb.Store{Manifest: &testpb.Manifest{SchemaVersion: 1}}
	for d := 0; d < days; d++ {
		for i := 0; i < perDay; i++ {
			v.Records = append(v.Records, dayRecord(d, i, "POS"))
		}
	}
	f.writer = dayClient(t, f.srv, nil)
	if _, err := f.writer.Branch("main").Write(ctx, protovcs.Commit("seed", "t", "t@x"), v); err != nil {
		t.Fatalf("Write: %v", err)
	}
	return f
}

// hint reads main in full with a fresh client, which sends the hint.
func (f *packFixture) hint(t *testing.T) *testpb.Store {
	t.Helper()
	got, err := dayClient(t, f.srv, nil).Branch("main").Read(context.Background())
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	return got.(*testpb.Store)
}

func (f *packFixture) policy() server.PackPolicy {
	return server.PackPolicy{
		MinNewChunks:  64,
		MinNewBytes:   256 << 10,
		DeadFraction:  0.5,
		MaxPacks:      16,
		PackBytes:     4 << 20,
		MaxTotalBytes: 1 << 40,
		Grace:         time.Hour,
		Now:           f.now,
	}
}

func (f *packFixture) pack(t *testing.T, p server.PackPolicy) server.PackResult {
	t.Helper()
	res, err := f.st.Pack(context.Background(), storeID, p)
	if err != nil {
		t.Fatalf("Pack: %v", err)
	}
	return res
}

func (f *packFixture) packObjects(t *testing.T) []string {
	t.Helper()
	prefix := "stores/" + hex.EncodeToString([]byte(storeID)) + "/packs/"
	objects, err := f.bucket.List(context.Background(), prefix)
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	var out []string
	for _, o := range objects {
		out = append(out, o.Key)
	}
	return out
}

// appendDays commits one new day of records at a time, the way a daily sync
// does, starting at day from.
func (f *packFixture) appendDays(t *testing.T, from, n, perDay int) {
	t.Helper()
	ctx := context.Background()
	for d := from; d < from+n; d++ {
		p, err := f.writer.Branch("main").Checkout(ctx, protovcs.Nothing())
		if err != nil {
			t.Fatalf("Checkout: %v", err)
		}
		for i := 0; i < perDay; i++ {
			if err := p.PutElement(ctx, "records", dayRecord(d, i, "POS")); err != nil {
				t.Fatalf("PutElement: %v", err)
			}
		}
		if _, err := p.Commit(ctx, protovcs.Commit("sync", "t", "t@x")); err != nil {
			t.Fatalf("Commit: %v", err)
		}
	}
}

// TestPackedColdRead reports how long a cold full read of ten years of daily
// items takes when every bucket request sleeps for a fixed time. It checks
// that every record comes back and that a packed read makes far fewer bucket
// requests, and reports the figures the design document quotes.
func TestPackedColdRead(t *testing.T) {
	const (
		days    = 3650
		perDay  = 14
		recent  = 30
		latency = 2 * time.Millisecond
	)
	ctx := context.Background()
	f := newPackFixture(t, days, perDay)

	// A full read sends the hint, and the sweep packs it, the way a nightly
	// pass would. A month of daily commits after that stays loose.
	f.hint(t)
	sweep, err := f.srv.Sweep(ctx)
	if err != nil || len(sweep.Failed()) > 0 {
		t.Fatalf("Sweep: %v %v", err, sweep.Failed())
	}
	t.Logf("sweep packed %d chunks into %d packs", sweep.Stores[0].Packed.Copied, sweep.Stores[0].Packed.Built)
	f.appendDays(t, days, recent, perDay)
	want := (days + recent) * perDay

	var calls atomic.Int64
	coldRead := func(name string, s *server.Server) int64 {
		t.Helper()
		f.bucket.delay = latency
		f.bucket.ops.Store(0)
		calls.Store(0)
		began := time.Now()
		got, err := dayClient(t, s, &calls).Branch("main").Read(ctx)
		took := time.Since(began)
		f.bucket.delay = 0
		if err != nil {
			t.Fatalf("%s: Read: %v", name, err)
		}
		if n := len(got.(*testpb.Store).GetRecords()); n != want {
			t.Fatalf("%s: read %d records, want %d", name, n, want)
		}
		ops := f.bucket.ops.Load()
		t.Logf("%-28s client round trips=%4d  bucket requests=%5d  wall=%v at %v per request",
			name, calls.Load(), ops, took.Round(time.Millisecond), latency)
		return ops
	}

	sequential := coldRead("one chunk at a time", server.New(sequentialStorage{f.st}))
	packed := coldRead("packed, last month loose", f.srv)
	if packed*10 > sequential {
		t.Errorf("a packed cold read made %d bucket requests and an unpacked one %d; packing is meant to cut that tenfold or more",
			packed, sequential)
	}
}

// TestAReadIsTheSameWithAMissingPack covers the rule that packs never change
// what a read returns: a pack deleted out from under the index is read loose.
func TestAReadIsTheSameWithAMissingPack(t *testing.T) {
	f := newPackFixture(t, 200, 3)
	want := len(f.hint(t).GetRecords())
	if res := f.pack(t, f.policy()); res.Built == 0 {
		t.Fatalf("the first pass built no pack: %+v", res)
	}
	for _, key := range f.packObjects(t) {
		if err := f.bucket.Delete(context.Background(), key); err != nil {
			t.Fatalf("Delete: %v", err)
		}
	}
	if got := len(f.hint(t).GetRecords()); got != want {
		t.Fatalf("with every pack gone a read returned %d records, want %d", got, want)
	}
}

// TestAPassBelowTheThresholdBuildsNothing checks a delta is built only from
// enough new chunks, and only from chunks no pack holds.
func TestAPassBelowTheThresholdBuildsNothing(t *testing.T) {
	f := newPackFixture(t, 200, 3)
	f.hint(t)
	first := f.pack(t, f.policy())
	if first.Built == 0 {
		t.Fatalf("the first pass built no pack: %+v", first)
	}

	// A few days is a handful of new chunks: under the threshold.
	f.appendDays(t, 200, 3, 3)
	f.hint(t)
	if res := f.pack(t, f.policy()); res.Built != 0 || res.Skipped == "" {
		t.Fatalf("three new days built %+v, want nothing and a reason", res)
	}

	// Over the threshold, only the new chunks are copied.
	f.appendDays(t, 203, 80, 3)
	f.hint(t)
	res := f.pack(t, f.policy())
	if res.Built != 1 || res.Copied >= first.Copied {
		t.Fatalf("eighty-three new days built %+v; want one pack holding fewer than the %d chunks of the first",
			res, first.Copied)
	}
}

// TestAMostlyDeadPackIsRewritten checks compaction by a pack's own dead
// fraction.
func TestAMostlyDeadPackIsRewritten(t *testing.T) {
	ctx := context.Background()
	f := newPackFixture(t, 100, 3)
	f.hint(t)
	f.pack(t, f.policy())

	// Rewriting most days replaces their items, so most of the pack is dead.
	if _, err := f.writer.Branch("main").Change(ctx, protovcs.Commit("recategorise", "t", "t@x"),
		func(_ context.Context, m proto.Message) error {
			for _, r := range m.(*testpb.Store).GetRecords() {
				r.Group = "CORRECTED " + r.Group
			}
			return nil
		}); err != nil {
		t.Fatalf("Change: %v", err)
	}
	f.hint(t)
	res := f.pack(t, f.policy())
	if res.Retired != 1 {
		t.Fatalf("a pack that is nearly all dead was not retired: %+v", res)
	}
}

// TestPacksAreMergedAboveTheLimit checks the pack count stays bounded.
func TestPacksAreMergedAboveTheLimit(t *testing.T) {
	f := newPackFixture(t, 10, 2)
	p := f.policy()
	p.MinNewChunks = 1
	p.MaxPacks = 2
	for i := 0; i < 5; i++ {
		f.appendDays(t, 10+i, 1, 2)
		f.hint(t)
		f.pack(t, p)
	}
	idx := f.packIndex(t)
	active := 0
	for _, pk := range idx.GetPacks() {
		if pk.GetRetiredAt() == nil {
			active++
		}
	}
	if active > p.MaxPacks {
		t.Fatalf("%d packs in use, and the limit is %d", active, p.MaxPacks)
	}
}

// TestPacksStayWithinTheAllowance checks the cap on pack bytes.
func TestPacksStayWithinTheAllowance(t *testing.T) {
	f := newPackFixture(t, 200, 3)
	f.hint(t)
	p := f.policy()
	p.MaxTotalBytes = 1024
	res := f.pack(t, p)
	if res.Built != 0 || !strings.Contains(res.Skipped, "allowance") {
		t.Fatalf("a pass over a 1 KiB allowance did %+v", res)
	}
}

// TestRetiredPacksAreDeletedAfterTheGracePeriod checks the index stops naming
// a retired pack before it is deleted, and that it is deleted only after the
// grace period.
func TestRetiredPacksAreDeletedAfterTheGracePeriod(t *testing.T) {
	ctx := context.Background()
	f := newPackFixture(t, 100, 3)
	f.hint(t)
	f.pack(t, f.policy())
	if _, err := f.writer.Branch("main").Change(ctx, protovcs.Commit("recategorise", "t", "t@x"),
		func(_ context.Context, m proto.Message) error {
			for _, r := range m.(*testpb.Store).GetRecords() {
				r.Group = "CORRECTED " + r.Group
			}
			return nil
		}); err != nil {
		t.Fatalf("Change: %v", err)
	}
	f.hint(t)
	f.pack(t, f.policy())
	before := len(f.packObjects(t))

	f.now = f.now.Add(30 * time.Minute)
	if res := f.pack(t, f.policy()); res.Deleted != 0 {
		t.Fatalf("a retired pack was deleted inside the grace period: %+v", res)
	}
	f.now = f.now.Add(time.Hour)
	res := f.pack(t, f.policy())
	if res.Deleted == 0 || len(f.packObjects(t)) != before-res.Deleted {
		t.Fatalf("past the grace period the pass did %+v, leaving %d of %d pack objects",
			res, len(f.packObjects(t)), before)
	}
	for _, pk := range f.packIndex(t).GetPacks() {
		if pk.GetRetiredAt() != nil {
			t.Fatalf("the index still names retired pack %s", pk.GetName())
		}
	}
}

// TestADeletedStoreKeepsNoPacks checks the sweep drops packs, index and hint
// of a store being deleted.
func TestADeletedStoreKeepsNoPacks(t *testing.T) {
	ctx := context.Background()
	f := newPackFixture(t, 200, 3)
	f.hint(t)
	f.pack(t, f.policy())
	if _, err := f.srv.DeleteStore(ctx, storeID); err != nil {
		t.Fatalf("DeleteStore: %v", err)
	}
	if _, err := f.srv.Sweep(ctx); err != nil {
		t.Fatalf("Sweep: %v", err)
	}
	if got := f.packObjects(t); len(got) != 0 {
		t.Fatalf("a deleted store still has %d pack objects", len(got))
	}
}

// TestAPackHintIsKeptOnlyForAHead checks the server refuses a hint for a
// commit that is no longer a head, and one naming too many chunks.
func TestAPackHintIsKeptOnlyForAHead(t *testing.T) {
	ctx := context.Background()
	f := newPackFixture(t, 10, 2)
	head, _, err := f.writer.Branch("main").Head(ctx)
	if err != nil {
		t.Fatalf("Head: %v", err)
	}
	old, err := protovcs.CommitIDFromBytes(head.GetId())
	if err != nil {
		t.Fatalf("CommitIDFromBytes: %v", err)
	}
	f.appendDays(t, 10, 1, 2)

	if ok, err := f.srv.PackHint(ctx, storeID, old, nil); err != nil || ok {
		t.Fatalf("a hint for a commit that is no longer a head returned %v, %v; want false, nil", ok, err)
	}
	if _, err := f.srv.PackHint(ctx, storeID, old, make([]protovcs.ChunkID, server.MaxPackHintChunks+1)); !errors.Is(err, server.ErrHintTooLarge) {
		t.Fatalf("a hint naming too many chunks returned %v, want ErrHintTooLarge", err)
	}
}

func (f *packFixture) packIndex(t *testing.T) *pb.PackIndex {
	t.Helper()
	key := "stores/" + hex.EncodeToString([]byte(storeID)) + "/packindex.pb"
	raw, _, err := f.bucket.Get(context.Background(), key)
	if err != nil {
		t.Fatalf("Get pack index: %v", err)
	}
	idx := &pb.PackIndex{}
	if err := proto.Unmarshal(raw, idx); err != nil {
		t.Fatalf("Unmarshal pack index: %v", err)
	}
	return idx
}

// TestPacksNeverChangeARead is the property the design rests on: whatever
// sequence of commits, hints, passes and lost pack objects a store goes
// through, a read through packs returns exactly what a read of loose chunks
// returns. Policies are small and random so that passes build many packs,
// compact, merge, retire and delete.
func TestPacksNeverChangeARead(t *testing.T) {
	var total server.PackResult
	for seed := int64(1); seed <= 6; seed++ {
		t.Run(fmt.Sprintf("seed%d", seed), func(t *testing.T) {
			r := packsNeverChangeARead(t, seed)
			total.Built += r.Built
			total.Retired += r.Retired
			total.Deleted += r.Deleted
		})
	}
	// The property is only worth something if the passes did the things that
	// could break it.
	t.Logf("over every seed: %d packs built, %d retired, %d deleted", total.Built, total.Retired, total.Deleted)
	if total.Built == 0 || total.Retired == 0 || total.Deleted == 0 {
		t.Fatalf("the random passes never built, retired and deleted packs (%+v), so the property was not exercised", total)
	}
}

func packsNeverChangeARead(t *testing.T, seed int64) server.PackResult {
	var did server.PackResult
	ctx := context.Background()
	rng := rand.New(rand.NewSource(seed))
	f := newPackFixture(t, 30, 3)
	next := 30
	loose := server.New(sequentialStorage{f.st})

	read := func(srv *server.Server, scope protovcs.Scope) proto.Message {
		t.Helper()
		c := dayClient(t, srv, nil)
		if scope.Ranges == nil {
			got, err := c.Branch("main").Read(ctx)
			if err != nil {
				t.Fatalf("Read: %v", err)
			}
			return got
		}
		p, err := c.Branch("main").Checkout(ctx, scope)
		if err != nil {
			t.Fatalf("Checkout: %v", err)
		}
		got, err := p.Materialized()
		if err != nil {
			t.Fatalf("Materialized: %v", err)
		}
		return got
	}

	for step := 0; step < 30; step++ {
		switch action := rng.Intn(6); action {
		case 0:
			n := 1 + rng.Intn(3)
			f.appendDays(t, next, n, 1+rng.Intn(3))
			next += n
		case 1:
			// Rewrite some days, which kills packed chunks.
			mod := 2 + rng.Intn(4)
			if _, err := f.writer.Branch("main").Change(ctx, protovcs.Commit("edit", "t", "t@x"),
				func(_ context.Context, m proto.Message) error {
					for i, r := range m.(*testpb.Store).GetRecords() {
						if i%mod == step%mod {
							r.Group = fmt.Sprintf("EDIT %d %s", step, r.Group)
						}
					}
					return nil
				}); err != nil {
				t.Fatalf("Change: %v", err)
			}
		case 2:
			f.hint(t)
		case 3, 4:
			p := f.policy()
			p.MinNewChunks = 1 + rng.Intn(80)
			p.MinNewBytes = 1 << 30
			p.DeadFraction = 0.2 + rng.Float64()*0.7
			p.MaxPacks = 1 + rng.Intn(4)
			p.PackBytes = uint64(2<<10 + rng.Intn(64<<10))
			f.now = f.now.Add(time.Duration(rng.Intn(90)) * time.Minute)
			p.Now = f.now
			r := f.pack(t, p)
			did.Built += r.Built
			did.Retired += r.Retired
			did.Deleted += r.Deleted
		case 5:
			if objects := f.packObjects(t); len(objects) > 0 {
				key := objects[rng.Intn(len(objects))]
				if err := f.bucket.Delete(ctx, key); err != nil {
					t.Fatalf("Delete: %v", err)
				}
				did.Deleted++
			}
		}

		// Through the server that caches the index, through a fresh one that
		// reads it again, and with no packs at all.
		want := read(loose, protovcs.Scope{})
		fresh := server.New(objectstorage.New(f.bucket))
		for name, srv := range map[string]*server.Server{"cached index": f.srv, "fresh index": fresh} {
			if got := read(srv, protovcs.Scope{}); !proto.Equal(got, want) {
				t.Fatalf("step %d: a full read through packs (%s) differs from a read of loose chunks", step, name)
			}
		}

		// A partial read of two days far apart, which reads two runs of a pack.
		day := func(d int) protovcs.ItemKey {
			at := packStart.AddDate(0, 0, d)
			return protovcs.ItemKey(fmt.Sprintf("records/%04d/%02d/%02d", at.Year(), at.Month(), at.Day()))
		}
		scope := protovcs.Union(protovcs.Prefix(string(day(0))), protovcs.Prefix(string(day(next-1))))
		if got := read(fresh, scope); !proto.Equal(got, read(loose, scope)) {
			t.Fatalf("step %d: a partial read through packs differs from a read of loose chunks", step)
		}
	}
	return did
}

// TestConcurrentPassesLeaveAReadableStore runs two passes over one store at
// once. The index is replaced by compare-and-swap, so one may fail, and either
// way a read through packs must still match a read of loose chunks.
func TestConcurrentPassesLeaveAReadableStore(t *testing.T) {
	ctx := context.Background()
	f := newPackFixture(t, 120, 3)
	f.hint(t)

	errs := make(chan error, 2)
	for i := 0; i < 2; i++ {
		p := f.policy()
		p.PackBytes = uint64(8<<10 + i*4<<10)
		go func() {
			_, err := f.st.Pack(ctx, storeID, p)
			errs <- err
		}()
	}
	failed := 0
	for i := 0; i < 2; i++ {
		if err := <-errs; err != nil {
			failed++
			if !errors.Is(err, objectstorage.ErrGenerationMismatch) {
				t.Fatalf("a losing pass failed with %v, want a generation mismatch", err)
			}
		}
	}
	if failed == 2 {
		t.Fatal("both passes failed; one should have won the index")
	}

	want, err := dayClient(t, server.New(sequentialStorage{f.st}), nil).Branch("main").Read(ctx)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	got, err := dayClient(t, server.New(objectstorage.New(f.bucket)), nil).Branch("main").Read(ctx)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if !proto.Equal(got, want) {
		t.Fatal("after two concurrent passes a read through packs differs from a read of loose chunks")
	}
}

// recordingScheduler records the stores it is asked to pack.
type recordingScheduler struct{ stores []string }

func (r *recordingScheduler) SchedulePack(_ context.Context, storeID string) error {
	r.stores = append(r.stores, storeID)
	return nil
}

// TestAHintAsksForAPassOncePerInterval checks that a kept hint asks for a
// pack pass, that more hints inside PackRequestInterval do not ask again, and
// that one after it does.
func TestAHintAsksForAPassOncePerInterval(t *testing.T) {
	ctx := context.Background()
	f := newPackFixture(t, 100, 2)
	now := time.Now()
	sched := &recordingScheduler{}
	f.srv.WithPackScheduler(sched).WithClock(func() time.Time { return now })

	hint := func() {
		t.Helper()
		head, _, err := f.writer.Branch("main").Head(ctx)
		if err != nil {
			t.Fatalf("Head: %v", err)
		}
		id, err := protovcs.CommitIDFromBytes(head.GetId())
		if err != nil {
			t.Fatalf("CommitIDFromBytes: %v", err)
		}
		ids, err := f.writer.ReachableChunks(ctx, head)
		if err != nil {
			t.Fatalf("ReachableChunks: %v", err)
		}
		if ok, err := f.srv.PackHint(ctx, storeID, id, ids); err != nil || !ok {
			t.Fatalf("PackHint returned %v, %v", ok, err)
		}
	}

	hint()
	hint()
	now = now.Add(server.PackRequestInterval / 2)
	hint()
	if len(sched.stores) != 1 {
		t.Fatalf("three hints inside the interval asked for %d passes, want 1", len(sched.stores))
	}
	now = now.Add(server.PackRequestInterval)
	hint()
	if len(sched.stores) != 2 || sched.stores[1] != storeID {
		t.Fatalf("a hint after the interval left the requests at %v, want a second for %s", sched.stores, storeID)
	}

	// The pass a delivery runs packs the hinted head.
	res, err := f.srv.PackNow(ctx, storeID)
	if err != nil || res.Built == 0 {
		t.Fatalf("PackNow did %+v, %v; want a pack built from the hint", res, err)
	}
}
