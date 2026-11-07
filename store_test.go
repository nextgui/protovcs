package protovcs_test

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"google.golang.org/protobuf/proto"

	protovcs "github.com/nextgui/protovcs"
	"github.com/nextgui/protovcs/memtransport"
	testpb "github.com/nextgui/protovcs/proto/protovcs/testpb"
	gen "github.com/nextgui/protovcs/proto/protovcs/testpb/teststorelayout"
	pb "github.com/nextgui/protovcs/proto/protovcs/v1"
)

func layout() *pb.Layout {
	return &pb.Layout{
		Name:          "teststore",
		RootMessage:   "nextgui.protovcs.testpb.Store",
		SchemaVersion: 1,
		Splits: []*pb.Split{
			{
				Field: "records",
				By: []*pb.ShardKey{
					{Field: "group", Transform: pb.Transform_TRANSFORM_IDENTITY},
					{Field: "date", Transform: pb.Transform_TRANSFORM_MONTH},
				},
				Order: []string{"id"},
			},
			{Field: "accounts", By: []*pb.ShardKey{{Field: "id", Transform: pb.Transform_TRANSFORM_IDENTITY}}},
		},
	}
}

func newStore(t *testing.T, tr protovcs.Transport, sealer protovcs.Sealer) *protovcs.Store {
	t.Helper()

	c, err := protovcs.CompileLayout(layout(), (&testpb.Store{}).ProtoReflect().Descriptor())
	if err != nil {
		t.Fatalf("CompileLayout: %v", err)
	}
	s, err := protovcs.NewBuilder().WithTransport(tr).WithSealer(sealer).WithLayout(c).Build()
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	return s
}

func keyring(t *testing.T) protovcs.Sealer {
	t.Helper()
	key := make([]byte, protovcs.RootKeyLen)
	for i := range key {
		key[i] = byte(i * 7)
	}
	k, err := protovcs.NewKeyring(key)
	if err != nil {
		t.Fatalf("NewKeyring: %v", err)
	}
	return k
}

func records(n int) []*testpb.Record {
	groups := []string{"alpha", "beta", "gamma"}
	out := make([]*testpb.Record, 0, n)
	for i := 0; i < n; i++ {
		out = append(out, &testpb.Record{
			Id:     fmt.Sprintf("rec-%04d", i),
			Group:  groups[i%len(groups)],
			Date:   &testpb.Date{Year: 2026, Month: int32(1 + i%12), Day: int32(1 + i%28)},
			Amount: int64(i),
		})
	}
	return out
}

func meta(msg string) protovcs.CommitMeta {
	return protovcs.Commit(msg, "test", "test@example.com")
}

// genMeta is meta for the generated package, whose Meta carries this store's
// declared details type rather than a proto.Message.
func genMeta(msg string) gen.Meta {
	return gen.Commit(msg, "test", "test@example.com")
}

func TestAStoreThatHasNeverBeenWrittenReadsEmpty(t *testing.T) {
	ctx := context.Background()
	s := newStore(t, memtransport.New(), keyring(t))

	got, err := s.Branch("main").Read(ctx)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if len(got.(*testpb.Store).GetRecords()) != 0 {
		t.Fatal("an unwritten store returned records")
	}
}

func TestWriteThenRead(t *testing.T) {
	ctx := context.Background()
	s := newStore(t, memtransport.New(), keyring(t))
	main := s.Branch("main")

	want := &testpb.Store{
		Manifest: &testpb.Manifest{SchemaVersion: 1},
		Name:     "a store",
		Revision: 3,
		Records:  records(90),
		Accounts: []*testpb.Account{{Id: "acc-1", Label: "one"}},
	}

	if _, err := main.Write(ctx, meta("first"), want); err != nil {
		t.Fatalf("Write: %v", err)
	}

	got, err := main.Read(ctx)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	gs := got.(*testpb.Store)

	if gs.GetName() != want.GetName() || gs.GetRevision() != want.GetRevision() {
		t.Fatalf("root fields did not survive: %v", gs)
	}
	if len(gs.GetRecords()) != len(want.GetRecords()) {
		t.Fatalf("read %d records, wrote %d", len(gs.GetRecords()), len(want.GetRecords()))
	}
	if len(gs.GetAccounts()) != 1 {
		t.Fatalf("read %d accounts, wrote 1", len(gs.GetAccounts()))
	}
}

// TestASecondWriteCostsOnlyWhatChanged is the claim the whole design is for.
func TestASecondWriteCostsOnlyWhatChanged(t *testing.T) {
	ctx := context.Background()
	tr := memtransport.New()
	s := newStore(t, tr, keyring(t))
	main := s.Branch("main")

	first := &testpb.Store{
		Manifest: &testpb.Manifest{SchemaVersion: 1},
		Records:  records(300),
	}
	if _, err := main.Write(ctx, meta("first"), first); err != nil {
		t.Fatalf("Write: %v", err)
	}
	after := tr.ChunkCount()

	if _, err := main.Change(ctx, meta("one more"), func(_ context.Context, m proto.Message) error {
		st := m.(*testpb.Store)
		st.Records = append(st.Records, &testpb.Record{
			Id: "rec-new", Group: "alpha", Date: &testpb.Date{Year: 2026, Month: 1, Day: 2},
		})
		return nil
	}); err != nil {
		t.Fatalf("Change: %v", err)
	}

	added := tr.ChunkCount() - after
	// One item, the manifest nodes on its path, the root manifest node, and the
	// schema chunk is already there. Anything near the item count would mean the
	// store rewrote itself.
	if added > 6 {
		t.Fatalf("adding one record stored %d new chunks, want a handful", added)
	}
	if added == 0 {
		t.Fatal("adding one record stored nothing")
	}
}

func TestUnchangedDataIsRewrittenAsNothing(t *testing.T) {
	ctx := context.Background()
	tr := memtransport.New()
	s := newStore(t, tr, keyring(t))
	main := s.Branch("main")

	store := &testpb.Store{Manifest: &testpb.Manifest{SchemaVersion: 1}, Records: records(120)}
	if _, err := main.Write(ctx, meta("first"), store); err != nil {
		t.Fatalf("Write: %v", err)
	}
	after := tr.ChunkCount()

	// Writing the same content again must store no new chunks. If it did, every
	// commit would grow the store whether or not anything changed.
	if _, err := main.Write(ctx, meta("again"), store); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if tr.ChunkCount() != after {
		t.Fatalf("rewriting unchanged data stored %d new chunks", tr.ChunkCount()-after)
	}
}

func TestChangeRetriesWhenSomebodyElseWritesFirst(t *testing.T) {
	ctx := context.Background()
	tr := memtransport.New()

	// Two clients on one store, as a phone and a laptop would be.
	a := newStore(t, tr, keyring(t))
	b := newStore(t, tr, keyring(t))

	if _, err := a.Branch("main").Write(ctx, meta("base"), &testpb.Store{
		Manifest: &testpb.Manifest{SchemaVersion: 1},
		Records:  records(10),
	}); err != nil {
		t.Fatalf("Write: %v", err)
	}

	raced := false
	_, err := a.Branch("main").Change(ctx, meta("from a"), func(ctx context.Context, m proto.Message) error {
		if !raced {
			raced = true
			// b commits between a reading and a publishing.
			if _, err := b.Branch("main").Change(ctx, meta("from b"), func(_ context.Context, m proto.Message) error {
				st := m.(*testpb.Store)
				st.Records = append(st.Records, &testpb.Record{
					Id: "from-b", Group: "beta", Date: &testpb.Date{Year: 2026, Month: 5, Day: 1},
				})
				return nil
			}); err != nil {
				return fmt.Errorf("b's write: %w", err)
			}
		}
		st := m.(*testpb.Store)
		st.Records = append(st.Records, &testpb.Record{
			Id: "from-a", Group: "alpha", Date: &testpb.Date{Year: 2026, Month: 6, Day: 1},
		})
		return nil
	})
	if err != nil {
		t.Fatalf("Change: %v", err)
	}

	// Neither writer's work is lost, which is the whole reason Change retries
	// rather than overwriting.
	got, err := a.Branch("main").Read(ctx)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	ids := map[string]bool{}
	for _, r := range got.(*testpb.Store).GetRecords() {
		ids[r.GetId()] = true
	}
	if !ids["from-a"] || !ids["from-b"] {
		t.Fatal("a concurrent write was lost")
	}
}

func TestBranchesAreIndependent(t *testing.T) {
	ctx := context.Background()
	s := newStore(t, memtransport.New(), keyring(t))

	if _, err := s.Branch("main").Write(ctx, meta("main"), &testpb.Store{
		Manifest: &testpb.Manifest{SchemaVersion: 1},
		Records:  records(5),
	}); err != nil {
		t.Fatalf("Write main: %v", err)
	}
	if _, err := s.Branch("import").Write(ctx, meta("import"), &testpb.Store{
		Manifest: &testpb.Manifest{SchemaVersion: 1},
		Records:  records(9),
	}); err != nil {
		t.Fatalf("Write import: %v", err)
	}

	main, err := s.Branch("main").Read(ctx)
	if err != nil {
		t.Fatalf("Read main: %v", err)
	}
	imported, err := s.Branch("import").Read(ctx)
	if err != nil {
		t.Fatalf("Read import: %v", err)
	}
	if len(main.(*testpb.Store).GetRecords()) != 5 {
		t.Fatalf("main has %d records, want 5", len(main.(*testpb.Store).GetRecords()))
	}
	if len(imported.(*testpb.Store).GetRecords()) != 9 {
		t.Fatalf("import has %d records, want 9", len(imported.(*testpb.Store).GetRecords()))
	}
}

func TestBranchNamesDoNotReachTheServer(t *testing.T) {
	ctx := context.Background()
	tr := memtransport.New()
	s := newStore(t, tr, keyring(t))

	if _, err := s.Branch("import/plaid-2026-08").Write(ctx, meta("x"), &testpb.Store{
		Manifest: &testpb.Manifest{SchemaVersion: 1},
		Records:  records(2),
	}); err != nil {
		t.Fatalf("Write: %v", err)
	}

	refs, err := tr.ListRefs(ctx)
	if err != nil {
		t.Fatalf("ListRefs: %v", err)
	}
	if len(refs) != 1 {
		t.Fatalf("got %d refs, want 1", len(refs))
	}
	if string(refs[0].Name) == "import/plaid-2026-08" {
		t.Fatal("the branch name reached the server in the clear")
	}
}

// TestAServerCannotRollAStoreBack covers the one thing encryption does not
// give: an old commit is genuine, so only remembering catches a replay.
func TestAServerCannotRollAStoreBack(t *testing.T) {
	ctx := context.Background()
	tr := memtransport.New()
	s := newStore(t, tr, keyring(t))
	main := s.Branch("main")

	first, err := main.Write(ctx, meta("first"), &testpb.Store{
		Manifest: &testpb.Manifest{SchemaVersion: 1},
		Records:  records(4),
	})
	if err != nil {
		t.Fatalf("Write: %v", err)
	}
	if _, err := main.Write(ctx, meta("second"), &testpb.Store{
		Manifest: &testpb.Manifest{SchemaVersion: 1},
		Records:  records(8),
	}); err != nil {
		t.Fatalf("Write: %v", err)
	}

	// The server hands back the older head. Everything about it verifies.
	tr.SetRef(keyring(t).RefName("main"), first)

	if _, err := main.Read(ctx); !errors.Is(err, protovcs.ErrWentBackwards) {
		t.Fatalf("reading a rolled-back store returned %v, want ErrWentBackwards", err)
	}
}

// forgingTransport serves the commit named forge as a record claiming parents,
// keeping its id and its sealed payload.
type forgingTransport struct {
	*memtransport.Transport
	forge   protovcs.CommitID
	parents [][]byte
}

func (f *forgingTransport) GetCommits(ctx context.Context, from protovcs.CommitID, depth uint32) ([]*pb.Commit, error) {
	commits, err := f.Transport.GetCommits(ctx, from, depth)
	if err != nil || from != f.forge || f.parents == nil {
		return commits, err
	}
	forged := proto.Clone(commits[0]).(*pb.Commit)
	forged.Parents = f.parents
	return []*pb.Commit{forged}, nil
}

// TestAServerCannotDressAnOldCommitAsANewOne covers a replay the ancestry check
// alone does not catch. The server serves an old commit's id and payload under
// a record whose parent is the newest head the client has seen, so the offered
// head looks like a descendant of it.
func TestAServerCannotDressAnOldCommitAsANewOne(t *testing.T) {
	ctx := context.Background()
	tr := &forgingTransport{Transport: memtransport.New()}
	writer := newStore(t, tr, keyring(t)).Branch("main")

	first, err := writer.Write(ctx, meta("first"), &testpb.Store{
		Manifest: &testpb.Manifest{SchemaVersion: 1},
		Records:  records(4),
	})
	if err != nil {
		t.Fatalf("Write: %v", err)
	}
	second, err := writer.Write(ctx, meta("second"), &testpb.Store{
		Manifest: &testpb.Manifest{SchemaVersion: 1},
		Records:  records(8),
	})
	if err != nil {
		t.Fatalf("Write: %v", err)
	}

	// A second client that has seen only the newer head.
	reader := newStore(t, tr, keyring(t)).Branch("main")
	if _, err := reader.Read(ctx); err != nil {
		t.Fatalf("Read: %v", err)
	}

	tr.forge, tr.parents = first, [][]byte{second.Bytes()}
	tr.SetRef(keyring(t).RefName("main"), first)

	if _, err := reader.Read(ctx); !errors.Is(err, protovcs.ErrTampered) {
		t.Fatalf("reading a forged head returned %v, want ErrTampered", err)
	}
}

// hidingTransport claims it holds no commit named hidden.
type hidingTransport struct {
	*memtransport.Transport
	hidden protovcs.CommitID
}

func (h *hidingTransport) GetCommits(ctx context.Context, from protovcs.CommitID, depth uint32) ([]*pb.Commit, error) {
	if from == h.hidden {
		return nil, nil
	}
	commits, err := h.Transport.GetCommits(ctx, from, depth)
	var out []*pb.Commit
	for _, c := range commits {
		if id, _ := protovcs.CommitName(c); id != h.hidden {
			out = append(out, c)
		}
	}
	return out, err
}

// TestHidingHistoryDoesNotPassTheRollbackCheck covers a server answering the
// ancestry walk with a missing commit, which used to read as "not an ancestor"
// and so let an older head through.
func TestHidingHistoryDoesNotPassTheRollbackCheck(t *testing.T) {
	ctx := context.Background()
	tr := &hidingTransport{Transport: memtransport.New()}
	writer := newStore(t, tr, keyring(t)).Branch("main")

	var heads []protovcs.CommitID
	for i := 1; i <= 3; i++ {
		id, err := writer.Write(ctx, meta(fmt.Sprintf("write %d", i)), &testpb.Store{
			Manifest: &testpb.Manifest{SchemaVersion: 1},
			Records:  records(4 * i),
		})
		if err != nil {
			t.Fatalf("Write: %v", err)
		}
		heads = append(heads, id)
	}

	reader := newStore(t, tr, keyring(t)).Branch("main")
	if _, err := reader.Read(ctx); err != nil {
		t.Fatalf("Read: %v", err)
	}

	// Back to the first head, hiding the commit between it and the newest.
	tr.hidden = heads[1]
	tr.SetRef(keyring(t).RefName("main"), heads[0])

	if _, err := reader.Read(ctx); !errors.Is(err, protovcs.ErrNoSuchCommit) {
		t.Fatalf("reading behind hidden history returned %v, want ErrNoSuchCommit", err)
	}
}

func TestAlteredChunksAreRefusedOnRead(t *testing.T) {
	ctx := context.Background()
	tr := memtransport.New()
	s := newStore(t, tr, keyring(t))
	main := s.Branch("main")

	if _, err := main.Write(ctx, meta("first"), &testpb.Store{
		Manifest: &testpb.Manifest{SchemaVersion: 1},
		Records:  records(30),
	}); err != nil {
		t.Fatalf("Write: %v", err)
	}

	// A fresh client, so nothing is served from the first one's cache.
	fresh := newStore(t, tr, keyring(t))

	head, _, err := fresh.Branch("main").Head(ctx)
	if err != nil {
		t.Fatalf("Head: %v", err)
	}
	for _, raw := range head.GetAdded() {
		id, err := protovcs.ChunkIDFromBytes(raw)
		if err != nil {
			t.Fatalf("ChunkIDFromBytes: %v", err)
		}
		tr.Corrupt(id, []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30})
		break
	}

	if _, err := fresh.Branch("main").Read(ctx); err == nil {
		t.Fatal("a store with an altered chunk was read without complaint")
	}
}

func TestReadingWithTheWrongKeyFails(t *testing.T) {
	ctx := context.Background()
	tr := memtransport.New()

	if _, err := newStore(t, tr, keyring(t)).Branch("main").Write(ctx, meta("x"), &testpb.Store{
		Manifest: &testpb.Manifest{SchemaVersion: 1},
		Records:  records(3),
	}); err != nil {
		t.Fatalf("Write: %v", err)
	}

	other := make([]byte, protovcs.RootKeyLen)
	for i := range other {
		other[i] = 0xAA
	}
	wrong, err := protovcs.NewKeyring(other)
	if err != nil {
		t.Fatalf("NewKeyring: %v", err)
	}

	// A branch is stored under a hash of its name, so the wrong key does not
	// find any branch at all and the store reads as one that was never written.
	// Nothing leaks, and nothing is mistaken for data.
	got, err := newStore(t, tr, wrong).Branch("main").Read(ctx)
	if err != nil {
		t.Fatalf("reading with the wrong key returned %v, want an empty store", err)
	}
	if len(got.(*testpb.Store).GetRecords()) != 0 {
		t.Fatal("a store opened with the wrong key returned records")
	}

	// And if a caller reaches a chunk anyway — by knowing its name from
	// somewhere else — it does not open.
	right := keyring(t)
	head, _, err := newStore(t, tr, right).Branch("main").Head(ctx)
	if err != nil {
		t.Fatalf("Head: %v", err)
	}
	id, err := protovcs.ChunkIDFromBytes(head.GetAdded()[0])
	if err != nil {
		t.Fatalf("ChunkIDFromBytes: %v", err)
	}
	sealed, err := tr.GetChunks(ctx, []protovcs.ChunkID{id})
	if err != nil {
		t.Fatalf("GetChunks: %v", err)
	}
	if _, err := wrong.Open(id, sealed[0].Sealed); !errors.Is(err, protovcs.ErrTampered) {
		t.Fatalf("opening a chunk with the wrong key returned %v, want ErrTampered", err)
	}
}

func TestAPlaintextStoreWorksTheSameWay(t *testing.T) {
	ctx := context.Background()
	s := newStore(t, memtransport.New(), protovcs.PlainSealer())
	main := s.Branch("main")

	want := &testpb.Store{
		Manifest: &testpb.Manifest{SchemaVersion: 1},
		Name:     "readable",
		Records:  records(20),
	}
	if _, err := main.Write(ctx, meta("first"), want); err != nil {
		t.Fatalf("Write: %v", err)
	}

	got, err := main.Read(ctx)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if got.(*testpb.Store).GetName() != "readable" {
		t.Fatal("a plaintext store did not round trip")
	}
}

func TestMissingBetweenIsTheIncrementalFetch(t *testing.T) {
	ctx := context.Background()
	tr := memtransport.New()
	s := newStore(t, tr, keyring(t))
	main := s.Branch("main")

	first, err := main.Write(ctx, meta("first"), &testpb.Store{
		Manifest: &testpb.Manifest{SchemaVersion: 1},
		Records:  records(200),
	})
	if err != nil {
		t.Fatalf("Write: %v", err)
	}
	second, err := main.Change(ctx, meta("one more"), func(_ context.Context, m proto.Message) error {
		st := m.(*testpb.Store)
		st.Records = append(st.Records, &testpb.Record{
			Id: "rec-new", Group: "alpha", Date: &testpb.Date{Year: 2026, Month: 3, Day: 3},
		})
		return nil
	})
	if err != nil {
		t.Fatalf("Change: %v", err)
	}

	// A client holding the first commit is told only what changed, computed
	// from plaintext fields alone with nothing decrypted.
	missing, full, err := tr.MissingBetween(ctx, []protovcs.CommitID{first}, second)
	if err != nil {
		t.Fatalf("MissingBetween: %v", err)
	}
	if full {
		t.Fatal("MissingBetween could not compute a delta between a commit and its child")
	}
	if len(missing) == 0 || len(missing) > 6 {
		t.Fatalf("an incremental fetch needs %d chunks, want a handful", len(missing))
	}

	// A client holding nothing is told everything.
	all, full, err := tr.MissingBetween(ctx, nil, second)
	if err != nil {
		t.Fatalf("MissingBetween: %v", err)
	}
	if full {
		t.Fatal("a client holding nothing should get a delta from the root, not a full-set flag")
	}
	if len(all) <= len(missing) {
		t.Fatal("a fresh clone did not need more chunks than an incremental fetch")
	}
}

// newStoreWithCommitMessage is a store over the test layout plus a declared
// commit message, for the paths that read or refuse commit details.
func newStoreWithCommitMessage(t *testing.T, tr protovcs.Transport) *protovcs.Store {
	t.Helper()

	l := layout()
	l.CommitMessage = "nextgui.protovcs.testpb.Change"

	c, err := protovcs.CompileLayout(l, (&testpb.Store{}).ProtoReflect().Descriptor())
	if err != nil {
		t.Fatalf("CompileLayout: %v", err)
	}
	s, err := protovcs.NewBuilder().WithTransport(tr).WithSealer(keyring(t)).WithLayout(c).Build()
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	return s
}
