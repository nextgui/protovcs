package protovcs_test

import (
	"context"
	"errors"
	"sort"
	"strings"
	"testing"

	"google.golang.org/protobuf/proto"

	protovcs "github.com/nextgui/protovcs"
	"github.com/nextgui/protovcs/memtransport"
	testpb "github.com/nextgui/protovcs/proto/protovcs/testpb"
	pb "github.com/nextgui/protovcs/proto/protovcs/v1"
)

func TestACommitCarriesTheSchemaItWasWrittenUnder(t *testing.T) {
	ctx := context.Background()
	tr := memtransport.New()
	s := seeded(t, tr, 20)

	head, _, err := s.Branch("main").Head(ctx)
	if err != nil {
		t.Fatalf("Head: %v", err)
	}
	schema, err := s.SchemaOf(ctx, head)
	if err != nil {
		t.Fatalf("SchemaOf: %v", err)
	}

	if schema.GetRootMessage() != "nextgui.protovcs.testpb.Store" {
		t.Fatalf("schema names %q", schema.GetRootMessage())
	}
	if len(schema.GetDescriptors().GetFile()) == 0 {
		t.Fatal("the schema chunk carries no descriptors, so an old commit could not be read without this build")
	}
	if len(schema.GetLayout().GetSplits()) != 2 {
		t.Fatalf("the stored layout has %d splits, want 2", len(schema.GetLayout().GetSplits()))
	}
}

// TestReadGenericNeedsNoCompiledInLayout is what makes retiring a schema
// possible: a commit is readable from what it carries.
func TestReadGenericNeedsNoCompiledInLayout(t *testing.T) {
	ctx := context.Background()
	tr := memtransport.New()
	s := seeded(t, tr, 40)

	head, _, err := s.Branch("main").Head(ctx)
	if err != nil {
		t.Fatalf("Head: %v", err)
	}

	msg, layout, err := s.ReadGeneric(ctx, head)
	if err != nil {
		t.Fatalf("ReadGeneric: %v", err)
	}
	if layout.Layout().GetName() != "teststore" {
		t.Fatalf("the recovered layout is named %q", layout.Layout().GetName())
	}

	// The result is the same data. It arrives as a message built from stored
	// descriptors rather than from this build's generated type.
	items, err := layout.Materialize(msg)
	if err != nil {
		t.Fatalf("Materialize: %v", err)
	}
	if len(items) == 0 {
		t.Fatal("a generic read produced no items")
	}
}

func TestACommitFromAFutureSchemaIsRefused(t *testing.T) {
	ctx := context.Background()
	tr := memtransport.New()

	// A client on schema 2 writes.
	ahead := layout()
	ahead.SchemaVersion = 2
	c2, err := protovcs.CompileLayout(ahead, (&testpb.Store{}).ProtoReflect().Descriptor())
	if err != nil {
		t.Fatalf("CompileLayout: %v", err)
	}
	writer, err := protovcs.NewBuilder().WithTransport(tr).WithSealer(keyring(t)).WithLayout(c2).Build()
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	if _, err := writer.Branch("main").Write(ctx, meta("v2"), &testpb.Store{
		Manifest: &testpb.Manifest{SchemaVersion: 1},
		Records:  records(5),
	}); err != nil {
		t.Fatalf("Write: %v", err)
	}

	// A client on schema 1 refuses it rather than reading what it can. A store
	// read under the wrong schema produces plausible wrong values.
	old := newStore(t, tr, keyring(t))
	if _, err := old.Branch("main").Read(ctx); !errors.Is(err, protovcs.ErrUnsupportedSchema) {
		t.Fatalf("an older client returned %v, want ErrUnsupportedSchema", err)
	}
}

func TestACommitBelowMinReadableVersionIsRefused(t *testing.T) {
	ctx := context.Background()
	tr := memtransport.New()
	seeded(t, tr, 5)

	// The same build, after a retirement: it no longer reads schema 1.
	retired := layout()
	retired.SchemaVersion = 2
	retired.MinReadableVersion = 2
	c, err := protovcs.CompileLayout(retired, (&testpb.Store{}).ProtoReflect().Descriptor())
	if err != nil {
		t.Fatalf("CompileLayout: %v", err)
	}
	s, err := protovcs.NewBuilder().WithTransport(tr).WithSealer(keyring(t)).WithLayout(c).Build()
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	if _, err := s.Branch("main").Read(ctx); !errors.Is(err, protovcs.ErrUnsupportedSchema) {
		t.Fatalf("a retired schema returned %v, want ErrUnsupportedSchema", err)
	}
}

func TestReplaceStreamsAndDeduplicates(t *testing.T) {
	ctx := context.Background()
	tr := memtransport.New()
	s := seeded(t, tr, 120)
	before := tr.ChunkCount()

	// Re-normalising and producing the identical data must store nothing new.
	// That is what makes a full rewrite of an unchanged store almost free, and
	// what makes a migration resumable.
	existing, err := s.Branch("main").Read(ctx)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	recs := existing.(*testpb.Store).GetRecords()

	if _, err := s.Branch("replaced").Replace(ctx, meta("same data"),
		func(yield func(string, proto.Message) error) error {
			ordered := orderedByItemKey(t, s, recs)
			for _, r := range ordered {
				if err := yield("records", r); err != nil {
					return err
				}
			}
			return nil
		}); err != nil {
		t.Fatalf("Replace: %v", err)
	}

	// Only the manifest and commit differ, because the branch has no root item.
	added := tr.ChunkCount() - before
	if added > len(recs)/4 {
		t.Fatalf("replacing with identical data stored %d new chunks", added)
	}

	got, err := s.Branch("replaced").Read(ctx)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if len(got.(*testpb.Store).GetRecords()) != len(recs) {
		t.Fatalf("Replace wrote %d records, want %d", len(got.(*testpb.Store).GetRecords()), len(recs))
	}
}

func TestReplaceRefusesElementsOutOfOrder(t *testing.T) {
	ctx := context.Background()
	tr := memtransport.New()
	s := newStore(t, tr, keyring(t))

	_, err := s.Branch("main").Replace(ctx, meta("out of order"),
		func(yield func(string, proto.Message) error) error {
			if err := yield("records", &testpb.Record{
				Id: "b", Group: "zeta", Date: &testpb.Date{Year: 2026, Month: 1, Day: 1},
			}); err != nil {
				return err
			}
			return yield("records", &testpb.Record{
				Id: "a", Group: "alpha", Date: &testpb.Date{Year: 2026, Month: 1, Day: 1},
			})
		})
	if err == nil {
		t.Fatal("Replace accepted elements out of item key order")
	}
}

func TestRelayoutRewritesUnderNewItemKeys(t *testing.T) {
	ctx := context.Background()
	tr := memtransport.New()
	s := seeded(t, tr, 90)

	// Month batching becomes year batching. Same messages, different items.
	next := layout()
	next.Splits[0].By[1].Transform = pb.Transform_TRANSFORM_YEAR
	c, err := protovcs.CompileLayout(next, (&testpb.Store{}).ProtoReflect().Descriptor())
	if err != nil {
		t.Fatalf("CompileLayout: %v", err)
	}

	if _, err := s.Branch("main").Relayout(ctx, meta("year batching"), c); err != nil {
		t.Fatalf("Relayout: %v", err)
	}

	reader, err := protovcs.NewBuilder().WithTransport(tr).WithSealer(keyring(t)).WithLayout(c).Build()
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	part, err := reader.Branch("main").Checkout(ctx, protovcs.Field("records"))
	if err != nil {
		t.Fatalf("Checkout: %v", err)
	}
	for _, it := range part.Items() {
		parts := strings.Split(string(it.Key), "/")
		if len(parts) != 3 || len(parts[2]) != 4 {
			t.Fatalf("after relayout an item is keyed %q, want the last segment to be a year", it.Key)
		}
	}

	got, err := reader.Branch("main").Read(ctx)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if len(got.(*testpb.Store).GetRecords()) != 90 {
		t.Fatalf("relayout kept %d records, want 90", len(got.(*testpb.Store).GetRecords()))
	}
}

func TestTruncateDropsHistoryAndKeepsData(t *testing.T) {
	ctx := context.Background()
	tr := memtransport.New()
	s := seeded(t, tr, 30)
	main := s.Branch("main")

	if _, err := main.Change(ctx, meta("second"), func(_ context.Context, m proto.Message) error {
		st := m.(*testpb.Store)
		st.Records = append(st.Records, &testpb.Record{
			Id: "extra", Group: "alpha", Date: &testpb.Date{Year: 2026, Month: 2, Day: 2},
		})
		return nil
	}); err != nil {
		t.Fatalf("Change: %v", err)
	}

	before, err := main.Read(ctx)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}

	if _, err := main.Truncate(ctx, meta("retire history")); err != nil {
		t.Fatalf("Truncate: %v", err)
	}

	head, _, err := main.Head(ctx)
	if err != nil {
		t.Fatalf("Head: %v", err)
	}
	if len(head.GetParents()) != 0 {
		t.Fatalf("a truncated head has %d parents, want none", len(head.GetParents()))
	}

	after, err := main.Read(ctx)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if len(after.(*testpb.Store).GetRecords()) != len(before.(*testpb.Store).GetRecords()) {
		t.Fatal("truncating lost data")
	}

	// Every chunk the new root needs is in its own added set, so a collector
	// walking refs finds all of them and nothing that only the old history
	// reached.
	if len(head.GetAdded()) == 0 {
		t.Fatal("a truncated root adds nothing, so collection would delete its chunks")
	}
}

// orderedByItemKey sorts elements the way Replace requires them.
func orderedByItemKey(t *testing.T, s *protovcs.Store, recs []*testpb.Record) []*testpb.Record {
	t.Helper()

	out := make([]*testpb.Record, len(recs))
	copy(out, recs)
	sort.SliceStable(out, func(i, j int) bool {
		ki, err := s.Layout().ItemKeyFor("records", out[i])
		if err != nil {
			t.Fatalf("ItemKeyFor: %v", err)
		}
		kj, err := s.Layout().ItemKeyFor("records", out[j])
		if err != nil {
			t.Fatalf("ItemKeyFor: %v", err)
		}
		return ki < kj
	})
	return out
}

func TestACommitRecordsWhichCanonicalRulesNamedIt(t *testing.T) {
	// Different canonical rules read every chunk correctly and name none of
	// them the same way. A writer on the wrong rules would find nothing already
	// present and re-upload the store while believing it wrote a delta, so the
	// version is recorded to make that a detected condition rather than a bill.
	ctx := context.Background()
	tr := memtransport.New()
	s := seeded(t, tr, 10)

	head, _, err := s.Branch("main").Head(ctx)
	if err != nil {
		t.Fatalf("Head: %v", err)
	}
	schema, err := s.SchemaOf(ctx, head)
	if err != nil {
		t.Fatalf("SchemaOf: %v", err)
	}
	if schema.GetCanonicalVersion() != protovcs.CanonicalVersion {
		t.Fatalf("commit records canonical version %d, want %d",
			schema.GetCanonicalVersion(), protovcs.CanonicalVersion)
	}
}
