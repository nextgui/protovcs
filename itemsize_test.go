package protovcs_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	protovcs "github.com/nextgui/protovcs"
	"github.com/nextgui/protovcs/memtransport"
	testpb "github.com/nextgui/protovcs/proto/protovcs/testpb"
	pb "github.com/nextgui/protovcs/proto/protovcs/v1"
)

// boundedLayout puts every record in one item and caps that item, so the limit
// is reachable without building a realistic store.
func boundedLayout(max uint32) *pb.Layout {
	return targetedLayout(0, max)
}

// targetedLayout sets the two sizes independently, which is the point of their
// being two.
func targetedLayout(target, max uint32) *pb.Layout {
	return &pb.Layout{
		Name:          "bounded",
		RootMessage:   "nextgui.protovcs.testpb.Store",
		SchemaVersion: 1,
		Splits: []*pb.Split{{
			Field:          "records",
			By:             []*pb.ShardKey{{Field: "group", Transform: pb.Transform_TRANSFORM_IDENTITY}},
			Order:          []string{"id"},
			SplitByteCount: target,
			MaxItemBytes:   max,
		}},
	}
}

func boundedStore(t *testing.T, tr protovcs.Transport, max uint32) *protovcs.Store {
	t.Helper()

	c, err := protovcs.CompileLayout(boundedLayout(max), (&testpb.Store{}).ProtoReflect().Descriptor())
	if err != nil {
		t.Fatalf("CompileLayout: %v", err)
	}
	s, err := protovcs.NewBuilder().WithTransport(tr).WithSealer(keyring(t)).WithLayout(c).Build()
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	return s
}

// TestAnOversizedItemIsRefusedBeforeAnythingIsUploaded is the case a layout can
// fix: several elements share one item, so a shard key component would divide
// them.
func TestAnOversizedItemIsRefusedBeforeAnythingIsUploaded(t *testing.T) {
	ctx := context.Background()
	tr := memtransport.New()
	s := boundedStore(t, tr, 512)

	var recs []*testpb.Record
	for i := 0; i < 40; i++ {
		recs = append(recs, &testpb.Record{
			Id:      fmt.Sprintf("%s-%02d", strings.Repeat("x", 18), i),
			Group:   "one-group",
			Payload: make([]byte, 64),
		})
	}

	_, err := s.Branch("main").Write(ctx, meta("too big"), &testpb.Store{Records: recs})
	if !errors.Is(err, protovcs.ErrItemTooLarge) {
		t.Fatalf("an oversized item returned %v, want ErrItemTooLarge", err)
	}

	// Refused before upload, so a layout that lets an item grow without bound
	// does not leave the store holding what it refused.
	if tr.ChunkCount() != 0 {
		t.Fatalf("a refused write stored %d chunks", tr.ChunkCount())
	}

	// The error has to say what to do. "Item too large" alone is not actionable.
	for _, want := range []string{"records", "add a shard key component", "40 elements"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("error %q does not mention %q", err, want)
		}
	}
}

// TestASingleOversizedElementIsAllowed is the case no layout can fix. One
// element is atomic, so no shard key divides it, and refusing would make the
// data unstorable.
func TestASingleOversizedElementIsAllowed(t *testing.T) {
	ctx := context.Background()
	tr := memtransport.New()
	s := boundedStore(t, tr, 512)

	big := &testpb.Record{
		Id:      "one-big-record",
		Group:   "solo",
		Payload: make([]byte, 4096),
	}

	if _, err := s.Branch("main").Write(ctx, meta("one big element"), &testpb.Store{
		Records: []*testpb.Record{big},
	}); err != nil {
		t.Fatalf("a single oversized element was refused: %v", err)
	}

	got, err := s.Branch("main").Read(ctx)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if len(got.(*testpb.Store).GetRecords()) != 1 {
		t.Fatal("the oversized element did not round trip")
	}
}

func TestOversizedElementIsReportable(t *testing.T) {
	// A caller that wants to warn about the case no layout can fix can ask.
	c, err := protovcs.CompileLayout(boundedLayout(512), (&testpb.Store{}).ProtoReflect().Descriptor())
	if err != nil {
		t.Fatalf("CompileLayout: %v", err)
	}

	if !c.OversizedElement("records/solo", 4096, 1) {
		t.Fatal("one element over the limit was not reported as oversized")
	}
	if c.OversizedElement("records/solo", 4096, 12) {
		t.Fatal("twelve elements over the limit were reported as one oversized element")
	}
	if c.OversizedElement("records/solo", 100, 1) {
		t.Fatal("an element inside the limit was reported as oversized")
	}
}

func TestNoLimitMeansNoRefusal(t *testing.T) {
	ctx := context.Background()
	tr := memtransport.New()
	s := boundedStore(t, tr, 0)

	var recs []*testpb.Record
	for i := 0; i < 200; i++ {
		recs = append(recs, &testpb.Record{
			Id: fmt.Sprintf("r-%03d", i), Group: "g", Payload: make([]byte, 64),
		})
	}
	if _, err := s.Branch("main").Write(ctx, meta("no limit"), &testpb.Store{Records: recs}); err != nil {
		t.Fatalf("a layout with no limit refused a large item: %v", err)
	}
}

func TestTheLimitAppliesToTheSplitThatOwnsTheItem(t *testing.T) {
	// A limit on one split must not refuse an item of another. Getting this
	// wrong would make the limit a store-wide cap, which is not what it says.
	ctx := context.Background()
	tr := memtransport.New()

	l := boundedLayout(512)
	l.Splits = append(l.Splits, &pb.Split{
		Field: "accounts",
		By:    []*pb.ShardKey{{Field: "id", Transform: pb.Transform_TRANSFORM_IDENTITY}},
	})
	c, err := protovcs.CompileLayout(l, (&testpb.Store{}).ProtoReflect().Descriptor())
	if err != nil {
		t.Fatalf("CompileLayout: %v", err)
	}
	s, err := protovcs.NewBuilder().WithTransport(tr).WithSealer(keyring(t)).WithLayout(c).Build()
	if err != nil {
		t.Fatalf("Build: %v", err)
	}

	accounts := []*testpb.Account{{Id: "solo", Label: strings.Repeat("y", 4096)}}
	if _, err := s.Branch("main").Write(ctx, meta("big account"), &testpb.Store{Accounts: accounts}); err != nil {
		t.Fatalf("a split with no limit was refused: %v", err)
	}
}

// TestATargetWithNoCeilingReportsWithoutRefusing is the case the two numbers
// exist to tell apart: aim for a size, tolerate anything.
func TestATargetWithNoCeilingReportsWithoutRefusing(t *testing.T) {
	ctx := context.Background()
	tr := memtransport.New()

	c, err := protovcs.CompileLayout(targetedLayout(256, 0), (&testpb.Store{}).ProtoReflect().Descriptor())
	if err != nil {
		t.Fatalf("CompileLayout: %v", err)
	}
	s, err := protovcs.NewBuilder().WithTransport(tr).WithSealer(keyring(t)).WithLayout(c).Build()
	if err != nil {
		t.Fatalf("Build: %v", err)
	}

	var recs []*testpb.Record
	for i := 0; i < 40; i++ {
		recs = append(recs, &testpb.Record{
			Id: fmt.Sprintf("r-%03d", i), Group: "one", Payload: make([]byte, 64),
		})
	}

	// Over the target, no ceiling: the write proceeds.
	if _, err := s.Branch("main").Write(ctx, meta("over target"), &testpb.Store{Records: recs}); err != nil {
		t.Fatalf("a target with no ceiling refused a write: %v", err)
	}

	// And is reportable, so a caller can warn without the write failing.
	over := c.OversizedItems([]protovcs.ItemRef{{Key: "records/one", Bytes: 4096, Elements: 40}})
	if len(over) != 1 {
		t.Fatal("an item over the target was not reported")
	}
	if got := c.ClassifyItem("records/one", 4096, 40); got != protovcs.ItemOverTarget {
		t.Fatalf("ClassifyItem = %v, want ItemOverTarget", got)
	}
}

func TestACeilingWithNoTargetRefusesOnlyTheIntolerable(t *testing.T) {
	c, err := protovcs.CompileLayout(targetedLayout(0, 1024), (&testpb.Store{}).ProtoReflect().Descriptor())
	if err != nil {
		t.Fatalf("CompileLayout: %v", err)
	}

	// No target, so nothing is "over target" however large, until the ceiling.
	if got := c.ClassifyItem("records/one", 900, 10); got != protovcs.ItemWithinTarget {
		t.Fatalf("ClassifyItem under the ceiling = %v, want ItemWithinTarget", got)
	}
	if got := c.ClassifyItem("records/one", 4096, 10); got != protovcs.ItemOverCeiling {
		t.Fatalf("ClassifyItem over the ceiling = %v, want ItemOverCeiling", got)
	}
}

func TestATargetAboveItsCeilingIsRefusedAtCompileTime(t *testing.T) {
	// Every item it aims for is one it refuses, which is a layout nobody meant
	// to write.
	_, err := protovcs.CompileLayout(targetedLayout(4096, 1024), (&testpb.Store{}).ProtoReflect().Descriptor())
	if err == nil {
		t.Fatal("a target above its own ceiling compiled")
	}
	if !strings.Contains(err.Error(), "every item it aims for is one it refuses") {
		t.Fatalf("error was %q", err)
	}
}

// TestALayoutNeedNotCoverTheMessage is the property that makes omission safe.
func TestALayoutNeedNotCoverTheMessage(t *testing.T) {
	ctx := context.Background()
	tr := memtransport.New()

	// This layout mentions records and nothing else. accounts, name, revision
	// and manifest are all unmentioned.
	c, err := protovcs.CompileLayout(targetedLayout(0, 0), (&testpb.Store{}).ProtoReflect().Descriptor())
	if err != nil {
		t.Fatalf("CompileLayout: %v", err)
	}
	s, err := protovcs.NewBuilder().WithTransport(tr).WithSealer(keyring(t)).WithLayout(c).Build()
	if err != nil {
		t.Fatalf("Build: %v", err)
	}

	want := &testpb.Store{
		Manifest: &testpb.Manifest{SchemaVersion: 1},
		Name:     "kept",
		Revision: 9,
		Records:  records(10),
		Accounts: []*testpb.Account{{Id: "acc-1", Label: "also kept"}},
	}
	if _, err := s.Branch("main").Write(ctx, meta("partial layout"), want); err != nil {
		t.Fatalf("Write: %v", err)
	}

	got, err := s.Branch("main").Read(ctx)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	gs := got.(*testpb.Store)
	if gs.GetName() != "kept" || gs.GetRevision() != 9 {
		t.Fatal("unmentioned scalars were lost")
	}
	if len(gs.GetAccounts()) != 1 || gs.GetAccounts()[0].GetLabel() != "also kept" {
		t.Fatal("an unmentioned repeated field was lost")
	}

	// It is not free, though: an unmentioned collection lives in the root item,
	// which every reader fetches. The layout can say which those are.
	unsplit := c.UnsplitCollections()
	found := false
	for _, f := range unsplit {
		if f == "accounts" {
			found = true
		}
	}
	if !found {
		t.Fatalf("UnsplitCollections = %v, want it to name accounts", unsplit)
	}
}

func TestAnOversizedRootItemIsRefusedAndSaysWhy(t *testing.T) {
	ctx := context.Background()
	tr := memtransport.New()

	l := targetedLayout(0, 0)
	l.RootMaxBytes = 512
	c, err := protovcs.CompileLayout(l, (&testpb.Store{}).ProtoReflect().Descriptor())
	if err != nil {
		t.Fatalf("CompileLayout: %v", err)
	}
	s, err := protovcs.NewBuilder().WithTransport(tr).WithSealer(keyring(t)).WithLayout(c).Build()
	if err != nil {
		t.Fatalf("Build: %v", err)
	}

	// accounts is unmentioned, so it grows the root item, which every reader
	// fetches.
	var accounts []*testpb.Account
	for i := 0; i < 60; i++ {
		accounts = append(accounts, &testpb.Account{Id: "acc", Label: strings.Repeat("z", 20)})
	}

	_, err = s.Branch("main").Write(ctx, meta("fat root"), &testpb.Store{Accounts: accounts})
	if !errors.Is(err, protovcs.ErrItemTooLarge) {
		t.Fatalf("an oversized root item returned %v, want ErrItemTooLarge", err)
	}
	// The number alone is not actionable; the message has to name the likely
	// cause.
	if !strings.Contains(err.Error(), "accounts") {
		t.Fatalf("error %q does not name the unsplit collection", err)
	}
}
