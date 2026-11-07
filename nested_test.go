package protovcs_test

import (
	"context"
	"strings"
	"testing"

	protovcs "github.com/nextgui/protovcs"
	"github.com/nextgui/protovcs/memtransport"
	testpb "github.com/nextgui/protovcs/proto/protovcs/testpb"
	pb "github.com/nextgui/protovcs/proto/protovcs/v1"
)

// A split's field is a path, so it does not have to name a top-level field. What
// it may pass through is the question, and the answer comes from reassembly:
// merging items is proto.Merge, which concatenates repeated fields rather than
// matching them up by identity.

// TestASplitCanReachThroughASingularMessage is the nested case that works.
func TestASplitCanReachThroughASingularMessage(t *testing.T) {
	ctx := context.Background()
	tr := memtransport.New()

	l := &pb.Layout{
		Name:          "nested",
		RootMessage:   "nextgui.protovcs.testpb.Store",
		SchemaVersion: 1,
		Splits: []*pb.Split{{
			// Not a top-level field. Singular all the way down, so the path
			// names exactly one list.
			Field: "group.members",
			By:    []*pb.ShardKey{{Field: "id", Transform: pb.Transform_TRANSFORM_IDENTITY}},
		}},
	}
	c, err := protovcs.CompileLayout(l, (&testpb.Store{}).ProtoReflect().Descriptor())
	if err != nil {
		t.Fatalf("a split through a singular message was refused: %v", err)
	}

	s, err := protovcs.NewBuilder().WithTransport(tr).WithSealer(keyring(t)).WithLayout(c).Build()
	if err != nil {
		t.Fatalf("Build: %v", err)
	}

	want := &testpb.Store{
		Name: "nested",
		Group: &testpb.Group{Members: []*testpb.Account{
			{Id: "a", Label: "first"},
			{Id: "b", Label: "second"},
			{Id: "c", Label: "third"},
		}},
	}
	if _, err := s.Branch("main").Write(ctx, meta("nested"), want); err != nil {
		t.Fatalf("Write: %v", err)
	}

	// One item per member, keyed by the whole path.
	part, err := s.Branch("main").Checkout(ctx, protovcs.Field("group.members"))
	if err != nil {
		t.Fatalf("Checkout: %v", err)
	}
	if len(part.Items()) != 3 {
		t.Fatalf("a nested split produced %d items, want 3", len(part.Items()))
	}
	for _, it := range part.Items() {
		if !strings.HasPrefix(string(it.Key), "group.members/") {
			t.Fatalf("item key %q does not carry the nested path", it.Key)
		}
	}

	got, err := s.Branch("main").Read(ctx)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	gs := got.(*testpb.Store)
	if len(gs.GetGroup().GetMembers()) != 3 {
		t.Fatalf("a nested split round-tripped %d members, want 3", len(gs.GetGroup().GetMembers()))
	}
	if gs.GetName() != "nested" {
		t.Fatal("the root item was lost by a nested split")
	}
}

// TestASplitCannotReachThroughARepeatedField is the nested case that cannot
// work, and the error has to say why rather than just no.
//
// Root.children[].grandchildren does not name one list, it names one per child.
// Splitting it would need fragments that each hold one child, and merging two
// such fragments concatenates the children rather than matching them by
// identity — so a child would come back twice, each holding half its
// grandchildren.
func TestASplitCannotReachThroughARepeatedField(t *testing.T) {
	l := &pb.Layout{
		Name:          "through-repeated",
		RootMessage:   "nextgui.protovcs.testpb.Store",
		SchemaVersion: 1,
		Splits: []*pb.Split{{
			Field: "records.nested.values",
			By:    []*pb.ShardKey{{Field: "note", Transform: pb.Transform_TRANSFORM_IDENTITY}},
		}},
	}
	_, err := protovcs.CompileLayout(l, (&testpb.Store{}).ProtoReflect().Descriptor())
	if err == nil {
		t.Fatal("a split reaching through a repeated field compiled")
	}

	// The three things the message must carry: which field is the problem, why
	// it is one, and what to do instead.
	for _, want := range []string{
		"repeated field \"records\"",
		"proto.Merge",
		"Flatten instead",
	} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("error %q does not mention %q", err, want)
		}
	}
}

// TestFlatteningIsTheAnswerForNestedCollections shows the shape that works, and
// is the shape a real ledger already has: entries carry the account they belong
// to rather than being nested under it.
func TestFlatteningIsTheAnswerForNestedCollections(t *testing.T) {
	ctx := context.Background()
	tr := memtransport.New()

	// records is the flattened collection: each element carries the group it
	// belongs to, so it can be sharded by that without reaching inside
	// anything.
	l := &pb.Layout{
		Name:          "flattened",
		RootMessage:   "nextgui.protovcs.testpb.Store",
		SchemaVersion: 1,
		Splits: []*pb.Split{
			{Field: "accounts", By: []*pb.ShardKey{{Field: "id", Transform: pb.Transform_TRANSFORM_IDENTITY}}},
			{Field: "records", By: []*pb.ShardKey{{Field: "group", Transform: pb.Transform_TRANSFORM_IDENTITY}}},
		},
	}
	c, err := protovcs.CompileLayout(l, (&testpb.Store{}).ProtoReflect().Descriptor())
	if err != nil {
		t.Fatalf("CompileLayout: %v", err)
	}
	s, err := protovcs.NewBuilder().WithTransport(tr).WithSealer(keyring(t)).WithLayout(c).Build()
	if err != nil {
		t.Fatalf("Build: %v", err)
	}

	if _, err := s.Branch("main").Write(ctx, meta("flattened"), &testpb.Store{
		Accounts: []*testpb.Account{{Id: "alpha"}, {Id: "beta"}},
		Records:  records(30),
	}); err != nil {
		t.Fatalf("Write: %v", err)
	}

	// The child and its collection are now independently checkoutable, which is
	// what nesting them would have prevented anyway.
	part, err := s.Branch("main").Checkout(ctx, protovcs.Prefix("records/alpha"))
	if err != nil {
		t.Fatalf("Checkout: %v", err)
	}
	if len(part.Items()) != 1 {
		t.Fatalf("checked out %d items, want 1", len(part.Items()))
	}
	if err := part.Require(protovcs.Field("accounts")); err == nil {
		t.Fatal("a checkout of one group's records also covered every account")
	}
}
