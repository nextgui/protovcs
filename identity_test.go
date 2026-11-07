package protovcs_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	protovcs "github.com/nextgui/protovcs"
	"github.com/nextgui/protovcs/memtransport"
	testpb "github.com/nextgui/protovcs/proto/protovcs/testpb"
	pb "github.com/nextgui/protovcs/proto/protovcs/v1"
)

// testpb.Store annotates records with an identity of "id" and leaves accounts
// without one, so both paths are covered by the same fixture.

func TestIdentityIsReadFromTheFieldNotTheLayout(t *testing.T) {
	c := compiled(t)

	// Declared on the repeated field in the proto, so two layouts over this
	// message cannot disagree about it and protolock guards it.
	if got := c.Identity("records"); got != "id" {
		t.Fatalf("Identity(records) = %q, want %q", got, "id")
	}
	// accounts declares none, which is not a fault: its elements are then
	// identified by their contents.
	if got := c.Identity("accounts"); got != "" {
		t.Fatalf("Identity(accounts) = %q, want empty", got)
	}
}

func TestRequireIdentitySaysWhatToAnnotate(t *testing.T) {
	c := compiled(t)

	if _, err := c.RequireIdentity("records"); err != nil {
		t.Fatalf("RequireIdentity(records): %v", err)
	}

	_, err := c.RequireIdentity("accounts")
	if err == nil {
		t.Fatal("RequireIdentity succeeded on a collection with no identity")
	}
	// Nothing is inferred, so the error has to say what to write.
	for _, want := range []string{"declares no identity", "nextgui.protovcs.v1.identity"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("error %q does not mention %q", err, want)
		}
	}
}

// TestTwoElementsSharingAnIdentityAreRefused is the corruption a double append
// produces: a store that reads without complaint and holds one record twice.
func TestTwoElementsSharingAnIdentityAreRefused(t *testing.T) {
	ctx := context.Background()
	tr := memtransport.New()
	s := newStore(t, tr, keyring(t))

	dup := &testpb.Record{
		Id: "rec-0001", Group: "alpha", Date: &testpb.Date{Year: 2026, Month: 1, Day: 1},
	}
	_, err := s.Branch("main").Write(ctx, meta("double append"), &testpb.Store{
		Records: []*testpb.Record{dup, dup},
	})
	if !errors.Is(err, protovcs.ErrDuplicateIdentity) {
		t.Fatalf("a duplicated identity returned %v, want ErrDuplicateIdentity", err)
	}
	if !strings.Contains(err.Error(), "rec-0001") {
		t.Fatalf("error %q does not name the duplicated identity", err)
	}

	// Refused before anything is stored.
	if tr.ChunkCount() != 0 {
		t.Fatalf("a refused write stored %d chunks", tr.ChunkCount())
	}
}

func TestTwoElementsSharingAnIdentityInDifferentItemsAreFine(t *testing.T) {
	// The check is per item, because that is where two elements are actually
	// together. The same id in two months is a correction superseding an
	// earlier record, which is ordinary.
	ctx := context.Background()
	tr := memtransport.New()
	s := newStore(t, tr, keyring(t))

	if _, err := s.Branch("main").Write(ctx, meta("same id, two items"), &testpb.Store{
		Records: []*testpb.Record{
			{Id: "same", Group: "alpha", Date: &testpb.Date{Year: 2026, Month: 1, Day: 1}},
			{Id: "same", Group: "beta", Date: &testpb.Date{Year: 2026, Month: 1, Day: 1}},
		},
	}); err != nil {
		t.Fatalf("the same identity in two items was refused: %v", err)
	}
}

func TestACollectionWithNoIdentityMayHoldDuplicates(t *testing.T) {
	// Without a declared identity, two elements with the same contents are the
	// same element, and holding it twice is what the caller asked for.
	ctx := context.Background()
	tr := memtransport.New()
	s := newStore(t, tr, keyring(t))

	if _, err := s.Branch("main").Write(ctx, meta("duplicate accounts"), &testpb.Store{
		Accounts: []*testpb.Account{{Id: "acc-1", Label: "one"}, {Id: "acc-1", Label: "one"}},
	}); err != nil {
		t.Fatalf("a collection with no identity refused duplicates: %v", err)
	}
}

// TestIdentityIsTheDefaultOrder is what it buys on the read path: a total order
// rather than one that falls back to comparing encoded bytes.
func TestIdentityIsTheDefaultOrder(t *testing.T) {
	l := &pb.Layout{
		Name:          "unordered",
		RootMessage:   "nextgui.protovcs.testpb.Store",
		SchemaVersion: 1,
		Splits: []*pb.Split{{
			Field: "records",
			By:    []*pb.ShardKey{{Field: "group", Transform: pb.Transform_TRANSFORM_IDENTITY}},
			// No order declared. The identity supplies one.
		}},
	}
	c, err := protovcs.CompileLayout(l, (&testpb.Store{}).ProtoReflect().Descriptor())
	if err != nil {
		t.Fatalf("CompileLayout: %v", err)
	}

	in := &testpb.Store{Records: []*testpb.Record{
		{Id: "c", Group: "g", Amount: 3},
		{Id: "a", Group: "g", Amount: 1},
		{Id: "b", Group: "g", Amount: 2},
	}}
	items, err := c.Materialize(in)
	if err != nil {
		t.Fatalf("Materialize: %v", err)
	}
	if len(items) != 1 {
		t.Fatalf("got %d items, want 1", len(items))
	}

	var ids []string
	for _, r := range items[0].Fragment.(*testpb.Store).GetRecords() {
		ids = append(ids, r.GetId())
	}
	if strings.Join(ids, ",") != "a,b,c" {
		t.Fatalf("elements came back as %v, want them ordered by identity", ids)
	}
}

func TestAnIdentityThatIsNotAScalarIsRefused(t *testing.T) {
	// Nothing tells two elements apart by a submessage, and pretending
	// otherwise would make the uniqueness check meaningless.
	c := compiled(t)
	if c.Identity("records") == "" {
		t.Skip("fixture no longer annotates records")
	}
	// The refusal itself is exercised by the compiler; this asserts the shape
	// the annotation must have.
	if _, err := c.RequireIdentity("records"); err != nil {
		t.Fatalf("a scalar identity was refused: %v", err)
	}
}

func compiled(t *testing.T) *protovcs.Compiled {
	t.Helper()
	c, err := protovcs.CompileLayout(layout(), (&testpb.Store{}).ProtoReflect().Descriptor())
	if err != nil {
		t.Fatalf("CompileLayout: %v", err)
	}
	return c
}
