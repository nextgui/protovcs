package protovcs_test

import (
	"context"
	"testing"

	protovcs "github.com/nextgui/protovcs"
	"github.com/nextgui/protovcs/memtransport"
	testpb "github.com/nextgui/protovcs/proto/protovcs/testpb"
	hashed "github.com/nextgui/protovcs/proto/protovcs/testpb/hashedlayout"
	gen "github.com/nextgui/protovcs/proto/protovcs/testpb/teststorelayout"
)

// These use the package //cmd/protovcsgen produced from
// teststore.layout.textproto. They exist to prove the generated facade agrees
// with the dynamic API rather than being a second implementation that can
// drift from it.

func TestGeneratedLayoutMatchesTheHandBuiltOne(t *testing.T) {
	desc := (&testpb.Store{}).ProtoReflect().Descriptor()

	fromFile, err := gen.Layout(desc)
	if err != nil {
		t.Fatalf("gen.Layout: %v", err)
	}
	if gen.RootMessage != string(desc.FullName()) {
		t.Fatalf("generated RootMessage is %q, want %q", gen.RootMessage, desc.FullName())
	}
	if gen.SchemaVersion != 1 {
		t.Fatalf("generated SchemaVersion is %d, want 1", gen.SchemaVersion)
	}

	// The same shard keys, so the same data lands in the same items whichever
	// route a caller took to the layout.
	rec := &testpb.Record{Id: "x", Group: "alpha", Date: &testpb.Date{Year: 2026, Month: 8, Day: 3}}

	fromCode, err := protovcs.CompileLayout(layout(), desc)
	if err != nil {
		t.Fatalf("CompileLayout: %v", err)
	}

	a, err := fromFile.ItemKeyFor("records", rec)
	if err != nil {
		t.Fatalf("ItemKeyFor: %v", err)
	}
	b, err := fromCode.ItemKeyFor("records", rec)
	if err != nil {
		t.Fatalf("ItemKeyFor: %v", err)
	}
	if a != b {
		t.Fatalf("the generated layout keys an element %q where the hand-built one keys it %q", a, b)
	}
}

func TestGeneratedScopesFetchWhatTheyName(t *testing.T) {
	ctx := context.Background()
	tr := memtransport.New()

	s, err := gen.Open(protovcs.NewBuilder().WithTransport(tr).WithSealer(keyring(t)))
	if err != nil {
		t.Fatalf("gen.Open: %v", err)
	}

	if _, err := s.Branch("main").Write(ctx, genMeta("seed"), &testpb.Store{
		Manifest: &testpb.Manifest{SchemaVersion: 1},
		Records:  records(60),
		Accounts: []*testpb.Account{{Id: "acc-1"}},
	}); err != nil {
		t.Fatalf("Write: %v", err)
	}

	// A generated scope constructor gets the key range right, which is the
	// thing a caller composing one by hand gets wrong. The view hands back
	// elements rather than fragments to assert on.
	view, err := s.Branch("main").CheckoutRecords(ctx, gen.RecordsByGroup("alpha"))
	if err != nil {
		t.Fatalf("CheckoutRecords: %v", err)
	}
	got := view.All()
	if len(got) == 0 {
		t.Fatal("the generated scope fetched nothing")
	}
	for _, r := range got {
		if r.GetGroup() != "alpha" {
			t.Fatalf("RecordsByGroup(\"alpha\") returned a record in group %q", r.GetGroup())
		}
	}

	// A declared scope spanning two collections is one checkout and one view
	// with an accessor per collection.
	whole, err := s.Branch("main").CheckoutEverything(ctx, gen.ScopeEverything())
	if err != nil {
		t.Fatalf("CheckoutEverything: %v", err)
	}
	if len(whole.Records()) == 0 {
		t.Fatal("the declared scope fetched no records")
	}
	if len(whole.Accounts()) != 1 {
		t.Fatalf("the declared scope fetched %d accounts, want 1", len(whole.Accounts()))
	}
}

// TestTheTypedApiNeedsNoAssertions is the point of all of it: a caller reads,
// changes and checks out without ever naming protovcs.Scope, an item key, or a
// proto.Message.
func TestTheTypedApiNeedsNoAssertions(t *testing.T) {
	ctx := context.Background()
	tr := memtransport.New()

	s, err := gen.Open(protovcs.NewBuilder().WithTransport(tr).WithSealer(keyring(t)))
	if err != nil {
		t.Fatalf("gen.Open: %v", err)
	}
	b := s.Branch("main")

	if _, err := b.Change(ctx, genMeta("typed"), func(_ context.Context, st *testpb.Store) error {
		st.Name = "typed all the way"
		st.Records = append(st.Records, records(30)...)
		return nil
	}); err != nil {
		t.Fatalf("Change: %v", err)
	}

	data, err := b.Read(ctx)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if data.GetName() != "typed all the way" {
		t.Fatalf("Read returned %q", data.GetName())
	}

	// A shard value, not a segment. The caller never spells "2026/08".
	view, err := b.CheckoutRecords(ctx, gen.RecordsForDate("alpha", 2026, 1))
	if err != nil {
		t.Fatalf("CheckoutRecords: %v", err)
	}
	for _, r := range view.All() {
		if r.GetGroup() != "alpha" || r.GetDate().GetMonth() != 1 {
			t.Fatalf("a scope for alpha in 2026-01 returned %v", r)
		}
	}

	if err := view.Put(ctx, &testpb.Record{
		Id: "typed-put", Group: "alpha", Date: &testpb.Date{Year: 2026, Month: 1, Day: 9},
	}); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if _, err := view.Commit(ctx, genMeta("typed put")); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	after, err := b.Read(ctx)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	found := false
	for _, r := range after.GetRecords() {
		if r.GetId() == "typed-put" {
			found = true
		}
	}
	if !found {
		t.Fatal("a typed Put did not land")
	}
}

func TestGeneratedItemKeyHelperAgreesWithTheLayout(t *testing.T) {
	desc := (&testpb.Store{}).ProtoReflect().Descriptor()
	c, err := gen.Layout(desc)
	if err != nil {
		t.Fatalf("gen.Layout: %v", err)
	}

	rec := &testpb.Record{Id: "y", Group: "beta", Date: &testpb.Date{Year: 2026, Month: 2, Day: 1}}

	viaHelper, err := gen.RecordsItemKey(c, rec)
	if err != nil {
		t.Fatalf("RecordsItemKey: %v", err)
	}
	viaLayout, err := c.ItemKeyFor("records", rec)
	if err != nil {
		t.Fatalf("ItemKeyFor: %v", err)
	}
	if viaHelper != viaLayout {
		t.Fatalf("the generated helper returned %q, the layout returned %q", viaHelper, viaLayout)
	}
}

// TestAScopeReadsAnUnsplitField is the generated half of the hole a layout falls
// into when it stops splitting a collection.
//
// The RecordsAndName scope names "name", which no split covers, so it lives in
// the root item. Before the scope carried Root() this returned empty — not an
// error, an empty string where a value was written.
func TestAScopeReadsAnUnsplitField(t *testing.T) {
	ctx := context.Background()
	s, err := gen.Open(protovcs.NewBuilder().WithTransport(memtransport.New()).WithSealer(keyring(t)))
	if err != nil {
		t.Fatalf("gen.Open: %v", err)
	}

	if _, err = s.Branch("main").Write(ctx, genMeta("seed"), &testpb.Store{
		Manifest: &testpb.Manifest{SchemaVersion: 1},
		Name:     "in the root item",
		Records:  records(30),
	}); err != nil {
		t.Fatalf("Write: %v", err)
	}

	view, err := s.Branch("main").CheckoutRecordsAndName(ctx, gen.ScopeRecordsAndName())
	if err != nil {
		t.Fatalf("CheckoutRecordsAndName: %v", err)
	}
	if got := view.Name(); got != "in the root item" {
		t.Fatalf("the scope named %q and read %q", "name", got)
	}
	if len(view.Records()) != 30 {
		t.Fatalf("the scope read %d records, want 30", len(view.Records()))
	}
}

func TestAGeneratedScopeForOneGroupExcludesGroupsItPrefixes(t *testing.T) {
	ctx := context.Background()
	s, err := gen.Open(protovcs.NewBuilder().WithTransport(memtransport.New()).WithSealer(keyring(t)))
	if err != nil {
		t.Fatalf("gen.Open: %v", err)
	}
	d := &testpb.Date{Year: 2026, Month: 1, Day: 1}
	if _, err := s.Branch("main").Write(ctx, genMeta("seed"), &testpb.Store{Records: []*testpb.Record{
		{Id: "1", Group: "al", Date: d}, {Id: "2", Group: "alpha", Date: d}, {Id: "3", Group: "al-x", Date: d},
	}}); err != nil {
		t.Fatalf("Write: %v", err)
	}
	view, err := s.Branch("main").CheckoutRecords(ctx, gen.RecordsByGroup("al"))
	if err != nil {
		t.Fatalf("CheckoutRecords: %v", err)
	}
	got := view.All()
	if len(got) != 1 || got[0].GetGroup() != "al" {
		t.Fatalf("RecordsByGroup(\"al\") returned %v, want the one record in group al", got)
	}
}

// A layout with a hashed shard key generates constructors that hash the value
// as a write does, so the scope for one value holds exactly its elements.
func TestAGeneratedHashedScopeFetchesItsValue(t *testing.T) {
	ctx := context.Background()
	s, err := hashed.Open(protovcs.NewBuilder().WithTransport(memtransport.New()).WithSealer(keyring(t)))
	if err != nil {
		t.Fatalf("hashed.Open: %v", err)
	}
	if _, err := s.Branch("main").Write(ctx, hashed.Commit("seed", "test", "test@example.com"), &testpb.Store{
		Records:  records(40),
		Accounts: []*testpb.Account{{Id: "1", Label: "one"}, {Id: "2", Label: "two"}, {Id: "3", Label: "three"}},
	}); err != nil {
		t.Fatalf("Write: %v", err)
	}
	for _, rec := range records(40)[:5] {
		key, err := hashed.RecordsItemKey(s.Unwrap().Layout(), rec)
		if err != nil {
			t.Fatal(err)
		}
		scope := hashed.RecordsForDate(rec.GetId(), int(rec.GetDate().GetYear()), int(rec.GetDate().GetMonth()))
		if !scope.Scope().Holds(key) {
			t.Fatalf("RecordsForDate(%q, ...) does not hold %q, where a write puts it", rec.GetId(), key)
		}
		view, err := s.Branch("main").CheckoutRecords(ctx, hashed.RecordsById(rec.GetId()))
		if err != nil {
			t.Fatal(err)
		}
		found := false
		for _, r := range view.All() {
			found = found || r.GetId() == rec.GetId()
		}
		if !found {
			t.Fatalf("RecordsById(%q) does not return that record", rec.GetId())
		}
	}
	view, err := s.Branch("main").CheckoutAccounts(ctx, hashed.AccountsForLabel("two"))
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, a := range view.All() {
		found = found || a.GetLabel() == "two"
	}
	if !found {
		t.Fatal("AccountsForLabel(\"two\") does not return that account")
	}
}
