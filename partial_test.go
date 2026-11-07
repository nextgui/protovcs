package protovcs_test

import (
	"context"
	"errors"
	"testing"

	protovcs "github.com/nextgui/protovcs"
	"github.com/nextgui/protovcs/memtransport"
	testpb "github.com/nextgui/protovcs/proto/protovcs/testpb"
)

func seeded(t *testing.T, tr *memtransport.Transport, n int) *protovcs.Store {
	t.Helper()

	s := newStore(t, tr, keyring(t))
	if _, err := s.Branch("main").Write(context.Background(), meta("seed"), &testpb.Store{
		Manifest: &testpb.Manifest{SchemaVersion: 1},
		Name:     "seeded",
		Records:  records(n),
	}); err != nil {
		t.Fatalf("Write: %v", err)
	}
	return s
}

func TestCheckoutFetchesOnlyItsScope(t *testing.T) {
	ctx := context.Background()
	tr := memtransport.New()
	seeded(t, tr, 300)

	// A fresh client so nothing comes from the writer's cache.
	fresh := newStore(t, tr, keyring(t))

	part, err := fresh.Branch("main").Checkout(ctx, protovcs.Prefix("records/alpha"))
	if err != nil {
		t.Fatalf("Checkout: %v", err)
	}

	items := part.Items()
	if len(items) == 0 {
		t.Fatal("a scoped checkout fetched nothing")
	}
	for _, it := range items {
		for _, r := range it.Fragment.(*testpb.Store).GetRecords() {
			if r.GetGroup() != "alpha" {
				t.Fatalf("a checkout of records/alpha returned a record in group %q", r.GetGroup())
			}
		}
	}

	full, err := fresh.Branch("main").Read(ctx)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if len(items) >= len(full.(*testpb.Store).GetRecords()) {
		t.Fatal("a scoped checkout was not smaller than the whole store")
	}
}

func TestReadingOutsideTheScopeIsRefused(t *testing.T) {
	ctx := context.Background()
	tr := memtransport.New()
	s := seeded(t, tr, 60)

	part, err := s.Branch("main").Checkout(ctx, protovcs.Prefix("records/alpha"))
	if err != nil {
		t.Fatalf("Checkout: %v", err)
	}

	// Outside the scope: a refusal, because the cost of fetching it is not
	// bounded and the caller should decide.
	if _, _, err := part.Get("records/beta/2026/02"); !errors.Is(err, protovcs.ErrOutOfScope) {
		t.Fatalf("reading outside the scope returned %v, want ErrOutOfScope", err)
	}

	// Inside the scope and absent: not an error. There is genuinely nothing
	// there, and telling the two apart is the point of having a scope.
	_, found, err := part.Get("records/alpha/2099/01")
	if err != nil {
		t.Fatalf("reading a missing item inside the scope returned %v", err)
	}
	if found {
		t.Fatal("an item that does not exist was found")
	}
}

func TestRequireChecksCoverage(t *testing.T) {
	ctx := context.Background()
	tr := memtransport.New()
	s := seeded(t, tr, 30)

	part, err := s.Branch("main").Checkout(ctx, protovcs.Prefix("records/alpha"))
	if err != nil {
		t.Fatalf("Checkout: %v", err)
	}
	if err := part.Require(protovcs.Prefix("records/alpha")); err != nil {
		t.Fatalf("Require on its own scope: %v", err)
	}
	if err := part.Require(protovcs.Everything()); !errors.Is(err, protovcs.ErrOutOfScope) {
		t.Fatalf("Require(Everything) returned %v, want ErrOutOfScope", err)
	}
}

func TestExtendWidensTheScope(t *testing.T) {
	ctx := context.Background()
	tr := memtransport.New()
	s := seeded(t, tr, 60)

	part, err := s.Branch("main").Checkout(ctx, protovcs.Prefix("records/alpha"))
	if err != nil {
		t.Fatalf("Checkout: %v", err)
	}
	before := len(part.Items())

	if err := part.Extend(ctx, protovcs.Prefix("records/beta")); err != nil {
		t.Fatalf("Extend: %v", err)
	}
	if len(part.Items()) <= before {
		t.Fatal("Extend did not fetch anything")
	}
	if _, _, err := part.Get("records/beta/2026/02"); err != nil {
		t.Fatalf("reading inside the widened scope returned %v", err)
	}
}

// TestAPartialCheckoutWritesACompleteCommit is the property that makes partial
// checkout worth having: a client holding three months of a store can still
// publish a version naming every item, carrying the rest across by name.
func TestAPartialCheckoutWritesACompleteCommit(t *testing.T) {
	ctx := context.Background()
	tr := memtransport.New()
	s := seeded(t, tr, 300)

	before, err := s.Branch("main").Read(ctx)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	wantCount := len(before.(*testpb.Store).GetRecords())

	fresh := newStore(t, tr, keyring(t))
	part, err := fresh.Branch("main").Checkout(ctx, protovcs.Prefix("records/alpha/2026/01"))
	if err != nil {
		t.Fatalf("Checkout: %v", err)
	}

	items := part.Items()
	if len(items) != 1 {
		t.Fatalf("checked out %d items, want 1", len(items))
	}
	edited := items[0]
	frag := edited.Fragment.(*testpb.Store)
	frag.Records = append(frag.Records, &testpb.Record{
		Id: "rec-added", Group: "alpha", Date: &testpb.Date{Year: 2026, Month: 1, Day: 9},
	})
	if err := part.Put(edited); err != nil {
		t.Fatalf("Put: %v", err)
	}

	if _, err := part.Commit(ctx, meta("edited one month")); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	// Everything outside the scope is still there, having never been fetched.
	after, err := newStore(t, tr, keyring(t)).Branch("main").Read(ctx)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	got := after.(*testpb.Store)
	if len(got.GetRecords()) != wantCount+1 {
		t.Fatalf("after a partial commit the store has %d records, want %d", len(got.GetRecords()), wantCount+1)
	}
	if got.GetName() != "seeded" {
		t.Fatalf("the root item was lost by a partial commit: name is %q", got.GetName())
	}

	found := false
	for _, r := range got.GetRecords() {
		if r.GetId() == "rec-added" {
			found = true
		}
	}
	if !found {
		t.Fatal("the edit made through a partial checkout was not published")
	}
}

func TestScopeCoverage(t *testing.T) {
	all := protovcs.Everything()
	alpha := protovcs.Prefix("records/alpha")
	beta := protovcs.Prefix("records/beta")

	if !all.Covers(alpha) {
		t.Fatal("Everything does not cover a prefix")
	}
	if alpha.Covers(all) {
		t.Fatal("a prefix covers Everything")
	}
	if alpha.Covers(beta) {
		t.Fatal("one prefix covers a disjoint one")
	}
	if !protovcs.Union(alpha, beta).Covers(alpha) {
		t.Fatal("a union does not cover its parts")
	}
	if !protovcs.Field("records").Covers(alpha) {
		t.Fatal("a whole field does not cover one of its shard values")
	}
	if protovcs.Field("records").Covers(protovcs.Field("accounts")) {
		t.Fatal("one field covers another")
	}
}

// TestAnUnsplitFieldNeedsTheRootScope is the hole a layout falls into when it
// stops splitting a collection.
//
// Everything no rule mentions lives in the root item: every scalar, and every
// collection the layout chose not to split. Field() covers a key of its own, so
// for one of those it covers nothing — and a scope covering nothing produces a
// message with the field empty rather than an error. ScopeForField is what
// answers correctly, because it knows which kind the field is.
func TestAnUnsplitFieldNeedsTheRootScope(t *testing.T) {
	ctx := context.Background()
	s := newStore(t, memtransport.New(), keyring(t))

	if _, err := s.Branch("main").Write(ctx, meta("seed"), &testpb.Store{
		Manifest: &testpb.Manifest{SchemaVersion: 1},
		Name:     "in the root item",
		Revision: 7,
		Records:  records(20),
	}); err != nil {
		t.Fatalf("Write: %v", err)
	}

	// A split field's scope is its own key range, and holds no root fields.
	got := storeAt(t, ctx, s, protovcs.Field("records"))
	if got.GetName() != "" {
		t.Fatalf("a records scope carried a root field: name %q", got.GetName())
	}
	if len(got.GetRecords()) == 0 {
		t.Fatal("a records scope carried no records")
	}

	// The root scope is how a caller asks for the rest.
	got = storeAt(t, ctx, s, protovcs.Root())
	if got.GetName() != "in the root item" || got.GetRevision() != 7 {
		t.Fatalf("the root scope lost the root item: name %q, revision %d",
			got.GetName(), got.GetRevision())
	}

	// And the layout answers which of the two a field needs.
	for field, want := range map[string]protovcs.Scope{
		"records": protovcs.Field("records"),
		"name":    protovcs.Root(),
	} {
		scope, err := s.Layout().ScopeForField(field)
		if err != nil {
			t.Fatalf("ScopeForField(%q): %v", field, err)
		}
		if scope.String() != want.String() {
			t.Fatalf("ScopeForField(%q) is %s, want %s", field, scope, want)
		}
	}

	if _, err := s.Layout().ScopeForField("nope"); err == nil {
		t.Fatal("ScopeForField accepted a field the message does not have")
	}
}

func storeAt(t *testing.T, ctx context.Context, s *protovcs.Store, scope protovcs.Scope) *testpb.Store {
	t.Helper()

	part, err := s.Branch("main").Checkout(ctx, scope)
	if err != nil {
		t.Fatalf("Checkout(%s): %v", scope, err)
	}
	m, err := part.Materialized()
	if err != nil {
		t.Fatalf("Materialized: %v", err)
	}
	got, ok := m.(*testpb.Store)
	if !ok {
		t.Fatalf("Materialized returned %T", m)
	}
	return got
}
