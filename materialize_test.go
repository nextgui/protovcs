package protovcs

import (
	"fmt"
	"math/rand"
	"testing"

	"google.golang.org/protobuf/proto"

	testpb "github.com/nextgui/protovcs/proto/protovcs/testpb"
	pb "github.com/nextgui/protovcs/proto/protovcs/v1"
)

// testLayout splits records by group and month and accounts by id, which is the
// shape a real layout has: one collection sharded on a composite key, one
// sharded one item per element, and scalars left in the root.
func testLayout() *pb.Layout {
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
			{
				Field: "accounts",
				By:    []*pb.ShardKey{{Field: "id", Transform: pb.Transform_TRANSFORM_IDENTITY}},
			},
		},
		RootMaxBytes: 65536,
	}
}

func compiledTestLayout(t *testing.T) *Compiled {
	t.Helper()
	c, err := CompileLayout(testLayout(), (&testpb.Store{}).ProtoReflect().Descriptor())
	if err != nil {
		t.Fatalf("CompileLayout: %v", err)
	}
	return c
}

// sampleStore builds a store with records spread over several groups and
// months, so that materializing it produces more than one item per split.
func sampleStore(n int) *testpb.Store {
	s := &testpb.Store{
		Manifest: &testpb.Manifest{SchemaVersion: 1},
		Name:     "a store",
		Revision: 7,
	}
	groups := []string{"alpha", "beta", "gamma"}
	for i := 0; i < n; i++ {
		s.Records = append(s.Records, &testpb.Record{
			Id:     fmt.Sprintf("rec-%04d", i),
			Group:  groups[i%len(groups)],
			Date:   &testpb.Date{Year: 2026, Month: int32(1 + i%12), Day: int32(1 + i%28)},
			Amount: int64(i * 13),
			Tags:   []string{"t1", "t2"},
			Attributes: map[string]string{
				"k": fmt.Sprintf("v%d", i),
			},
			Nested:   &testpb.Nested{Note: "n", Values: []int64{int64(i)}},
			Payload:  []byte{byte(i), byte(i >> 8)},
			Score:    float64(i) / 3,
			Counters: []int32{int32(i), int32(-i)},
		})
	}
	for i := 0; i < 4; i++ {
		s.Accounts = append(s.Accounts, &testpb.Account{
			Id:    fmt.Sprintf("acc-%d", i),
			Label: fmt.Sprintf("account %d", i),
		})
	}
	return s
}

// canonicalize is one round trip: decompose a message and reassemble it.
func canonicalize(t *testing.T, c *Compiled, m proto.Message) proto.Message {
	t.Helper()

	items, err := c.Materialize(m)
	if err != nil {
		t.Fatalf("Materialize: %v", err)
	}
	out, err := c.Merge(items)
	if err != nil {
		t.Fatalf("Merge: %v", err)
	}
	return out
}

// TestMaterializeLosesNothing is the property the whole design rests on:
// decomposing a message and reassembling it returns every element and every
// field that went in.
//
// It does not return the split fields in the order they went in. A split field
// is a multiset, and reading returns the layout's canonical order, which is
// what makes the same data always encode the same way and therefore name the
// same chunks. So the check is that nothing is lost rather than that nothing
// moved.
func TestMaterializeLosesNothing(t *testing.T) {
	c := compiledTestLayout(t)

	for _, n := range []int{0, 1, 2, 37, 200} {
		want := sampleStore(n)
		got := canonicalize(t, c, want).(*testpb.Store)

		if got.GetName() != want.GetName() || got.GetRevision() != want.GetRevision() {
			t.Fatalf("%d records: the root fields changed", n)
		}
		if !proto.Equal(want.GetManifest(), got.GetManifest()) {
			t.Fatalf("%d records: the manifest changed", n)
		}
		assertSameElements(t, want.GetRecords(), got.GetRecords())
		assertSameElements(t, want.GetAccounts(), got.GetAccounts())
	}
}

// TestCanonicalFormIsAFixedPoint is what stops a store from oscillating.
//
// Reassembling a message puts its split fields in canonical order. Doing it
// again must change nothing, or rewriting data that nobody edited would produce
// different bytes and therefore different chunk names, and every commit would
// re-upload a store that did not change.
func TestCanonicalFormIsAFixedPoint(t *testing.T) {
	c := compiledTestLayout(t)

	once := canonicalize(t, c, sampleStore(200))
	twice := canonicalize(t, c, once)

	if !proto.Equal(once, twice) {
		t.Fatal("a second round trip changed the message, so canonical form is not stable")
	}
}

// TestRoundTripIsExactOnceCanonical states the identity that does hold:
// a message already in canonical order comes back unchanged.
func TestRoundTripIsExactOnceCanonical(t *testing.T) {
	c := compiledTestLayout(t)

	want := canonicalize(t, c, sampleStore(120))
	got := canonicalize(t, c, want)

	if !proto.Equal(want, got) {
		t.Fatal("a canonical message did not survive a round trip unchanged")
	}
}

// assertSameElements compares two element lists as multisets, by their
// canonical bytes.
func assertSameElements[T proto.Message](t *testing.T, want, got []T) {
	t.Helper()

	count := func(list []T) map[string]int {
		out := map[string]int{}
		for _, e := range list {
			b, err := CanonicalBytes(e)
			if err != nil {
				t.Fatalf("CanonicalBytes: %v", err)
			}
			out[string(b)]++
		}
		return out
	}

	a, b := count(want), count(got)
	if len(a) != len(b) {
		t.Fatalf("round trip returned %d distinct elements, want %d", len(b), len(a))
	}
	for k, n := range a {
		if b[k] != n {
			t.Fatalf("round trip returned an element %d times, want %d", b[k], n)
		}
	}
}

// TestMaterializeIsAHomomorphism is the law that lets a writer append without
// reading the store.
//
// Materializing two messages separately and merging the results keywise gives
// the same items as merging the messages first. So a new element materializes
// on its own into exactly the item it belongs in, and adding it is a local
// operation on that one item.
func TestMaterializeIsAHomomorphism(t *testing.T) {
	c := compiledTestLayout(t)

	a := sampleStore(20)
	b := &testpb.Store{
		Records: sampleStore(45).Records[20:],
		Accounts: []*testpb.Account{
			{Id: "acc-9", Label: "late arrival"},
		},
	}

	// Left: merge the messages, then materialize.
	merged := proto.Clone(a).(*testpb.Store)
	proto.Merge(merged, b)
	left, err := c.Materialize(merged)
	if err != nil {
		t.Fatalf("Materialize(a merged with b): %v", err)
	}

	// Right: materialize each, then merge keywise.
	itemsA, err := c.Materialize(a)
	if err != nil {
		t.Fatalf("Materialize(a): %v", err)
	}
	itemsB, err := c.Materialize(b)
	if err != nil {
		t.Fatalf("Materialize(b): %v", err)
	}
	right := mergeKeywise(t, c, itemsA, itemsB)

	assertSameItems(t, c, left, right)
}

// TestMaterializeIgnoresInputOrder is what makes a split field a multiset.
//
// Two clients that assembled the same data in different orders must produce
// identical items, or they produce different chunk names for the same data and
// nothing deduplicates.
func TestMaterializeIgnoresInputOrder(t *testing.T) {
	c := compiledTestLayout(t)

	ordered := sampleStore(60)

	shuffled := proto.Clone(ordered).(*testpb.Store)
	rng := rand.New(rand.NewSource(1))
	rng.Shuffle(len(shuffled.Records), func(i, j int) {
		shuffled.Records[i], shuffled.Records[j] = shuffled.Records[j], shuffled.Records[i]
	})
	rng.Shuffle(len(shuffled.Accounts), func(i, j int) {
		shuffled.Accounts[i], shuffled.Accounts[j] = shuffled.Accounts[j], shuffled.Accounts[i]
	})

	a, err := c.Materialize(ordered)
	if err != nil {
		t.Fatalf("Materialize(ordered): %v", err)
	}
	b, err := c.Materialize(shuffled)
	if err != nil {
		t.Fatalf("Materialize(shuffled): %v", err)
	}
	assertSameItems(t, c, a, b)
}

// TestMaterializeProducesStableChunkNames joins the two halves: the same data
// always names the same chunks, which is what deduplication, incremental fetch
// and cache validity all depend on.
func TestMaterializeProducesStableChunkNames(t *testing.T) {
	c := compiledTestLayout(t)
	k := testKeyring(t)

	names := func(s *testpb.Store) map[ItemKey]ChunkID {
		items, err := c.Materialize(s)
		if err != nil {
			t.Fatalf("Materialize: %v", err)
		}
		out := map[ItemKey]ChunkID{}
		for _, it := range items {
			b, err := CanonicalBytes(it.Fragment)
			if err != nil {
				t.Fatalf("CanonicalBytes: %v", err)
			}
			out[it.Key] = k.ChunkID(RoleItem, string(it.Key), b)
		}
		return out
	}

	first := names(sampleStore(40))
	second := names(sampleStore(40))

	if len(first) != len(second) {
		t.Fatalf("the same store produced %d items and then %d", len(first), len(second))
	}
	for key, id := range first {
		if second[key] != id {
			t.Fatalf("item %q was named %s and then %s", key, id, second[key])
		}
	}
}

// TestChangingOneItemLeavesTheRestAlone is the property that makes a commit
// cost the change rather than the whole message.
func TestChangingOneItemLeavesTheRestAlone(t *testing.T) {
	c := compiledTestLayout(t)

	before := sampleStore(60)
	after := proto.Clone(before).(*testpb.Store)
	after.Records = append(after.Records, &testpb.Record{
		Id:    "rec-new",
		Group: "alpha",
		Date:  &testpb.Date{Year: 2026, Month: 1, Day: 5},
	})

	itemsBefore, err := c.Materialize(before)
	if err != nil {
		t.Fatalf("Materialize(before): %v", err)
	}
	itemsAfter, err := c.Materialize(after)
	if err != nil {
		t.Fatalf("Materialize(after): %v", err)
	}

	changed := 0
	byKey := map[ItemKey][]byte{}
	for _, it := range itemsBefore {
		b, err := CanonicalBytes(it.Fragment)
		if err != nil {
			t.Fatalf("CanonicalBytes: %v", err)
		}
		byKey[it.Key] = b
	}
	for _, it := range itemsAfter {
		b, err := CanonicalBytes(it.Fragment)
		if err != nil {
			t.Fatalf("CanonicalBytes: %v", err)
		}
		if old, ok := byKey[it.Key]; !ok || string(old) != string(b) {
			changed++
		}
	}
	if changed != 1 {
		t.Fatalf("adding one record changed %d items, want 1", changed)
	}
}

func TestItemKeysAreWhatTheyLookLike(t *testing.T) {
	c := compiledTestLayout(t)

	key, err := c.ItemKeyFor("records", &testpb.Record{
		Id:    "x",
		Group: "alpha",
		Date:  &testpb.Date{Year: 2026, Month: 8, Day: 14},
	})
	if err != nil {
		t.Fatalf("ItemKeyFor: %v", err)
	}
	if want := ItemKey("records/alpha/2026/08"); key != want {
		t.Fatalf("ItemKeyFor = %q, want %q", key, want)
	}
}

func TestItemKeySegmentsAreEscaped(t *testing.T) {
	// A shard value containing a separator would create a directory nobody
	// declared, and two different values could name one item.
	c := compiledTestLayout(t)

	withSlash, err := c.ItemKeyFor("records", &testpb.Record{
		Group: "a/b",
		Date:  &testpb.Date{Year: 2026, Month: 8, Day: 1},
	})
	if err != nil {
		t.Fatalf("ItemKeyFor: %v", err)
	}
	plain, err := c.ItemKeyFor("records", &testpb.Record{
		Group: "a%2Fb",
		Date:  &testpb.Date{Year: 2026, Month: 8, Day: 1},
	})
	if err != nil {
		t.Fatalf("ItemKeyFor: %v", err)
	}
	if withSlash == plain {
		t.Fatal("two different group values produced the same item key")
	}
	if withSlash != "records/a%2Fb/2026/08" {
		t.Fatalf("ItemKeyFor = %q, want the separator escaped", withSlash)
	}
}

func TestRootItemHoldsWhatNoRuleMentions(t *testing.T) {
	c := compiledTestLayout(t)

	items, err := c.Materialize(sampleStore(5))
	if err != nil {
		t.Fatalf("Materialize: %v", err)
	}

	var root proto.Message
	for _, it := range items {
		if it.Key == RootItemKey {
			root = it.Fragment
		}
	}
	if root == nil {
		t.Fatal("no root item")
	}

	s := root.(*testpb.Store)
	if s.GetName() != "a store" || s.GetRevision() != 7 || s.GetManifest().GetSchemaVersion() != 1 {
		t.Fatalf("the root item lost fields no rule mentions: %v", s)
	}
	if len(s.GetRecords()) != 0 || len(s.GetAccounts()) != 0 {
		t.Fatal("the root item kept split fields")
	}
}

func TestEmptyStoreProducesNoItems(t *testing.T) {
	c := compiledTestLayout(t)

	items, err := c.Materialize(&testpb.Store{})
	if err != nil {
		t.Fatalf("Materialize: %v", err)
	}
	if len(items) != 0 {
		t.Fatalf("an empty store produced %d items, want 0", len(items))
	}
}

func TestMaterializeRefusesTheWrongMessage(t *testing.T) {
	c := compiledTestLayout(t)
	if _, err := c.Materialize(&testpb.Record{Id: "x"}); err == nil {
		t.Fatal("materializing the wrong message type was accepted")
	}
}

func mergeKeywise(t *testing.T, c *Compiled, groups ...[]Item) []Item {
	t.Helper()

	byKey := map[ItemKey]proto.Message{}
	for _, g := range groups {
		for _, it := range g {
			if existing, ok := byKey[it.Key]; ok {
				proto.Merge(existing, it.Fragment)
				continue
			}
			byKey[it.Key] = proto.Clone(it.Fragment)
		}
	}

	// Keywise merge concatenates within an item, so the result has to be put
	// back into canonical order the way Materialize would have left it. This is
	// the canonicalization the homomorphism holds up to.
	out := make([]Item, 0, len(byKey))
	for k, frag := range byKey {
		normalized, err := c.Materialize(frag)
		if err != nil {
			t.Fatalf("normalizing item %q: %v", k, err)
		}
		for _, it := range normalized {
			if it.Key == k {
				out = append(out, it)
			}
		}
	}
	return out
}

func assertSameItems(t *testing.T, c *Compiled, a, b []Item) {
	t.Helper()

	index := func(items []Item) map[ItemKey]string {
		out := map[ItemKey]string{}
		for _, it := range items {
			encoded, err := CanonicalBytes(it.Fragment)
			if err != nil {
				t.Fatalf("CanonicalBytes for %q: %v", it.Key, err)
			}
			out[it.Key] = string(encoded)
		}
		return out
	}

	ia, ib := index(a), index(b)
	for k, va := range ia {
		vb, ok := ib[k]
		if !ok {
			t.Fatalf("item %q is missing from the second set", k)
		}
		if va != vb {
			t.Fatalf("item %q differs between the two sets", k)
		}
	}
	for k := range ib {
		if _, ok := ia[k]; !ok {
			t.Fatalf("item %q is missing from the first set", k)
		}
	}
}
