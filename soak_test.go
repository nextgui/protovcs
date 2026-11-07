package protovcs_test

import (
	"context"
	"fmt"
	"math/rand"
	"testing"

	protovcs "github.com/nextgui/protovcs"
	"github.com/nextgui/protovcs/memtransport"
	testpb "github.com/nextgui/protovcs/proto/protovcs/testpb"
	gen "github.com/nextgui/protovcs/proto/protovcs/testpb/teststorelayout"
)

// A store is written across many versions, through the generated package, with
// the operations mixed the way a real client mixes them. Every version is then
// read back and checked against what it held when it was written.
//
// The properties here are the ones that only fail after a store has some history
// on it. A version that stops reading correctly three commits later is not
// something a single-commit test can find, and it is the failure a vault cannot
// survive.

const (
	soakCommits = 60
	soakGroups  = 6
)

// soakModel is what the store should hold, kept alongside it so every version
// can be checked against what it was.
type soakModel struct {
	amounts map[string]int64
}

func (m soakModel) clone() soakModel {
	out := soakModel{amounts: make(map[string]int64, len(m.amounts))}
	for k, v := range m.amounts {
		out.amounts[k] = v
	}
	return out
}

func TestASoakOfManyVersions(t *testing.T) {
	ctx := context.Background()
	tr := memtransport.New()

	// Through the generated package rather than a layout built in the test, so
	// the code a caller actually uses is what gets exercised.
	s, err := gen.Open(protovcs.NewBuilder().WithTransport(tr).WithSealer(keyring(t)))
	if err != nil {
		t.Fatalf("gen.Open: %v", err)
	}

	rng := rand.New(rand.NewSource(20260821))
	model := soakModel{amounts: map[string]int64{}}

	type version struct {
		commit protovcs.CommitID
		model  soakModel
	}
	var history []version

	next := 0
	for i := 0; i < soakCommits; i++ {
		switch i % 4 {
		case 0, 1:
			// Append a batch through the whole-message path.
			added := 20 + rng.Intn(30)
			if _, err := s.Branch("main").Change(ctx, genMeta(fmt.Sprintf("append %d", i)),
				func(_ context.Context, st *testpb.Store) error {
					for j := 0; j < added; j++ {
						r := soakRecord(next, rng)
						next++
						st.Records = append(st.Records, r)
						model.amounts[r.GetId()] = r.GetAmount()
					}
					return nil
				}); err != nil {
				t.Fatalf("append at %d: %v", i, err)
			}

		case 2:
			// Edit one shard through a partial checkout, which is the path a
			// real client uses and the one that goes through UpdateManifest.
			group := fmt.Sprintf("g%d", rng.Intn(soakGroups))
			view, err := s.Branch("main").CheckoutRecords(ctx, gen.RecordsByGroup(group))
			if err != nil {
				t.Fatalf("checkout at %d: %v", i, err)
			}
			items := view.Partial().Items()
			if len(items) == 0 {
				continue
			}
			victim := items[rng.Intn(len(items))]
			frag := victim.Fragment.(*testpb.Store)
			if len(frag.GetRecords()) == 0 {
				continue
			}
			target := frag.GetRecords()[rng.Intn(len(frag.GetRecords()))]
			target.Amount += 1000
			model.amounts[target.GetId()] = target.GetAmount()
			if err := view.Partial().Put(victim); err != nil {
				t.Fatalf("put at %d: %v", i, err)
			}
			if _, err := view.Commit(ctx, genMeta(fmt.Sprintf("edit %d", i))); err != nil {
				t.Fatalf("partial commit at %d: %v", i, err)
			}

		case 3:
			// Delete a record, so the manifest has to prune as well as grow.
			if len(model.amounts) == 0 {
				continue
			}
			var victimID string
			for id := range model.amounts {
				victimID = id
				break
			}
			if _, err := s.Branch("main").Change(ctx, genMeta(fmt.Sprintf("delete %d", i)),
				func(_ context.Context, st *testpb.Store) error {
					kept := st.Records[:0]
					for _, r := range st.GetRecords() {
						if r.GetId() != victimID {
							kept = append(kept, r)
						}
					}
					st.Records = kept
					return nil
				}); err != nil {
				t.Fatalf("delete at %d: %v", i, err)
			}
			delete(model.amounts, victimID)
		}

		head, _, err := s.Branch("main").Head(ctx)
		if err != nil {
			t.Fatalf("head at %d: %v", i, err)
		}
		id, err := protovcs.CommitIDFromBytes(head.GetId())
		if err != nil {
			t.Fatalf("CommitIDFromBytes: %v", err)
		}
		history = append(history, version{commit: id, model: model.clone()})
	}

	t.Logf("%d commits, %d records, %d chunks", len(history), len(model.amounts), tr.ChunkCount())

	if len(model.amounts) < 400 {
		t.Fatalf("the soak only reached %d records, which is not enough data to be a soak", len(model.amounts))
	}

	// Every version still reads what it held. This is the property a version
	// control system exists for, and the one a single-commit test cannot check.
	fresh := freshSoakClient(t, tr)
	for i, v := range history {
		commits, err := tr.GetCommits(ctx, v.commit, 1)
		if err != nil || len(commits) == 0 {
			t.Fatalf("commit %d: %v", i, err)
		}
		got, err := fresh.Unwrap().ReadCommit(ctx, commits[0])
		if err != nil {
			t.Fatalf("reading version %d: %v", i, err)
		}

		have := map[string]int64{}
		for _, r := range got.(*testpb.Store).GetRecords() {
			if _, dup := have[r.GetId()]; dup {
				t.Fatalf("version %d holds %s twice", i, r.GetId())
			}
			have[r.GetId()] = r.GetAmount()
		}
		if len(have) != len(v.model.amounts) {
			t.Fatalf("version %d holds %d records, held %d when written", i, len(have), len(v.model.amounts))
		}
		for id, amount := range v.model.amounts {
			if have[id] != amount {
				t.Fatalf("version %d: %s is %d, was %d when written", i, id, have[id], amount)
			}
		}
	}
}

// TestASoakDeduplicatesAcrossVersions is the cost side of the same run: sixty
// versions of a growing store must not cost sixty copies of it.
func TestASoakDeduplicatesAcrossVersions(t *testing.T) {
	ctx := context.Background()
	tr := memtransport.New()

	s, err := gen.Open(protovcs.NewBuilder().WithTransport(tr).WithSealer(keyring(t)))
	if err != nil {
		t.Fatalf("gen.Open: %v", err)
	}

	rng := rand.New(rand.NewSource(1))
	next := 0

	// One large write, then many small ones.
	if _, err := s.Branch("main").Change(ctx, genMeta("seed"), func(_ context.Context, st *testpb.Store) error {
		for j := 0; j < 600; j++ {
			st.Records = append(st.Records, soakRecord(next, rng))
			next++
		}
		return nil
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}
	afterSeed := tr.ChunkCount()

	for i := 0; i < 40; i++ {
		if _, err := s.Branch("main").Change(ctx, genMeta("one more"), func(_ context.Context, st *testpb.Store) error {
			st.Records = append(st.Records, soakRecord(next, rng))
			next++
			return nil
		}); err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
	}

	const commits = 40
	added := tr.ChunkCount() - afterSeed
	perCommit := float64(added) / commits
	t.Logf("%d chunks after the seed, %d added by %d single-record commits (%.1f each)",
		afterSeed, added, commits, perCommit)

	// What sharing between versions means: each commit costs its own item and
	// the nodes on that item's path, and nothing else. Without it a commit
	// would cost a copy of the store, so the figure to beat is the store's own
	// size.
	if perCommit > 12 {
		t.Fatalf("each one-record commit cost %.1f chunks;"+
			" a commit is meant to cost its item and the nodes above it", perCommit)
	}
	if float64(added) >= float64(afterSeed)*commits*0.5 {
		t.Fatalf("%d one-record commits added %d chunks to a store of %d;"+
			" versions are not sharing anything", commits, added, afterSeed)
	}
}

// TestGeneratedScopesReadWhatTheDynamicApiReads checks the generated package
// against the API it wraps, over a store with history rather than one commit.
func TestGeneratedScopesReadWhatTheDynamicApiReads(t *testing.T) {
	ctx := context.Background()
	tr := memtransport.New()

	s, err := gen.Open(protovcs.NewBuilder().WithTransport(tr).WithSealer(keyring(t)))
	if err != nil {
		t.Fatalf("gen.Open: %v", err)
	}

	rng := rand.New(rand.NewSource(3))
	next := 0
	for i := 0; i < 10; i++ {
		if _, err := s.Branch("main").Change(ctx, genMeta("batch"), func(_ context.Context, st *testpb.Store) error {
			for j := 0; j < 40; j++ {
				st.Records = append(st.Records, soakRecord(next, rng))
				next++
			}
			return nil
		}); err != nil {
			t.Fatalf("batch %d: %v", i, err)
		}
	}

	for g := 0; g < soakGroups; g++ {
		group := fmt.Sprintf("g%d", g)

		viaGenerated, err := s.Branch("main").CheckoutRecords(ctx, gen.RecordsByGroup(group))
		if err != nil {
			t.Fatalf("generated scope: %v", err)
		}
		viaDynamic, err := s.Unwrap().Branch("main").Checkout(ctx, protovcs.Prefix("records/"+group))
		if err != nil {
			t.Fatalf("dynamic scope: %v", err)
		}

		a, b := itemKeys(viaGenerated.Partial().Items()), itemKeys(viaDynamic.Items())
		if len(a) != len(b) {
			t.Fatalf("group %s: generated scope fetched %d items, dynamic fetched %d", group, len(a), len(b))
		}
		for i := range a {
			if a[i] != b[i] {
				t.Fatalf("group %s: generated scope fetched %q where dynamic fetched %q", group, a[i], b[i])
			}
		}
		for _, r := range viaGenerated.All() {
			if r.GetGroup() != group {
				t.Fatalf("the generated scope for %s returned a record in %s", group, r.GetGroup())
			}
		}
	}
}

func soakRecord(n int, rng *rand.Rand) *testpb.Record {
	return &testpb.Record{
		Id:     fmt.Sprintf("rec-%08d", n),
		Group:  fmt.Sprintf("g%d", n%soakGroups),
		Date:   &testpb.Date{Year: int32(2020 + n%5), Month: int32(1 + n%12), Day: int32(1 + n%28)},
		Amount: int64(rng.Intn(100000)),
		Tags:   []string{"a", "b"},
		Nested: &testpb.Nested{Note: "n", Values: []int64{int64(n)}},
	}
}

func itemKeys(items []protovcs.Item) []protovcs.ItemKey {
	out := make([]protovcs.ItemKey, 0, len(items))
	for _, it := range items {
		out = append(out, it.Key)
	}
	return out
}

func freshSoakClient(t *testing.T, tr protovcs.Transport) *gen.Store {
	t.Helper()
	s, err := gen.Open(protovcs.NewBuilder().WithTransport(tr).WithSealer(keyring(t)))
	if err != nil {
		t.Fatalf("gen.Open: %v", err)
	}
	return s
}
