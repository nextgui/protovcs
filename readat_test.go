package protovcs_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	protovcs "github.com/nextgui/protovcs"
	"github.com/nextgui/protovcs/memtransport"
	testpb "github.com/nextgui/protovcs/proto/protovcs/testpb"
	pb "github.com/nextgui/protovcs/proto/protovcs/v1"
)

// countingTransport records how often a store reaches the server, so a test can
// say what a read costs rather than that it worked.
type countingTransport struct {
	protovcs.Transport

	listRefs   atomic.Int64
	getCommits atomic.Int64
	getChunks  atomic.Int64
}

func (c *countingTransport) ListRefs(ctx context.Context) ([]protovcs.RefState, error) {
	c.listRefs.Add(1)
	return c.Transport.ListRefs(ctx)
}

func (c *countingTransport) GetCommits(ctx context.Context, from protovcs.CommitID, depth uint32) ([]*pb.Commit, error) {
	c.getCommits.Add(1)
	return c.Transport.GetCommits(ctx, from, depth)
}

func (c *countingTransport) GetChunks(ctx context.Context, ids []protovcs.ChunkID) ([]protovcs.SealedChunk, error) {
	c.getChunks.Add(1)
	return c.Transport.GetChunks(ctx, ids)
}

// TestReadAtCostsNothingOnceTheCommitHasBeenSeen is the whole point of pinning:
// a screen producing several figures from one state of the store reads it once
// over the network and then not at all.
func TestReadAtCostsNothingOnceTheCommitHasBeenSeen(t *testing.T) {
	ctx := context.Background()
	counting := &countingTransport{Transport: memtransport.New()}
	s := newStore(t, counting, keyring(t))
	main := s.Branch("main")

	commit, err := main.Write(ctx, protovcs.CommitMeta{Title: "first"}, &testpb.Store{
		Name:    "a store",
		Records: []*testpb.Record{{Id: "rec-1", Group: "alpha", Date: &testpb.Date{Year: 2026, Month: 1, Day: 1}}},
	})
	if err != nil {
		t.Fatalf("Write: %v", err)
	}

	// From here, so the write's own requests are not counted against the read.
	beforeReads := counting.listRefs.Load()

	// The first read has to fetch the commit and the chunks under it.
	if _, err := s.ReadAt(ctx, commit); err != nil {
		t.Fatalf("first ReadAt: %v", err)
	}
	afterFirst := counting.getCommits.Load() + counting.getChunks.Load() + counting.listRefs.Load()

	// Every read after it is served from what the first one already holds.
	for i := 0; i < 5; i++ {
		if _, err := s.ReadAt(ctx, commit); err != nil {
			t.Fatalf("ReadAt %d: %v", i, err)
		}
	}
	afterMore := counting.getCommits.Load() + counting.getChunks.Load() + counting.listRefs.Load()

	if afterMore != afterFirst {
		t.Errorf("five more reads at the same commit cost %d further requests, wanted none",
			afterMore-afterFirst)
	}
	// And none of them asked where the branch points, which is what makes two
	// reads at one commit agree even if somebody writes in between.
	if got := counting.listRefs.Load() - beforeReads; got != 0 {
		t.Errorf("ReadAt asked where a branch points %d times; it must ask nobody", got)
	}
}

// A read at a commit is a read of that commit, whatever the branch does
// afterwards. This is the property a screen relies on to make its panels agree.
func TestReadAtIsUnaffectedByALaterWrite(t *testing.T) {
	ctx := context.Background()
	s := newStore(t, memtransport.New(), keyring(t))
	main := s.Branch("main")

	first, err := main.Write(ctx, protovcs.CommitMeta{Title: "first"}, &testpb.Store{Name: "before"})
	if err != nil {
		t.Fatalf("first Write: %v", err)
	}
	if _, err := main.Write(ctx, protovcs.CommitMeta{Title: "second"}, &testpb.Store{Name: "after"}); err != nil {
		t.Fatalf("second Write: %v", err)
	}

	got, err := s.ReadAt(ctx, first)
	if err != nil {
		t.Fatalf("ReadAt: %v", err)
	}
	if name := got.(*testpb.Store).GetName(); name != "before" {
		t.Errorf("reading at the first commit gave %q, so it followed the branch instead", name)
	}
}

// A commit the store no longer holds is reported rather than substituted. A
// long-lived page can name one that garbage collection has taken, and answering
// with a different state than the caller asked for would be undetectable
// downstream.
func TestReadAtRefusesACommitTheStoreDoesNotHold(t *testing.T) {
	ctx := context.Background()
	s := newStore(t, memtransport.New(), keyring(t))

	var missing protovcs.CommitID
	for i := range missing {
		missing[i] = byte(i + 1)
	}

	_, err := s.ReadAt(ctx, missing)
	if err == nil {
		t.Fatal("reading at a commit the store does not hold succeeded")
	}
	if !errors.Is(err, protovcs.ErrNoSuchCommit) {
		t.Errorf("error is %v, wanted ErrNoSuchCommit so a caller can tell it from damage", err)
	}
}

// KnownHead reports what the store remembers and asks nobody, so a caller can
// tell "nothing has told me yet" from "it is here".
func TestKnownHeadReportsWhatIsRememberedWithoutAsking(t *testing.T) {
	ctx := context.Background()
	counting := &countingTransport{Transport: memtransport.New()}
	s := newStore(t, counting, keyring(t))

	if _, ok := s.KnownHead("main"); ok {
		t.Error("a store that has heard nothing claimed to know where main points")
	}

	commit, err := s.Branch("main").Write(ctx, protovcs.CommitMeta{Title: "first"}, &testpb.Store{Name: "x"})
	if err != nil {
		t.Fatalf("Write: %v", err)
	}

	before := counting.listRefs.Load()
	got, ok := s.KnownHead("main")
	if !ok {
		t.Fatal("a store that has just written claimed not to know where main points")
	}
	if got != commit {
		t.Errorf("KnownHead is %s, the write made %s", got, commit)
	}
	if counting.listRefs.Load() != before {
		t.Error("KnownHead asked the server; it must report what is remembered")
	}
}

// TestAColdReadCostsOneRequestPerLevel checks that a full read fetches the
// manifest a level at a time rather than a node at a time.
//
// Two stores with the same depth are read cold: one with every record in one
// group, one with them spread over three. The wider tree has three times the
// nodes at each level below the groups, and costs the same number of requests.
func TestAColdReadCostsOneRequestPerLevel(t *testing.T) {
	ctx := context.Background()
	l := layout()
	l.Splits[0].By = []*pb.ShardKey{
		{Field: "group", Transform: pb.Transform_TRANSFORM_IDENTITY},
		{Field: "date", Transform: pb.Transform_TRANSFORM_MONTH},
		{Field: "date", Transform: pb.Transform_TRANSFORM_DAY},
	}
	c, err := protovcs.CompileLayout(l, (&testpb.Store{}).ProtoReflect().Descriptor())
	if err != nil {
		t.Fatalf("CompileLayout: %v", err)
	}
	open := func(tr protovcs.Transport) *protovcs.Store {
		s, err := protovcs.NewBuilder().WithTransport(tr).WithSealer(keyring(t)).WithLayout(c).Build()
		if err != nil {
			t.Fatalf("Build: %v", err)
		}
		return s
	}
	coldRead := func(recs []*testpb.Record) int64 {
		tr := memtransport.New()
		if _, err := open(tr).Branch("main").Write(ctx, meta("seed"), &testpb.Store{Records: recs}); err != nil {
			t.Fatalf("Write: %v", err)
		}
		counting := &countingTransport{Transport: tr}
		if _, err := open(counting).Branch("main").Read(ctx); err != nil {
			t.Fatalf("Read: %v", err)
		}
		return counting.getChunks.Load()
	}

	wide := records(360)
	narrow := records(360)
	for _, r := range narrow {
		r.Group = "alpha"
	}
	if w, n := coldRead(wide), coldRead(narrow); w != n {
		t.Fatalf("a cold read of three groups made %d GetChunks requests and of one group %d; want the same", w, n)
	}
}
