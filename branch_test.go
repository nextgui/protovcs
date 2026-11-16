package protovcs_test

import (
	"context"
	"errors"
	"testing"

	protovcs "github.com/nextgui/protovcs"
	"github.com/nextgui/protovcs/memtransport"
	testpb "github.com/nextgui/protovcs/proto/protovcs/testpb"
)

func TestCreateBranchStartsAtTheGivenCommit(t *testing.T) {
	ctx := context.Background()
	s := newStore(t, memtransport.New(), keyring(t))

	at, err := s.Branch("main").Write(ctx, meta("main"), &testpb.Store{
		Manifest: &testpb.Manifest{SchemaVersion: 1},
		Records:  records(5),
	})
	if err != nil {
		t.Fatalf("Write main: %v", err)
	}
	if err := s.CreateBranch(ctx, "proposal", at); err != nil {
		t.Fatalf("CreateBranch: %v", err)
	}

	_, state, err := s.Branch("proposal").Head(ctx)
	if err != nil {
		t.Fatalf("Head: %v", err)
	}
	if state.Commit != at {
		t.Fatalf("proposal is at %s, want %s", state.Commit, at)
	}

	// Writing the new branch leaves main where it was.
	if _, err := s.Branch("proposal").Write(ctx, meta("proposal"), &testpb.Store{
		Manifest: &testpb.Manifest{SchemaVersion: 1},
		Records:  records(7),
	}); err != nil {
		t.Fatalf("Write proposal: %v", err)
	}
	main, err := s.Branch("main").Read(ctx)
	if err != nil {
		t.Fatalf("Read main: %v", err)
	}
	if n := len(main.(*testpb.Store).GetRecords()); n != 5 {
		t.Fatalf("main has %d records, want 5", n)
	}
}

func TestCreateBranchRefusesAnExistingName(t *testing.T) {
	ctx := context.Background()
	s := newStore(t, memtransport.New(), keyring(t))

	first, err := s.Branch("main").Write(ctx, meta("one"), &testpb.Store{Records: records(1)})
	if err != nil {
		t.Fatalf("Write: %v", err)
	}
	second, err := s.Branch("main").Write(ctx, meta("two"), &testpb.Store{Records: records(2)})
	if err != nil {
		t.Fatalf("Write: %v", err)
	}

	err = s.CreateBranch(ctx, "main", first)
	if !errors.Is(err, protovcs.ErrBranchExists) {
		t.Fatalf("CreateBranch over main: got %v, want ErrBranchExists", err)
	}
	_, state, err := s.Branch("main").Head(ctx)
	if err != nil {
		t.Fatalf("Head: %v", err)
	}
	if state.Commit != second {
		t.Fatalf("main moved to %s, want it left at %s", state.Commit, second)
	}
}

func TestDeleteRemovesOnlyThatBranch(t *testing.T) {
	ctx := context.Background()
	s := newStore(t, memtransport.New(), keyring(t))

	older, err := s.Branch("main").Write(ctx, meta("one"), &testpb.Store{Records: records(1)})
	if err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := s.CreateBranch(ctx, "proposal", older); err != nil {
		t.Fatalf("CreateBranch: %v", err)
	}
	if _, err := s.Branch("proposal").Write(ctx, meta("two"), &testpb.Store{Records: records(2)}); err != nil {
		t.Fatalf("Write proposal: %v", err)
	}

	if err := s.Branch("proposal").Delete(ctx); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if _, _, err := s.Branch("proposal").Head(ctx); !errors.Is(err, protovcs.ErrNoSuchRef) {
		t.Fatalf("Head after Delete: got %v, want ErrNoSuchRef", err)
	}
	if err := s.Branch("proposal").Delete(ctx); !errors.Is(err, protovcs.ErrNoSuchRef) {
		t.Fatalf("second Delete: got %v, want ErrNoSuchRef", err)
	}
	if _, err := s.Branch("main").Read(ctx); err != nil {
		t.Fatalf("Read main after deleting proposal: %v", err)
	}

	// The name can be used again, at a commit older than the one it was last
	// seen at, without being taken for a rollback.
	if err := s.CreateBranch(ctx, "proposal", older); err != nil {
		t.Fatalf("CreateBranch again: %v", err)
	}
	if _, _, err := s.Branch("proposal").Head(ctx); err != nil {
		t.Fatalf("Head after re-create: %v", err)
	}
}

func TestDeleteClearsTheHeadMarkForAnotherStore(t *testing.T) {
	ctx := context.Background()
	tr := memtransport.New()
	marks := protovcs.NewMemoryHeadMarks()

	first := newStoreWithMarks(t, tr, marks)
	older, err := first.Branch("main").Write(ctx, meta("one"), &testpb.Store{Records: records(1)})
	if err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := first.CreateBranch(ctx, "proposal", older); err != nil {
		t.Fatalf("CreateBranch: %v", err)
	}
	if _, err := first.Branch("proposal").Write(ctx, meta("two"), &testpb.Store{Records: records(2)}); err != nil {
		t.Fatalf("Write proposal: %v", err)
	}
	if err := first.Branch("proposal").Delete(ctx); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	// A store opened later reads the marks the first one left. The deleted
	// branch has none, so creating it again at the older commit is not a
	// rollback.
	second := newStoreWithMarks(t, tr, marks)
	if err := second.CreateBranch(ctx, "proposal", older); err != nil {
		t.Fatalf("CreateBranch in a second store: %v", err)
	}
	if _, _, err := second.Branch("proposal").Head(ctx); err != nil {
		t.Fatalf("Head in a second store: %v", err)
	}
}
