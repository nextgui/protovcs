package protovcs_test

import (
	"context"
	"sync"
	"testing"

	protovcs "github.com/nextgui/protovcs"
	"github.com/nextgui/protovcs/memtransport"
	testpb "github.com/nextgui/protovcs/proto/protovcs/testpb"
)

// TestAFullReadHintsItsHeadOnce checks that a store configured for pack hints
// sends one after a full read of that branch's head, names every chunk the head
// reaches, and does not send the same head twice.
func TestAFullReadHintsItsHeadOnce(t *testing.T) {
	ctx := context.Background()
	tr := memtransport.New()
	writer := newStore(t, tr, keyring(t))
	if _, err := writer.Branch("main").Write(ctx, meta("seed"), &testpb.Store{
		Manifest: &testpb.Manifest{SchemaVersion: 1},
		Records:  records(20),
	}); err != nil {
		t.Fatalf("Write: %v", err)
	}

	c, err := protovcs.CompileLayout(layout(), (&testpb.Store{}).ProtoReflect().Descriptor())
	if err != nil {
		t.Fatalf("CompileLayout: %v", err)
	}
	reader, err := protovcs.NewBuilder().WithTransport(tr).WithSealer(keyring(t)).
		WithLayout(c).WithPackHints("main").Build()
	if err != nil {
		t.Fatalf("Build: %v", err)
	}

	for i := 0; i < 2; i++ {
		if _, err := reader.Branch("main").Read(ctx); err != nil {
			t.Fatalf("Read: %v", err)
		}
	}
	commit, ids, hints := tr.LastPackHint()
	if hints != 1 {
		t.Fatalf("two reads of one head sent %d hints, want 1", hints)
	}

	head, _, err := reader.Branch("main").Head(ctx)
	if err != nil {
		t.Fatalf("Head: %v", err)
	}
	if got, _ := protovcs.CommitIDFromBytes(head.GetId()); got != commit {
		t.Fatalf("the hint named %s, and the head is %s", commit, got)
	}
	want, err := reader.ReachableChunks(ctx, head)
	if err != nil {
		t.Fatalf("ReachableChunks: %v", err)
	}
	if len(ids) != len(want) {
		t.Fatalf("the hint named %d chunks, and the head reaches %d", len(ids), len(want))
	}

	// Without the option, nothing is sent.
	plain := newStore(t, tr, keyring(t))
	if _, err := plain.Branch("main").Read(ctx); err != nil {
		t.Fatalf("Read: %v", err)
	}
	if _, _, after := tr.LastPackHint(); after != 1 {
		t.Fatalf("a store without pack hints sent one")
	}
}

// TestConcurrentReadsOfOneHeadSendOneHint checks that reads of the same head
// finishing together send a single hint between them.
func TestConcurrentReadsOfOneHeadSendOneHint(t *testing.T) {
	ctx := context.Background()
	tr := memtransport.New()
	if _, err := newStore(t, tr, keyring(t)).Branch("main").Write(ctx, meta("seed"), &testpb.Store{
		Records: records(50),
	}); err != nil {
		t.Fatalf("Write: %v", err)
	}

	c, err := protovcs.CompileLayout(layout(), (&testpb.Store{}).ProtoReflect().Descriptor())
	if err != nil {
		t.Fatalf("CompileLayout: %v", err)
	}
	reader, err := protovcs.NewBuilder().WithTransport(tr).WithSealer(keyring(t)).
		WithLayout(c).WithPackHints("main").Build()
	if err != nil {
		t.Fatalf("Build: %v", err)
	}

	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := reader.Branch("main").Read(ctx); err != nil {
				t.Errorf("Read: %v", err)
			}
		}()
	}
	wg.Wait()
	if _, _, hints := tr.LastPackHint(); hints != 1 {
		t.Fatalf("eight reads of one head sent %d hints, want 1", hints)
	}
}
