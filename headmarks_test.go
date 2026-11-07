package protovcs_test

import (
	"context"
	"errors"
	"fmt"
	"testing"

	protovcs "github.com/nextgui/protovcs"
	"github.com/nextgui/protovcs/memtransport"
	testpb "github.com/nextgui/protovcs/proto/protovcs/testpb"
)

// newStoreWithMarks is newStore with marks kept in marks.
func newStoreWithMarks(t *testing.T, tr protovcs.Transport, marks protovcs.HeadMarks) *protovcs.Store {
	t.Helper()
	c, err := protovcs.CompileLayout(layout(), (&testpb.Store{}).ProtoReflect().Descriptor())
	if err != nil {
		t.Fatalf("CompileLayout: %v", err)
	}
	s, err := protovcs.NewBuilder().WithTransport(tr).WithSealer(keyring(t)).WithLayout(c).
		WithHeadMarks(marks).Build()
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	return s
}

// writeN writes n versions to main and returns their commits, oldest first.
func writeN(t *testing.T, tr protovcs.Transport, n int) []protovcs.CommitID {
	t.Helper()
	writer := newStore(t, tr, keyring(t)).Branch("main")
	var heads []protovcs.CommitID
	for i := 1; i <= n; i++ {
		id, err := writer.Write(context.Background(), meta(fmt.Sprintf("write %d", i)), &testpb.Store{
			Manifest: &testpb.Manifest{SchemaVersion: 1},
			Records:  records(4 * i),
		})
		if err != nil {
			t.Fatalf("Write: %v", err)
		}
		heads = append(heads, id)
	}
	return heads
}

// TestARollbackIsRefusedAfterAReload covers a server serving an old head to a
// Store that has never read this branch, as after a page reload. The marks
// another Store left behind are what catch it.
func TestARollbackIsRefusedAfterAReload(t *testing.T) {
	ctx := context.Background()
	tr := memtransport.New()
	heads := writeN(t, tr, 3)
	marks := protovcs.NewMemoryHeadMarks()

	before := newStoreWithMarks(t, tr, marks).Branch("main")
	if _, err := before.Read(ctx); err != nil {
		t.Fatalf("Read: %v", err)
	}

	tr.SetRef(keyring(t).RefName("main"), heads[0])

	after := newStoreWithMarks(t, tr, marks).Branch("main")
	if _, err := after.Read(ctx); !errors.Is(err, protovcs.ErrWentBackwards) {
		t.Fatalf("reading a rolled-back store after a reload returned %v, want ErrWentBackwards", err)
	}

	// Without the marks a new Store has nothing to compare against.
	if _, err := newStore(t, tr, keyring(t)).Branch("main").Read(ctx); err != nil {
		t.Fatalf("a Store without marks refused the old head: %v", err)
	}
}

// TestAnOlderReadDoesNotLowerTheMark has a Store that last saw an older head
// read after another Store recorded a newer one. The record keeps the newer
// head, and the older Store adopts it.
func TestAnOlderReadDoesNotLowerTheMark(t *testing.T) {
	ctx := context.Background()
	tr := memtransport.New()
	ref := keyring(t).RefName("main")
	heads := writeN(t, tr, 3)
	marks := protovcs.NewMemoryHeadMarks()

	// behind sees the first head; ahead then sees the third.
	tr.SetRef(ref, heads[0])
	behind := newStoreWithMarks(t, tr, marks)
	if _, err := behind.Branch("main").Read(ctx); err != nil {
		t.Fatalf("Read: %v", err)
	}
	tr.SetRef(ref, heads[2])
	if _, err := newStoreWithMarks(t, tr, marks).Branch("main").Read(ctx); err != nil {
		t.Fatalf("Read: %v", err)
	}

	// The second head is progress for behind, and a rollback for the record.
	tr.SetRef(ref, heads[1])
	if _, err := behind.Branch("main").Read(ctx); err != nil {
		t.Fatalf("Read: %v", err)
	}
	if got, _ := behind.KnownHead("main"); got != heads[2] {
		t.Fatalf("behind knows head %s, want the newer recorded head %s", got, heads[2])
	}
	loaded, err := marks.Load(ctx)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if loaded["main"] != heads[2] {
		t.Fatalf("the record holds %s, want %s", loaded["main"], heads[2])
	}

	if _, err := newStoreWithMarks(t, tr, marks).Branch("main").Read(ctx); !errors.Is(err, protovcs.ErrWentBackwards) {
		t.Fatalf("reading the second head after a reload returned %v, want ErrWentBackwards", err)
	}
}

// TestAWriteIsRecorded checks that a Store's own commit reaches the marks, so
// a reload refuses the head before it.
func TestAWriteIsRecorded(t *testing.T) {
	ctx := context.Background()
	tr := memtransport.New()
	marks := protovcs.NewMemoryHeadMarks()
	main := newStoreWithMarks(t, tr, marks).Branch("main")

	var heads []protovcs.CommitID
	for i := 1; i <= 2; i++ {
		id, err := main.Write(ctx, meta(fmt.Sprintf("write %d", i)), &testpb.Store{
			Manifest: &testpb.Manifest{SchemaVersion: 1},
			Records:  records(4 * i),
		})
		if err != nil {
			t.Fatalf("Write: %v", err)
		}
		heads = append(heads, id)
	}

	tr.SetRef(keyring(t).RefName("main"), heads[0])
	if _, err := newStoreWithMarks(t, tr, marks).Branch("main").Read(ctx); !errors.Is(err, protovcs.ErrWentBackwards) {
		t.Fatalf("reading behind a recorded write returned %v, want ErrWentBackwards", err)
	}
}
