package protovcs_test

import (
	"context"
	"fmt"
	"testing"

	"google.golang.org/protobuf/proto"

	protovcs "github.com/nextgui/protovcs"
	"github.com/nextgui/protovcs/memtransport"
	testpb "github.com/nextgui/protovcs/proto/protovcs/testpb"
	gen "github.com/nextgui/protovcs/proto/protovcs/testpb/teststorelayout"
)

// A store's history is the only record of why it looks the way it does, and
// until Log existed nothing could read it: the metadata went in and no API
// brought it back. These tests are about the way out.

func TestLogReturnsWhatWasWritten(t *testing.T) {
	ctx := context.Background()
	s := newStore(t, memtransport.New(), keyring(t))

	want := protovcs.CommitMeta{
		Title:       "import from the bank",
		Description: "three months of statements, deduplicated against what was there",
		Authors: []protovcs.Author{
			{Name: "ada", Email: "ada@example.com", Role: "author"},
			{Name: "importer", Email: "svc@example.com", Role: "service"},
		},
		Client: "teststore/1.2.3",
		Labels: map[string]string{"source": "csv", "batch": "17"},
	}
	if _, err := s.Branch("main").Write(ctx, want, &testpb.Store{
		Manifest: &testpb.Manifest{SchemaVersion: 1},
		Records:  records(10),
	}); err != nil {
		t.Fatalf("Write: %v", err)
	}

	entries, err := s.Branch("main").Log(ctx, protovcs.LogOptions{})
	if err != nil {
		t.Fatalf("Log: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("Log returned %d entries, want 1", len(entries))
	}

	got := entries[0]
	switch {
	case got.Title != want.Title:
		t.Errorf("title is %q, want %q", got.Title, want.Title)
	case got.Description != want.Description:
		t.Errorf("description is %q, want %q", got.Description, want.Description)
	case got.Client != want.Client:
		t.Errorf("client is %q, want %q", got.Client, want.Client)
	case len(got.Authors) != 2:
		t.Errorf("got %d authors, want 2", len(got.Authors))
	case got.Authors[1].Role != "service":
		t.Errorf("the second author's role is %q, want \"service\"", got.Authors[1].Role)
	case got.Labels["source"] != "csv" || got.Labels["batch"] != "17":
		t.Errorf("labels are %v", got.Labels)
	case got.Branch != "main":
		t.Errorf("branch is %q, want \"main\"", got.Branch)
	case got.Time.IsZero():
		t.Error("the commit records no time")
	}
}

// TestLogIsNewestFirst is the order a history is read in, and the order the
// parent chain has to be walked to produce.
func TestLogIsNewestFirst(t *testing.T) {
	ctx := context.Background()
	s := newStore(t, memtransport.New(), keyring(t))

	for i := 0; i < 5; i++ {
		if _, err := s.Branch("main").Change(ctx, meta(fmt.Sprintf("commit %d", i)),
			func(_ context.Context, m proto.Message) error {
				st := m.(*testpb.Store)
				st.Records = append(st.Records, &testpb.Record{
					Id:    fmt.Sprintf("rec-%d", i),
					Group: "alpha",
					Date:  &testpb.Date{Year: 2026, Month: 1, Day: 1 + int32(i)},
				})
				return nil
			}); err != nil {
			t.Fatalf("Change %d: %v", i, err)
		}
	}

	entries, err := s.Branch("main").Log(ctx, protovcs.LogOptions{})
	if err != nil {
		t.Fatalf("Log: %v", err)
	}
	if len(entries) != 5 {
		t.Fatalf("Log returned %d entries, want 5", len(entries))
	}
	for i, e := range entries {
		if want := fmt.Sprintf("commit %d", 4-i); e.Title != want {
			t.Fatalf("entry %d is %q, want %q", i, e.Title, want)
		}
	}

	// Each entry names the one after it, which is what makes the walk possible
	// and what compare-and-swap on the ref is there to guarantee.
	for i := 0; i < len(entries)-1; i++ {
		if len(entries[i].Parents) != 1 || entries[i].Parents[0] != entries[i+1].ID {
			t.Fatalf("entry %d does not name entry %d as its parent", i, i+1)
		}
	}
	if len(entries[len(entries)-1].Parents) != 0 {
		t.Fatal("the first commit has a parent")
	}
}

func TestLogStopsAtLimitAndAtUntil(t *testing.T) {
	ctx := context.Background()
	s := newStore(t, memtransport.New(), keyring(t))

	var ids []protovcs.CommitID
	for i := 0; i < 6; i++ {
		id, err := s.Branch("main").Change(ctx, meta(fmt.Sprintf("commit %d", i)),
			func(_ context.Context, m proto.Message) error {
				st := m.(*testpb.Store)
				st.Records = append(st.Records, &testpb.Record{
					Id: fmt.Sprintf("rec-%d", i), Group: "alpha",
					Date: &testpb.Date{Year: 2026, Month: 2, Day: 1 + int32(i)},
				})
				return nil
			})
		if err != nil {
			t.Fatalf("Change %d: %v", i, err)
		}
		ids = append(ids, id)
	}

	entries, err := s.Branch("main").Log(ctx, protovcs.LogOptions{Limit: 2})
	if err != nil {
		t.Fatalf("Log: %v", err)
	}
	if len(entries) != 2 {
		t.Fatalf("a limit of 2 returned %d entries", len(entries))
	}

	// Until is exclusive: "what has happened since I last looked" should not
	// include the thing that was already looked at.
	entries, err = s.Branch("main").Log(ctx, protovcs.LogOptions{Until: ids[3]})
	if err != nil {
		t.Fatalf("Log: %v", err)
	}
	if len(entries) != 2 {
		t.Fatalf("Until returned %d entries, want the 2 written after it", len(entries))
	}
	for _, e := range entries {
		if e.ID == ids[3] {
			t.Fatal("Until included the commit it was told to stop at")
		}
	}
}

// TestLogOnAnUnwrittenBranchIsEmpty is what a history view asks for before
// anything has been written. Nothing has happened, which is a list of nothing
// rather than a failure.
func TestLogOnAnUnwrittenBranchIsEmpty(t *testing.T) {
	s := newStore(t, memtransport.New(), keyring(t))

	entries, err := s.Branch("main").Log(context.Background(), protovcs.LogOptions{})
	if err != nil {
		t.Fatalf("Log on a branch with no commits: %v", err)
	}
	if len(entries) != 0 {
		t.Fatalf("an unwritten branch has %d entries", len(entries))
	}
}

// TestDetailsRoundTripTyped is the point of declaring a commit_message: the
// application's own record comes back as its own type rather than as bytes
// somebody has to know how to parse.
func TestDetailsRoundTripTyped(t *testing.T) {
	ctx := context.Background()
	s, err := gen.Open(protovcs.NewBuilder().
		WithTransport(memtransport.New()).WithSealer(keyring(t)))
	if err != nil {
		t.Fatalf("gen.Open: %v", err)
	}

	want := &testpb.Change{
		Source:        "import",
		RecordsAdded:  43,
		TouchedGroups: []string{"alpha", "beta"},
	}
	if _, err := s.Branch("main").Write(ctx, gen.Meta{
		Title:   "imported 43 records",
		Details: want,
	}, &testpb.Store{
		Manifest: &testpb.Manifest{SchemaVersion: 1},
		Records:  records(43),
	}); err != nil {
		t.Fatalf("Write: %v", err)
	}

	entries, err := s.Branch("main").Log(ctx, protovcs.LogOptions{})
	if err != nil {
		t.Fatalf("Log: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("Log returned %d entries", len(entries))
	}
	if !proto.Equal(entries[0].Details, want) {
		t.Fatalf("details came back as %v, want %v", entries[0].Details, want)
	}
}

// TestDetailsOpenWithoutGeneratedCode is why details are bytes under a declared
// type rather than a google.protobuf.Any.
//
// The reader here has the store and no generated code for the message. It
// resolves the type against the descriptors the store itself carries, which is
// what keeps a history readable after the Go type is deleted from the codebase.
func TestDetailsOpenWithoutGeneratedCode(t *testing.T) {
	ctx := context.Background()
	tr := memtransport.New()

	typed, err := gen.Open(protovcs.NewBuilder().WithTransport(tr).WithSealer(keyring(t)))
	if err != nil {
		t.Fatalf("gen.Open: %v", err)
	}
	if _, err := typed.Branch("main").Write(ctx, gen.Meta{
		Title:   "imported",
		Details: &testpb.Change{Source: "import", RecordsAdded: 7},
	}, &testpb.Store{
		Manifest: &testpb.Manifest{SchemaVersion: 1},
		Records:  records(7),
	}); err != nil {
		t.Fatalf("Write: %v", err)
	}

	// A store whose own layout declares no commit message at all. It has to
	// read the type out of the commit's schema chunk or it cannot read it.
	dynamic := newStore(t, tr, keyring(t))
	head, _, err := dynamic.Branch("main").Head(ctx)
	if err != nil {
		t.Fatalf("Head: %v", err)
	}
	entries, err := dynamic.Branch("main").Log(ctx, protovcs.LogOptions{})
	if err != nil {
		t.Fatalf("Log: %v", err)
	}
	msg, err := dynamic.OpenDetails(ctx, head, entries[0])
	if err != nil {
		t.Fatalf("OpenDetails: %v", err)
	}

	fields := msg.ProtoReflect().Descriptor().Fields()
	source := msg.ProtoReflect().Get(fields.ByName("source")).String()
	added := msg.ProtoReflect().Get(fields.ByName("records_added")).Int()
	if source != "import" || added != 7 {
		t.Fatalf("a dynamic reader saw source %q and %d records", source, added)
	}
}

// TestDetailsTheLayoutDidNotDeclareAreRefused is the check that keeps a history
// readable by anybody but its writer.
//
// The descriptors travel in the SchemaChunk because the layout named the type.
// A commit carrying something else arrives somewhere with no way to resolve it,
// and the write is the last point at which that is still fixable.
func TestDetailsTheLayoutDidNotDeclareAreRefused(t *testing.T) {
	ctx := context.Background()

	// A layout declaring no commit message at all.
	plain := newStore(t, memtransport.New(), keyring(t))
	_, err := plain.Branch("main").Write(ctx, protovcs.CommitMeta{
		Title:   "with details",
		Details: &testpb.Change{Source: "import"},
	}, &testpb.Store{Manifest: &testpb.Manifest{SchemaVersion: 1}})
	if err == nil {
		t.Fatal("a store whose layout declares no commit_message accepted details")
	}

	// A layout declaring one, handed a different message.
	declared := newStoreWithCommitMessage(t, memtransport.New())
	_, err = declared.Branch("main").Write(ctx, protovcs.CommitMeta{
		Title:   "wrong type",
		Details: &testpb.Manifest{SchemaVersion: 1},
	}, &testpb.Store{Manifest: &testpb.Manifest{SchemaVersion: 1}})
	if err == nil {
		t.Fatal("a store accepted details of a type its layout did not declare")
	}
}

// TestTheServerSeesNoCommitText is the property the whole design rests on,
// asserted against the record the server actually holds.
func TestTheServerSeesNoCommitText(t *testing.T) {
	ctx := context.Background()
	tr := memtransport.New()
	s := newStore(t, tr, keyring(t))

	const secret = "sold the house to pay for the divorce"
	if _, err := s.Branch("main").Write(ctx, protovcs.CommitMeta{
		Title:   secret,
		Authors: []protovcs.Author{{Name: "ada", Email: "ada@example.com"}},
		Labels:  map[string]string{"reason": secret},
	}, &testpb.Store{
		Manifest: &testpb.Manifest{SchemaVersion: 1},
		Records:  records(3),
	}); err != nil {
		t.Fatalf("Write: %v", err)
	}

	head, _, err := s.Branch("main").Head(ctx)
	if err != nil {
		t.Fatalf("Head: %v", err)
	}
	raw, err := proto.Marshal(head)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	for _, plaintext := range []string{secret, "ada", "ada@example.com", "main"} {
		if bytesContain(raw, plaintext) {
			t.Fatalf("the stored commit record contains %q in the clear", plaintext)
		}
	}
}

func bytesContain(haystack []byte, needle string) bool {
	return len(needle) > 0 && indexOf(haystack, []byte(needle)) >= 0
}

func indexOf(haystack, needle []byte) int {
	for i := 0; i+len(needle) <= len(haystack); i++ {
		match := true
		for j := range needle {
			if haystack[i+j] != needle[j] {
				match = false
				break
			}
		}
		if match {
			return i
		}
	}
	return -1
}
