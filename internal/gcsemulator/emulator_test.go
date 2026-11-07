package gcsemulator_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"testing"

	"cloud.google.com/go/storage"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"

	"github.com/nextgui/protovcs/internal/gcsemulator"
)

// newClient starts an emulator and returns a GCS client pointed at it.
func newClient(t *testing.T) (*storage.Client, *gcsemulator.Server) {
	t.Helper()

	emu := gcsemulator.NewServer()
	t.Cleanup(emu.Close)

	client, err := storage.NewClient(context.Background(),
		option.WithEndpoint(emu.URL()), option.WithoutAuthentication())
	if err != nil {
		t.Fatalf("storage.NewClient: %v", err)
	}
	t.Cleanup(func() { client.Close() })

	return client, emu
}

// write puts one object with the given name and body.
func write(t *testing.T, client *storage.Client, bucket, name, body string) {
	t.Helper()

	w := client.Bucket(bucket).Object(name).NewWriter(context.Background())
	if _, err := io.WriteString(w, body); err != nil {
		t.Fatalf("write %s: %v", name, err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("close %s: %v", name, err)
	}
}

// list returns the names matching a query, in the order the emulator served
// them.
func list(t *testing.T, client *storage.Client, bucket string, q *storage.Query) []string {
	t.Helper()

	var got []string
	it := client.Bucket(bucket).Objects(context.Background(), q)
	for {
		attrs, err := it.Next()
		if errors.Is(err, iterator.Done) {
			break
		}
		if err != nil {
			t.Fatalf("iterate: %v", err)
		}
		got = append(got, attrs.Name)
	}
	return got
}

// TestListOffsetsBoundTheListing covers the startOffset and endOffset query
// parameters. GitService's retention reaper encodes a zero-padded timestamp in
// the object name and asks the server for the names below a cutoff, so an
// emulator that ignores the bound would let a broken cutoff pass unnoticed.
func TestListOffsetsBoundTheListing(t *testing.T) {
	client, _ := newClient(t)
	const bucket = "test-bucket"

	for _, n := range []string{"idle/00000000009", "idle/00000000010", "idle/00000000100"} {
		write(t, client, bucket, n, "x")
	}

	tests := []struct {
		name  string
		query *storage.Query
		want  []string
	}{
		{
			name:  "prefix only returns all three",
			query: &storage.Query{Prefix: "idle/"},
			want:  []string{"idle/00000000009", "idle/00000000010", "idle/00000000100"},
		},
		{
			name:  "endOffset is exclusive",
			query: &storage.Query{Prefix: "idle/", EndOffset: "idle/00000000010"},
			want:  []string{"idle/00000000009"},
		},
		{
			name:  "endOffset above the largest returns all",
			query: &storage.Query{Prefix: "idle/", EndOffset: "idle/00000000999"},
			want:  []string{"idle/00000000009", "idle/00000000010", "idle/00000000100"},
		},
		{
			name:  "startOffset is inclusive",
			query: &storage.Query{Prefix: "idle/", StartOffset: "idle/00000000010"},
			want:  []string{"idle/00000000010", "idle/00000000100"},
		},
		{
			name: "both bounds give a half-open range",
			query: &storage.Query{
				Prefix:      "idle/",
				StartOffset: "idle/00000000010",
				EndOffset:   "idle/00000000100",
			},
			want: []string{"idle/00000000010"},
		},
		{
			name:  "empty range returns nothing",
			query: &storage.Query{Prefix: "idle/", EndOffset: "idle/00000000000"},
			want:  nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := list(t, client, bucket, tt.query)
			if fmt.Sprint(got) != fmt.Sprint(tt.want) {
				t.Errorf("list = %v, want %v", got, tt.want)
			}
		})
	}
}

// TestListIsLexicographic pins the ordering the reaper's key layout depends on.
// GCS lists in lexicographic order; a listing in Go map order would make the
// reaper's results vary between runs.
func TestListIsLexicographic(t *testing.T) {
	client, _ := newClient(t)
	const bucket = "test-bucket"

	for _, n := range []string{"c", "a", "b"} {
		write(t, client, bucket, n, "x")
	}

	got := list(t, client, bucket, nil)
	want := []string{"a", "b", "c"}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Errorf("list = %v, want %v", got, want)
	}
}

// TestPreconditionOnCreate covers the create-if-absent primitive that the store
// package's uniqueness guarantee rests on.
func TestPreconditionOnCreate(t *testing.T) {
	client, _ := newClient(t)
	const bucket = "test-bucket"

	obj := client.Bucket(bucket).Object("unique")
	w := obj.If(storage.Conditions{DoesNotExist: true}).NewWriter(context.Background())
	if _, err := io.WriteString(w, "first"); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("first create should succeed: %v", err)
	}

	w2 := obj.If(storage.Conditions{DoesNotExist: true}).NewWriter(context.Background())
	if _, err := io.WriteString(w2, "second"); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := w2.Close(); err == nil {
		t.Fatal("second create-if-absent succeeded, want a precondition failure")
	}
}

// TestBucketCreateAndGet covers the bucket routes. GitService's provisioner
// creates a realm's bucket and asks whether it is already there, so without
// these the control plane cannot be run against the emulator at all.
func TestBucketCreateAndGet(t *testing.T) {
	client, _ := newClient(t)
	ctx := context.Background()
	bkt := client.Bucket("gitsvc-devrealm-us-central1")

	if _, err := bkt.Attrs(ctx); !errors.Is(err, storage.ErrBucketNotExist) {
		t.Fatalf("Attrs on an absent bucket = %v, want ErrBucketNotExist", err)
	}

	if err := bkt.Create(ctx, "test-project", nil); err != nil {
		t.Fatalf("Create: %v", err)
	}

	attrs, err := bkt.Attrs(ctx)
	if err != nil {
		t.Fatalf("Attrs after Create: %v", err)
	}
	if attrs.Name != "gitsvc-devrealm-us-central1" {
		t.Errorf("Attrs().Name = %q, want the created bucket", attrs.Name)
	}

	// A second create of the same name is a conflict, which is what lets two
	// provisioners race and still end up with one bucket.
	if err := bkt.Create(ctx, "test-project", nil); err == nil {
		t.Error("creating an existing bucket succeeded, want a conflict")
	}
}
