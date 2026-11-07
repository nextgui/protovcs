package objectstorage_test

import (
	"context"
	"errors"
	"testing"

	protovcs "github.com/nextgui/protovcs"
	"github.com/nextgui/protovcs/objectstorage"
	pb "github.com/nextgui/protovcs/proto/protovcs/v1"
	"github.com/nextgui/protovcs/server"
)

type hookBucket struct {
	objectstorage.Bucket
	onDelete func(key string)
	onPutIf  func(key string, data []byte, expected int64) (int64, error, bool)
}

func (h *hookBucket) DeleteIfGeneration(ctx context.Context, key string, expected int64) error {
	if f := h.onDelete; f != nil {
		h.onDelete = nil
		f(key)
	}
	return h.Bucket.DeleteIfGeneration(ctx, key, expected)
}

func (h *hookBucket) Delete(ctx context.Context, key string) error {
	if f := h.onDelete; f != nil {
		h.onDelete = nil
		f(key)
	}
	return h.Bucket.Delete(ctx, key)
}

func (h *hookBucket) PutIfGeneration(ctx context.Context, key string, data []byte, expected int64) (int64, error) {
	if f := h.onPutIf; f != nil {
		if gen, err, handled := f(key, data, expected); handled {
			h.onPutIf = nil
			return gen, err
		}
	}
	return h.Bucket.PutIfGeneration(ctx, key, data, expected)
}

func openDir(t *testing.T) *objectstorage.DirBucket {
	t.Helper()
	b, err := objectstorage.OpenDir(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { b.Close() })
	return b
}

func casCommit(t *testing.T, srv *server.Server, n byte, parents ...protovcs.CommitID) protovcs.CommitID {
	t.Helper()
	c := &pb.Commit{FormatVersion: protovcs.FormatVersion, NamingNonce: []byte{n}}
	for _, p := range parents {
		c.Parents = append(c.Parents, p.Bytes())
	}
	id, err := srv.PutCommit(context.Background(), storeID, "", c)
	if err != nil {
		t.Fatalf("PutCommit: %v", err)
	}
	return id
}

func TestDeletingABranchIsCompareAndSwap(t *testing.T) {
	ctx := context.Background()
	hb := &hookBucket{Bucket: openDir(t)}
	srv, st := newServer(t, hb, &pb.Quota{})
	main := []byte("main")
	c1 := casCommit(t, srv, 1)
	c2 := casCommit(t, srv, 2, c1)
	ref, err := srv.UpdateRef(ctx, storeID, main, c1, "", false)
	if err != nil {
		t.Fatal(err)
	}

	hb.onDelete = func(key string) {
		// Another writer fast-forwards main to c2 with the same token.
		if _, err := st.UpdateRef(ctx, storeID, main, c2, ref.CASToken, false); err != nil {
			t.Errorf("concurrent move: %v", err)
		}
	}
	_, err = srv.UpdateRef(ctx, storeID, main, protovcs.CommitID{}, ref.CASToken, false)
	refs, _ := srv.ListRefs(ctx, storeID)
	if err == nil && len(refs) == 0 {
		t.Errorf("a delete with a stale token succeeded and removed the branch another writer had just moved to %s", c2)
	}
}

func TestATokenForADeletedBranchDoesNotMatchANewOne(t *testing.T) {
	ctx := context.Background()
	srv, _ := newServer(t, openDir(t), &pb.Quota{})
	main := []byte("main")
	c1 := casCommit(t, srv, 1)
	c2 := casCommit(t, srv, 2)
	c3 := casCommit(t, srv, 3, c1)

	old, err := srv.UpdateRef(ctx, storeID, main, c1, "", false)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := srv.UpdateRef(ctx, storeID, main, protovcs.CommitID{}, old.CASToken, false); err != nil {
		t.Fatal(err)
	}
	if _, err := srv.UpdateRef(ctx, storeID, main, c2, "", false); err != nil {
		t.Fatal(err)
	}
	// A writer still holding the first branch's token.
	if _, err := srv.UpdateRef(ctx, storeID, main, c3, old.CASToken, true); !errors.Is(err, protovcs.ErrRaceLost) {
		t.Errorf("a token from the deleted branch moved the new one: err=%v", err)
	}
}
