package objectstorage_test

import (
	"context"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/nextgui/protovcs/objectstorage"
	pb "github.com/nextgui/protovcs/proto/protovcs/v1"
)

func TestOneUnreadableStoreDoesNotStopTheSweep(t *testing.T) {
	ctx := context.Background()
	b := openDir(t)
	srv, _ := newServer(t, b, &pb.Quota{})
	key := "stores/" + hex.EncodeToString([]byte("broken")) + "/store.pb"
	if err := b.Put(ctx, key, []byte{0xff, 0xff, 0xff}); err != nil {
		t.Fatal(err)
	}
	res, err := srv.Sweep(ctx)
	if err != nil || len(res.Stores) == 0 {
		t.Errorf("one corrupt store record left %d stores swept: %v", len(res.Stores), err)
	}
}

func TestALockLeftByAnEarlierProcessWithOurPidIsTakenOver(t *testing.T) {
	dir := t.TempDir()
	host, _ := os.Hostname()
	lock := fmt.Sprintf("pid %d\nhost %s\n", os.Getpid(), host)
	if err := os.WriteFile(filepath.Join(dir, ".protovcs-lock"), []byte(lock), 0o644); err != nil {
		t.Fatal(err)
	}
	b, err := objectstorage.OpenDir(dir)
	if err != nil {
		t.Errorf("a crashed predecessor with this pid blocks the directory forever: %v", err)
		return
	}
	b.Close()
}
