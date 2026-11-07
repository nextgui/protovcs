package objectstorage_test

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/nextgui/protovcs/objectstorage"
)

// deadPid returns the pid of a process that has started and exited.
func deadPid(t *testing.T) int {
	t.Helper()
	cmd := exec.Command(os.Args[0], "-test.run=^$")
	if err := cmd.Run(); err != nil {
		t.Fatalf("running a child to exit: %v", err)
	}
	return cmd.Process.Pid
}

func writeLock(t *testing.T, dir, content string) string {
	t.Helper()
	path := filepath.Join(dir, ".protovcs-lock")
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	return path
}

// A process killed with SIGKILL never removes its lock. Its pid is not running,
// so the next process takes the directory over.
func TestALockLeftByAnExitedProcessIsTakenOver(t *testing.T) {
	dir := t.TempDir()
	path := writeLock(t, dir, fmt.Sprintf("pid %d\n", deadPid(t)))

	b, err := objectstorage.OpenDir(dir)
	if err != nil {
		t.Fatalf("OpenDir over a lock whose pid has exited: %v", err)
	}
	defer b.Close()

	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if want := fmt.Sprintf("pid %d\n", os.Getpid()); !strings.HasPrefix(string(got), want) {
		t.Errorf("lock after takeover = %q, want it to start %q", got, want)
	}
}

// A lock whose pid is running is held, even when nothing else says so.
func TestALockHeldByARunningProcessIsRefused(t *testing.T) {
	dir := t.TempDir()
	writeLock(t, dir, fmt.Sprintf("pid %d\n", os.Getpid()))

	if b, err := objectstorage.OpenDir(dir); err == nil {
		b.Close()
		t.Fatal("OpenDir took a lock whose pid is running")
	}
}

// A pid is only meaningful on the machine that wrote it. A directory shared
// with another host, or with a guest, is left alone.
func TestALockFromAnotherHostIsRefused(t *testing.T) {
	dir := t.TempDir()
	writeLock(t, dir, fmt.Sprintf("pid %d\nhost not-%s\n", deadPid(t), hostname(t)))

	if b, err := objectstorage.OpenDir(dir); err == nil {
		b.Close()
		t.Fatal("OpenDir took a lock written on another host")
	}
}

// Several processes that find the same stale lock must not all take it over.
// One does, and the rest see the lock it wrote.
func TestAStaleLockHasOneTaker(t *testing.T) {
	dir := t.TempDir()
	writeLock(t, dir, fmt.Sprintf("pid %d\n", deadPid(t)))

	const openers = 16
	var (
		wg      sync.WaitGroup
		mu      sync.Mutex
		opened  []*objectstorage.DirBucket
		start   = make(chan struct{})
		refused int
	)
	for range openers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			b, err := objectstorage.OpenDir(dir)
			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				refused++
				return
			}
			opened = append(opened, b)
		}()
	}
	close(start)
	wg.Wait()
	for _, b := range opened {
		defer b.Close()
	}

	if len(opened) != 1 || refused != openers-1 {
		t.Fatalf("%d openers took the stale lock and %d were refused; want 1 and %d", len(opened), refused, openers-1)
	}
}

func hostname(t *testing.T) string {
	t.Helper()
	h, err := os.Hostname()
	if err != nil {
		t.Fatal(err)
	}
	return h
}
