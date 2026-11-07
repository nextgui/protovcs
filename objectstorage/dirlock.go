package objectstorage

import (
	"bufio"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
)

// lockName is the file whose existence claims a directory.
const lockName = ".protovcs-lock"

// processToken tells this process's locks from those of an earlier process
// that had the same pid, as a restarted container often does.
var processToken = func() string {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		panic("objectstorage: reading randomness for the lock token: " + err.Error())
	}
	return hex.EncodeToString(b)
}()

// claimDir creates the lock file that claims dir for this process.
//
// O_EXCL is the claim. A lock left behind by a process that was killed before
// it could remove the file is taken over, but only when the lock says which
// process wrote it, on this host, and that process is not running. A timeout is
// not used, because it would eventually let a second writer in while the first
// is merely slow.
func claimDir(dir string) (*os.File, error) {
	path := filepath.Join(dir, lockName)
	for attempt := 0; ; attempt++ {
		lock, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o644)
		if err == nil {
			host, _ := os.Hostname()
			fmt.Fprintf(lock, "pid %d\nhost %s\ntoken %s\n", os.Getpid(), host, processToken)
			return lock, nil
		}
		if !errors.Is(err, os.ErrExist) {
			return nil, err
		}
		if attempt > 0 {
			return nil, heldError(dir, path, lockOwner{})
		}
		owner, removed, err := removeStaleLock(dir, path)
		if err != nil {
			return nil, err
		}
		if !removed {
			return nil, heldError(dir, path, owner)
		}
	}
}

// lockOwner is what a lock file says about the process that wrote it.
type lockOwner struct {
	pid   int
	host  string
	token string
}

func readLockOwner(path string) (lockOwner, error) {
	f, err := os.Open(path)
	if err != nil {
		return lockOwner{}, err
	}
	defer f.Close()

	var o lockOwner
	s := bufio.NewScanner(f)
	for s.Scan() {
		key, value, _ := strings.Cut(s.Text(), " ")
		switch key {
		case "pid":
			o.pid, _ = strconv.Atoi(value)
		case "host":
			o.host = value
		case "token":
			o.token = value
		}
	}
	return o, s.Err()
}

// removeStaleLock removes the lock at path if the process that wrote it has
// exited. It reports the owner it read and whether the lock is gone, so the
// caller can try to create it again.
//
// Two processes can find the same stale lock. Each must not remove the lock
// the other has just created in its place, so the read and the remove happen
// under an flock on the directory. A lock is only ever removed by its owner or
// under that flock, so a stale lock read under it is still the file at path
// when it is removed.
func removeStaleLock(dir, path string) (lockOwner, bool, error) {
	d, err := os.Open(dir)
	if err != nil {
		return lockOwner{}, false, err
	}
	defer d.Close()
	if err := syscall.Flock(int(d.Fd()), syscall.LOCK_EX); err != nil {
		return lockOwner{}, false, fmt.Errorf("protovcs: locking %s: %w", dir, err)
	}
	defer syscall.Flock(int(d.Fd()), syscall.LOCK_UN)

	owner, err := readLockOwner(path)
	if errors.Is(err, os.ErrNotExist) {
		return lockOwner{}, true, nil
	}
	if err != nil {
		return lockOwner{}, false, err
	}
	if !owner.stale() {
		return owner, false, nil
	}

	log.Printf("protovcs: %s was claimed by pid %d, which is not running; taking it over", dir, owner.pid)
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return owner, false, err
	}
	return owner, true, nil
}

// stale reports whether the owner is known to have exited.
//
// A lock without a pid is not stale, because nothing says who holds it. A lock
// from another host is not stale, because a pid means nothing off the machine
// that issued it. A lock without a host line was written before hosts were
// recorded, by a process on this machine. A pid equal to this process's is
// this process only if the lock carries this process's token; otherwise an
// earlier process with the same pid wrote it and has exited.
func (o lockOwner) stale() bool {
	if o.pid <= 0 {
		return false
	}
	if host, err := os.Hostname(); o.host != "" && (err != nil || o.host != host) {
		return false
	}
	if o.pid == os.Getpid() {
		return o.token != processToken
	}
	// Signal 0 checks that the process exists without signalling it. EPERM
	// means it exists and belongs to another user.
	return errors.Is(syscall.Kill(o.pid, 0), syscall.ESRCH)
}

func heldError(dir, path string, o lockOwner) error {
	who := "another process"
	if o.pid > 0 {
		who = fmt.Sprintf("pid %d", o.pid)
		if o.host != "" {
			who += " on " + o.host
		}
		who += ", which is running"
	}
	return fmt.Errorf(
		"protovcs: %s is held by %s.\n"+
			"A filesystem cannot arbitrate a compare-and-swap between two writers, so only\n"+
			"one may hold a directory. A lock whose process has exited is taken over; if\n"+
			"this one is wrong, remove %s.\n"+
			"For more than one process, use a bucket that offers preconditions.",
		dir, who, path)
}
