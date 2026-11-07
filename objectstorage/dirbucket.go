package objectstorage

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

// DirBucket keeps objects as files in a directory.
//
// For local development, for vmland, for a self-hosted box, and for a test that
// wants to reopen a store and find it there.
//
// # One writing process
//
// A conditional write needs the storage layer to arbitrate, and a POSIX
// directory does not: two processes could each read the same generation, each
// see it match, and each write. So this holds a mutex, which arbitrates within
// one process, and claims the directory so there is only one.
//
// That is not a shortcoming of this code. It is why [GCSBucket] exists.
type DirBucket struct {
	root string

	// Held across the read-compare-write of a conditional put, which is the
	// only thing that makes the comparison mean anything here.
	mu sync.Mutex

	lock *os.File

	// Where UploadHandler is served, from WithUploadBase. Empty means nothing
	// serves it and SignUpload has no URL to return.
	uploadBase string

	// What SignUpload signs with and UploadHandler verifies against. Drawn at
	// OpenDir, so it is the same secret for both and different for every run.
	uploadSecret []byte
}

// generationHeader is the eight bytes each object carries in front of its
// contents.
//
// A file has no generation, so one is kept with it. Written and read under the
// mutex, so it cannot be seen half-updated.
const generationHeader = 8

// OpenDir returns a bucket over a directory, creating it if it is not there.
//
// Fails if another running process holds it, because two would each believe
// they had won a compare-and-swap that neither did. A lock left by a process
// that has exited is taken over.
//
// Pass [WithUploadBase] to make [DirBucket.SignUpload] usable; without it a
// directory bucket has no address to hand anybody.
func OpenDir(dir string, opts ...DirOption) (*DirBucket, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("protovcs: preparing %s: %w", dir, err)
	}

	lock, err := claimDir(dir)
	if err != nil {
		return nil, err
	}

	secret, err := newUploadSecret()
	if err != nil {
		lock.Close()
		os.Remove(lock.Name())
		return nil, err
	}

	b := &DirBucket{root: dir, lock: lock, uploadSecret: secret}
	for _, opt := range opts {
		opt(b)
	}
	return b, nil
}

func (b *DirBucket) Close() error {
	if b.lock == nil {
		return nil
	}
	name := b.lock.Name()
	if err := b.lock.Close(); err != nil {
		return err
	}
	b.lock = nil
	return os.Remove(name)
}

// path turns a key into a file. Keys are made by this package and hold only
// hex, slashes and fixed words, so no key can reach outside the root.
func (b *DirBucket) path(key string) string {
	return filepath.Join(b.root, filepath.FromSlash(key))
}

// writeFile writes atomically: to a temporary name in the same directory, then
// renamed into place.
//
// Same directory because rename is only atomic within a filesystem, and a
// temporary directory may be on another one. A reader therefore sees the
// previous contents or the new ones and never half of either.
func writeFile(path string, data []byte) error {
	tmp, err := writeTemp(path, data)
	if err != nil {
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		os.Remove(tmp)
		return err
	}
	return nil
}

// writeTemp writes data to a new temporary file beside path, flushed to disk,
// and returns its name. The caller renames it into place or removes it.
func writeTemp(path string, data []byte) (string, error) {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return "", err
	}

	tmp, err := os.CreateTemp(dir, ".tmp-*")
	if err != nil {
		return "", err
	}
	fail := func(err error) (string, error) {
		tmp.Close()
		os.Remove(tmp.Name())
		return "", err
	}

	if _, err := tmp.Write(data); err != nil {
		return fail(err)
	}
	// Flushed before the rename, or a crash between the two leaves a file that
	// exists under its real name and holds nothing.
	if err := tmp.Sync(); err != nil {
		return fail(err)
	}
	if err := tmp.Close(); err != nil {
		os.Remove(tmp.Name())
		return "", err
	}
	return tmp.Name(), nil
}

func framed(gen int64, data []byte) []byte {
	out := make([]byte, generationHeader+len(data))
	binary.BigEndian.PutUint64(out[:generationHeader], uint64(gen))
	copy(out[generationHeader:], data)
	return out
}

func unframe(raw []byte) ([]byte, int64, error) {
	if len(raw) < generationHeader {
		return nil, 0, fmt.Errorf("protovcs: object is shorter than its header")
	}
	return raw[generationHeader:], int64(binary.BigEndian.Uint64(raw[:generationHeader])), nil
}

// Put writes an object unconditionally.
//
// The file is written and flushed without holding the mutex, so writers of
// different objects wait for their flushes at the same time rather than in
// turn. A protovcs write stores hundreds of chunks, and each flush takes about
// 10 ms on a workstation disk.
//
// The mutex is held only to check that the object's generation is still the
// one the file was framed with, and to rename the file into place. If another
// writer replaced the object in between, the file carries a stale generation,
// so it is discarded and written again. Without that check a Put could put back
// a generation a conditional write had already used, and that write's
// compare-and-swap would no longer mean anything.
//
// Each file is still flushed before its rename. Flushing once for many files
// would need every file written before any is renamed, and Bucket has no call
// that takes many objects. Renaming before the flush is not safe: after a crash
// the file can exist under its real name and hold nothing, and a chunk that
// exists is never uploaded again. The directory is not flushed after the
// rename, here or anywhere else in DirBucket, so there was no per-file directory
// flush to combine.
func (b *DirBucket) Put(ctx context.Context, key string, data []byte) error {
	path := b.path(key)
	for {
		if err := ctx.Err(); err != nil {
			return err
		}

		b.mu.Lock()
		gen, err := b.generation(key)
		b.mu.Unlock()
		if err != nil {
			return err
		}

		tmp, err := writeTemp(path, framed(nextGeneration(gen), data))
		if err != nil {
			return err
		}

		b.mu.Lock()
		now, err := b.generation(key)
		if err == nil && now == gen {
			err = os.Rename(tmp, path)
			b.mu.Unlock()
			if err != nil {
				os.Remove(tmp)
			}
			return err
		}
		b.mu.Unlock()
		os.Remove(tmp)
		if err != nil {
			return err
		}
	}
}

// generation reads an object's generation from its header without reading the
// rest of it. An absent object is generation zero. The caller holds the mutex.
func (b *DirBucket) generation(key string) (int64, error) {
	f, err := os.Open(b.path(key))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return 0, nil
		}
		return 0, err
	}
	defer f.Close()

	var header [generationHeader]byte
	if _, err := f.ReadAt(header[:], 0); err != nil {
		return 0, fmt.Errorf("protovcs: reading the generation of %s: %w", key, err)
	}
	return int64(binary.BigEndian.Uint64(header[:])), nil
}

func (b *DirBucket) PutIfGeneration(ctx context.Context, key string, data []byte, expected int64) (int64, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	_, gen, exists, err := b.read(key)
	if err != nil {
		return 0, err
	}
	if !exists && expected != 0 {
		return 0, ErrGenerationMismatch
	}
	if exists && gen != expected {
		return 0, ErrGenerationMismatch
	}

	next := nextGeneration(gen)
	if err := writeFile(b.path(key), framed(next, data)); err != nil {
		return 0, err
	}
	return next, nil
}

func (b *DirBucket) read(key string) ([]byte, int64, bool, error) {
	raw, err := os.ReadFile(b.path(key))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, 0, false, nil
		}
		return nil, 0, false, err
	}
	data, gen, err := unframe(raw)
	if err != nil {
		return nil, 0, false, err
	}
	return data, gen, true, nil
}

func (b *DirBucket) Get(ctx context.Context, key string) ([]byte, int64, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	data, gen, exists, err := b.read(key)
	if err != nil {
		return nil, 0, err
	}
	if !exists {
		return nil, 0, fmt.Errorf("%w: %s", ErrNoSuchObject, key)
	}
	return data, gen, nil
}

// GetRange reads at most length bytes from offset, seeking rather than reading
// the whole file.
//
// A retained CI log runs to tens of megabytes and a reader tailing it wants the
// few kilobytes appended since it last looked, so reading the file to serve a
// suffix of it would make following a log cost more the longer the log got.
//
// The generation header sits in front of the contents, so a caller's offset is
// shifted past it. Callers count bytes of object, and the header is this
// backend's business.
func (b *DirBucket) GetRange(ctx context.Context, key string, offset, length int64) ([]byte, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	f, err := os.Open(b.path(key))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, fmt.Errorf("%w: %s", ErrNoSuchObject, key)
		}
		return nil, err
	}
	defer f.Close()

	info, err := f.Stat()
	if err != nil {
		return nil, err
	}
	size := info.Size() - generationHeader
	if size < 0 {
		size = 0
	}
	if offset < 0 {
		offset = 0
	}
	// At or past the end is a reader that has caught up, which is the ordinary
	// state of one following a log and not a failure to report.
	if offset >= size {
		return nil, nil
	}
	if length < 0 || offset+length > size {
		length = size - offset
	}

	out := make([]byte, length)
	if _, err := f.ReadAt(out, generationHeader+offset); err != nil {
		return nil, fmt.Errorf("protovcs: reading %s at %d: %w", key, offset, err)
	}
	return out, nil
}

// Compose replaces dest with the concatenation of sources.
//
// Cloud Storage joins the bytes on its own servers. A directory has no such
// facility, so this reads them and writes the result, which is the one place
// this backend is meaningfully slower than the other rather than merely
// unarbitrated. It is still correct, and correct on a workstation is what this
// backend is for.
func (b *DirBucket) Compose(ctx context.Context, dest string, sources []string, expected int64) (int64, error) {
	if len(sources) == 0 {
		return 0, fmt.Errorf("protovcs: composing %s from no sources", dest)
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	_, gen, exists, err := b.read(dest)
	if err != nil {
		return 0, err
	}
	if !exists && expected != 0 {
		return 0, ErrGenerationMismatch
	}
	if exists && gen != expected {
		return 0, ErrGenerationMismatch
	}

	// Every source is read before anything is written, because dest is
	// ordinarily the first of them and writing it early would compose the rest
	// onto what this call had already replaced.
	var joined []byte
	for _, src := range sources {
		data, _, ok, err := b.read(src)
		if err != nil {
			return 0, err
		}
		if !ok {
			return 0, fmt.Errorf("%w: %s", ErrNoSuchObject, src)
		}
		joined = append(joined, data...)
	}

	next := nextGeneration(gen)
	if err := writeFile(b.path(dest), framed(next, joined)); err != nil {
		return 0, err
	}
	return next, nil
}

// Stat reports what is known about an object without reading it.
//
// The generation costs an eight byte read of the header, which is what a caller
// resynchronising after a lost race needs: it has to learn both where the object
// now ends and which generation to write against, and two calls to learn them
// could straddle a third party's write.
func (b *DirBucket) Stat(ctx context.Context, key string) (ObjectInfo, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	f, err := os.Open(b.path(key))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return ObjectInfo{}, fmt.Errorf("%w: %s", ErrNoSuchObject, key)
		}
		return ObjectInfo{}, err
	}
	defer f.Close()

	info, err := f.Stat()
	if err != nil {
		return ObjectInfo{}, err
	}
	size := info.Size() - generationHeader
	if size < 0 {
		size = 0
	}

	var header [generationHeader]byte
	if _, err := f.ReadAt(header[:], 0); err != nil {
		return ObjectInfo{}, fmt.Errorf("protovcs: reading the generation of %s: %w", key, err)
	}
	return ObjectInfo{
		Key:        key,
		Size:       size,
		Updated:    info.ModTime().UTC(),
		Generation: int64(binary.BigEndian.Uint64(header[:])),
	}, nil
}

// nextGeneration is the generation of a write over an object at gen, or of a
// new object when gen is 0.
//
// A timestamp rather than gen+1, as Cloud Storage's are, so that an object
// deleted and written again does not start over at a generation an earlier
// holder of a token for it may still have.
func nextGeneration(gen int64) int64 {
	next := time.Now().UnixNano()
	if next <= gen {
		next = gen + 1
	}
	return next
}

func (b *DirBucket) DeleteIfGeneration(ctx context.Context, key string, expected int64) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	_, gen, exists, err := b.read(key)
	if err != nil {
		return err
	}
	if !exists || gen != expected {
		return ErrGenerationMismatch
	}
	return os.Remove(b.path(key))
}

func (b *DirBucket) Delete(ctx context.Context, key string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	err := os.Remove(b.path(key))
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	return err
}

func (b *DirBucket) List(ctx context.Context, prefix string) ([]ObjectInfo, error) {
	base := b.root
	if prefix != "" {
		if strings.HasSuffix(prefix, "/") {
			base = b.path(prefix)
		} else {
			base = filepath.Dir(b.path(prefix))
		}
	}

	var out []ObjectInfo
	err := filepath.WalkDir(base, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				return nil
			}
			return err
		}
		if d.IsDir() {
			return nil
		}
		name := d.Name()
		if name == lockName || strings.HasPrefix(name, ".tmp-") {
			// A write in flight. Not an object until it is renamed, and listing
			// it would offer collection something to delete out from under a
			// writer.
			return nil
		}
		rel, err := filepath.Rel(b.root, path)
		if err != nil {
			return err
		}
		key := filepath.ToSlash(rel)
		if !strings.HasPrefix(key, prefix) {
			return nil
		}
		info, err := d.Info()
		if err != nil {
			return err
		}
		size := info.Size() - generationHeader
		if size < 0 {
			size = 0
		}
		out = append(out, ObjectInfo{
			Key:     key,
			Size:    size,
			Updated: info.ModTime().UTC(),
		})
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

var _ Bucket = (*DirBucket)(nil)

// unusedTime keeps the time import honest when the file is edited.
var _ = time.Time{}
