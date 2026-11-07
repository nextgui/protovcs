package objectstorage_test

import (
	"bytes"
	"context"
	"crypto/md5"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"cloud.google.com/go/storage"
	"google.golang.org/api/option"

	"github.com/nextgui/protovcs/internal/gcsemulator"
	"github.com/nextgui/protovcs/objectstorage"
	"github.com/nextgui/protovcs/objectstorage/gcsbucket"
	"github.com/nextgui/protovcs/server"
)

// One suite, run against every bucket.
//
// The point of extracting a Bucket was that a filesystem and a GCS bucket
// should differ in exactly one thing — how a conditional write is arbitrated —
// and in nothing else. A suite that ran only against one of them would let the
// other drift, and the drift would be found by whichever store was on the
// backend nobody tested.

type factory struct {
	name string
	open func(t *testing.T) objectstorage.Bucket
}

func buckets() []factory {
	return []factory{
		{
			name: "dir",
			open: func(t *testing.T) objectstorage.Bucket {
				return openUploadableDir(t)
			},
		},
		{
			name: "gcs",
			open: func(t *testing.T) objectstorage.Bucket {
				emu := gcsemulator.NewServer()
				t.Cleanup(emu.Close)

				client, err := storage.NewClient(context.Background(),
					option.WithEndpoint(emu.URL()), option.WithoutAuthentication())
				if err != nil {
					t.Fatalf("storage.NewClient: %v", err)
				}
				t.Cleanup(func() { client.Close() })

				if err := client.Bucket("protovcs-test").Create(context.Background(), "test-project", nil); err != nil {
					t.Fatalf("creating the bucket: %v", err)
				}
				return gcsbucket.New(client, "protovcs-test", gcsbucket.WithSigner(testAccessID, testSignBytes))
			},
		},
	}
}

// The emulator has no credentials, so signing a URL against it needs an
// identity and a signature supplied from here.
//
// The bytes are not a real RSA signature and nothing verifies them. What the
// tests below check is what a signed URL commits the uploader to — the method,
// the expiry and the headers — which is decided before anything is signed.
const testAccessID = "protovcs-test@example.iam.gserviceaccount.com"

func testSignBytes(b []byte) ([]byte, error) {
	sum := sha256.Sum256(b)
	return sum[:], nil
}

// openUploadableDir returns a directory bucket with the local stack in front of
// it.
//
// A directory has no address, so a signed upload URL from this backend points
// at whatever serves [objectstorage.DirBucket.UploadHandler]. That is what
// [objectstorage.WithUploadBase] names, and a test that skipped it would be
// testing a backend nobody could upload to.
func openUploadableDir(t *testing.T) *objectstorage.DirBucket {
	t.Helper()

	// The handler is reached through the variable rather than fetched once,
	// because the bucket needs the server's URL and the server needs the
	// bucket.
	var b *objectstorage.DirBucket
	srv := httptest.NewServer(http.StripPrefix("/uploads", http.HandlerFunc(
		func(w http.ResponseWriter, r *http.Request) { b.UploadHandler().ServeHTTP(w, r) })))
	t.Cleanup(srv.Close)

	b, err := objectstorage.OpenDir(t.TempDir(), objectstorage.WithUploadBase(srv.URL+"/uploads"))
	if err != nil {
		t.Fatalf("OpenDir: %v", err)
	}
	t.Cleanup(func() { b.Close() })
	return b
}

func eachBucket(t *testing.T, fn func(t *testing.T, b objectstorage.Bucket)) {
	t.Helper()
	for _, f := range buckets() {
		t.Run(f.name, func(t *testing.T) { fn(t, f.open(t)) })
	}
}

// eachAgeableBucket is eachBucket for tests that need an object to look old.
//
// Collection spares a chunk younger than the grace period, so testing it means
// making one older rather than waiting an hour. Each backend has its own way:
// a filesystem has the file's time, and the emulator has a clock.
func eachAgeableBucket(t *testing.T, fn func(t *testing.T, b objectstorage.Bucket, age func(key string))) {
	t.Helper()

	t.Run("dir", func(t *testing.T) {
		dir := t.TempDir()
		b, err := objectstorage.OpenDir(dir)
		if err != nil {
			t.Fatalf("OpenDir: %v", err)
		}
		t.Cleanup(func() { b.Close() })

		fn(t, b, func(key string) {
			old := time.Now().Add(-2 * server.CollectionGrace)
			path := filepath.Join(dir, filepath.FromSlash(key))
			if err := os.Chtimes(path, old, old); err != nil {
				t.Fatalf("aging %s: %v", key, err)
			}
		})
	})

	t.Run("gcs", func(t *testing.T) {
		emu := gcsemulator.NewServer()
		t.Cleanup(emu.Close)

		client, err := storage.NewClient(context.Background(),
			option.WithEndpoint(emu.URL()), option.WithoutAuthentication())
		if err != nil {
			t.Fatalf("storage.NewClient: %v", err)
		}
		t.Cleanup(func() { client.Close() })
		if err := client.Bucket("protovcs-test").Create(context.Background(), "test-project", nil); err != nil {
			t.Fatalf("creating the bucket: %v", err)
		}
		b := gcsbucket.New(client, "protovcs-test")

		// Backdated rather than rewritten, because rewriting a chunk would give
		// it a new stamp — which is exactly what collection must not be fooled
		// by, so a test that did it would be testing the wrong thing.
		fn(t, b, func(key string) {
			old := time.Now().Add(-2 * server.CollectionGrace)
			if err := emu.SetObjectUpdated("protovcs-test", key, old); err != nil {
				t.Fatalf("aging %s: %v", key, err)
			}
		})
	})
}

func TestPutAndGet(t *testing.T) {
	eachBucket(t, func(t *testing.T, b objectstorage.Bucket) {
		ctx := context.Background()

		if err := b.Put(ctx, "a/b/c", []byte("contents")); err != nil {
			t.Fatalf("Put: %v", err)
		}
		data, _, err := b.Get(ctx, "a/b/c")
		if err != nil {
			t.Fatalf("Get: %v", err)
		}
		if string(data) != "contents" {
			t.Fatalf("Get returned %q", data)
		}
	})
}

func TestGetMissingIsDistinct(t *testing.T) {
	eachBucket(t, func(t *testing.T, b objectstorage.Bucket) {
		if _, _, err := b.Get(context.Background(), "nothing/here"); !errors.Is(err, objectstorage.ErrNoSuchObject) {
			t.Fatalf("Get on a missing object returned %v, want ErrNoSuchObject", err)
		}
	})
}

// TestPutIfGenerationCreatesOnlyOnce is what makes a branch's creation safe:
// two callers creating the same one must not both believe they did.
func TestPutIfGenerationCreatesOnlyOnce(t *testing.T) {
	eachBucket(t, func(t *testing.T, b objectstorage.Bucket) {
		ctx := context.Background()

		gen, err := b.PutIfGeneration(ctx, "ref", []byte("first"), 0)
		if err != nil {
			t.Fatalf("creating: %v", err)
		}
		if gen == 0 {
			t.Fatal("a created object reports generation zero, which means absent")
		}

		if _, err := b.PutIfGeneration(ctx, "ref", []byte("second"), 0); !errors.Is(err, objectstorage.ErrGenerationMismatch) {
			t.Fatalf("creating an existing object returned %v, want ErrGenerationMismatch", err)
		}

		data, _, err := b.Get(ctx, "ref")
		if err != nil {
			t.Fatalf("Get: %v", err)
		}
		if string(data) != "first" {
			t.Fatalf("a refused create overwrote the object: %q", data)
		}
	})
}

// TestPutIfGenerationIsACompareAndSwap is the whole of protovcs's concurrency
// control, checked against each backend rather than reasoned about.
func TestPutIfGenerationIsACompareAndSwap(t *testing.T) {
	eachBucket(t, func(t *testing.T, b objectstorage.Bucket) {
		ctx := context.Background()

		first, err := b.PutIfGeneration(ctx, "ref", []byte("one"), 0)
		if err != nil {
			t.Fatalf("creating: %v", err)
		}

		second, err := b.PutIfGeneration(ctx, "ref", []byte("two"), first)
		if err != nil {
			t.Fatalf("moving: %v", err)
		}
		if second == first {
			t.Fatal("the generation did not change on a write")
		}

		// A writer still holding the first generation has lost, and must not
		// write.
		if _, err := b.PutIfGeneration(ctx, "ref", []byte("three"), first); !errors.Is(err, objectstorage.ErrGenerationMismatch) {
			t.Fatalf("a stale generation returned %v, want ErrGenerationMismatch", err)
		}
		data, _, err := b.Get(ctx, "ref")
		if err != nil {
			t.Fatalf("Get: %v", err)
		}
		if string(data) != "two" {
			t.Fatalf("a refused write landed anyway: %q", data)
		}
	})
}

func TestGetReportsTheGenerationItRead(t *testing.T) {
	eachBucket(t, func(t *testing.T, b objectstorage.Bucket) {
		ctx := context.Background()

		if _, err := b.PutIfGeneration(ctx, "ref", []byte("one"), 0); err != nil {
			t.Fatalf("creating: %v", err)
		}
		_, gen, err := b.Get(ctx, "ref")
		if err != nil {
			t.Fatalf("Get: %v", err)
		}

		// The generation a read saw must be the one a conditional write
		// accepts, or a token from the wrong call would let a stale write in.
		if _, err := b.PutIfGeneration(ctx, "ref", []byte("two"), gen); err != nil {
			t.Fatalf("writing against the generation Get reported: %v", err)
		}
	})
}

func TestStatDoesNotReadTheObject(t *testing.T) {
	eachBucket(t, func(t *testing.T, b objectstorage.Bucket) {
		ctx := context.Background()

		body := []byte("some contents worth measuring")
		if err := b.Put(ctx, "obj", body); err != nil {
			t.Fatalf("Put: %v", err)
		}

		info, err := b.Stat(ctx, "obj")
		if err != nil {
			t.Fatalf("Stat: %v", err)
		}
		if info.Size != int64(len(body)) {
			t.Fatalf("Stat reports %d bytes, wrote %d", info.Size, len(body))
		}
		if info.Updated.IsZero() {
			t.Fatal("Stat reports no write time, so collection could not tell an abandoned upload from a live one")
		}

		if _, err := b.Stat(ctx, "missing"); !errors.Is(err, objectstorage.ErrNoSuchObject) {
			t.Fatalf("Stat on a missing object returned %v, want ErrNoSuchObject", err)
		}
	})
}

func TestListReturnsWhatIsUnderAPrefix(t *testing.T) {
	eachBucket(t, func(t *testing.T, b objectstorage.Bucket) {
		ctx := context.Background()

		for i := 0; i < 5; i++ {
			if err := b.Put(ctx, fmt.Sprintf("wanted/%02d", i), []byte("x")); err != nil {
				t.Fatalf("Put: %v", err)
			}
		}
		if err := b.Put(ctx, "elsewhere/one", []byte("x")); err != nil {
			t.Fatalf("Put: %v", err)
		}

		got, err := b.List(ctx, "wanted/")
		if err != nil {
			t.Fatalf("List: %v", err)
		}
		if len(got) != 5 {
			t.Fatalf("List returned %d objects under the prefix, want 5", len(got))
		}
		for _, o := range got {
			if len(o.Key) < 7 || o.Key[:7] != "wanted/" {
				t.Fatalf("List returned %q, which is not under the prefix", o.Key)
			}
		}
	})
}

func TestDeleteIsIdempotent(t *testing.T) {
	eachBucket(t, func(t *testing.T, b objectstorage.Bucket) {
		ctx := context.Background()

		if err := b.Put(ctx, "obj", []byte("x")); err != nil {
			t.Fatalf("Put: %v", err)
		}
		if err := b.Delete(ctx, "obj"); err != nil {
			t.Fatalf("Delete: %v", err)
		}
		// A collector that raced another collector must not fail.
		if err := b.Delete(ctx, "obj"); err != nil {
			t.Fatalf("deleting twice: %v", err)
		}
	})
}

// TestADirectoryAdmitsOneWriter is the claim [DirBucket] rests on.
//
// A filesystem cannot arbitrate a conditional write, so the mutex is what does
// — and a mutex only arbitrates within one process. The claim is what makes
// that true, so it has to be enforced rather than documented.
func TestADirectoryAdmitsOneWriter(t *testing.T) {
	dir := t.TempDir()

	first, err := objectstorage.OpenDir(dir)
	if err != nil {
		t.Fatalf("OpenDir: %v", err)
	}

	if second, err := objectstorage.OpenDir(dir); err == nil {
		second.Close()
		t.Fatal("a second writer opened a directory the first holds")
	}

	// Released on close, or an orderly restart would need a human.
	if err := first.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	third, err := objectstorage.OpenDir(dir)
	if err != nil {
		t.Fatalf("reopening a released directory: %v", err)
	}
	third.Close()
}

// TestConcurrentPutIfGenerationHasOneWinner is the property with real
// goroutines rather than an interleaving arranged by hand.
//
// Every writer reads the same generation and writes on it. Exactly one may
// succeed: a second success is a lost update, which is the failure the whole
// interface exists to prevent, and it is silent everywhere else.
func TestConcurrentPutIfGenerationHasOneWinner(t *testing.T) {
	eachBucket(t, func(t *testing.T, b objectstorage.Bucket) {
		ctx := context.Background()
		const key = "refs/main"

		gen, err := b.PutIfGeneration(ctx, key, []byte("v0"), 0)
		if err != nil {
			t.Fatalf("seeding: %v", err)
		}

		const writers = 8
		var wg sync.WaitGroup
		results := make([]error, writers)
		start := make(chan struct{})
		for i := 0; i < writers; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				<-start
				_, err := b.PutIfGeneration(ctx, key, []byte(fmt.Sprintf("v-%d", i)), gen)
				results[i] = err
			}(i)
		}
		close(start)
		wg.Wait()

		won := 0
		for i, err := range results {
			switch {
			case err == nil:
				won++
			case errors.Is(err, objectstorage.ErrGenerationMismatch):
			default:
				t.Fatalf("writer %d failed with %v, want nil or ErrGenerationMismatch", i, err)
			}
		}
		if won != 1 {
			t.Fatalf("%d of %d writers succeeded against one generation, want 1", won, writers)
		}

		// And the survivor is a whole value written by one of them, not a mix.
		got, _, err := b.Get(ctx, key)
		if err != nil {
			t.Fatalf("Get: %v", err)
		}
		if !strings.HasPrefix(string(got), "v-") {
			t.Fatalf("the winning value is %q, which no writer wrote", got)
		}
	})
}

// TestGetRangeReadsASuffix is the read half of tailing a log: a reader that has
// seen the first n bytes asks for what came after them.
func TestGetRangeReadsASuffix(t *testing.T) {
	eachBucket(t, func(t *testing.T, b objectstorage.Bucket) {
		ctx := context.Background()

		if err := b.Put(ctx, "logs/one", []byte("0123456789")); err != nil {
			t.Fatalf("Put: %v", err)
		}

		got, err := b.GetRange(ctx, "logs/one", 4, -1)
		if err != nil {
			t.Fatalf("GetRange to the end: %v", err)
		}
		if string(got) != "456789" {
			t.Fatalf("GetRange(4, -1) returned %q, want \"456789\"", got)
		}

		got, err = b.GetRange(ctx, "logs/one", 2, 3)
		if err != nil {
			t.Fatalf("GetRange bounded: %v", err)
		}
		if string(got) != "234" {
			t.Fatalf("GetRange(2, 3) returned %q, want \"234\"", got)
		}
	})
}

// TestGetRangeAtTheEndIsNotAnError is what a reader following a live log sees
// on most of its polls, and it must not have to tell that from a fault.
func TestGetRangeAtTheEndIsNotAnError(t *testing.T) {
	eachBucket(t, func(t *testing.T, b objectstorage.Bucket) {
		ctx := context.Background()

		if err := b.Put(ctx, "logs/one", []byte("0123456789")); err != nil {
			t.Fatalf("Put: %v", err)
		}

		for _, offset := range []int64{10, 25} {
			got, err := b.GetRange(ctx, "logs/one", offset, -1)
			if err != nil {
				t.Fatalf("GetRange at offset %d: %v", offset, err)
			}
			if len(got) != 0 {
				t.Fatalf("GetRange at offset %d returned %q, want nothing", offset, got)
			}
		}
	})
}

func TestGetRangeOnAMissingObjectIsDistinct(t *testing.T) {
	eachBucket(t, func(t *testing.T, b objectstorage.Bucket) {
		_, err := b.GetRange(context.Background(), "nothing/here", 0, -1)
		if !errors.Is(err, objectstorage.ErrNoSuchObject) {
			t.Fatalf("GetRange on a missing object returned %v, want ErrNoSuchObject", err)
		}
	})
}

// TestComposeAppendsToItself is the write half of a growing log. Naming the
// destination as its own first source is the only way to add bytes to an object
// that cannot be modified.
func TestComposeAppendsToItself(t *testing.T) {
	eachBucket(t, func(t *testing.T, b objectstorage.Bucket) {
		ctx := context.Background()

		if err := b.Put(ctx, "parts/1", []byte("first ")); err != nil {
			t.Fatalf("Put: %v", err)
		}
		gen, err := b.Compose(ctx, "logs/one", []string{"parts/1"}, 0)
		if err != nil {
			t.Fatalf("Compose creating: %v", err)
		}

		if err := b.Put(ctx, "parts/2", []byte("second ")); err != nil {
			t.Fatalf("Put: %v", err)
		}
		gen, err = b.Compose(ctx, "logs/one", []string{"logs/one", "parts/2"}, gen)
		if err != nil {
			t.Fatalf("Compose appending: %v", err)
		}

		if err := b.Put(ctx, "parts/3", []byte("third")); err != nil {
			t.Fatalf("Put: %v", err)
		}
		if _, err := b.Compose(ctx, "logs/one", []string{"logs/one", "parts/3"}, gen); err != nil {
			t.Fatalf("Compose appending again: %v", err)
		}

		data, _, err := b.Get(ctx, "logs/one")
		if err != nil {
			t.Fatalf("Get: %v", err)
		}
		if string(data) != "first second third" {
			t.Fatalf("the composed object is %q, want \"first second third\"", data)
		}
	})
}

// TestComposeCreatesOnlyOnce keeps two runners that were each handed the same
// check from both believing they started its log.
func TestComposeCreatesOnlyOnce(t *testing.T) {
	eachBucket(t, func(t *testing.T, b objectstorage.Bucket) {
		ctx := context.Background()

		if err := b.Put(ctx, "parts/1", []byte("body")); err != nil {
			t.Fatalf("Put: %v", err)
		}
		if _, err := b.Compose(ctx, "logs/one", []string{"parts/1"}, 0); err != nil {
			t.Fatalf("the first Compose: %v", err)
		}
		if _, err := b.Compose(ctx, "logs/one", []string{"parts/1"}, 0); !errors.Is(err, objectstorage.ErrGenerationMismatch) {
			t.Fatalf("the second Compose returned %v, want ErrGenerationMismatch", err)
		}
	})
}

// TestComposeIsACompareAndSwap is what stops a second writer's append from
// silently discarding a first writer's.
func TestComposeIsACompareAndSwap(t *testing.T) {
	eachBucket(t, func(t *testing.T, b objectstorage.Bucket) {
		ctx := context.Background()

		if err := b.Put(ctx, "parts/1", []byte("body")); err != nil {
			t.Fatalf("Put: %v", err)
		}
		gen, err := b.Compose(ctx, "logs/one", []string{"parts/1"}, 0)
		if err != nil {
			t.Fatalf("Compose creating: %v", err)
		}
		if _, err := b.Compose(ctx, "logs/one", []string{"logs/one", "parts/1"}, gen); err != nil {
			t.Fatalf("Compose at the generation it is at: %v", err)
		}
		// Stale: the generation the caller read is no longer the one there.
		if _, err := b.Compose(ctx, "logs/one", []string{"logs/one", "parts/1"}, gen); !errors.Is(err, objectstorage.ErrGenerationMismatch) {
			t.Fatalf("Compose at a stale generation returned %v, want ErrGenerationMismatch", err)
		}
	})
}

func TestComposeFromAMissingSourceIsDistinct(t *testing.T) {
	eachBucket(t, func(t *testing.T, b objectstorage.Bucket) {
		_, err := b.Compose(context.Background(), "logs/one", []string{"parts/absent"}, 0)
		if !errors.Is(err, objectstorage.ErrNoSuchObject) {
			t.Fatalf("Compose from a missing source returned %v, want ErrNoSuchObject", err)
		}
	})
}

// conditionsFor is the conditions a caller holding these bytes would sign for.
func conditionsFor(body []byte) objectstorage.UploadConditions {
	sum := md5.Sum(body)
	return objectstorage.UploadConditions{
		ContentLength: int64(len(body)),
		ContentMD5:    sum[:],
		ContentType:   "application/octet-stream",
		TTL:           15 * time.Minute,
	}
}

// TestSignUploadCommitsToOneBody is the property the whole method exists for: a
// caller that never sees the bytes still bounds them exactly, so it can decide
// a quota before any of them arrive.
func TestSignUploadCommitsToOneBody(t *testing.T) {
	eachBucket(t, func(t *testing.T, b objectstorage.Bucket) {
		body := []byte("encrypted document bytes")
		conditions := conditionsFor(body)

		signed, err := b.SignUpload(context.Background(), "blobs/one", conditions)
		if err != nil {
			t.Fatalf("SignUpload: %v", err)
		}
		if signed.URL == "" {
			t.Fatal("SignUpload returned no URL")
		}
		if signed.Method != http.MethodPut {
			t.Fatalf("SignUpload asks for %s, want PUT", signed.Method)
		}
		if !signed.Expires.After(time.Now()) {
			t.Fatalf("the URL expires at %s, which is not in the future", signed.Expires)
		}

		// An exact length rather than a ceiling. A caller told a maximum could
		// only reserve the maximum.
		if got, want := signed.Headers["Content-Length"], strconv.Itoa(len(body)); got != want {
			t.Fatalf("the signed Content-Length is %q, want %q", got, want)
		}
		sum := md5.Sum(body)
		if got, want := signed.Headers["Content-MD5"], base64.StdEncoding.EncodeToString(sum[:]); got != want {
			t.Fatalf("the signed Content-MD5 is %q, want %q", got, want)
		}
		if got := signed.Headers["Content-Type"]; got != conditions.ContentType {
			t.Fatalf("the signed Content-Type is %q, want %q", got, conditions.ContentType)
		}

		// Signing does not reserve the name. A caller that treated a signed URL
		// as a written object would count bytes that may never arrive.
		if _, err := b.Stat(context.Background(), "blobs/one"); !errors.Is(err, objectstorage.ErrNoSuchObject) {
			t.Fatalf("Stat after signing returned %v, want ErrNoSuchObject", err)
		}
	})
}

// TestSignUploadRefusesAnUnboundedBody keeps a missing field from quietly
// becoming an upload of any size, which is what a signed URL naming an object
// would otherwise be.
func TestSignUploadRefusesAnUnboundedBody(t *testing.T) {
	sum := md5.Sum([]byte("body"))

	cases := map[string]objectstorage.UploadConditions{
		"no length":       {ContentMD5: sum[:], TTL: time.Minute},
		"no digest":       {ContentLength: 4, TTL: time.Minute},
		"a short digest":  {ContentLength: 4, ContentMD5: sum[:8], TTL: time.Minute},
		"no lifetime":     {ContentLength: 4, ContentMD5: sum[:]},
		"a long lifetime": {ContentLength: 4, ContentMD5: sum[:], TTL: objectstorage.SignedUploadMaxTTL + time.Hour},
	}

	eachBucket(t, func(t *testing.T, b objectstorage.Bucket) {
		for name, conditions := range cases {
			t.Run(name, func(t *testing.T) {
				_, err := b.SignUpload(context.Background(), "blobs/one", conditions)
				if !errors.Is(err, objectstorage.ErrUploadConditions) {
					t.Fatalf("SignUpload with %s returned %v, want ErrUploadConditions", name, err)
				}
			})
		}
	})
}

// TestSignedUploadAcceptsOnlyWhatWasSignedFor is the enforcement, exercised
// end to end against the backend that can be made to answer here.
//
// Cloud Storage refuses a body of another length or another content because the
// V4 signature names those headers; the emulator does not verify signatures, so
// the round trip runs against the directory backend. What both backends are
// checked on above is what the URL commits the uploader to.
func TestSignedUploadAcceptsOnlyWhatWasSignedFor(t *testing.T) {
	ctx := context.Background()
	b := openUploadableDir(t)

	body := []byte("encrypted document bytes")
	signed, err := b.SignUpload(ctx, "blobs/one", conditionsFor(body))
	if err != nil {
		t.Fatalf("SignUpload: %v", err)
	}

	if code := putSigned(t, signed, body); code != http.StatusOK {
		t.Fatalf("uploading the signed body answered %d, want 200", code)
	}
	info, err := b.Stat(ctx, "blobs/one")
	if err != nil {
		t.Fatalf("Stat after the upload: %v", err)
	}
	if info.Size != int64(len(body)) {
		t.Fatalf("the object is %d bytes, uploaded %d", info.Size, len(body))
	}

	// A different length is a different request. This is what stops a URL for
	// a reserved quota being spent on a larger file.
	signed, err = b.SignUpload(ctx, "blobs/two", conditionsFor(body))
	if err != nil {
		t.Fatalf("SignUpload: %v", err)
	}
	if code := putSigned(t, signed, append(body, " and more"...)); code != http.StatusForbidden {
		t.Fatalf("uploading a longer body answered %d, want 403", code)
	}

	// And the same length with different content, which the length alone
	// cannot catch and the digest does.
	swapped := append([]byte(nil), body...)
	swapped[0] ^= 0xff
	if code := putSigned(t, signed, swapped); code != http.StatusForbidden {
		t.Fatalf("uploading different content answered %d, want 403", code)
	}

	if _, err := b.Stat(ctx, "blobs/two"); !errors.Is(err, objectstorage.ErrNoSuchObject) {
		t.Fatalf("Stat after two refused uploads returned %v, want ErrNoSuchObject", err)
	}
}

// TestSignedUploadExpires is what bounds a URL that leaks, since nothing
// revokes one.
func TestSignedUploadExpires(t *testing.T) {
	b := openUploadableDir(t)

	body := []byte("encrypted document bytes")
	conditions := conditionsFor(body)
	conditions.TTL = time.Second
	signed, err := b.SignUpload(context.Background(), "blobs/one", conditions)
	if err != nil {
		t.Fatalf("SignUpload: %v", err)
	}

	// Signing truncates the expiry to a whole second, so a URL a second old is
	// reliably past it.
	time.Sleep(2 * time.Second)
	if code := putSigned(t, signed, body); code != http.StatusForbidden {
		t.Fatalf("uploading after the URL expired answered %d, want 403", code)
	}
}

// TestSignedUploadRefusesATamperedURL keeps the conditions from being
// negotiable by whoever holds the URL.
func TestSignedUploadRefusesATamperedURL(t *testing.T) {
	b := openUploadableDir(t)

	body := []byte("encrypted document bytes")
	signed, err := b.SignUpload(context.Background(), "blobs/one", conditionsFor(body))
	if err != nil {
		t.Fatalf("SignUpload: %v", err)
	}

	longer := append(body, " and more"...)
	raised := strings.Replace(signed.URL,
		"length="+strconv.Itoa(len(body)), "length="+strconv.Itoa(len(longer)), 1)
	if raised == signed.URL {
		t.Fatal("the URL does not carry the length, so this test raised nothing")
	}

	signed.URL = raised
	signed.Headers["Content-Length"] = strconv.Itoa(len(longer))
	if code := putSigned(t, signed, longer); code != http.StatusForbidden {
		t.Fatalf("uploading against a raised length answered %d, want 403", code)
	}
}

// putSigned performs the upload exactly as the caller was told to, and returns
// the status.
func putSigned(t *testing.T, signed objectstorage.SignedUpload, body []byte) int {
	t.Helper()

	req, err := http.NewRequest(signed.Method, signed.URL, bytes.NewReader(body))
	if err != nil {
		t.Fatalf("building the upload request: %v", err)
	}
	for name, value := range signed.Headers {
		if name == "Content-Length" {
			// net/http sets this from the body and ignores the header, so a
			// body of another length arrives declaring its own.
			continue
		}
		req.Header.Set(name, value)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("uploading: %v", err)
	}
	defer resp.Body.Close()
	io.Copy(io.Discard, resp.Body)
	return resp.StatusCode
}

// TestStatReportsTheGeneration is what a writer that lost a race reads to find
// out what to write against next. A backend that left it at zero would send
// that writer into a retry loop it could never win.
func TestStatReportsTheGeneration(t *testing.T) {
	eachBucket(t, func(t *testing.T, b objectstorage.Bucket) {
		ctx := context.Background()

		gen, err := b.PutIfGeneration(ctx, "a/b/c", []byte("first"), 0)
		if err != nil {
			t.Fatalf("PutIfGeneration: %v", err)
		}
		info, err := b.Stat(ctx, "a/b/c")
		if err != nil {
			t.Fatalf("Stat: %v", err)
		}
		if info.Generation != gen {
			t.Fatalf("Stat reported generation %d, want %d", info.Generation, gen)
		}
		if info.Size != int64(len("first")) {
			t.Fatalf("Stat reported size %d, want %d", info.Size, len("first"))
		}

		// And it moves when the object does, or a writer resynchronising would
		// be handed the generation it already knew was stale.
		next, err := b.PutIfGeneration(ctx, "a/b/c", []byte("second"), gen)
		if err != nil {
			t.Fatalf("PutIfGeneration again: %v", err)
		}
		info, err = b.Stat(ctx, "a/b/c")
		if err != nil {
			t.Fatalf("Stat again: %v", err)
		}
		if info.Generation != next {
			t.Fatalf("Stat reported generation %d after a second write, want %d", info.Generation, next)
		}
	})
}

// TestConcurrentPutsNeverShareAGeneration has writers replace one object at
// once while a reader watches it.
//
// A generation names one version of an object, so a compare-and-swap against
// it is a promise about the contents. A directory bucket writes the file for an
// unconditional Put outside its mutex. If it renamed that file into place
// without checking the generation again, two writers that read the same
// generation would each install a different value under the next one. A
// compare-and-swap made after reading the first would then overwrite the
// second unseen.
func TestConcurrentPutsNeverShareAGeneration(t *testing.T) {
	eachBucket(t, func(t *testing.T, b objectstorage.Bucket) {
		ctx := context.Background()
		const key = "stores/s/reservations/r"

		if _, err := b.PutIfGeneration(ctx, key, []byte("v0"), 0); err != nil {
			t.Fatalf("seeding: %v", err)
		}

		const writers, rounds = 8, 4
		var wg sync.WaitGroup
		errs := make([]error, writers)
		start := make(chan struct{})
		for i := 0; i < writers; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				<-start
				for r := 0; r < rounds && errs[i] == nil; r++ {
					errs[i] = b.Put(ctx, key, []byte(fmt.Sprintf("put-%d-%d", i, r)))
				}
			}(i)
		}
		done := make(chan struct{})
		go func() { wg.Wait(); close(done) }()
		close(start)

		seen := map[int64]string{}
		last := int64(0)
		for watching := true; watching; {
			select {
			case <-done:
				watching = false
			default:
			}
			data, gen, err := b.Get(ctx, key)
			if err != nil {
				t.Fatalf("Get: %v", err)
			}
			if prev, ok := seen[gen]; ok && prev != string(data) {
				t.Fatalf("generation %d held both %q and %q", gen, prev, data)
			}
			if gen < last {
				t.Fatalf("the generation went back from %d to %d", last, gen)
			}
			seen[gen], last = string(data), gen
		}
		for i, err := range errs {
			if err != nil {
				t.Fatalf("writer %d: %v", i, err)
			}
		}
	})
}

// TestSignDownloadIsRefusedWithoutALifetime keeps a download URL from lasting
// forever, on both backends.
func TestSignDownloadIsRefusedWithoutALifetime(t *testing.T) {
	eachBucket(t, func(t *testing.T, b objectstorage.Bucket) {
		if _, err := b.SignDownload(context.Background(), "blobs/one", 0); err == nil {
			t.Fatal("SignDownload with no lifetime was allowed")
		}
		if _, err := b.SignDownload(context.Background(), "blobs/one", objectstorage.SignedUploadMaxTTL+time.Hour); err == nil {
			t.Fatal("SignDownload longer than a V4 signature allows was allowed")
		}
	})
}

// TestSignedDownloadReadsOnlyWhatWasSigned is the round trip, against the
// directory backend for the reason TestSignedUploadAcceptsOnlyWhatWasSignedFor
// gives.
func TestSignedDownloadReadsOnlyWhatWasSigned(t *testing.T) {
	ctx := context.Background()
	b := openUploadableDir(t)
	body := []byte("encrypted document bytes")
	if err := b.Put(ctx, "blobs/one", body); err != nil {
		t.Fatal(err)
	}

	signed, err := b.SignDownload(ctx, "blobs/one", time.Minute)
	if err != nil {
		t.Fatalf("SignDownload: %v", err)
	}
	code, got := getSigned(t, signed.URL)
	if code != http.StatusOK || !bytes.Equal(got, body) {
		t.Fatalf("reading the signed URL answered %d with %q, want 200 with %q", code, got, body)
	}

	// The signature names the object, so it reads nothing else.
	other := strings.Replace(signed.URL, "blobs/one", "blobs/two", 1)
	if code, _ := getSigned(t, other); code != http.StatusForbidden {
		t.Fatalf("a download URL moved to another object answered %d, want 403", code)
	}

	// A download signature does not authorise a write.
	req, err := http.NewRequest(http.MethodPut, signed.URL, bytes.NewReader([]byte("replacement")))
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("a PUT to a download URL answered %d, want 403", resp.StatusCode)
	}
}

func getSigned(t *testing.T, url string) (int, []byte) {
	t.Helper()
	resp, err := http.Get(url)
	if err != nil {
		t.Fatalf("downloading: %v", err)
	}
	defer resp.Body.Close()
	data, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, data
}
