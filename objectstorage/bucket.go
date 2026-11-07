// Package objectstorage is where a protovcs store's bytes actually go.
//
// # What a store is made of
//
// A protovcs store holds three kinds of thing. Chunks are sealed blobs, each
// named by a fingerprint of its own contents. Commits are records naming a set
// of chunks, each named by a fingerprint of itself. Refs are branches: short
// names like "main" pointing at one commit.
//
// The first two are immutable. A chunk's name is derived from its bytes, so a
// chunk with a given name can only ever hold one thing — two writers storing
// the same chunk are storing the same bytes, and one of them is redundant
// rather than conflicting. Writing them can be retried, duplicated, reordered
// or interrupted with no coordination whatsoever.
//
// A ref is the exception, and it is the only one.
//
// # Why a ref needs compare-and-swap
//
// Committing means: read where "main" points, build a new version on top of it,
// point "main" at the new version. Two people doing that at once is ordinary —
// a phone and a laptop, two browser tabs, a sync worker and a user.
//
// Suppose both read that "main" is at version A. One builds B (A plus their
// transaction), the other builds C (A plus theirs). Both write. Whichever writes
// second wins, and the other's transaction is gone — not failed, gone, with the
// app having told them it was saved.
//
// Reading the ref back afterwards does not fix this. It tells you what the ref
// said at the moment you looked, and it can be overwritten immediately after.
// A writer can observe its own write, report success, and be wrong.
//
// Compare-and-swap fixes it: "point main at B, but only if it is still at A."
// The storage layer checks and refuses, so the loser finds out at write time —
// before it has told anyone it succeeded — and can rebuild on what actually
// happened and try again. That is why this interface has a conditional write on
// it rather than a plain one.
//
// # What is in this package and what is not
//
// [Storage] is everything about how a store is laid out: which key a chunk
// lives at, how a commit is encoded, what a ref object holds, how usage is
// counted, how a caller-supplied store id is kept from naming a location.
// None of it depends on where the objects are kept.
//
// [Bucket] is the small interface underneath — put, conditional put, get,
// ranged get, compose, stat, delete, list, sign an upload — and it is the only
// thing a backend implements. protovcs itself uses neither the ranged get nor
// compose; those are for a log that grows one object by appending to it and
// has readers following it from a byte offset while it grows. Signing an
// upload is for an application that stores files it may not read, and so does
// not want them passing through it. [DirBucket],
// in this package, keeps objects as files in a directory. A Cloud Storage one
// is in the gcsbucket subpackage, kept separate so that a caller who does not
// want Google's client library does not link it.
//
// The two differ in one thing: who enforces the conditional write. Cloud
// Storage does it itself, so any number of processes can share a bucket safely.
// A filesystem has no such facility — two processes can each check and each
// write, with nothing in between to say no — so [DirBucket] uses a mutex, which
// only works within one process, and then claims the directory so that there is
// only one.
package objectstorage

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"strconv"
	"time"
)

// ErrGenerationMismatch is returned by [Bucket.PutIfGeneration] when the object
// has changed since the caller read it.
//
// This is what losing a race looks like, and the caller's answer is to reread,
// rebuild on what is actually there, and try again — so it must be
// distinguishable from a backend that is broken, which is not retryable.
var ErrGenerationMismatch = errors.New("protovcs: the object is not at the expected generation")

// ErrNoSuchObject is returned when an object is not there.
var ErrNoSuchObject = errors.New("protovcs: no such object")

// ErrUploadConditions is returned by [Bucket.SignUpload] when the conditions
// do not describe exactly one body: a length that is not positive, a digest
// that is not an MD5, or a lifetime that is not positive.
//
// A caller cannot sign an unconstrained upload by leaving a field out, which is
// the mistake this separates from a backend that could not sign.
var ErrUploadConditions = errors.New("protovcs: the upload conditions do not describe one body")

// MD5Len is the length in bytes of the raw digest [UploadConditions.ContentMD5]
// carries.
const MD5Len = 16

// UploadConditions is what a signed upload must satisfy to be accepted.
//
// Every field is exact rather than a bound. A V4 signature covers the headers
// it names, so a signed Content-Length makes any other length a different
// request, and the backend answers 403 rather than storing it. Exact is
// possible because a caller signing an upload of bytes somebody else holds
// still knows how many there are and what they hash to.
type UploadConditions struct {
	// The body's length in bytes, exactly. Must be positive.
	ContentLength int64

	// The raw [MD5Len]-byte MD5 digest of the body.
	//
	// MD5 because Content-MD5 is the header Cloud Storage checks a body
	// against, and this pins content the uploader declared against content the
	// uploader sent. It is not resisting an adversary who chooses both: what
	// stops that is the length and the signature.
	ContentMD5 []byte

	// The body's media type, sent as Content-Type and covered by the signature.
	// Optional; empty signs no Content-Type and the uploader must send none.
	ContentType string

	// How long the URL stays valid, from now.
	//
	// Nothing revokes an unexpired URL, so this is what bounds one that leaks.
	TTL time.Duration
}

// SignedUpload is one URL that accepts one object.
type SignedUpload struct {
	URL string

	// The HTTP method the uploader must use.
	Method string

	// The headers the uploader must send verbatim, canonically capitalised.
	//
	// Part of the signature rather than advice: a request that omits one or
	// sends a different value is a different request and is refused by the
	// backend.
	Headers map[string]string

	// When the URL stops being accepted.
	Expires time.Time
}

// SignedDownload is a URL that reads one object, from whoever is given it.
type SignedDownload struct {
	URL string

	// When the URL stops being accepted.
	Expires time.Time
}

// SignedUploadMaxTTL is the longest lifetime a signed upload may be given,
// which is the seven days Cloud Storage allows a V4 signature. Both backends
// enforce it, so a lifetime that works locally works in production.
const SignedUploadMaxTTL = 7 * 24 * time.Hour

// PrepareSignedUpload checks conditions and returns the headers an uploader
// must send and the instant the URL stops being accepted.
//
// For implementations of [Bucket.SignUpload] rather than for callers of it.
// Both backends go through it, so a caller who leaves out a digest is refused
// identically by each, and the headers a URL is signed over cannot drift from
// the headers the caller is told to send.
func PrepareSignedUpload(c UploadConditions) (headers map[string]string, expires time.Time, err error) {
	switch {
	case c.ContentLength <= 0:
		return nil, time.Time{}, fmt.Errorf("%w: a content length of %d", ErrUploadConditions, c.ContentLength)
	case len(c.ContentMD5) != MD5Len:
		return nil, time.Time{}, fmt.Errorf("%w: a content digest of %d bytes, want %d",
			ErrUploadConditions, len(c.ContentMD5), MD5Len)
	case c.TTL <= 0:
		return nil, time.Time{}, fmt.Errorf("%w: a lifetime of %s", ErrUploadConditions, c.TTL)
	case c.TTL > SignedUploadMaxTTL:
		return nil, time.Time{}, fmt.Errorf("%w: a lifetime of %s, over the limit of %s",
			ErrUploadConditions, c.TTL, SignedUploadMaxTTL)
	}

	headers = map[string]string{
		"Content-Length": strconv.FormatInt(c.ContentLength, 10),
		"Content-MD5":    base64.StdEncoding.EncodeToString(c.ContentMD5),
	}
	if c.ContentType != "" {
		headers["Content-Type"] = c.ContentType
	}
	return headers, time.Now().Add(c.TTL).UTC(), nil
}

// ObjectInfo is what a bucket knows about an object without reading it.
type ObjectInfo struct {
	Key  string
	Size int64

	// When it was last written. Collection uses this to tell an upload still in
	// flight from an abandoned one, so a bucket that cannot report it honestly
	// makes collection unsafe.
	Updated time.Time

	// Changes on every write. Object storage supplies one; a filesystem bucket
	// keeps its own, which is enough because it also holds the lock that makes
	// the comparison mean anything.
	Generation int64
}

// Bucket is where objects are kept.
//
// Deliberately small: everything built on this gets durable storage from these
// nine methods, so adding a backend means implementing them and nothing else.
// Keeping it small is also what stops the rules above it being reimplemented
// differently by accident, once per backend.
//
// Six of them are protovcs's. [Bucket.GetRange] and [Bucket.Compose] are here
// for a log that appends to one object and has readers following it from a
// byte offset while it grows. [Bucket.SignUpload] is for a blob store that
// holds documents it cannot decrypt, and hands the browser a URL rather than
// carrying large files through a service that may not look at them.
type Bucket interface {
	// Put writes an object, replacing whatever was there.
	//
	// For objects nothing races on: chunks, which are immutable and named by
	// their contents, and commits, which are immutable and named by theirs.
	Put(ctx context.Context, key string, data []byte) error

	// PutIfGeneration writes only if the object is at the generation given, and
	// returns the generation it is at afterwards.
	//
	// Generation zero means "only if the object does not exist", which is how a
	// branch is created. A mismatch returns [ErrGenerationMismatch] and must
	// not write.
	//
	// This is the whole of protovcs's concurrency control. Everything else can
	// be retried, duplicated or interrupted without coordination.
	PutIfGeneration(ctx context.Context, key string, data []byte, expected int64) (int64, error)

	// Get reads an object and the generation it is at.
	Get(ctx context.Context, key string) ([]byte, int64, error)

	// GetRange reads at most length bytes starting at offset. A negative length
	// reads to the end.
	//
	// An offset at or past the end of the object returns no bytes and no error,
	// because the caller asking is a reader tailing an object that is still
	// being written, and "nothing new yet" is its ordinary answer rather than a
	// failure. Cloud Storage reports that case as HTTP 416, which this
	// flattens, so a caller does not have to know the difference between a
	// backend that said 416 and a directory that returned short.
	GetRange(ctx context.Context, key string, offset, length int64) ([]byte, error)

	// Compose replaces dest with the concatenation of sources, and returns the
	// generation dest is at afterwards.
	//
	// The bytes are joined by the backend, not moved through this process, so
	// appending to a large object costs the size of what is added rather than
	// the size of the result. dest may appear in sources — naming it first is
	// how an object is appended to, and is the only way to append at all,
	// because an object cannot be modified once written.
	//
	// expected works as it does in [Bucket.PutIfGeneration]: zero means dest
	// must not exist, which is how a log is started, and any other value means
	// dest must still be at that generation. There is no unconditional form.
	// Appending is read-modify-write however server-side it looks, so two
	// writers with no precondition would each extend the object they read and
	// one extension would vanish.
	Compose(ctx context.Context, dest string, sources []string, expected int64) (int64, error)

	// Stat reports what is known about an object without reading it.
	Stat(ctx context.Context, key string) (ObjectInfo, error)

	// Delete removes an object. Removing one that is not there is not an error,
	// because a collector that raced another collector should not fail.
	Delete(ctx context.Context, key string) error

	// DeleteIfGeneration removes an object only if it is at the generation
	// given. A mismatch, including an object that is not there, returns
	// [ErrGenerationMismatch] and removes nothing.
	DeleteIfGeneration(ctx context.Context, key string, expected int64) error

	// List returns every object under a prefix.
	List(ctx context.Context, prefix string) ([]ObjectInfo, error)

	// SignUpload returns a URL that accepts one write of one object, from
	// whoever is given the URL.
	//
	// The only method here whose bytes do not pass through this process. A
	// caller storing a file a user is uploading otherwise carries the file, and
	// a caller that is not permitted to read the file carries it anyway.
	//
	// The conditions are exact, and the returned headers are part of the
	// signature. An upload of a different length or different content is a
	// different request and is refused by the backend rather than by the
	// caller, so the caller does not have to see the bytes to bound them.
	// Conditions that do not describe one body return [ErrUploadConditions].
	//
	// Signing does not create the object and does not reserve the name. Until
	// the upload lands, [Bucket.Stat] reports [ErrNoSuchObject].
	SignUpload(ctx context.Context, key string, conditions UploadConditions) (SignedUpload, error)

	// SignDownload returns a URL that reads one object with a GET, from
	// whoever is given the URL, until ttl has passed.
	//
	// The counterpart of [Bucket.SignUpload], for the same reason: a caller
	// handing a user back a file it is not permitted to read should not carry
	// the bytes. Signing does not check the object exists; a GET for one that
	// does not answers 404. A ttl that is not positive, or is longer than
	// [SignedUploadMaxTTL], is refused.
	SignDownload(ctx context.Context, key string, ttl time.Duration) (SignedDownload, error)

	// Close releases whatever the bucket holds.
	Close() error
}
