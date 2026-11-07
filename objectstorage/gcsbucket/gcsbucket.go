// Package gcsbucket keeps protovcs objects in Google Cloud Storage.
//
// Separate from its parent package for two reasons. It is the only part that
// knows anything about Google's client library, so a caller who does not want
// that dependency does not link it. And it makes the boundary visible: anything
// in this package is Cloud Storage's, and anything in objectstorage is every
// backend's.
//
// # What Cloud Storage supplies that a filesystem does not
//
// protovcs needs one conditional write: point a branch at a new version, but
// only if it is still at the version the writer read. Without that, two people
// committing at once each read the same state, each write, and the second
// silently discards the first's work — see the objectstorage package comment.
//
// Every object in Cloud Storage carries a generation number that changes on
// every write, and a write can be made conditional on it. Google's servers do
// the checking, so any number of processes can share a bucket and exactly one
// of them wins each race. That is the whole reason this backend exists: a
// directory cannot offer it, so a directory allows one writing process.
//
// # What this package does
//
// It translates. Generations are what [objectstorage.Bucket] asks for, so they
// pass straight through. The rest is turning Google's errors into the two that
// the code above distinguishes — an object that is not there, and a write that
// lost a race — because everything else is a failure to report rather than a
// condition to handle.
//
// Nothing about how a store is laid out is here. Which key a chunk lives at,
// how a commit is encoded, what a ref holds: all of that is in objectstorage
// and is identical whichever backend keeps the bytes.
package gcsbucket

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"

	"cloud.google.com/go/storage"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"

	"github.com/nextgui/protovcs/objectstorage"
)

// Bucket is a Cloud Storage bucket, as an [objectstorage.Bucket].
type Bucket struct {
	client *storage.Client
	bucket *storage.BucketHandle

	// Set when this opened the client, so Close knows whether it may shut one
	// down that a caller is still using.
	ownsClient bool

	// Who signs upload URLs, from WithSigner. Left unset in production, where
	// the Cloud Storage client works it out from the credentials it already
	// has. See SignUpload.
	googleAccessID string
	signBytes      func([]byte) ([]byte, error)
}

// Option configures a [Bucket] at [New].
type Option func(*Bucket)

// WithSigner names the identity signed upload URLs are issued under, and the
// function that signs them.
//
// Not needed in production. Given neither, the Cloud Storage client derives
// both from the credentials the process already holds: a service account key
// signs directly, and Cloud Run's metadata credentials, which have no private
// key, sign through the IAM credentials API.
//
// It is here for a test pointed at an emulator, which has no credentials at
// all, so that [Bucket.SignUpload] can be exercised on this backend.
func WithSigner(googleAccessID string, signBytes func([]byte) ([]byte, error)) Option {
	return func(b *Bucket) {
		b.googleAccessID = googleAccessID
		b.signBytes = signBytes
	}
}

// Open returns a bucket, opening a Cloud Storage client for it.
func Open(ctx context.Context, bucketName string, opts ...option.ClientOption) (*Bucket, error) {
	client, err := storage.NewClient(ctx, opts...)
	if err != nil {
		return nil, fmt.Errorf("protovcs: opening a storage client: %w", err)
	}
	return &Bucket{client: client, bucket: client.Bucket(bucketName), ownsClient: true}, nil
}

// New returns a bucket over a client the caller already has.
//
// For a process that opens one client and uses it for several things, and for a
// test pointed at an emulator.
func New(client *storage.Client, bucketName string, opts ...Option) *Bucket {
	b := &Bucket{client: client, bucket: client.Bucket(bucketName)}
	for _, opt := range opts {
		opt(b)
	}
	return b
}

func (b *Bucket) Close() error {
	if !b.ownsClient {
		return nil
	}
	return b.client.Close()
}

// asObjectError turns the storage package's errors into the two this package's
// callers distinguish.
//
// A precondition failure is what "somebody else got there first" looks like on
// this backend, and it must not be confused with anything broken.
func asObjectError(key string, err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, storage.ErrObjectNotExist) {
		return fmt.Errorf("%w: %s", objectstorage.ErrNoSuchObject, key)
	}
	var apiErr *googleapi.Error
	if errors.As(err, &apiErr) && apiErr.Code == 412 {
		return objectstorage.ErrGenerationMismatch
	}
	return err
}

func (b *Bucket) Put(ctx context.Context, key string, data []byte) error {
	w := b.bucket.Object(key).NewWriter(ctx)
	if _, err := w.Write(data); err != nil {
		w.Close()
		return asObjectError(key, err)
	}
	return asObjectError(key, w.Close())
}

func (b *Bucket) PutIfGeneration(ctx context.Context, key string, data []byte, expected int64) (int64, error) {
	obj := b.bucket.Object(key)

	// Generation zero means "only if absent", which is how a branch is created.
	// DoesNotExist is GCS's way of saying that, and it is a different
	// precondition from a generation match rather than a special value of one.
	if expected == 0 {
		obj = obj.If(storage.Conditions{DoesNotExist: true})
	} else {
		obj = obj.If(storage.Conditions{GenerationMatch: expected})
	}

	w := obj.NewWriter(ctx)
	if _, err := w.Write(data); err != nil {
		w.Close()
		return 0, asObjectError(key, err)
	}
	if err := w.Close(); err != nil {
		return 0, asObjectError(key, err)
	}
	return w.Attrs().Generation, nil
}

func (b *Bucket) Get(ctx context.Context, key string) ([]byte, int64, error) {
	r, err := b.bucket.Object(key).NewReader(ctx)
	if err != nil {
		return nil, 0, asObjectError(key, err)
	}
	defer r.Close()

	data, err := io.ReadAll(r)
	if err != nil {
		return nil, 0, asObjectError(key, err)
	}

	// The generation the read actually saw, rather than one fetched separately
	// — between two calls the object could move, and a token from the wrong one
	// would let a stale write through.
	return data, r.Attrs.Generation, nil
}

// GetRange reads at most length bytes from offset. A negative length reads to
// the end.
//
// Cloud Storage answers an offset at or past the end of the object with HTTP
// 416, and a reader tailing a log that has not grown since it last looked hits
// that on most of its polls. It is flattened to no bytes and no error, because
// caught up is this caller's ordinary state and every caller would otherwise
// have to recognise 416 to tell it from a real failure.
func (b *Bucket) GetRange(ctx context.Context, key string, offset, length int64) ([]byte, error) {
	r, err := b.bucket.Object(key).NewRangeReader(ctx, offset, length)
	if err != nil {
		var apiErr *googleapi.Error
		if errors.As(err, &apiErr) && apiErr.Code == http.StatusRequestedRangeNotSatisfiable {
			return nil, nil
		}
		return nil, asObjectError(key, err)
	}
	defer r.Close()

	data, err := io.ReadAll(r)
	if err != nil {
		return nil, asObjectError(key, err)
	}
	return data, nil
}

// Compose replaces dest with the concatenation of sources.
//
// The bytes never reach this process. Cloud Storage joins them itself, so
// appending a kilobyte to a hundred-megabyte log costs a kilobyte of upload
// rather than rewriting the log — which is the whole reason a log can be one
// object that grows rather than a pile of fragments a reader has to assemble.
//
// Cloud Storage takes at most 32 sources per call. Appending uses two, dest and
// the new part, and the resulting object has no limit on how many components it
// accumulates, so a log may be appended to indefinitely.
func (b *Bucket) Compose(ctx context.Context, dest string, sources []string, expected int64) (int64, error) {
	if len(sources) == 0 {
		return 0, fmt.Errorf("protovcs: composing %s from no sources", dest)
	}
	if len(sources) > maxComposeSources {
		return 0, fmt.Errorf("protovcs: composing %s from %d sources, over the limit of %d",
			dest, len(sources), maxComposeSources)
	}

	obj := b.bucket.Object(dest)
	if expected == 0 {
		obj = obj.If(storage.Conditions{DoesNotExist: true})
	} else {
		obj = obj.If(storage.Conditions{GenerationMatch: expected})
	}

	srcs := make([]*storage.ObjectHandle, 0, len(sources))
	for _, name := range sources {
		srcs = append(srcs, b.bucket.Object(name))
	}

	attrs, err := obj.ComposerFrom(srcs...).Run(ctx)
	if err != nil {
		return 0, asObjectError(dest, err)
	}
	return attrs.Generation, nil
}

// maxComposeSources is how many objects Cloud Storage will join in one call.
const maxComposeSources = 32

func (b *Bucket) Stat(ctx context.Context, key string) (objectstorage.ObjectInfo, error) {
	attrs, err := b.bucket.Object(key).Attrs(ctx)
	if err != nil {
		return objectstorage.ObjectInfo{}, asObjectError(key, err)
	}
	return objectstorage.ObjectInfo{
		Key:        key,
		Size:       attrs.Size,
		Updated:    attrs.Updated.UTC(),
		Generation: attrs.Generation,
	}, nil
}

func (b *Bucket) Delete(ctx context.Context, key string) error {
	err := b.bucket.Object(key).Delete(ctx)
	if errors.Is(err, storage.ErrObjectNotExist) {
		// A collector that raced another collector should not fail.
		return nil
	}
	return asObjectError(key, err)
}

// List returns every object under a prefix.
//
// # Eventual consistency
//
// GCS object listing is strongly consistent, so an object written before a
// listing appears in it. That is what makes collection safe here: the reachable
// set is computed from commits rather than from a listing, and the listing only
// decides what to consider — so an object it missed is one that is not deleted,
// which is the safe direction.
//
// The unsafe direction would be a listing that returned an object a writer had
// deleted, and deletion here is the collector's own.
func (b *Bucket) List(ctx context.Context, prefix string) ([]objectstorage.ObjectInfo, error) {
	it := b.bucket.Objects(ctx, &storage.Query{Prefix: prefix})

	var out []objectstorage.ObjectInfo
	for {
		attrs, err := it.Next()
		if errors.Is(err, iterator.Done) {
			break
		}
		if err != nil {
			return nil, err
		}
		out = append(out, objectstorage.ObjectInfo{
			Key:        attrs.Name,
			Size:       attrs.Size,
			Updated:    attrs.Updated.UTC(),
			Generation: attrs.Generation,
		})
	}
	return out, nil
}

// SignUpload returns a V4 signed URL that accepts one PUT of exactly the body
// the conditions describe.
//
// The signature covers Content-Length and Content-MD5, so an upload of any
// other length or any other content is a different request and Cloud Storage
// answers 403. That is what makes it safe to hand out: the URL names one
// object and admits one body, so a caller can commit quota to an exact figure
// before any bytes exist.
//
// Exact rather than a range, which is what a signed POST policy would give.
// Nothing is lost by it here, because whoever asks for the URL is holding the
// bytes and can measure them.
func (b *Bucket) SignUpload(ctx context.Context, key string, conditions objectstorage.UploadConditions) (objectstorage.SignedUpload, error) {
	headers, expires, err := objectstorage.PrepareSignedUpload(conditions)
	if err != nil {
		return objectstorage.SignedUpload{}, err
	}

	// Content-Type and Content-MD5 have fields of their own; every other
	// header the uploader must send goes in Headers, which is what puts it in
	// the signature and in X-Goog-SignedHeaders.
	opts := &storage.SignedURLOptions{
		Scheme:         storage.SigningSchemeV4,
		Method:         http.MethodPut,
		Expires:        expires,
		ContentType:    conditions.ContentType,
		MD5:            headers["Content-MD5"],
		Headers:        []string{"Content-Length:" + headers["Content-Length"]},
		GoogleAccessID: b.googleAccessID,
		SignBytes:      b.signBytes,
	}

	signed, err := b.bucket.SignedURL(key, opts)
	if err != nil {
		return objectstorage.SignedUpload{}, fmt.Errorf("protovcs: signing an upload of %s: %w", key, err)
	}
	return objectstorage.SignedUpload{
		URL:     signed,
		Method:  http.MethodPut,
		Headers: headers,
		Expires: expires,
	}, nil
}

// SignDownload returns a V4 signed URL that reads one object with a GET.
func (b *Bucket) SignDownload(ctx context.Context, key string, ttl time.Duration) (objectstorage.SignedDownload, error) {
	if ttl <= 0 || ttl > objectstorage.SignedUploadMaxTTL {
		return objectstorage.SignedDownload{}, fmt.Errorf("protovcs: a signed download lasting %s; want more than zero and at most %s", ttl, objectstorage.SignedUploadMaxTTL)
	}
	expires := time.Now().Add(ttl)
	signed, err := b.bucket.SignedURL(key, &storage.SignedURLOptions{
		Scheme:         storage.SigningSchemeV4,
		Method:         http.MethodGet,
		Expires:        expires,
		GoogleAccessID: b.googleAccessID,
		SignBytes:      b.signBytes,
	})
	if err != nil {
		return objectstorage.SignedDownload{}, fmt.Errorf("protovcs: signing a download of %s: %w", key, err)
	}
	return objectstorage.SignedDownload{URL: signed, Expires: expires}, nil
}

var _ objectstorage.Bucket = (*Bucket)(nil)
