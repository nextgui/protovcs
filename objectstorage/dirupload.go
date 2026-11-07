package objectstorage

import (
	"context"
	"crypto/hmac"
	"crypto/md5"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

// DirOption configures a [DirBucket] at [OpenDir].
type DirOption func(*DirBucket)

// WithUploadBase says where [DirBucket.UploadHandler] is served, so that
// [DirBucket.SignUpload] can name it.
//
// A directory has no address. Signing an upload means handing somebody a URL,
// and only whoever started the local stack knows what that URL is, so a bucket
// opened without this cannot sign one and says so rather than returning a URL
// that reaches nothing.
//
// The base is the prefix the handler is mounted under, with no trailing slash:
// "http://127.0.0.1:8080/uploads" for a handler mounted with
// [http.StripPrefix] at "/uploads/".
func WithUploadBase(baseURL string) DirOption {
	return func(b *DirBucket) { b.uploadBase = strings.TrimSuffix(baseURL, "/") }
}

// SignUpload returns a URL the local stack serves, honouring the same
// conditions Cloud Storage does.
//
// The signature is an HMAC rather than RSA because nothing outside this process
// verifies it: [DirBucket.UploadHandler] is the only reader, and it holds the
// same secret. The secret is drawn at [OpenDir] and kept in memory, so a
// restart refuses URLs signed before it — which is the right answer for a
// backend whose objects also live in whatever directory the developer pointed
// it at.
func (b *DirBucket) SignUpload(ctx context.Context, key string, conditions UploadConditions) (SignedUpload, error) {
	headers, expires, err := PrepareSignedUpload(conditions)
	if err != nil {
		return SignedUpload{}, err
	}
	if b.uploadBase == "" {
		return SignedUpload{}, fmt.Errorf(
			"protovcs: this directory bucket cannot sign an upload because nothing serves one.\n" +
				"Pass WithUploadBase to OpenDir naming where UploadHandler is mounted.")
	}

	expires = expires.Truncate(time.Second)
	q := url.Values{}
	q.Set("expires", strconv.FormatInt(expires.Unix(), 10))
	q.Set("length", strconv.FormatInt(conditions.ContentLength, 10))
	q.Set("md5", base64.StdEncoding.EncodeToString(conditions.ContentMD5))
	q.Set("type", conditions.ContentType)
	q.Set("sig", hex.EncodeToString(b.uploadSignature(key, q)))

	return SignedUpload{
		URL:     b.uploadBase + "/" + escapeKey(key) + "?" + q.Encode(),
		Method:  http.MethodPut,
		Headers: headers,
		Expires: expires,
	}, nil
}

// SignDownload returns a URL the local stack serves, from the same handler as
// uploads, signed with the same secret over a different message so that a
// download signature cannot be used to write.
func (b *DirBucket) SignDownload(ctx context.Context, key string, ttl time.Duration) (SignedDownload, error) {
	if ttl <= 0 || ttl > SignedUploadMaxTTL {
		return SignedDownload{}, fmt.Errorf("protovcs: a signed download lasting %s; want more than zero and at most %s", ttl, SignedUploadMaxTTL)
	}
	if b.uploadBase == "" {
		return SignedDownload{}, fmt.Errorf(
			"protovcs: this directory bucket cannot sign a download because nothing serves one.\n" +
				"Pass WithUploadBase to OpenDir naming where UploadHandler is mounted.")
	}
	expires := time.Now().Add(ttl).Truncate(time.Second)
	q := url.Values{}
	q.Set("expires", strconv.FormatInt(expires.Unix(), 10))
	q.Set("sig", hex.EncodeToString(b.downloadSignature(key, q)))
	return SignedDownload{
		URL:     b.uploadBase + "/" + escapeKey(key) + "?" + q.Encode(),
		Expires: expires,
	}, nil
}

// downloadSignature covers the key and the expiry. It begins with a line no
// upload signature begins with, so neither can stand in for the other.
func (b *DirBucket) downloadSignature(key string, q url.Values) []byte {
	mac := hmac.New(sha256.New, b.uploadSecret)
	fmt.Fprintf(mac, "download\n%s\nexpires=%s\n", key, q.Get("expires"))
	return mac.Sum(nil)
}

// serveDownload answers a GET at a URL [DirBucket.SignDownload] returned.
func (b *DirBucket) serveDownload(w http.ResponseWriter, r *http.Request, key string) {
	q := r.URL.Query()
	got, err := hex.DecodeString(q.Get("sig"))
	if err != nil || !hmac.Equal(b.downloadSignature(key, q), got) {
		http.Error(w, "the signature does not cover this request", http.StatusForbidden)
		return
	}
	expires, err := strconv.ParseInt(q.Get("expires"), 10, 64)
	if err != nil || time.Now().After(time.Unix(expires, 0)) {
		http.Error(w, "the URL has expired", http.StatusForbidden)
		return
	}
	data, _, err := b.Get(r.Context(), key)
	if errors.Is(err, ErrNoSuchObject) {
		http.Error(w, "no such object", http.StatusNotFound)
		return
	}
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Content-Length", strconv.Itoa(len(data)))
	_, _ = w.Write(data)
}

// escapeKey escapes a key for the path of an upload URL, leaving the slashes
// that separate its segments.
func escapeKey(key string) string {
	parts := strings.Split(key, "/")
	for i, p := range parts {
		parts[i] = url.PathEscape(p)
	}
	return strings.Join(parts, "/")
}

// uploadSignature covers the key and every condition, so changing any of them
// produces a different signature and the handler refuses the request.
//
// The sig parameter itself is skipped, because it is what this produces.
func (b *DirBucket) uploadSignature(key string, q url.Values) []byte {
	mac := hmac.New(sha256.New, b.uploadSecret)
	fmt.Fprintf(mac, "%s\n", key)
	for _, name := range []string{"expires", "length", "md5", "type"} {
		fmt.Fprintf(mac, "%s=%s\n", name, q.Get(name))
	}
	return mac.Sum(nil)
}

// UploadHandler serves the URLs [DirBucket.SignUpload] and
// [DirBucket.SignDownload] return: a PUT writes, a GET reads.
//
// Mount it under the same prefix [WithUploadBase] names, stripped, so that the
// path this sees is the object's key.
//
// It refuses with 403 whatever Cloud Storage would refuse with 403: an expired
// URL, a signature that does not cover this request, and a body of a different
// length or different content from the one signed for. That equivalence is the
// point of the method being on [Bucket] at all — an upload path exercised
// locally behaves as it will in production, rather than only existing there.
func (b *DirBucket) UploadHandler() http.Handler {
	return http.HandlerFunc(b.serveUpload)
}

func (b *DirBucket) serveUpload(w http.ResponseWriter, r *http.Request) {
	key := strings.TrimPrefix(r.URL.Path, "/")
	if key == "" {
		http.Error(w, "no object named", http.StatusBadRequest)
		return
	}
	if r.Method == http.MethodGet {
		b.serveDownload(w, r, key)
		return
	}
	if r.Method != http.MethodPut {
		http.Error(w, "a signed upload is a PUT", http.StatusMethodNotAllowed)
		return
	}

	q := r.URL.Query()
	want := b.uploadSignature(key, q)
	got, err := hex.DecodeString(q.Get("sig"))
	if err != nil || !hmac.Equal(want, got) {
		http.Error(w, "the signature does not cover this request", http.StatusForbidden)
		return
	}

	expires, err := strconv.ParseInt(q.Get("expires"), 10, 64)
	if err != nil || time.Now().After(time.Unix(expires, 0)) {
		http.Error(w, "the URL has expired", http.StatusForbidden)
		return
	}

	length, err := strconv.ParseInt(q.Get("length"), 10, 64)
	if err != nil {
		http.Error(w, "the URL names no length", http.StatusForbidden)
		return
	}
	if r.ContentLength != length {
		http.Error(w, "the body is not the length the URL was signed for", http.StatusForbidden)
		return
	}
	if r.Header.Get("Content-MD5") != q.Get("md5") {
		http.Error(w, "the declared digest is not the one the URL was signed for", http.StatusForbidden)
		return
	}
	if r.Header.Get("Content-Type") != q.Get("type") {
		http.Error(w, "the media type is not the one the URL was signed for", http.StatusForbidden)
		return
	}

	// Read one byte more than was signed for, so a body longer than its own
	// Content-Length is caught rather than truncated into a valid object.
	data, err := io.ReadAll(io.LimitReader(r.Body, length+1))
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if int64(len(data)) != length {
		http.Error(w, "the body is not the length the URL was signed for", http.StatusForbidden)
		return
	}

	sum := md5.Sum(data)
	if base64.StdEncoding.EncodeToString(sum[:]) != q.Get("md5") {
		http.Error(w, "the body is not the content the URL was signed for", http.StatusForbidden)
		return
	}

	if err := b.Put(r.Context(), key, data); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
}

// newUploadSecret draws the key [DirBucket.SignUpload] signs with.
func newUploadSecret() ([]byte, error) {
	secret := make([]byte, sha256.Size)
	if _, err := rand.Read(secret); err != nil {
		return nil, fmt.Errorf("protovcs: drawing an upload signing key: %w", err)
	}
	return secret, nil
}
