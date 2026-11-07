package gcsemulator

import (
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"errors"
	"fmt"
	"net/http"
)

// Header names Cloud Storage carries a customer-supplied encryption key in. The
// Go client sets all three together on every object read, write, and metadata
// request made through a handle that [cloud.google.com/go/storage.ObjectHandle.Key]
// was called on.
const (
	encryptionAlgorithmHeader = "x-goog-encryption-algorithm"
	encryptionKeyHeader       = "x-goog-encryption-key"
	encryptionKeyHashHeader   = "x-goog-encryption-key-sha256"
)

// aes256Algorithm is the only algorithm Cloud Storage accepts for a
// customer-supplied key.
const aes256Algorithm = "AES256"

// customerKeySize is the length of a customer-supplied AES-256 key in bytes.
const customerKeySize = 32

// errNoCustomerKey reports a request that carried no customer-supplied key
// headers, which is the ordinary case.
var errNoCustomerKey = errors.New("gcsemulator: request carries no customer-supplied encryption key")

// customerKey is the customer-supplied encryption key one request presented,
// reduced to the digest the emulator stores.
//
// The emulator keeps the digest and discards the key, which is what Cloud
// Storage does: the key is used for the operation in hand and never written
// down, so an object can only be read again by a request presenting the same
// key. That property is the whole of what this file models, and it is what
// makes a test able to tell an object stored under a client's key apart from
// one stored in the clear.
type customerKey struct {
	// hash is the base64 SHA-256 digest of the raw key, as the wire carries it.
	hash string
}

// readCustomerKey extracts the customer-supplied encryption key headers from a
// request.
//
// It returns errNoCustomerKey when no key was presented. It returns a
// descriptive error, which callers answer with 400, when the headers are there
// and wrong: an algorithm other than AES256, a key that is not base64, a key
// that is not 32 bytes, or a digest that is not the digest of the key. Cloud
// Storage rejects every one of those the same way, and a fake that accepted
// them would let a caller ship a bug that only appears against the real API.
func readCustomerKey(r *http.Request) (customerKey, error) {
	raw := r.Header.Get(encryptionKeyHeader)
	if raw == "" {
		return customerKey{}, errNoCustomerKey
	}
	if algorithm := r.Header.Get(encryptionAlgorithmHeader); algorithm != aes256Algorithm {
		return customerKey{}, fmt.Errorf("encryption algorithm %q is not %s", algorithm, aes256Algorithm)
	}
	key, err := base64.StdEncoding.DecodeString(raw)
	if err != nil {
		return customerKey{}, errors.New("encryption key is not base64")
	}
	if len(key) != customerKeySize {
		return customerKey{}, fmt.Errorf("encryption key is %d bytes, want %d", len(key), customerKeySize)
	}
	sum := sha256.Sum256(key)
	hash := base64.StdEncoding.EncodeToString(sum[:])
	if presented := r.Header.Get(encryptionKeyHashHeader); presented != hash {
		return customerKey{}, errors.New("encryption key does not match the presented SHA-256 digest")
	}
	return customerKey{hash: hash}, nil
}

// authorize reports whether a request may act on an object that was stored
// under storedHash, and explains the refusal when it may not.
//
// The two mismatches are both refusals, matching Cloud Storage:
//
//   - An object stored under a customer key, reached by a request that presents
//     no key or a different one, cannot be read. This is the property a test
//     relies on to show that an object really was written with the client's key.
//   - An object stored without one, reached by a request that presents a key, is
//     refused rather than served. Serving it would hide a bug in which a key
//     reaches a repository that does not use one.
func (k customerKey) authorize(storedHash string) error {
	switch {
	case storedHash == "" && k.hash == "":
		return nil
	case storedHash == "" && k.hash != "":
		return errors.New("an encryption key was supplied for an object that is not customer-encrypted")
	case storedHash != "" && k.hash == "":
		return errors.New("the object is encrypted by a customer-supplied key, which the request did not present")
	case subtle.ConstantTimeCompare([]byte(storedHash), []byte(k.hash)) != 1:
		return errors.New("the object is encrypted by a different customer-supplied key")
	default:
		return nil
	}
}

// checkCustomerKey reads the request's key headers and checks them against the
// key an object was stored under, writing a 400 and reporting false when either
// step fails.
//
// It is the single gate every object read goes through, so a route that forgets
// it is a route that hands out plaintext.
func checkCustomerKey(w http.ResponseWriter, r *http.Request, storedHash string) bool {
	key, err := readCustomerKey(r)
	if err != nil && !errors.Is(err, errNoCustomerKey) {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return false
	}
	if err := key.authorize(storedHash); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return false
	}
	return true
}

// ObjectKeySHA256 returns the base64 SHA-256 digest of the customer-supplied
// encryption key an object was written with, and whether the object exists.
//
// The digest is empty for an object stored without a customer key. A test uses
// this to assert that objects a server wrote really did carry the client's key,
// rather than inferring it from the absence of an error.
func (s *Server) ObjectKeySHA256(bucket, name string) (string, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	obj, ok := s.objects[fmt.Sprintf("%s/%s", bucket, name)]
	if !ok {
		return "", false
	}
	return obj.keyHash, true
}
