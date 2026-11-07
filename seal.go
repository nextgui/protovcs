package protovcs

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/hkdf"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"errors"
	"fmt"
)

// Sealer names chunks and protects their contents.
//
// Two implementations. [NewKeyring] is the real one and is what a store on a
// server uses. [PlainSealer] does neither, and exists for local development,
// for tests, and for exporting a store to a directory somebody can read.
//
// Sealing and filing are independent choices. A backend may store a chunk under
// its name or under the item key it holds; that is the backend's decision and
// this interface does not know about it.
type Sealer interface {
	// ChunkID names a chunk. The same role, key and bytes always produce the
	// same name, which is what lets a writer skip uploading what the store
	// already has and a reader skip downloading what it already holds.
	ChunkID(role Role, key string, canonical []byte) ChunkID

	// Seal protects a chunk's contents. The id is bound in, so a chunk cannot
	// be served under another name.
	Seal(id ChunkID, plaintext []byte) ([]byte, error)

	// Open reverses Seal, and fails if the bytes are not what this id names.
	Open(id ChunkID, sealed []byte) ([]byte, error)

	// RefName is what a branch is stored under. A branch name is something a
	// person chose, so the real one does not reach the server.
	RefName(branch string) []byte

	// Sealed reports whether contents are protected. A store that expected
	// protection and opened one that has none must refuse rather than carry on,
	// so this is asked rather than assumed.
	Sealed() bool
}

// Key derivation labels.
//
// Deriving means putting the root key and one of these labels through a one-way
// function and using the result. Each label yields a different key, and none of
// them can be worked back to the root or to each other.
//
// One label per job. Reusing a single key for naming and for encryption would
// mean a fingerprint and a ciphertext were computed from the same secret, and
// whether those two uses interact is an analysis nobody should have to do.
const (
	labelName  = "protovcs/v1/chunk-name"
	labelChunk = "protovcs/v1/chunk-content"
	labelRef   = "protovcs/v1/ref-name"
)

// RootKeyLen is the length of the key a store is opened with.
const RootKeyLen = 32

type keyring struct {
	name  []byte
	chunk []byte
	ref   []byte
	aead  cipher.AEAD
}

// NewKeyring derives a store's keys from its root key.
//
// The root key never leaves the client and is not stored anywhere protovcs
// controls. Where it comes from is the caller's problem: an application
// might unwrap it from the user's unlock material.
//
// Every other key is derived from it: the root key and a short label go through
// a one-way function, and the result is a key of its own. HKDF is that function
// — the standard one for turning a single secret into several independent ones,
// and in Go's standard library. A derived key cannot be worked back to the root
// or to any of its siblings.
//
// Derived per store rather than per user, so two stores holding identical data
// share no names and nothing correlates between them.
func NewKeyring(rootKey []byte) (Sealer, error) {
	if len(rootKey) != RootKeyLen {
		return nil, fmt.Errorf("protovcs: a root key is %d bytes, got %d", RootKeyLen, len(rootKey))
	}

	derive := func(label string) ([]byte, error) {
		return hkdf.Key(sha256.New, rootKey, nil, label, 32)
	}

	k := &keyring{}
	var err error
	if k.name, err = derive(labelName); err != nil {
		return nil, fmt.Errorf("protovcs: deriving the naming key: %w", err)
	}
	if k.chunk, err = derive(labelChunk); err != nil {
		return nil, fmt.Errorf("protovcs: deriving the content key: %w", err)
	}
	if k.ref, err = derive(labelRef); err != nil {
		return nil, fmt.Errorf("protovcs: deriving the ref key: %w", err)
	}

	block, err := aes.NewCipher(k.chunk)
	if err != nil {
		return nil, fmt.Errorf("protovcs: preparing the cipher: %w", err)
	}
	if k.aead, err = cipher.NewGCM(block); err != nil {
		return nil, fmt.Errorf("protovcs: preparing the cipher: %w", err)
	}
	return k, nil
}

func (k *keyring) Sealed() bool { return true }

func (k *keyring) ChunkID(role Role, key string, canonical []byte) ChunkID {
	mac := hmac.New(sha256.New, k.name)
	writeNamed(mac, role, key, canonical)
	var id ChunkID
	copy(id[:], mac.Sum(nil))
	return id
}

func (k *keyring) RefName(branch string) []byte {
	mac := hmac.New(sha256.New, k.ref)
	mac.Write([]byte(branch))
	return mac.Sum(nil)
}

// nonceLen is twelve bytes, the size AES-GCM is specified for.
const nonceLen = 12

func (k *keyring) Seal(id ChunkID, plaintext []byte) ([]byte, error) {
	// Drawn fresh every time. A chunk is written once and never rewritten,
	// because its name is a fingerprint of its contents, so a nonce is never
	// used twice with this key even though it is chosen at random.
	nonce := make([]byte, nonceLen)
	if _, err := rand.Read(nonce); err != nil {
		return nil, fmt.Errorf("protovcs: drawing a nonce: %w", err)
	}

	out := make([]byte, 0, 1+nonceLen+len(plaintext)+k.aead.Overhead())
	out = append(out, FormatVersion)
	out = append(out, nonce...)

	// The chunk's name is the additional data, so the bytes only open under the
	// name they were stored as. A server that swaps two chunks produces a
	// failure rather than two files that both look genuine.
	return k.aead.Seal(out, nonce, plaintext, id[:]), nil
}

func (k *keyring) Open(id ChunkID, sealed []byte) ([]byte, error) {
	if len(sealed) < 1+nonceLen+k.aead.Overhead() {
		return nil, fmt.Errorf("protovcs: chunk %s is too short to be a sealed chunk", id)
	}
	// The version is checked before anything else is read. If it came later, a
	// build meeting a newer layout would already have taken some other field's
	// bytes and used them as the nonce.
	if sealed[0] != FormatVersion {
		return nil, fmt.Errorf("%w: chunk %s is format %d", ErrUnsupportedFormat, id, sealed[0])
	}

	nonce := sealed[1 : 1+nonceLen]
	plaintext, err := k.aead.Open(nil, nonce, sealed[1+nonceLen:], id[:])
	if err != nil {
		return nil, fmt.Errorf("%w: chunk %s", ErrTampered, id)
	}
	return plaintext, nil
}

// PlainSealer names chunks without a key and stores their contents as they are.
//
// For local development, for tests, and for exporting a store into a directory
// a person can read. Combined with a backend that files a chunk under its item
// key rather than its name, a store on disk becomes an ordinary tree of files
// with its metadata beside it — which is how a store is exported to git.
//
// It is not a degraded mode of the real one. A store either has a key or does
// not, and one that does not is a different object rather than the same object
// with protection switched off.
func PlainSealer() Sealer { return plainSealer{} }

type plainSealer struct{}

func (plainSealer) Sealed() bool { return false }

func (plainSealer) ChunkID(role Role, key string, canonical []byte) ChunkID {
	h := sha256.New()
	writeNamed(h, role, key, canonical)
	var id ChunkID
	copy(id[:], h.Sum(nil))
	return id
}

func (plainSealer) RefName(branch string) []byte { return []byte(branch) }

func (plainSealer) Seal(id ChunkID, plaintext []byte) ([]byte, error) {
	out := make([]byte, 0, 1+len(plaintext))
	out = append(out, FormatVersion)
	return append(out, plaintext...), nil
}

func (plainSealer) Open(id ChunkID, sealed []byte) ([]byte, error) {
	if len(sealed) < 1 {
		return nil, fmt.Errorf("protovcs: chunk %s is empty", id)
	}
	if sealed[0] != FormatVersion {
		return nil, fmt.Errorf("%w: chunk %s is format %d", ErrUnsupportedFormat, id, sealed[0])
	}
	// Nothing here can detect a substitution, because nothing is authenticated.
	// Recomputing the name would need the role and item key, which Open is not
	// given. A plaintext store is one whose backend is trusted; that is what
	// choosing it means.
	return sealed[1:], nil
}

// writeNamed feeds the parts of a chunk's identity to a hash, separated so that
// no two different triples can produce the same input.
//
// Length-prefixing the role and the key rather than joining them with a
// separator means an item key containing the separator cannot be made to look
// like a different role and key.
func writeNamed(h interface{ Write([]byte) (int, error) }, role Role, key string, canonical []byte) {
	var lengths [16]byte
	n := putUvarint(lengths[:], uint64(len(role)))
	h.Write(lengths[:n])
	h.Write([]byte(role))

	n = putUvarint(lengths[:], uint64(len(key)))
	h.Write(lengths[:n])
	h.Write([]byte(key))

	h.Write(canonical)
}

func putUvarint(buf []byte, x uint64) int {
	i := 0
	for x >= 0x80 {
		buf[i] = byte(x) | 0x80
		x >>= 7
		i++
	}
	buf[i] = byte(x)
	return i + 1
}

// VerifyChunk reports whether sealed bytes really are the chunk this id names.
//
// Open already refuses a chunk that does not authenticate, so this is for the
// paths where the plaintext is recomputed anyway and the two can be compared
// directly: reading back a store to check an export, and the tests that assert
// a hostile server cannot substitute anything.
func VerifyChunk(s Sealer, role Role, key string, id ChunkID, canonical []byte) error {
	want := s.ChunkID(role, key, canonical)
	if subtle.ConstantTimeCompare(want[:], id[:]) != 1 {
		return fmt.Errorf("%w: %s holds the contents of %s", ErrTampered, id, want)
	}
	return nil
}

// ErrNoKey is returned when an operation needs a key and the store was opened
// without one.
var ErrNoKey = errors.New("protovcs: this store was opened without a key")
