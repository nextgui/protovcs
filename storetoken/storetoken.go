// Package storetoken decides who may touch a protovcs store.
//
// # Why a store needs this at all
//
// A protovcs server cannot read what it stores. That is a statement about
// confidentiality and it says nothing about who may destroy it: encryption does
// not stop a delete, a branch rolled back to last month, or a quota exhausted by
// somebody who learned a store id. A store id appears in a URL path and in a
// log; it is not a secret and cannot be made into one.
//
// So a request has to carry proof that its sender is allowed to touch the store
// it names. That proof is a token.
//
// # Why it is signed rather than looked up
//
// A server that checked a token against a database would need that database
// available on every read of every chunk, which is the hottest path there is.
// A signature is checked with a public key and nothing else.
//
// The cost is that nothing is stored, so nothing can be struck off. A leaked
// token works until it expires, which is why [DefaultLifetime] is short and why
// an issuer should mint per session rather than per account.
//
// # Why Ed25519 rather than an HMAC
//
// The party that mints and the party that verifies are different, and they are
// in different trust domains. With a shared secret the storage server could mint
// tokens for any store, so anyone who read that secret off it could reach every
// other store through the ordinary API from anywhere.
//
// A storage server can already delete the bytes it physically holds — that is
// not something a token scheme can fix. What this does fix is the difference
// between physical control of one machine and remote access to everything. The
// server holds a public key, verifies, and cannot forge.
//
// # What is not here
//
// The key that encrypts a store's contents. It is derived from the user's own
// unlock material, never leaves the client, and appears nowhere in this package.
// A token grants access to ciphertext.
package storetoken

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"fmt"
	"strings"
	"time"

	"google.golang.org/protobuf/proto"

	pb "github.com/nextgui/protovcs/proto/protovcs/v1"
)

// DefaultLifetime is how long a minted token is good for.
//
// Short because nothing can revoke one: a leaked token works until it expires,
// so the exposure is this long and no shorter. Long enough that a session does
// not spend its time reminting, and an issuer that wants a different figure
// passes one.
const DefaultLifetime = time.Hour

// SeedLen is the length of an Ed25519 seed, which is what an issuer is
// configured with.
const SeedLen = ed25519.SeedSize

// PublicKeyLen is the length of the key a verifier is configured with.
const PublicKeyLen = ed25519.PublicKeySize

var (
	// ErrMalformed is returned for anything that is not a token: wrong shape,
	// undecodable, or a payload that does not parse.
	//
	// Distinct from a bad signature so that a caller sending the wrong header
	// entirely is told that rather than being told it was tampered with.
	ErrMalformed = errors.New("storetoken: not a token")

	// ErrBadSignature is returned when a token does not verify under the
	// configured key. Either forged, or minted by somebody else.
	ErrBadSignature = errors.New("storetoken: the signature does not verify")

	// ErrExpired is returned for a token outside its validity window.
	//
	// Its own error because it is the one a caller can act on: remint and try
	// again. A forged token is not something a caller retries.
	ErrExpired = errors.New("storetoken: the token has expired")

	// ErrWrongStore is returned by [Claims.Allows] when a token is for a
	// different store than the request names.
	ErrWrongStore = errors.New("storetoken: the token is for another store")

	// ErrInsufficientAccess is returned when a token is for the right store and
	// does not allow what was asked.
	ErrInsufficientAccess = errors.New("storetoken: the token does not allow this")
)

// Issuer mints tokens. Held by whoever authenticates users.
type Issuer struct {
	key ed25519.PrivateKey
	now func() time.Time
}

// NewIssuer returns an issuer signing with the given Ed25519 seed.
//
// A seed rather than a full private key because a seed is what a secret manager
// holds well: 32 bytes, with the public half derived rather than stored
// alongside and able to disagree with it.
func NewIssuer(seed []byte) (*Issuer, error) {
	if len(seed) != SeedLen {
		return nil, fmt.Errorf("storetoken: the seed is %d bytes; it must be %d", len(seed), SeedLen)
	}
	return &Issuer{key: ed25519.NewKeyFromSeed(seed), now: time.Now}, nil
}

// SetClock replaces the issuer's clock, for tests.
func (i *Issuer) SetClock(now func() time.Time) { i.now = now }

// PublicKey returns the half a verifier is configured with.
func (i *Issuer) PublicKey() []byte {
	return append([]byte(nil), i.key.Public().(ed25519.PublicKey)...)
}

// Issue returns a token allowing access to one store for a bounded time.
func (i *Issuer) Issue(storeID string, access pb.Access, lifetime time.Duration) (string, error) {
	if storeID == "" {
		return "", fmt.Errorf("storetoken: a token needs a store id")
	}
	if access == pb.Access_ACCESS_UNSPECIFIED {
		return "", fmt.Errorf("storetoken: a token needs an access level; %v allows nothing", access)
	}
	if lifetime <= 0 {
		lifetime = DefaultLifetime
	}

	nonce := make([]byte, 8)
	if _, err := rand.Read(nonce); err != nil {
		return "", fmt.Errorf("storetoken: drawing a nonce: %w", err)
	}

	now := i.now().UTC()
	claim := &pb.StoreToken{
		StoreId: storeID,
		Access:  access,
		// A minute of slack before, so that a verifier whose clock is slightly
		// behind the minter's does not refuse a token issued for right now.
		NotBeforeUnix: now.Add(-time.Minute).Unix(),
		ExpiresUnix:   now.Add(lifetime).Unix(),
		Nonce:         nonce,
	}

	// Deterministic marshalling is not enough here: a verifier reads the bytes
	// that were signed rather than re-encoding the message, so what is signed
	// and what is checked are the same bytes by construction.
	body, err := proto.Marshal(claim)
	if err != nil {
		return "", fmt.Errorf("storetoken: encoding the claim: %w", err)
	}
	sig := ed25519.Sign(i.key, body)

	return encode(body) + "." + encode(sig), nil
}

// Verifier checks tokens. Held by the protovcs server, and holding only the
// public half, so it can check a token and cannot make one.
type Verifier struct {
	key ed25519.PublicKey
	now func() time.Time
}

// NewVerifier returns a verifier for the given Ed25519 public key.
func NewVerifier(key []byte) (*Verifier, error) {
	if len(key) != PublicKeyLen {
		return nil, fmt.Errorf("storetoken: the public key is %d bytes; it must be %d", len(key), PublicKeyLen)
	}
	return &Verifier{key: ed25519.PublicKey(append([]byte(nil), key...)), now: time.Now}, nil
}

// SetClock replaces the verifier's clock, for tests.
func (v *Verifier) SetClock(now func() time.Time) { v.now = now }

// Claims is what a verified token says.
type Claims struct {
	StoreID string
	Access  pb.Access

	NotBefore time.Time
	Expires   time.Time
}

// Verify checks a token's signature and its validity window.
//
// It says what the token claims and not whether those claims cover a particular
// request. [Claims.Allows] is that, and it is separate because the two are asked
// at different points: the signature once per request, the store and the access
// once per method.
func (v *Verifier) Verify(token string) (Claims, error) {
	body, sig, ok := strings.Cut(token, ".")
	if !ok {
		return Claims{}, ErrMalformed
	}

	raw, err := decode(body)
	if err != nil {
		return Claims{}, ErrMalformed
	}
	rawSig, err := decode(sig)
	if err != nil {
		return Claims{}, ErrMalformed
	}

	// The signature first, before anything in the payload is believed. What
	// follows parses attacker-supplied bytes, and doing that after the check
	// means the parser is only ever fed things this key signed.
	if len(rawSig) != ed25519.SignatureSize || !ed25519.Verify(v.key, raw, rawSig) {
		return Claims{}, ErrBadSignature
	}

	claim := &pb.StoreToken{}
	if err := proto.Unmarshal(raw, claim); err != nil {
		return Claims{}, ErrMalformed
	}
	if claim.GetStoreId() == "" || claim.GetAccess() == pb.Access_ACCESS_UNSPECIFIED {
		return Claims{}, ErrMalformed
	}

	now := v.now().UTC()
	notBefore := time.Unix(claim.GetNotBeforeUnix(), 0).UTC()
	expires := time.Unix(claim.GetExpiresUnix(), 0).UTC()
	if claim.GetExpiresUnix() == 0 || !now.Before(expires) {
		return Claims{}, fmt.Errorf("%w at %s", ErrExpired, expires.Format(time.RFC3339))
	}
	if now.Before(notBefore) {
		return Claims{}, fmt.Errorf("%w: it is not valid until %s", ErrExpired, notBefore.Format(time.RFC3339))
	}

	return Claims{
		StoreID:   claim.GetStoreId(),
		Access:    claim.GetAccess(),
		NotBefore: notBefore,
		Expires:   expires,
	}, nil
}

// Allows reports whether these claims cover a request against storeID needing
// at least the given access.
//
// Both halves in one call, because checking one and forgetting the other is the
// mistake worth designing out: a token for the right store with read access must
// not write, and a token with write access must not write to another store.
func (c Claims) Allows(storeID string, need pb.Access) error {
	if c.StoreID != storeID {
		return fmt.Errorf("%w: it is for %q", ErrWrongStore, c.StoreID)
	}
	if c.Access < need {
		return fmt.Errorf("%w: it allows %v and this needs %v", ErrInsufficientAccess, c.Access, need)
	}
	return nil
}

// encode and decode use raw URL-safe base64, so a token is a single header value
// with nothing needing escaping.
//
// Strict on the way in. Without it the decoder ignores the unused bits in a
// final character, so several distinct strings decode to the same token and
// every one of them verifies. Nothing is gained by an attacker there — the
// claims are identical — but a credential with more than one spelling is a
// credential a log, a cache or a rate limiter can be made to see as two.
func encode(b []byte) string { return base64.RawURLEncoding.EncodeToString(b) }

func decode(s string) ([]byte, error) { return base64.RawURLEncoding.Strict().DecodeString(s) }
