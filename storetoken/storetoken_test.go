package storetoken_test

import (
	"errors"
	"strings"
	"testing"
	"time"

	pb "github.com/nextgui/protovcs/proto/protovcs/v1"
	"github.com/nextgui/protovcs/storetoken"
)

// A token decides who may delete a store, so every test here is something a
// caller can do rather than an API misuse.

func newPair(t *testing.T) (*storetoken.Issuer, *storetoken.Verifier) {
	t.Helper()

	seed := make([]byte, storetoken.SeedLen)
	for i := range seed {
		seed[i] = byte(i + 3)
	}
	issuer, err := storetoken.NewIssuer(seed)
	if err != nil {
		t.Fatalf("NewIssuer: %v", err)
	}
	verifier, err := storetoken.NewVerifier(issuer.PublicKey())
	if err != nil {
		t.Fatalf("NewVerifier: %v", err)
	}
	return issuer, verifier
}

func TestATokenRoundTrips(t *testing.T) {
	issuer, verifier := newPair(t)

	token, err := issuer.Issue("store-1", pb.Access_ACCESS_WRITE, time.Hour)
	if err != nil {
		t.Fatalf("Issue: %v", err)
	}

	claims, err := verifier.Verify(token)
	if err != nil {
		t.Fatalf("Verify: %v", err)
	}
	if claims.StoreID != "store-1" || claims.Access != pb.Access_ACCESS_WRITE {
		t.Fatalf("claims are %+v", claims)
	}
	if err := claims.Allows("store-1", pb.Access_ACCESS_WRITE); err != nil {
		t.Fatalf("a write token does not allow a write: %v", err)
	}
}

// TestAVerifierCannotMint is the property Ed25519 buys over a shared secret.
//
// The storage server holds what a Verifier holds. If that were enough to make a
// token, reading it off one server would grant access to every store through the
// ordinary API from anywhere.
func TestAVerifierCannotMint(t *testing.T) {
	issuer, verifier := newPair(t)

	// A public key is the same length as a seed, so nothing stops somebody
	// feeding one to NewIssuer. What stops them is that the keypair it derives
	// is a different keypair: the tokens it mints do not verify.
	forger, err := storetoken.NewIssuer(issuer.PublicKey())
	if err != nil {
		t.Fatalf("NewIssuer: %v", err)
	}
	token, err := forger.Issue("store-1", pb.Access_ACCESS_ADMIN, time.Hour)
	if err != nil {
		t.Fatalf("Issue: %v", err)
	}
	if _, err := verifier.Verify(token); !errors.Is(err, storetoken.ErrBadSignature) {
		t.Fatalf("a token minted from the public key returned %v, want ErrBadSignature", err)
	}

	// The compile-time half of the property is that Verifier has no Issue
	// method at all, so a server holding one has nothing to mint with.
}

func TestATokenFromAnotherKeyIsRefused(t *testing.T) {
	_, verifier := newPair(t)

	other := make([]byte, storetoken.SeedLen)
	for i := range other {
		other[i] = byte(i * 11)
	}
	stranger, err := storetoken.NewIssuer(other)
	if err != nil {
		t.Fatalf("NewIssuer: %v", err)
	}
	token, err := stranger.Issue("store-1", pb.Access_ACCESS_ADMIN, time.Hour)
	if err != nil {
		t.Fatalf("Issue: %v", err)
	}

	if _, err := verifier.Verify(token); !errors.Is(err, storetoken.ErrBadSignature) {
		t.Fatalf("a token from another key returned %v, want ErrBadSignature", err)
	}
}

// TestEveryAlteredByteIsRefused is the whole point of a signature, asserted at
// every position rather than at one.
func TestEveryAlteredByteIsRefused(t *testing.T) {
	issuer, verifier := newPair(t)

	token, err := issuer.Issue("store-1", pb.Access_ACCESS_READ, time.Hour)
	if err != nil {
		t.Fatalf("Issue: %v", err)
	}

	for i := 0; i < len(token); i++ {
		altered := []byte(token)
		// Any different character in the alphabet; the point is that it differs.
		if altered[i] == 'A' {
			altered[i] = 'B'
		} else {
			altered[i] = 'A'
		}
		if _, err := verifier.Verify(string(altered)); err == nil {
			t.Fatalf("altering byte %d produced a token that verified", i)
		}
	}
}

func TestAnExpiredTokenIsRefusedDistinctly(t *testing.T) {
	issuer, verifier := newPair(t)

	base := time.Date(2026, 8, 22, 12, 0, 0, 0, time.UTC)
	issuer.SetClock(func() time.Time { return base })

	token, err := issuer.Issue("store-1", pb.Access_ACCESS_WRITE, time.Hour)
	if err != nil {
		t.Fatalf("Issue: %v", err)
	}

	// Still good half an hour in.
	verifier.SetClock(func() time.Time { return base.Add(30 * time.Minute) })
	if _, err := verifier.Verify(token); err != nil {
		t.Fatalf("a token inside its window was refused: %v", err)
	}

	// Not good two hours in, and the caller is told which failure it is: an
	// expired token is reminted and retried, a forged one is not.
	verifier.SetClock(func() time.Time { return base.Add(2 * time.Hour) })
	if _, err := verifier.Verify(token); !errors.Is(err, storetoken.ErrExpired) {
		t.Fatalf("an expired token returned %v, want ErrExpired", err)
	}

	// And a clock far enough behind refuses rather than accepting.
	verifier.SetClock(func() time.Time { return base.Add(-time.Hour) })
	if _, err := verifier.Verify(token); !errors.Is(err, storetoken.ErrExpired) {
		t.Fatalf("a token used before its window returned %v, want ErrExpired", err)
	}
}

// TestATokenIsForOneStore is the failure a scheme without this check has: a user
// with a valid token for their own vault reading somebody else's.
func TestATokenIsForOneStore(t *testing.T) {
	issuer, verifier := newPair(t)

	token, err := issuer.Issue("mine", pb.Access_ACCESS_ADMIN, time.Hour)
	if err != nil {
		t.Fatalf("Issue: %v", err)
	}
	claims, err := verifier.Verify(token)
	if err != nil {
		t.Fatalf("Verify: %v", err)
	}

	if err := claims.Allows("theirs", pb.Access_ACCESS_READ); !errors.Is(err, storetoken.ErrWrongStore) {
		t.Fatalf("an admin token for another store returned %v, want ErrWrongStore", err)
	}
}

// TestAccessIsAnOrdering is why Access is a level rather than a set: granting
// write must not take read away.
func TestAccessIsAnOrdering(t *testing.T) {
	issuer, verifier := newPair(t)

	for _, tc := range []struct {
		held  pb.Access
		need  pb.Access
		allow bool
	}{
		{pb.Access_ACCESS_READ, pb.Access_ACCESS_READ, true},
		{pb.Access_ACCESS_READ, pb.Access_ACCESS_WRITE, false},
		{pb.Access_ACCESS_READ, pb.Access_ACCESS_ADMIN, false},
		{pb.Access_ACCESS_WRITE, pb.Access_ACCESS_READ, true},
		{pb.Access_ACCESS_WRITE, pb.Access_ACCESS_WRITE, true},
		{pb.Access_ACCESS_WRITE, pb.Access_ACCESS_ADMIN, false},
		{pb.Access_ACCESS_ADMIN, pb.Access_ACCESS_READ, true},
		{pb.Access_ACCESS_ADMIN, pb.Access_ACCESS_WRITE, true},
		{pb.Access_ACCESS_ADMIN, pb.Access_ACCESS_ADMIN, true},
	} {
		token, err := issuer.Issue("store-1", tc.held, time.Hour)
		if err != nil {
			t.Fatalf("Issue: %v", err)
		}
		claims, err := verifier.Verify(token)
		if err != nil {
			t.Fatalf("Verify: %v", err)
		}

		err = claims.Allows("store-1", tc.need)
		if tc.allow && err != nil {
			t.Errorf("%v should allow %v: %v", tc.held, tc.need, err)
		}
		if !tc.allow && !errors.Is(err, storetoken.ErrInsufficientAccess) {
			t.Errorf("%v allowing %v returned %v, want ErrInsufficientAccess", tc.held, tc.need, err)
		}
	}
}

// TestNothingIsATokenUntilItVerifies covers the shapes a server is sent by
// somebody who is not a client at all. None may panic and none may pass.
func TestNothingIsATokenUntilItVerifies(t *testing.T) {
	_, verifier := newPair(t)

	for _, s := range []string{
		"", ".", "..", "a.b", "not-base64!.also-not",
		strings.Repeat("A", 4096), "AAAA", "AAAA.AAAA",
	} {
		if _, err := verifier.Verify(s); err == nil {
			t.Fatalf("%q verified", s)
		}
	}
}

// TestATokenAllowingNothingIsRefusedAtTheMint stops a caller minting one by
// leaving the access field out and discovering later that it grants nothing.
func TestATokenAllowingNothingIsRefusedAtTheMint(t *testing.T) {
	issuer, _ := newPair(t)

	if _, err := issuer.Issue("store-1", pb.Access_ACCESS_UNSPECIFIED, time.Hour); err == nil {
		t.Fatal("a token with no access level was minted")
	}
	if _, err := issuer.Issue("", pb.Access_ACCESS_WRITE, time.Hour); err == nil {
		t.Fatal("a token with no store was minted")
	}
}

// TestTwoTokensDiffer is what the nonce is for: two sessions opened in the same
// second are still distinguishable in a log.
func TestTwoTokensDiffer(t *testing.T) {
	issuer, _ := newPair(t)
	issuer.SetClock(func() time.Time { return time.Date(2026, 8, 22, 12, 0, 0, 0, time.UTC) })

	first, err := issuer.Issue("store-1", pb.Access_ACCESS_WRITE, time.Hour)
	if err != nil {
		t.Fatalf("Issue: %v", err)
	}
	second, err := issuer.Issue("store-1", pb.Access_ACCESS_WRITE, time.Hour)
	if err != nil {
		t.Fatalf("Issue: %v", err)
	}
	if first == second {
		t.Fatal("two tokens issued in the same second are identical")
	}
}
