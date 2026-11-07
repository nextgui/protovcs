package connectserver

import (
	"errors"
	"fmt"
	"net/http"
	"strings"

	pb "github.com/nextgui/protovcs/proto/protovcs/v1"
	"github.com/nextgui/protovcs/storetoken"
)

// ErrDenied wraps every refusal [Allow] and [AllowEveryStore] return.
var ErrDenied = errors.New("protovcs: access denied")

// Authenticator turns a request's headers into the claims its access is
// checked against.
//
// It establishes what a request claims and decides nothing. A request with no
// token, or one that does not verify, is an error, and the handler then checks
// the method's access against empty claims, which allow nothing.
type Authenticator interface {
	Authenticate(h http.Header) (storetoken.Claims, error)
}

// Tokens returns an authenticator that verifies a bearer token in the
// Authorization header against verifier.
func Tokens(verifier *storetoken.Verifier) Authenticator {
	return tokens{verifier: verifier}
}

type tokens struct {
	verifier *storetoken.Verifier
}

func (t tokens) Authenticate(h http.Header) (storetoken.Claims, error) {
	if t.verifier == nil {
		return storetoken.Claims{}, errors.New("protovcs: no token verifier was configured")
	}
	raw := h.Get("Authorization")
	if raw == "" {
		return storetoken.Claims{}, errors.New("protovcs: the request carries no store token")
	}
	token, ok := strings.CutPrefix(raw, "Bearer ")
	if !ok {
		return storetoken.Claims{}, errors.New("protovcs: the Authorization header is not a bearer token")
	}
	return t.verifier.Verify(token)
}

// Open returns an authenticator that gives every request full access to
// whichever store it names.
//
// For a server whose only client is the person running it, reachable from
// nowhere else. A named constructor rather than a nil verifier, so that a
// deployment which meant to check tokens and does not is a visible line of
// code rather than a missing one.
func Open() Authenticator { return open{} }

type open struct{}

// Authenticate returns admin claims naming no store, which [Allow] reads as
// "whichever store was asked for". A minted token always names a store, so
// these claims exist only where a process was told to serve every request.
func (open) Authenticate(http.Header) (storetoken.Claims, error) {
	return storetoken.Claims{Access: pb.Access_ACCESS_ADMIN}, nil
}

// Allow reports whether claims cover a request against storeID needing at
// least the given access.
//
// Both halves in one call, because checking one and forgetting the other is the
// mistake worth designing out: a token for the right store with read access must
// not write, and a token with write access must not write to another store.
func Allow(c storetoken.Claims, storeID string, need pb.Access) error {
	if c.Access == pb.Access_ACCESS_UNSPECIFIED {
		return fmt.Errorf("%w: the request carries no store token", ErrDenied)
	}
	if storeID == "" {
		return fmt.Errorf("%w: the request names no store", ErrDenied)
	}

	// An empty store id on the claims is the open server's, and means
	// whichever store was asked for. A minted token always names one.
	if c.StoreID != "" && c.StoreID != storeID {
		return fmt.Errorf("%w: the token is for another store", ErrDenied)
	}
	if c.Access < need {
		return fmt.Errorf("%w: the token allows %v and this needs %v", ErrDenied, c.Access, need)
	}
	return nil
}

// AllowEveryStore reports whether claims cover a method that names no store,
// which is only ListStores.
//
// A store token names one store, so no token grants this and it is always
// refused. Only the open server allows it: listing every store is an
// operator's question, and an operator has the storage.
func AllowEveryStore(c storetoken.Claims) error {
	if c.StoreID == "" && c.Access == pb.Access_ACCESS_ADMIN {
		return nil
	}
	return fmt.Errorf("%w: a store token grants one store, and this asks about every store", ErrDenied)
}
