package connectserver

import (
	"net/http"

	connectcors "connectrpc.com/cors"
	"github.com/rs/cors"
)

// Browsable lets a page on one of origins call h. "*" allows any origin.
//
// Connect over HTTP sends headers a browser treats as non-simple, so every call
// from a tab is preceded by an OPTIONS the server has to answer. Without this a
// store is unreachable from a browser, and the fetch rejects with no status.
//
// Any origin is safe for a protovcs server. The only credential it accepts is a
// bearer token in a header, which a page has only because something handed it
// one, and it accepts no cookie. Credentials are off, so a wildcard origin is
// not an invitation to spend a user's ambient authority.
func Browsable(h http.Handler, origins []string) http.Handler {
	return cors.New(cors.Options{
		AllowedOrigins: origins,
		AllowedMethods: connectcors.AllowedMethods(),
		// Authorization carries the store token and is not a Connect header. The
		// two encoding headers are Connect's own, and a Go client compiled to
		// WebAssembly sends Connect-Accept-Encoding on every request. A header
		// the preflight does not allow fails the whole request before it is
		// sent.
		AllowedHeaders: append(connectcors.AllowedHeaders(),
			"Authorization", "Connect-Accept-Encoding", "Connect-Content-Encoding"),
		// Connect-Content-Encoding says whether a stream's messages are
		// compressed. Hidden from the page, the client builds no decompressor
		// and fails on the first compressed message.
		ExposedHeaders: append(connectcors.ExposedHeaders(),
			"Connect-Content-Encoding", "Connect-Accept-Encoding"),
	}).Handler(h)
}
