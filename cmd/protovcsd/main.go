// Command protovcsd serves protovcs stores.
//
// From a directory, which allows one process, or from a Cloud Storage bucket,
// which allows as many as there are. A compare-and-swap needs the storage layer
// to arbitrate it, and a POSIX directory cannot.
//
// It reads nothing it stores. Every chunk arrives sealed and leaves sealed, and
// the key lives on whichever client sent it.
//
// # Who may reach it
//
// A store id is not a secret, so a server reachable by more than one person
// needs a token key: the public half of an Ed25519 pair whose private half is
// held by whatever authenticated the user. Requests carry a bearer token naming
// a store and an access level, and this process verifies them and cannot mint
// them. -open serves every request without a check, for a server whose only
// client is the person running it.
//
// # Collecting garbage
//
// A store keeps chunks nothing reaches once a branch is rewritten. This process
// sweeps every store on a timer, every -sweep_interval. A chunk is deleted on
// the second sweep that finds it unreachable, at least an hour after the first.
package main

import (
	"context"
	"encoding/base64"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/nextgui/protovcs/connectserver"
	"github.com/nextgui/protovcs/objectstorage"
	"github.com/nextgui/protovcs/objectstorage/gcsbucket"
	"github.com/nextgui/protovcs/server"
	"github.com/nextgui/protovcs/storetoken"
)

var (
	addr     = flag.String("addr", ":8080", "address to listen on")
	dir      = flag.String("dir", "", "directory to keep stores in; one process only")
	bucket   = flag.String("bucket", "", "Cloud Storage bucket to keep stores in; allows several processes")
	tokenKey = flag.String("token_key", "", "file holding the base64 Ed25519 public key that store tokens are signed with")
	open     = flag.Bool("open", false, "serve every request without checking a token; for a single-user server only")
	origins  = flag.String("allow_origin", "*", "comma-separated page origins allowed to call this server from a browser; * for any")
	sweep    = flag.Duration("sweep_interval", 6*time.Hour, "how often to collect garbage in every store; 0 to never")
	retain   = flag.Duration("deletion_retention", server.DeletionRetention, "how long a deleted store can be restored before collection purges it; 0 to purge on the next collection")
)

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	if err := run(logger); err != nil {
		logger.Error("protovcsd", "err", err)
		os.Exit(1)
	}
}

func run(logger *slog.Logger) error {
	flag.Parse()
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	storage, closeStorage, err := openStorage(ctx)
	if err != nil {
		return err
	}
	defer closeStorage()
	auth, err := authenticator()
	if err != nil {
		return err
	}

	srv := server.New(storage).WithDeletionRetention(*retain)
	mux := http.NewServeMux()
	if err := connectserver.Mount(mux, srv, auth); err != nil {
		return err
	}
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })

	if *sweep > 0 {
		go connectserver.Sweep(ctx, srv, *sweep, func(res server.SweepResult, err error) {
			if err != nil {
				logger.Error("sweep", "err", err)
				return
			}
			logger.Info("sweep", "stores", len(res.Stores), "failed", len(res.Failed()))
		})
	}

	allowed := strings.Split(*origins, ",")
	for i := range allowed {
		allowed[i] = strings.TrimSpace(allowed[i])
	}
	hs := &http.Server{
		Addr:              *addr,
		Handler:           connectserver.Browsable(mux, allowed),
		ReadHeaderTimeout: 10 * time.Second,
		// Long enough for a slow client to upload the largest request. No
		// write timeout: a ref watch is a response that never ends.
		ReadTimeout: 10 * time.Minute,
		IdleTimeout: 2 * time.Minute,
	}
	go func() {
		<-ctx.Done()
		shutdown, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		hs.Shutdown(shutdown)
	}()
	logger.Info("serving", "addr", *addr)
	if err := hs.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}

// openStorage opens whichever backend was named. Both, or neither, is an error:
// naming two is asking for two answers to which one arbitrates a
// compare-and-swap.
func openStorage(ctx context.Context) (*objectstorage.Storage, func(), error) {
	switch {
	case *dir != "" && *bucket != "":
		return nil, nil, errors.New("protovcsd: -dir and -bucket were both given; a store lives in one place")
	case *dir == "" && *bucket == "":
		return nil, nil, errors.New("protovcsd: pass -dir or -bucket to say where stores are kept")
	case *dir != "":
		b, err := objectstorage.OpenDir(*dir)
		if err != nil {
			return nil, nil, fmt.Errorf("protovcsd: %w", err)
		}
		return objectstorage.New(b), func() { b.Close() }, nil
	}
	b, err := gcsbucket.Open(ctx, *bucket)
	if err != nil {
		return nil, nil, fmt.Errorf("protovcsd: %w", err)
	}
	return objectstorage.New(b), func() { b.Close() }, nil
}

// authenticator refuses to start without being told whether to check tokens.
// A default of open serves everybody's store to anybody; a default of closed
// serves nothing and looks like a bug.
func authenticator() (connectserver.Authenticator, error) {
	switch {
	case *open && *tokenKey != "":
		return nil, errors.New("protovcsd: -token_key and -open were both given")
	case *open:
		return connectserver.Open(), nil
	case *tokenKey == "":
		return nil, errors.New("protovcsd: pass -token_key to check store tokens, or -open to serve every request without checking")
	}
	raw, err := os.ReadFile(*tokenKey)
	if err != nil {
		return nil, fmt.Errorf("protovcsd: reading the token key: %w", err)
	}
	key, err := base64.StdEncoding.DecodeString(strings.TrimSpace(string(raw)))
	if err != nil {
		return nil, fmt.Errorf("protovcsd: the token key is not base64: %w", err)
	}
	verifier, err := storetoken.NewVerifier(key)
	if err != nil {
		return nil, fmt.Errorf("protovcsd: %w", err)
	}
	return connectserver.Tokens(verifier), nil
}
