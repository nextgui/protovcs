# protovcs

[![CI](https://github.com/nextgui/protovcs/actions/workflows/ci.yml/badge.svg)](https://github.com/nextgui/protovcs/actions/workflows/ci.yml)
[![Go Reference](https://pkg.go.dev/badge/github.com/nextgui/protovcs.svg)](https://pkg.go.dev/github.com/nextgui/protovcs)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)

A version-controlled store for structured data. Like git, but the unit is a typed protobuf message and the server never sees the bytes in the clear.

protovcs stores one protobuf message, such as an application's whole document or database, as a history of commits on branches. Clients encrypt everything before it leaves the device. A client can check out part of a message, so the message can grow far larger than one request or one process could hold.

## Example

An application's data is one protobuf message:

```proto
// notes.proto
syntax = "proto3";
package example.notes.v1;

import "google/protobuf/timestamp.proto";

message Notebook {
  string title = 1;
  repeated Note notes = 2;
}

message Note {
  string id = 1;
  string body = 2;
  google.protobuf.Timestamp created = 3;
}
```

A layout says how to store it. This one keeps each note as its own item, so changing one note uploads only that note:

```textproto
# notebook.layout.textproto
name: "notebook"
root_message: "example.notes.v1.Notebook"
schema_version: 1
splits { field: "notes" by { field: "id" transform: TRANSFORM_IDENTITY } }
```

`protovcsgen` turns the layout into a typed Go package. Open a store, then read and change the message like a local value:

```go
store, err := notebookvcs.Open(protovcs.NewBuilder().
	WithTransport(connecttransport.New(http.DefaultClient, serverURL, token)).
	WithSealer(keyring))
if err != nil {
	return err
}
main := store.Branch("main")

// Change re-runs the function on the latest version if another client wrote first.
id, err := main.Change(ctx, notebookvcs.Meta{Title: "Add a note"},
	func(ctx context.Context, nb *notesv1.Notebook) error {
		nb.Notes = append(nb.Notes, &notesv1.Note{Id: "n1", Body: "Buy milk", Created: timestamppb.Now()})
		return nil
	})
if err != nil {
	return fmt.Errorf("adding a note: %w", err)
}

nb, err := main.Read(ctx)
if err != nil {
	return err
}

history, err := main.Log(ctx, protovcs.LogOptions{Limit: 20})
if err != nil {
	return err
}

// Called whenever another client moves the branch.
err = main.Watch(ctx, func(protovcs.RefState) error { return reload(ctx) })
```

## Messages larger than memory

A protobuf message is normally read and written whole. A protovcs message can be read whole or in part. The layout splits a large repeated field into items, each stored as its own chunk, and a client checks out only the items it needs:

```textproto
# ledger.layout.textproto: one item per day of entries
name: "ledger"
root_message: "example.ledger.v1.Ledger"
schema_version: 1
splits {
  field: "entries"
  by { field: "date" transform: TRANSFORM_DAY }
  max_item_bytes: 262144
}
```

```go
// Fetch one day. Nothing else is downloaded.
day, err := ledger.CheckoutEntries(ctx, ledgervcs.EntriesForDate(2026, 8, 14))
if err != nil {
	return err
}
for _, e := range day.All() {
	fmt.Println(e.GetDescription(), e.GetAmount())
}

// Change that day and commit. Every other day is carried over by reference.
aug14 := timestamppb.New(time.Date(2026, 8, 14, 9, 30, 0, 0, time.UTC))
if err := day.Put(ctx, &ledgerv1.Entry{Id: "e9", Date: aug14, Amount: -420}); err != nil {
	return err
}
if _, err := day.Commit(ctx, ledgervcs.Meta{Title: "Add a coffee"}); err != nil {
	return err
}
```

A commit made from a partial checkout is a complete version: items outside the checkout keep their existing chunks and are not uploaded again.

## Features

- **Client-side encryption.** Chunks are sealed with AES-GCM under keys derived from a root key the server never sees. Chunk and branch names are keyed hashes, so they don't reveal content either.
- **History.** Every change is a commit with authors, a title and an optional typed details message. `Log` lists them, and `Store.ReadAt` reads any earlier version.
- **Safe concurrent writes.** A branch moves by compare-and-swap. `Change` re-runs the edit on the latest version when another client wrote first, so no write is silently lost.
- **Change notification.** `Watch` reports when another client moves a branch.
- **Partial checkout.** Read and write one range of a large collection without the rest.
- **Tamper detection.** Commits are named by a hash of their contents. The client refuses a commit whose name doesn't match, and refuses a head older than one it has already seen on that branch.
- **Typed API.** `protovcsgen` generates a package for each layout: no casts, and one scope type per collection, so one collection's scope can't be passed for another's.
- **Go and WebAssembly.** The same client runs on servers, in command-line tools, and in the browser, with an IndexedDB chunk cache (`idbcache`).
- **A server to run.** `protovcsd` keeps stores in a directory or a Cloud Storage bucket, with per-store quotas and garbage collection.

## How it works

```
 message ──layout──▶ items ──canonical bytes──▶ sealed chunks ──▶ server
                                                     ▲
 commit ──▶ manifest tree naming the chunks ─────────┘
   │
 branch ── compare-and-swap ──▶ commit
```

1. The layout splits the message into items. Each item is encoded canonically, so the same value always gives the same bytes.
2. Each item is sealed and named by a keyed hash of its contents. A chunk the server already holds is never uploaded again.
3. A commit names a tree of chunks and its parent commits. A branch points at a commit and moves by compare-and-swap.
4. The server stores chunks, commits and branches. It enforces quotas and access, and collects chunks no commit reaches. It never holds a key.

## Security model

- **The server can't read stored data.** It holds sealed chunks, sealed commit payloads and keyed names.
- **The server can see metadata:** how many chunks a store has, their sizes, when they change, and which commits are whose parents.
- **Access is by store token.** A request carries an Ed25519-signed token, valid for an hour by default, naming one store and an access level (read, write or admin). The server verifies tokens with a public key and can't mint them.
- **A hostile server is detected, not prevented.** It can refuse service or delete data. It can't make a client accept forged contents, and a client refuses a head older than the newest it has seen on that branch.
- **Known limit:** the rollback check accepts a head on a different line of history, which is what a deliberate force-move looks like.

Report vulnerabilities through [SECURITY.md](SECURITY.md).

## Running a server

```bash
go install github.com/nextgui/protovcs/cmd/protovcsd@latest
```

```bash
protovcsd -dir ./stores -token_key token.pub
```

| Flag | Meaning |
|---|---|
| `-dir` or `-bucket` | Keep stores in a directory (one process) or a Cloud Storage bucket (many processes). |
| `-token_key` | File holding the base64 Ed25519 public key that store tokens are signed with. |
| `-open` | Serve every request without a token. For a server only its owner can reach. |
| `-allow_origin` | Page origins allowed to call the server from a browser. Defaults to any. |
| `-sweep_interval` | How often to collect garbage. Defaults to 6 hours. |
| `-deletion_retention` | How long a deleted store can be restored with `RestoreStore` before collection removes it. Defaults to an hour; 0 removes it on the next collection. |

To embed the server in another process instead, mount `connectserver` on an `http.ServeMux`.

## Generating a typed client

With Bazel:

```starlark
load("@protovcs//bazel:defs.bzl", "protovcs_layout_go")

protovcs_layout_go(
    name = "notebookvcs",
    src = "notebook.layout.textproto",
    protos = [":notes_proto"],
    locks = ["notes.lock"],
    go_package = "notebookvcs",
    importpath = "example.com/notes/notebookvcs",
    deps = [":notes_go_proto"],
)
```

`locks` is required. A chunk's name is a hash of its canonical bytes, and those bytes are field numbers and values. Renumbering a field would change what every stored byte means. The generator refuses a layout over any message that isn't covered by a schema lock from [stableproto](https://github.com/nextgui/stableproto).

## Packages

| Package | What it is |
|---|---|
| [`protovcs`](https://pkg.go.dev/github.com/nextgui/protovcs) | The client: layouts, canonical encoding, sealing, the manifest tree, partial checkout, history |
| [`connecttransport`](connecttransport) | Reaching a server over Connect |
| [`memtransport`](memtransport) | An in-memory server, for tests |
| [`storetoken`](storetoken) | Store tokens: minting and verifying |
| [`idbcache`](idbcache) | A chunk cache in the browser's IndexedDB |
| [`server`](server) | The server's rules: quotas, reservations, compare-and-swap, garbage collection |
| [`objectstorage`](objectstorage) | Where a server keeps bytes: a directory or a Cloud Storage bucket |
| [`connectserver`](connectserver) | The two services over Connect, with access checks |
| [`cmd/protovcsd`](cmd/protovcsd) | The server |
| [`cmd/protovcsgen`](cmd/protovcsgen) | The layout compiler and typed-code generator |

## Rust client

[`rust/`](rust) holds a second client in Rust, built with Cargo. It writes the same bytes as the Go client, so either can read the other's stores. The Rust tests check this against the vectors in [`testdata/golden/vectors.json`](testdata/golden/vectors.json), which `TestGoldenVectors` in [`golden_test.go`](golden_test.go) writes.

| Crate | What it is |
|---|---|
| `protovcs` | The client, over any connect-rust `ClientTransport`. With the `fetch` feature, on wasm32, it adds `FetchTransport` for the browser's `fetch` |
| `protovcs-idbcache` | The IndexedDB chunk cache and head marks, sharing a database with the Go `idbcache` |
| `protovcs-memtransport` | An in-memory server, for tests |
| `protovcs-proto` | The protos, generated by a build script from [`proto/`](proto). It needs no `protoc` |

Messages are buffa `DynamicMessage`s, described by a descriptor pool the caller supplies, where the Go client uses protoreflect. The crates aren't on crates.io yet. Depend on them by git.

`rust/` is also a Bazel module of its own, `protovcs_rust`, so the Go module never needs Rust. It builds the crates at the versions in `rust/Cargo.lock`:

```starlark
bazel_dep(name = "protovcs_rust")
git_override(
    module_name = "protovcs_rust",
    remote = "https://github.com/nextgui/protovcs.git",
    commit = "...",
    strip_prefix = "rust",
)
```

After changing the Go client in a way that changes its bytes, rewrite the vectors:

```sh
go test -run TestGoldenVectors -golden_out=$PWD/testdata/golden/vectors.json .
```

## Status

protovcs is in use, but it hasn't reached 1.0. The storage format is stable. The Go and Rust APIs may still change.

## License

Apache 2.0. Copyright 2026 Next Generation Interfaces Inc.
