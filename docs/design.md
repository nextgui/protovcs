# protovcs design

**Authors:** Next Generation Interfaces Inc., with Claude Opus 5.5
**Date:** 2026-11-06

protovcs is a version-controlled store for one protobuf message. Clients split
the message into pieces, encrypt each piece, and upload only the pieces the
server does not already hold. The server keeps sealed bytes, a commit graph and
branch pointers, and holds no key. Commits and branches work as they do in git,
and a client can read or write part of the message without fetching the rest.

This document describes how protovcs works and why. The [README](../README.md)
shows how to use it. The [format specification](format.md) defines the bytes,
and this document leaves byte-level detail to it.

## Why protovcs exists

An application that keeps a user's data encrypted end to end usually stores it
as one sealed blob. The server cannot read the blob, which is the point. It also
cannot help with anything else:

- **Every save uploads everything.** Changing one record re-encrypts and
  re-uploads the whole document.
- **Every read downloads everything.** A client that needs one month of a
  ten-year ledger fetches ten years, and the data must fit in memory.
- **There is no history.** The blob is overwritten, so an earlier version is
  gone unless the application keeps copies.
- **Concurrent writers lose data.** Two devices that save at once overwrite each
  other, and neither is told.

A server that reads the data can solve all four, as a database does. protovcs
solves them without the server reading anything. It names each encrypted piece
by a keyed hash of its contents, so equal pieces are stored once and only
changed pieces move. It versions the pieces as commits, so history is kept and
any version can be read. It moves branches by compare-and-swap, so a lost race
is reported and retried rather than silently overwriting.

The data is a protobuf message, not files or rows, because the application
already has a schema. The schema tells protovcs where the message can be split,
and gives every client the same encoding of the same data.

## Goals

- **The server never holds a key.** It cannot read contents, item keys, branch
  names or field names, and cannot forge data a client accepts.
- **A change costs what changed.** Writing one record uploads that record's
  piece and the manifest nodes above it, not the message.
- **A read costs what is read.** A client can check out one range of a large
  collection, and a read outside it fails rather than returning partial data as
  if it were whole.
- **No write is silently lost.** A writer that loses a race learns of it and can
  re-run its change on the new head.
- **Old versions stay readable.** Each commit carries its own schema, so it can
  be read after the generated code that wrote it is gone.
- **Two implementations agree byte for byte.** The format is specified
  separately and checked against shared test vectors.

## Non-goals

- **Merging.** protovcs does not combine two branches or two concurrent edits.
  A writer that loses a race re-runs its own change on the winner's result.
- **Queries on the server.** The server cannot filter or search, because it sees
  nothing to filter on. A client reads by key range, and compiling a layout
  refuses a declared query that would need the whole store.
- **Key management.** The application supplies each store's root key. Storing
  it, sharing it between devices and recovering a lost one are the
  application's job, and a lost key loses the store.
- **Hiding access patterns.** The server sees which pieces change and which a
  client fetches. The security model lists what it learns.
- **Several messages per store, or transactions across stores.** One store holds
  one message.

## Terms

| Term | Meaning |
|---|---|
| **root key** | A 32-byte secret per store, held only by clients. Every other key is derived from it. |
| **layout** | Data that says how the message is divided into items. |
| **item** | One piece of the message: a copy of the root message with only some fields set. |
| **chunk** | One immutable sealed object on the server, named by a keyed hash of its contents. Items, manifest nodes and schemas are all chunks. |
| **manifest** | A tree of chunks that names every item in one version. |
| **commit** | One version: a manifest, a schema chunk, and parent commits. |
| **branch** | A named pointer to a commit. The server stores it as a **ref** under a hash of the name. |

```
 branch ──▶ commit ──▶ manifest root ──▶ manifest nodes ──▶ item chunks
              │    └──▶ schema chunk (layout and descriptors)
              └── parents ──▶ commit ──▶ …
```

*What a branch reaches. Only the parent links are readable by the server; every
other arrow is inside a sealed payload or chunk.*

Every commit names a complete copy of the message, though it cost only the
items that changed. Nothing is rebuilt by replaying diffs, so a client can read
one commit and stop.

## The model

### Items and layouts

A layout, the `Layout` message in
[layout.proto](../proto/protovcs/v1/layout.proto), lists the fields that become
separate items. Everything it does not mention stays in the **root item**.
[layout.go](../layout.go) compiles a layout against the message's descriptor.
The layout is data rather than Go code because each commit stores the layout it
was written under, and a reader without the generated code needs it.

A **split** divides a repeated field into one item per distinct **shard key**
value. A shard key is a field path within the element plus a transform:
identity, day, month, quarter, year, or hash into a fixed number of buckets.
Several components make a composite key, and their order decides which ranges
are contiguous, as in a composite database index. The transforms are a closed
set because every client must compute the same key for the life of the store.
An item key such as `entries/2026/08/14` puts each date part in its own segment,
so a date range is a contiguous run of keys.

Items reassemble by `proto.Merge` in key order. Two rules follow.

- **An element's item is a function of the element alone.** A writer adds an
  element by computing its key, without reading the rest of the collection.
- **A split field is a multiset.** Reading returns elements in shard key order,
  then by the split's `order` fields, not in the order they were written.

Compiling refuses a layout that names a missing field or applies a date
transform to a non-date field. It also refuses a declared query that the splits
cannot serve as a key range, because that query would download the whole store.
Three shapes are not supported:

- **A split cannot reach through a repeated field.** Merging concatenates
  repeated fields, so two fragments of one outer element would come back as two
  elements. Nested collections are flattened instead, with each inner element
  carrying its outer element's id.
- **Map fields cannot be split.**
- **An oversized item is not subdivided.** A sub-key based on an element's
  neighbours would break the first rule above. An item over `max_item_bytes` is
  refused, and the fix is another shard key component.

The `identity` field option in
[annotations.proto](../proto/protovcs/v1/annotations.proto) names the field that
tells elements of a collection apart. It is the default order within an item,
and two elements sharing one in an item are refused.

A **scope** is a named subset a caller may check out, such as one collection or
a date range of it. `protovcsgen` generates a type per scope with accessors only
for what it covers, so reading outside a scope fails to compile.

### Canonical encoding

A chunk's name is a hash of its bytes, so two clients holding the same data must
produce the same bytes, or nothing deduplicates. The protobuf library's
deterministic mode only sorts maps, and its output is not promised to be stable
across releases. protovcs therefore defines its own rules, in
[canonical.go](../canonical.go). The output is still ordinary protobuf. Unknown
fields are kept byte for byte, so a client on an older schema that rewrites an
item keeps what it did not understand.

`CanonicalVersion` names these rules. A client refuses a commit written under
other rules, because it would name every chunk differently and re-upload the
whole store on its next write.

### Naming and sealing

[seal.go](../seal.go) derives three keys from the root key: one names chunks,
one encrypts them, and one names branches. Keys are per store, so stores holding
identical data share no names.

**Naming chunks by their contents is the decision the rest of the design rests
on.** A chunk id is an HMAC over the chunk's role, its key and its canonical
bytes. A writer uploads only chunks the server lacks. A reader updating from an
older version fetches only what differs. A client cache keyed by chunk id never
needs invalidating. The cost is that the server sees which chunks two versions
share, and so which parts of a store change and how often.

A chunk is sealed with AES-256-GCM, with its id as additional authenticated
data. It therefore opens only under the name it was stored as, and a server that
swaps two chunks causes a failure. `PlainSealer` seals nothing and uses plain
SHA-256 names, for tests and readable exports.

### The manifest

[manifest.go](../manifest.go) builds the manifest as a tree along the `/`
segments of item keys. Each entry names a child node or an item chunk, with the
total bytes and element count beneath it.

The tree's shape depends only on the set of item keys, so two clients holding
the same items build identical nodes with identical ids. Changing one item
rewrites only the nodes on its path to the root. Subtrees with equal ids are
equal, so a diff skips them and a scoped read never opens them.

### Commits

A `Commit` record in [store.proto](../proto/protovcs/v1/store.proto) has a part
the server reads and a sealed payload.

| Field | Readable by the server | Why |
|---|---|---|
| `parents` | yes | The server walks history for collection and incremental fetch. |
| `added` | yes | The server computes reachability from it. Ids reveal no contents. |
| `format_version`, `schema_version` | yes | The server can refuse an old schema and report which stores use one. |
| `payload` | no | Manifest root, schema chunk id, branch name, time, and commit message. |

`added` lists the chunks this commit holds that its first parent did not. The
union of `added` between two commits is what a client holding the older one may
lack, and `MissingBetween` returns it in one round trip.

A commit id is a SHA-256 hash of the readable part. The server refuses a commit
stored under any other name, and the client checks every commit it receives, so
a server cannot attach a commit to a history it is not part of.

The commit message holds a title, description, authors and optional `details`
of a type the layout names. `Log` reads only commit records, so history costs
the number of commits, not the size of the store.

A write, in `recordVersion` in [store.go](../store.go), runs in this order:

1. Split the message into items, encode them, and compute every chunk id.
2. Ask the server which added chunks it lacks (`HasChunks`).
3. Seal those, and reserve quota for their sealed size (`BeginWrite`).
4. Upload them (`PutChunks`).
5. Record the commit (`PutCommit`).
6. Move the branch under compare-and-swap (`UpdateRef`).

The ref moves last, so a crash leaves only unnamed chunks for collection to
remove.

### Branches

A ref is stored under a keyed hash of the branch name, because a name a person
chose is content.

A ref carries an opaque `cas_token`, and `UpdateRef` succeeds only with the
token the caller last read. Refs are the only mutable objects, so they are the
only place writers conflict. `Branch.Change` re-runs the caller's function on
the new head when it loses a race, so the function must be safe to repeat.

The server refuses a move to a commit that does not descend from the current one
unless the caller passes `force`, because such a move orphans history.

### Partial checkout

`Branch.Checkout` in [partial.go](../partial.go) walks the manifest to the key
ranges a scope names and fetches those items. Reads inside the scope make no
network calls.

A read outside the scope returns `ErrOutOfScope`, and a read of an absent item
inside it returns "not found". Without that distinction a caller could total
three months of a five-year store and report it as the whole.

Writes may fall outside the scope, because their cost is bounded by the element.
**A commit from a partial checkout is a complete version.** Items outside the
checkout are carried over by their existing ids without being fetched.

### The schema chunk and versions

Each commit names a sealed schema chunk holding the layout and the message's
descriptors. `ReadGeneric` in [version.go](../version.go) reads a commit through
its schema chunk, so old commits stay readable after their generated code is
deleted.

| Version | Governs | A client on the wrong one |
|---|---|---|
| `FormatVersion` | how chunks and records are framed | cannot read anything |
| `CanonicalVersion` | how chunks are named | reads everything and names nothing the same |
| `schema_version` | what the data means | reads correctly and misinterprets it |

`schema_version` is set by hand, for a field that keeps its number and type but
changes meaning, which no tool detects. The server refuses a commit below a
parent's `schema_version`.

Renumbering a field would change what every stored byte means. `protovcsgen`
therefore refuses a layout over a message not covered by a schema lock from
[stableproto](https://github.com/nextgui/stableproto).

## The server

[server](../server) holds the rules apart from any transport.
[connectserver](../connectserver) serves them over Connect, and
[objectstorage](../objectstorage) keeps bytes in a directory or a Cloud Storage
bucket.

### Reservations and quotas

`BeginWrite` checks the store's limits and reserves room before anything is
uploaded, because at commit time the chunks would already be stored. Reserved
amounts count as used, so two writers are never both told there is room for the
same last bytes. A reservation expires after 30 minutes. A store may hold at
most 32 reservations. Each one is a stored object, and a reservation for no
chunks passes every other limit.

The limits are total bytes, object count, bytes per chunk, commits, refs, and
bytes per calendar day. Commits are never collected, so they are charged against
the byte limits too. A chunk the store already holds is not charged again.

Usage is kept in the store's record and changes in the same storage update as
the write that moves it. `Reconcile` recounts it from storage on every sweep.

Two limits are approximate. Parallel writes can together pass the daily limit,
because reserved bytes do not count toward it. Concurrent ref creation can pass
`max_refs` by a few refs.

### Packs

Each chunk is its own storage object, so a cold read of a large store can take
thousands of storage requests. A **pack** is one object of up to 4 MiB holding a
second copy of chunks a branch head reaches, read with range requests.

The server cannot choose these chunks itself, because manifests are sealed. A
client built with `WithPackHints(branch)` sends a **pack hint** after a full read
of that branch, listing the chunk ids it fetched in order. The hint reveals
nothing new, because the read just fetched those chunks from the same server.

Packs are only a cache. The loose chunk always exists and any read failure falls
back to it, and collection, quotas and `HasChunks` ignore packs.

### Garbage collection

`Collect` in [server/collect.go](../server/collect.go) is the only operation that
deletes chunks. Commits are never deleted. Any error while computing
reachability abandons the run, so a failure deletes nothing.

A chunk is reachable if a commit reachable from a ref, or written within the
last hour, lists it in `added`. Young commits count because a writer stores its
commit before it moves the ref. Young chunks are spared too, because a writer
uploads chunks before the commit that names them.

**Deletion takes two runs.** The first run that finds a chunk unreachable marks
it, and a run at least an hour later deletes it if it is still unreachable. A
writer that needs a marked chunk uploads it again, which removes the mark.
Without marking, a commit recorded between a run's last check and its deletes
could lose a chunk.

**Deleting a store can be undone for a while.** `DeleteStore` refuses the
store's reads and writes at once. Until the retention window passes, an hour by
default, `RestoreStore` makes it active again. After that, collection deletes
everything it holds. A window of zero deletes on the next collection, for an
application whose deletion has to be immediate.

## Access

A request carries a **store token**: a `StoreToken` message from
[token.proto](../proto/protovcs/v1/token.proto) and an Ed25519 signature over it.
It names one store, one access level and an expiry, an hour after minting by
default. [storetoken](../storetoken) mints and verifies them.

The service that authenticated the user mints tokens, and the protovcs server
holds only the public key. With a shared secret, anyone who read it off the
server could mint access to every store. Tokens are checked by signature rather
than looked up, which keeps a database off the read path. The cost is that a
leaked token works until it expires.

Levels are ordered, `ACCESS_READ` < `ACCESS_WRITE` < `ACCESS_ADMIN`, and a method
accepts any level at or above its own. Each method's level is set in
[connectserver/handler.go](../connectserver/handler.go):

| Level | Methods |
|---|---|
| read | `ListRefs`, `WatchRefs`, `GetCommits`, `MissingBetween`, `HasChunks`, `GetChunks`, `GetUsage` |
| write | `UpdateRef`, `PutCommit`, `BeginWrite`, `PutChunks`, `PackHint` |
| admin | `CreateStore`, `GetStore`, `SetQuota`, `DeleteStore`, `RestoreStore`, `TriggerMaintenance` |

A method missing from the table is refused to everyone. `SetQuota` is admin, so
a deployment that gives users admin tokens lets them raise their own limits.
`protovcsd` starts only with exactly one of `-token_key` or `-open`, so it never
serves every store by default.

## Security model

**The server cannot read contents.** Items, manifest nodes, schemas and commit
payloads are sealed with keys it never holds, and chunk ids and ref names are
keyed hashes. **It cannot substitute data**, because a chunk opens only under its
own id and the client recomputes every commit id. **It cannot issue access**,
because it holds only the token verification key.

The server can see:

- how many chunks a store holds, their sizes, and when they were written
- the commit graph, and when each commit arrived
- which chunks each commit added, and so which parts of a store change often
- which chunks a client fetches, and when
- each commit's `schema_version`, and when a client watches a store's refs

A hostile server can refuse service, delete data, withhold a ref, or serve an
old head, and encryption detects none of these. The rollback check covers old
heads.

### The rollback check

An old commit is genuine, so nothing about it fails to verify. `Branch.Head` in
[store.go](../store.go) remembers the newest head seen on each branch and refuses
a head that is an ancestor of it, with `ErrWentBackwards`. The memory is per
`Store` value. [HeadMarks](../headmarks.go) persists it, and
[idbcache](../idbcache) implements it in the browser's IndexedDB.

Known limits:

- A head on a different line of history is accepted, because a deliberate forced
  move looks the same.
- A `Store` without `HeadMarks` accepts any head after a restart.
- Marks are per device, so a device that never saw a newer head cannot tell it
  is being withheld.
- Marks are not protected against local tampering. Anything able to alter them
  could read the root key.

## Alternatives not taken

| Alternative | Why not |
|---|---|
| A random chunk name on every write | Every commit would re-upload the whole message, and reads could skip nothing. |
| Rotating the naming key periodically | It limits what the server can correlate, at the cost of a full rewrite each period. It may be added later. |
| A secondary index tree | Its shape would depend on insertion order, so equal data would get different names. A field in the message can act as an index when one is needed. |
| Packs replacing loose chunks | Collection, quotas and deduplication would all have to understand packs. |
| A token naming several stores | A leaked token would expose all of them. |
