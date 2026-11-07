# protovcs design

**Authors:** Next Generation Interfaces Inc., with Claude Opus 5.5
**Date:** 2026-11-06

protovcs is a version-controlled store for one protobuf message. Clients split
the message into pieces, encrypt each piece, and upload only the pieces the
server does not already hold. The server keeps sealed bytes, a commit graph and
branch pointers, and holds no key. Commits and branches work as they do in git,
and a client can read or write part of the message without fetching the rest.

This document describes how protovcs works and why. The [README](../README.md)
shows how to use it. A separate `docs/format.md` will define the byte-level
format, so formats appear here only as far as the design needs them.

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
separate items. Everything it does not mention stays in the **root item**, whose
key is the empty string. [layout.go](../layout.go) compiles a layout against the
message's descriptor. The layout is data rather than Go code because each commit
stores the layout it was written under, and a reader without the generated code
needs it.

A **split** divides a repeated field into one item per distinct **shard key**
value. A shard key is a field path within the element plus a transform: identity,
day, month, quarter, year, or hash into a fixed number of buckets. Several
components make a composite key, and their order decides which ranges are
contiguous, as in a composite database index. The transforms are a closed set
rather than an expression language, because every client must compute the same
key for the life of the store.

An item key is the field name and the shard values joined by `/`, as in
`entries/2026/08/14`. Dates become separate segments so the manifest nests by
year and month, and a date range stays a contiguous run of keys.

Items reassemble by `proto.Merge` in key order. Two rules follow.

- **An element's item is a function of the element alone.** A writer adds an
  element by computing its key, without reading the rest of the collection.
- **A split field is a multiset.** Reading returns elements in shard key order,
  then by the split's `order` fields, not in the order they were written.

Compiling refuses a missing field path, a date transform on a non-date field, a
non-scalar order key, and a declared query that would scan the whole store
because the splits cannot serve it as a key range. Some shapes are also refused:

- **A split cannot reach through a repeated field.** Merging concatenates
  repeated fields, so two fragments of one outer element would come back as two
  elements. Nested collections are flattened instead, with each inner element
  carrying its outer element's id.
- **Map fields cannot be split yet.** When they can, the map key will be the
  shard key, because protobuf guarantees it is unique.
- **An oversized item is not subdivided.** An item over `split_byte_count` is
  reported, and one over `max_item_bytes` is refused. A sub-key based on an
  element's neighbours would break the first rule above, and one based on the
  element alone is just another shard key component. A single element over the
  ceiling is allowed, bounded by the server's `max_chunk_bytes`.

The `identity` field option in
[annotations.proto](../proto/protovcs/v1/annotations.proto) names the field that
tells elements of a collection apart. It is the default order within an item,
and two elements sharing one in an item are refused. Nothing merges elements by
identity, so a wrong one cannot fold two records together.

`Layout` also declares `extracts` and `blobs`. The current client does not apply
them, and the fields they name stay in the root item.

A **scope** is a named subset a caller may check out, such as one collection or
a date range of it. `protovcsgen` generates a type per scope with accessors only
for what it covers, so reading outside a scope fails to compile.

### Canonical encoding

`CanonicalBytes` in [canonical.go](../canonical.go) encodes every item, manifest
node and schema chunk. Fields are written in ascending field number, with unknown
fields kept byte for byte at their own numbers. Repeated scalars are always
packed, and map entries are sorted by key. A client on an older schema that
rewrites an item therefore keeps what it did not understand.

protovcs defines its own encoding because a chunk's name is a hash of these
bytes. Two clients holding the same data must produce the same bytes, or nothing
deduplicates. The protobuf library's deterministic mode only sorts maps, and its
output is not promised to be stable across releases. The result is still
ordinary protobuf.

`CanonicalVersion` names these rules. A client refuses a commit written under
other rules, because it would name every chunk differently and re-upload the
whole store on its next write.

### Key derivation, naming and sealing

[seal.go](../seal.go) derives three keys from the root key with HKDF-SHA256:

| Key | HKDF label | Used for |
|---|---|---|
| naming key | `protovcs/v1/chunk-name` | chunk ids |
| content key | `protovcs/v1/chunk-content` | AES-256-GCM over chunks and commit payloads |
| ref key | `protovcs/v1/ref-name` | branch names |

Separate keys mean no analysis is needed of whether two uses interact. Keys are
per store, so stores holding identical data share no names.

A **chunk id** is HMAC-SHA256 under the naming key over a role, a key and the
canonical bytes, with the role and key length-prefixed. The role is `item`,
`manifest` or `schema`, so a manifest node cannot be served where an item was
asked for. The key is the item key for an item, the covered prefix for a
manifest node, and empty for a schema.

Naming chunks by their contents is the decision the rest of the design rests on.
A writer uploads only chunks the server lacks. A reader updating from an older
version fetches only what differs. A client cache keyed by chunk id never needs
invalidating. The cost is that the server sees which chunks two versions share,
and so which parts of a store change and how often.

A **sealed chunk** is a format version byte, a random 12-byte nonce, and the
AES-256-GCM ciphertext and tag, with the chunk id as additional authenticated
data. A chunk therefore opens only under the name it was stored as, and a server
that swaps two chunks causes a failure. A random nonce is safe because a chunk
is written once: different contents are a different chunk. An unknown version
byte gives `ErrUnsupportedFormat`, a different error from `ErrTampered`.

`PlainSealer` uses plain SHA-256 and seals nothing, for tests and readable
exports.

### The manifest

[manifest.go](../manifest.go) builds the manifest as a tree along the `/`
segments of item keys. Each node entry names a child node or an item chunk, with
the total bytes and element count beneath it.

The tree's shape depends only on the set of item keys, so two clients holding
the same items build identical nodes with identical ids. Changing one item
rewrites only the nodes on its path to the root. Every other node keeps its id
and is neither uploaded nor fetched again. Subtrees with equal ids are equal, so
a diff skips them and a scoped read never opens them.

A node's fanout is the number of distinct values at one key position, which is
small for dates and ids. A node with more than 65,536 entries is refused, and the
error asks for another shard key component.

### Commits

A `Commit` record in [store.proto](../proto/protovcs/v1/store.proto) has a part
the server reads and a sealed payload.

| Field | Readable by the server | Why |
|---|---|---|
| `parents` | yes | The server walks history for collection and incremental fetch. |
| `added` | yes | The server computes reachability from it. Ids reveal no contents. |
| `format_version`, `schema_version` | yes | The server can refuse an old schema and report which stores use one. |
| `naming_nonce` | yes | Random and not secret; explained below. |
| `payload` | no | Manifest root, schema chunk id, branch name, time, and commit message. |

`added` lists chunks this commit holds that its first parent did not, found by
walking both manifests and stopping where subtree ids match. The full set would
cost megabytes per commit on a large store. The union of `added` between two
commits is what a client holding the older one may lack, and `MissingBetween`
returns it in one round trip.

A **commit id** is SHA-256 over the canonical record with `id` and `payload`
cleared. The payload is sealed with the content key and the commit id as
additional data, so the id cannot also cover the payload. The random
`naming_nonce` keeps two commits with the same parents and chunks but different
messages from sharing an id. The server recomputes the id and refuses a commit
stored under any other name. The client checks every commit it receives, so a
server cannot attach a commit to a history it is not part of.

The commit message holds a title, description, authors, the writing client's
build, labels, and optional `details` of a type the layout names. That type
resolves through the commit's own schema chunk, which a `google.protobuf.Any`
would not. `Log` reads only commit records, so history costs the number of
commits, not the size of the store.

A write, in `recordVersion` in [store.go](../store.go), runs in this order:

1. Split the message into items, encode them, and compute every chunk id.
2. Ask the server which added chunks it lacks (`HasChunks`).
3. Seal those, and reserve quota for their sealed size (`BeginWrite`).
4. Upload them (`PutChunks`).
5. Record the commit (`PutCommit`).
6. Move the branch under compare-and-swap (`UpdateRef`).

The ref moves last, so a crash leaves only unnamed chunks for collection to
remove. Asking first avoids reserving for chunks the store holds, and sealing
first makes the reservation cover the sealed size.

### Branches

A ref is stored under HMAC-SHA256 of the branch name with the ref key, because a
name a person chose is content. A client finds a branch by computing the hash.

A ref carries an opaque `cas_token`, and `UpdateRef` succeeds only with the token
the caller last read. Refs are the only mutable objects, so they are the only
place writers conflict. `Branch.Change` re-runs the caller's function on
the new head when it loses a race, so the function must be safe to repeat. A
retry fetches only the items the other writer changed.

The server also refuses a move to a commit that does not descend from the
current one unless the caller passes `force`. Such a move orphans history and
makes the chunks only it reached collectable. `Truncate` and `Rewrite` use it.

### Partial checkout

`Branch.Checkout` in [partial.go](../partial.go) walks the manifest to the key
ranges a scope names and fetches those items in batched requests. Reads inside
the scope make no network calls.

A read outside the scope returns `ErrOutOfScope`, and a read of an absent item
inside it returns "not found". Without that distinction a caller could total
three months of a five-year store and report it as the whole. `Extend` widens a
scope explicitly.

Writes may fall outside the scope, because their cost is bounded by the element.
`PutElement` fetches the one item an element belongs to if it is not held, so
appending keeps what the item already contains.

**A commit from a partial checkout is a complete version.** `Partial.Commit`
keeps only the held items whose id changed, and `UpdateManifest` rewrites the
manifest path above each one. Every other item and subtree is carried over by
its existing id without being fetched.

### The schema chunk and versions

Each commit names a schema chunk holding the layout, the root message name, a
`FileDescriptorSet` covering the message and its imports, `schema_version` and
`CanonicalVersion`. It is sealed, so the server does not learn field names or how
the message is divided. `ReadGeneric` in [version.go](../version.go) reads a
commit as a dynamic message through its schema chunk, so old commits stay
readable after their generated code is deleted.

| Version | Governs | A client on the wrong one |
|---|---|---|
| `FormatVersion` | how chunks and records are framed | cannot read anything |
| `CanonicalVersion` | how chunks are named | reads everything and names nothing the same |
| `schema_version` | what the data means | reads correctly and misinterprets it |

`schema_version` is set by hand, for a field that keeps its number and type but
changes meaning, which no tool detects. A layout's `min_readable_version` makes a
client refuse older commits. The server refuses a commit below a parent's
`schema_version` or below the store's `min_writable_schema_version`, and records
each store's highest version so an operator can tell when an old one is unused.

Renumbering a field would change what every stored byte means. `protovcsgen`
therefore refuses a layout over a message not covered by a schema lock from
[stableproto](https://github.com/nextgui/stableproto).

## The server

[server](../server) holds the rules apart from any transport, so a second
transport cannot carry a second copy. [connectserver](../connectserver) serves
them over Connect, and [objectstorage](../objectstorage) keeps bytes in a
directory or a Cloud Storage bucket.

### Reservations and quotas

`BeginWrite` checks the limits and adds a reservation to the store's usage in
one storage update, before anything is uploaded. Checking at commit time would
be too late, because the chunks would already be stored. Reserved amounts count
as used, so two writers are never both told there is room for the same last
bytes. A reservation expires after 30 minutes. A refused reservation first
releases the store's expired ones and tries again.

The limits are total bytes, object count, bytes per chunk, commits, refs, and
bytes per calendar day. The object count catches a layout that divides the
message too finely. Commits are never collected, so they are charged against the
byte limits too. A chunk the store already holds is not charged again. A refusal
names the limit, the usage and what the write needed.

Usage counters change in the same storage update as the write that moves them.
`Reconcile` recounts them from storage on every sweep, so drift is reported.

### Packs

Each chunk is its own storage object, so a cold read of a large store can take
thousands of storage requests. A **pack**, built by
[objectstorage/pack.go](../objectstorage/pack.go) under rules in
[server/pack.go](../server/pack.go), is one object of up to 4 MiB holding a
second copy of chunks a branch head reaches.

The server cannot choose these chunks, because manifests are sealed and it cannot
tell live chunks from replaced ones. A client built with `WithPackHints(branch)`
sends a **pack hint** after a full read of that branch: the commit, and every
chunk id the read fetched, in fetch order. The hint reveals nothing new, because
the read just fetched those chunks from the same server. It names no branch.
The server keeps one hint per store, and only if the commit is a current head
and the hint names at most 100,000 ids.

A pack pass runs on every sweep, or after a hint when a deployment supplies a
`PackScheduler`, at most every ten minutes per store. It packs unpacked hinted
chunks once there are 64 of them or 256 KiB, rewrites packs at least half dead,
and merges the smallest while there are more than 16. New packs are written
before the pack index is replaced by compare-and-swap, and retired packs are
deleted an hour later. A store's packs are capped at its `max_bytes`, or its
usage when it has none, and do not count against the quota.

A read groups packed chunks by pack and reads each run with one range request,
split where more than 1 MiB of unwanted bytes would lie between two wanted
chunks. Any failure falls back to the loose object, which always exists. Packs
are only a cache: collection, quotas and `HasChunks` ignore them.

### Garbage collection

`Collect` in [server/collect.go](../server/collect.go) is the only operation that
deletes chunks. Commits are never deleted. Any error while computing
reachability abandons the run, so a failure deletes nothing.

**Reachability** uses only readable fields. It is the union of `added` over every
commit reachable from a ref or written within the last hour, the **grace
period**. Young commits count because a writer stores its commit before it moves
the ref, and the commit may reuse an old chunk nothing else reaches.

An unreachable chunk is spared when:

- **it is younger than the grace period**, because a writer uploads chunks
  before the commit that names them;
- **the store has a live reservation**, in which case the run marks and deletes
  nothing;
- **storage cannot report its age**, so such storage never loses an unreachable
  chunk;
- **a commit was recorded during the run**, which may have reused a chunk the run
  did not count, so the run deletes nothing.

**Deletion takes two runs.** The first run that finds a chunk unreachable marks
it, and a run at least an hour later deletes it if it is still unreachable.
Meanwhile writers take it back. `HasChunks` reports a marked chunk as missing,
the writer uploads it again, and `PutChunks` removes the mark. A chunk a run has
already claimed fails the upload as a lost race, and the client retries.
`PutCommit` refuses a commit that adds a marked chunk. Without marking, a commit
recorded between a run's last check and its deletes could lose a chunk.

**Deleting a store can be undone for a while.** `DeleteStore` marks a store
as deleting, records when, and refuses its reads and writes. For the server's
retention window, an hour by default, `RestoreStore` makes it active again.
The first collection after the window moves the store to purging, then deletes
all its chunks without the grace period or marking, and the pack pass drops its
packs. A purging store cannot be restored. Collection and `RestoreStore` change
the state in the same conditional update of the store's record, so a restore
and a purge cannot both succeed. A window of zero purges on the next
collection, for an application whose deletion has to be immediate.
`protovcsd` sweeps every six hours by default.

## Access

A request carries a **store token** in an `Authorization: Bearer` header: a
`StoreToken` message from [token.proto](../proto/protovcs/v1/token.proto) and an
Ed25519 signature over it. It names one store, one access level, a start time
and an expiry, plus a random nonce that tells tokens apart in logs.
[storetoken](../storetoken) mints and verifies them. The default lifetime is one
hour.

The service that authenticated the user mints tokens, and the protovcs server
holds only the public key. With a shared HMAC secret, anyone who read it off the
server could mint access to every store. Tokens are checked by signature rather
than looked up, which keeps a database off the chunk read path. The cost is that
a leaked token works until it expires.

Levels are ordered, `ACCESS_READ` < `ACCESS_WRITE` < `ACCESS_ADMIN`, and a method
accepts any level at or above its own. Each method's level is set in
[connectserver/handler.go](../connectserver/handler.go):

| Level | Methods |
|---|---|
| read | `ListRefs`, `WatchRefs`, `GetCommits`, `MissingBetween`, `HasChunks`, `GetChunks`, `GetUsage` |
| write | `UpdateRef`, `PutCommit`, `BeginWrite`, `PutChunks`, `PackHint` |
| admin | `CreateStore`, `GetStore`, `SetQuota`, `DeleteStore`, `RestoreStore`, `TriggerMaintenance` |

Levels are per method because reading refs and moving them share a service.
`PackHint` needs write because it makes the server copy bytes at the store's
expense. A method missing from the tables is refused to everyone. `ListStores`
names no store, so only a server run with `-open` serves it. Every refusal is
`permission_denied`.

`protovcsd` starts only with exactly one of `-token_key` or `-open`. Defaulting to
open would serve every store to anyone, and defaulting to closed would serve
nothing and look like a bug.

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

Hiding which chunks a client reads would need oblivious RAM or private
information retrieval, which protovcs does not attempt.

A hostile server can refuse service, delete data, withhold a ref, or serve an
old head, and encryption detects none of these. The rollback check below covers
old heads.

### The rollback check

An old commit is genuine, so nothing about it fails to verify. `Branch.Head` in
[store.go](../store.go) remembers the newest head seen on each branch and refuses
a head that is an ancestor of it, with `ErrWentBackwards`. It first checks
whether the offered head descends from the remembered one, which is cheap and is
the normal case, including a retry after a lost race. Only then does it walk
back from the remembered head.

The memory is per `Store` value. [HeadMarks](../headmarks.go) persists it, and
[idbcache](../idbcache) implements it in the browser's IndexedDB. A mark holds
only a commit id, which the server already knows.

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
