# protovcs format

**Authors:** Next Generation Interfaces Inc., with Claude Opus 5.5
**Date:** 2026-11-06

This document specifies the bytes protovcs writes and reads: how a message
becomes items, how items, manifest nodes, schemas and commits are encoded,
named and sealed, and what a reader checks. A second implementation that
follows it produces the same chunk ids and commit ids as the Go
implementation in this repository. The reasons behind each rule are in the
[design document](design.md) and are not repeated here.

Each rule names the source file it is derived from. Where the golden vector
file `testdata/golden/vectors.json` has a test for a rule, the rule names the
vector by its JSON path. That file and the `golden_test.go` that writes it
arrive with the Rust client. [Vector coverage](#vector-coverage) lists the
rules that have no vector yet.

## Conventions

| Notation | Meaning |
|---|---|
| `a ‖ b` | byte concatenation |
| `uvarint(n)` | `n` as a protobuf base-128 varint: 7 bits per byte, least significant group first, high bit set on every byte but the last |
| `SHA-256(x)` | the 32-byte SHA-256 digest of `x` |
| `HMAC(k, x)` | HMAC-SHA256 of `x` under key `k`, 32 bytes |
| `canonical(m)` | the canonical encoding of message `m`, defined [below](#canonical-encoding) |
| "bytewise" | comparison of two byte strings as unsigned bytes, a shorter prefix sorting first |

All strings are UTF-8 bytes. All string ordering in this document is bytewise.

### Constants

| Name | Value | Source |
|---|---|---|
| format version | `1` | [types.go](../types.go) |
| canonical version | `1` | [types.go](../types.go) |
| root key length | 32 bytes | [seal.go](../seal.go) |
| chunk id and commit id length | 32 bytes | [types.go](../types.go) |
| envelope nonce length | 12 bytes | [seal.go](../seal.go) |
| AES-GCM tag length | 16 bytes | [seal.go](../seal.go) |
| commit naming nonce length | 16 bytes | [store.go](../store.go) |
| maximum entries in one manifest node | 65,536 | [manifest.go](../manifest.go) |

A field holding a chunk id or commit id must be exactly 32 bytes. A reader
refuses any other length ([types.go](../types.go)).

## Canonical encoding

The canonical encoding is the one byte string protovcs produces for a message.
Items, manifest nodes, schema chunks, commit records, commit payloads and commit
`details` are all encoded this way. The output is protobuf wire format, except
for groups, noted below. Source: [canonical.go](../canonical.go).

### Which fields are written

A field is written when it is populated:

| Field kind | Populated when |
|---|---|
| proto3 scalar without presence | the value is not zero; for `float` and `double`, the bit pattern is not all zeros, so `-0.0` and NaN are written |
| proto3 `string` or `bytes` without presence | the length is not zero |
| `optional` field (proto3 or proto2), oneof member | it is set, even to the zero value |
| singular message | it is set, even when it is empty |
| repeated field or map | it has at least one element |
| extension | it is set |

An empty message encodes to zero bytes. Vector: `canonical[name=explicit_zeros]`
writes `5: 0` and `4: ""` explicitly and expects empty output.

### Field order

1. Collect one record per populated known field and extension.
2. Split the message's unknown bytes into records. A record is one tag and its
   value, where a group record runs through its matching end tag.
3. Sort all records by field number, ascending, with a stable sort. At one
   number, a known field comes first, then unknown records in the order they
   were received.
4. Write the records in that order.

Unknown records are copied byte for byte, never re-encoded. A non-minimal
varint or an unpacked repeated field inside an unknown record stays as it was.
A malformed unknown tag or value makes encoding fail.

Vector: `canonical[name=scrambled]`, which has fields out of order, unknown
fields at 12, 50 and 99, and an unknown record placed before a known one.

### Singular values

| Kind | Record |
|---|---|
| `int32`, `int64` | tag (wire type 0), varint of the value as a 64-bit two's complement integer, so a negative value takes 10 bytes |
| `uint32`, `uint64` | tag (wire type 0), varint |
| `sint32`, `sint64` | tag (wire type 0), varint of the zigzag encoding |
| `bool` | tag (wire type 0), `0x00` or `0x01` |
| `enum` | tag (wire type 0), varint of the number as a 64-bit two's complement integer |
| `fixed32`, `sfixed32` | tag (wire type 5), 4 bytes little-endian |
| `fixed64`, `sfixed64` | tag (wire type 1), 8 bytes little-endian |
| `float` | tag (wire type 5), the IEEE 754 binary32 bits, little-endian |
| `double` | tag (wire type 1), the IEEE 754 binary64 bits, little-endian |
| `string`, `bytes` | tag (wire type 2), `uvarint(length)`, the bytes; strings are not checked for valid UTF-8 |
| message | tag (wire type 2), `uvarint(length)`, the canonical encoding of the message |
| group | tag (wire type 2), `uvarint(length)`, the canonical encoding of the group's message |

NaN payloads are not normalised: the bits written are the bits held.

A group is written as a length-delimited record, not between start and end
tags. A standard protobuf parser does not read that record as the group field.
No message in this repository uses groups.

### Repeated fields

A repeated field of a numeric, `bool` or `enum` kind is always written packed:
one record with wire type 2, `uvarint(length)`, then each element's value
without a tag. This holds even when the field is declared unpacked. Vector:
`canonical[name=scrambled]` supplies `counters` unpacked and expects it packed.

A repeated `string`, `bytes` or message field is written as one record per
element, in list order.

### Maps

A map is written as one record per entry, each with the map field's number and
wire type 2. An entry's content is the key record at field 1 followed by the
value record at field 2. Both are always written, even when zero or empty.

Entries are sorted by key:

| Key kind | Order |
|---|---|
| `bool` | `false` before `true` |
| signed integer kinds | numeric |
| unsigned integer kinds | numeric |
| `string` | bytewise |

Vector: `canonical[name=scrambled]` and `canonical[name=store]` hold maps
supplied out of order.

## Items and item keys

A layout (the `Layout` message in
[layout.proto](../proto/protovcs/v1/layout.proto)) divides a root message into
items. Each item is a copy of the root message type with only some fields set,
stored under an item key. Sources: [materialize.go](../materialize.go),
[layout.go](../layout.go).

### The root item

The root item is a copy of the message with every split field cleared. Clearing
a split whose path runs through singular messages clears only the repeated field
at the end of the path, so those messages remain set even when they become
empty. Unknown fields stay in the root item. The root item's key is the empty
string. It is stored only when at least one field or unknown byte remains.

### Split items

A split names a repeated message field by a dot-separated path from the root
message. Every step before the last must be a singular message field. For each
split, in layout order, each element of that field goes to the item whose key
is:

```
key = split.field ‖ "/" ‖ segment(by[0]) ‖ "/" ‖ segment(by[1]) ‖ …
```

`split.field` is the layout's path string as written, dots included, as in
`group.members/m1`. A segment produced by a date transform contains `/`
itself, so it adds two or three path segments.

The item's message is an empty root message with the path to the split field
set and the field holding that item's elements.

A value is **present** when every message on its path is set and, for a final
field with presence, that field is set. A proto3 scalar without presence is
always present.

### Segment forms

Each shard key component (`ShardKey`) turns one field of the element into a
segment. The transform decides how.

| Transform | Segment |
|---|---|
| `IDENTITY` or `UNSPECIFIED` | the scalar form below, or the empty string when the value is absent |
| `DAY` | `YYYY/MM/DD` |
| `MONTH` | `YYYY/MM` |
| `QUARTER` | `YYYY/Qn`, with `n` = `(month − 1) / 3 + 1` in integer division |
| `YEAR` | `YYYY` |
| `HASH` | `b` followed by the bucket number, zero-padded to the number of decimal digits in `buckets − 1` |

**Dates.** `YYYY` is the year in decimal padded with zeros to at least four
characters, `MM` and `DD` to two. A year above 9999 takes more digits. The
field must be either a message with `int32` fields named `year`, `month` and
`day`, or `google.protobuf.Timestamp`. For the first, a month outside 1–12 or a
day outside 1–31 is refused. For a timestamp, the date is the proleptic
Gregorian calendar date in UTC of `seconds`, and `nanos` is ignored. An absent
date is refused. Vectors: `segments[kind=date_*]` and `dates`.

**Hash.** The input is the scalar form of the value, or the empty string when
it is absent. The bucket is the first 8 bytes of `SHA-256(input)` read as a
big-endian unsigned 64-bit integer, modulo `buckets`. `buckets` must not be
zero. With 16 buckets, the segments run `b00` to `b15`. Vector:
`layouts[name=hashed]`.

**Scalar forms.** Used by `IDENTITY` and `HASH`, and for sorting elements:

| Kind | Form |
|---|---|
| `string` | the bytes, with each `/` (0x2F), `%` (0x25), byte below 0x20, and 0x7F replaced by `%` and two uppercase hex digits |
| `bytes` | the same escaping, applied to the raw bytes |
| `bool` | `true` or `false` |
| signed integers and `enum` | value ≥ 0: `0` then the value in 19 decimal digits; value < 0: `-` then the absolute value in 19 decimal digits |
| unsigned integers | the value in 20 decimal digits |
| `float`, `double` | the shortest decimal that rounds back to the same value at the field's width; exponent form `d.ddde±XX` with at least two exponent digits when the decimal exponent is below −4 or at least 6; `NaN`, `+Inf`, `-Inf` |

Bytes at or above 0x80 are not escaped. The signed form sorts negative numbers
in order of increasing magnitude, so `-1` sorts before `-2`. The most negative
64-bit integer has no 19-digit absolute value, and the Go implementation writes
`--9223372036854775808` for it. Vectors: `segments[kind=int]`, `uint`,
`string`, `float64`, `float32`, `bool`.

### Element order within an item

When an item holds more than one element of a split, the elements are sorted
with a stable sort. The sort keys are, in order:

1. each path in the split's `order`, or, when `order` is empty, the identity
   path described below. For each path, an element whose value is absent sorts
   before one whose value is present. Two present values compare by their
   scalar forms, bytewise.
2. the canonical encoding of the whole element, bytewise.

The identity path comes from the `identity` field option in
[annotations.proto](../proto/protovcs/v1/annotations.proto), set on the
repeated field and naming a scalar field of the element. Within one item, two
elements whose identity values are present and have the same scalar form are
refused.

Floating-point order keys compare as text, not as numbers. Vector:
`layouts[name=yearly]` orders by `score`, a `double`, then by `id`.

### Item bytes and counts

An item's bytes are `canonical(item message)`. Its element count is the total
number of elements across every split field the item message holds. The root
item's count is zero. Vectors: `layouts[*].items[*]`, with `canonical`, `id` and
`elements`.

## Key derivation

Three 32-byte keys are derived from the 32-byte root key with HKDF-SHA256 (RFC
5869). Source: [seal.go](../seal.go).

| Parameter | Value |
|---|---|
| hash | SHA-256 |
| input keying material | the root key |
| salt | none, which RFC 5869 defines as 32 zero bytes |
| output length | 32 bytes |

| Key | `info` (ASCII) | `info` (hex) | Length |
|---|---|---|---|
| naming key | `protovcs/v1/chunk-name` | `70726f746f7663732f76312f6368756e6b2d6e616d65` | 22 |
| content key | `protovcs/v1/chunk-content` | `70726f746f7663732f76312f6368756e6b2d636f6e74656e74` | 25 |
| ref key | `protovcs/v1/ref-name` | `70726f746f7663732f76312f7265662d6e616d65` | 20 |

Vector: `root_key` with `keys.name`, `keys.chunk` and `keys.ref`.

## Chunk ids

A chunk id names an item, a manifest node or a schema chunk. Source:
[seal.go](../seal.go).

```
named(role, key, bytes) = uvarint(len(role)) ‖ role ‖ uvarint(len(key)) ‖ key ‖ bytes
chunk id               = HMAC(naming key, named(role, key, bytes))
```

| Chunk | `role` | `key` | `bytes` |
|---|---|---|---|
| item | `item` | the item key | the item's canonical bytes |
| manifest node | `manifest` | the node's prefix | the node's canonical bytes |
| schema chunk | `schema` | empty | the schema chunk's canonical bytes |

Lengths are byte counts. The canonical bytes are not length-prefixed, because
they are last.

A store opened without a key, the plain mode used for tests and readable exports,
uses `SHA-256(named(role, key, bytes))` instead.

Vector: `chunk_ids`, with `keyring` and `plain` results. It includes the pair
(`a`, `bc`) and (`ab`, `c`), which differ only in where the key ends, and a
200-byte key, whose length takes a two-byte varint.

## Sealed envelope

Chunks and commit payloads are stored sealed. Source: [seal.go](../seal.go).

| Offset | Length | Content |
|---|---|---|
| 0 | 1 | format version, `0x01` |
| 1 | 12 | nonce, drawn at random for each seal |
| 13 | n | AES-256-GCM ciphertext of the plaintext |
| 13 + n | 16 | AES-256-GCM tag |

The AES key is the content key. The additional authenticated data is the 32-byte
id the bytes are stored under: the chunk id for a chunk, the commit id for a
commit payload. The version byte is not part of the additional data.

To open sealed bytes under an id:

1. Refuse fewer than 29 bytes.
2. Refuse a first byte other than `0x01` as an unsupported format. This error is
   distinct from tampering, and is checked before anything else is read.
3. Open bytes 13 onward with AES-256-GCM, using bytes 1–12 as the nonce and the
   id as additional data. A failure is tampering.

In plain mode, sealed bytes are `0x01 ‖ plaintext`. Opening checks only that
the bytes are not empty and that the first is `0x01`.

Vector: `seals`, with fixed nonces, and `plain_sealed` for plain mode.

## Manifest nodes

A version's manifest is a tree of `ManifestNode` messages from
[store.proto](../proto/protovcs/v1/store.proto). Each is stored as a chunk.
Source: [manifest.go](../manifest.go).

| Field | Number | Type | Content |
|---|---|---|---|
| `entries` | 1 | repeated `Entry` | sorted by `segment`, bytewise |
| `Entry.segment` | 1 | string | one path segment |
| `Entry.child` | 2 | bytes, oneof `target` | chunk id of a child node |
| `Entry.item` | 3 | bytes, oneof `target` | chunk id of an item |
| `Entry.total_bytes` | 4 | uint64 | canonical bytes of the item, or the sum over every item in the child's subtree |
| `Entry.element_count` | 5 | uint32 | element count of the item, or the sum over the subtree, modulo 2³² |

### Building the tree

An item key's segments are the parts between `/` characters. The empty key has
one segment, the empty string. A node covers a **prefix**: the empty string for
the root, otherwise its first `d` segments joined by `/`. A node at depth `d`
holds one entry per distinct segment at position `d` among the keys it covers:

- When exactly one key has that segment and the key ends there, the entry names
  the item.
- Otherwise the entry names a child node covering `prefix/segment`, or
  `segment` when the prefix is empty. A key that ends at that segment while
  other keys continue below it is refused.

A chain of single children is not collapsed. One item at
`records/alpha/2026/01` produces nodes at `""`, `records`, `records/alpha` and
`records/alpha/2026`.

Duplicate item keys and a node with more than 65,536 entries are refused. A
version with no items has one root node with no entries, whose canonical bytes
are empty.

A node's id is the chunk id with role `manifest`, its prefix as key, and its
canonical bytes. Vectors: `layouts[*].nodes`, for full builds, and
`update_manifest`, for a tree rebuilt after changing, deleting and adding items.

## Schema chunk

Every commit names a schema chunk, a `SchemaChunk` message from
[store.proto](../proto/protovcs/v1/store.proto). Source:
[store.go](../store.go).

| Field | Number | Content |
|---|---|---|
| `layout` | 1 | the writer's `Layout` message |
| `descriptors` | 2 | a `FileDescriptorSet`, described below |
| `root_message` | 3 | the root message's full name, such as `nextgui.protovcs.testpb.Store` |
| `schema_version` | 4 | the layout's `schema_version` |
| `canonical_version` | 5 | `1` |

The descriptor set holds the root message's file and every file it imports,
transitively, each once. Files are ordered depth first: each file's imports, in
declaration order, come before the file. Each entry is the file's
`FileDescriptorProto` as compiled.

The schema chunk's id is the chunk id with role `schema` and an empty key.
Descriptor bytes that differ from another implementation's produce a different
schema id. Reads still work, but each writer then uploads its own schema chunk.

Vector: `schema`, with `descriptors`, `canonical` and `id`.

## Commits

A commit is a `Commit` record with a sealed `CommitPayload`, both from
[store.proto](../proto/protovcs/v1/store.proto). Source:
[store.go](../store.go).

### The record

| Field | Number | Readable by the server | Content |
|---|---|---|---|
| `id` | 1 | yes | the commit id |
| `parents` | 2 | yes | parent commit ids; the first is the branch head the write started from |
| `added` | 3 | yes | chunk ids this commit holds and its first parent did not |
| `payload` | 4 | no | the sealed `CommitPayload` |
| `format_version` | 5 | yes | `1` |
| `schema_version` | 6 | yes | the layout's `schema_version` |
| `naming_nonce` | 7 | yes | 16 random bytes, not secret |

### The commit id

```
commit id = SHA-256(canonical(record with id and payload unset))
```

The hash is not keyed, and is the same in plain mode. The name covers
`parents`, `added`, `format_version`, `schema_version` and `naming_nonce`,
written in field-number order, so `schema_version` (6) comes before
`naming_nonce` (7). It does not cover the payload, which is sealed under the
name. Readers hash the record as received, so the order of `parents` and
`added` is part of the name.

### The payload

| Field | Number | Content |
|---|---|---|
| `manifest_root` | 1 | the root manifest node's chunk id |
| `schema_chunk` | 2 | the schema chunk's id |
| `tree_digest` | 3 | `SHA-256(manifest_root)`, from [digest.go](../digest.go) |
| `branch` | 4 | the branch name, as text |
| `time` | 7 | a `google.protobuf.Timestamp` from the writer's clock |
| `message` | 8 | a `CommitMessage`: `title`, `description`, `authors`, `client`, `labels`, `details` |

Fields 5 and 6 are reserved. `details` holds the canonical encoding of the
message type the layout's `commit_message` names, or is empty.

The payload is sealed as `seal(canonical(payload))` in the
[envelope](#sealed-envelope), with the content key and the 32-byte commit id
as additional data. A writer therefore computes the id first, then seals the
payload, then sets `id` and `payload`.

### The added list

A writer lists `added` by walking the new tree against the first parent's tree,
starting at the two roots ([manifest.go](../manifest.go)):

1. If the new node's id equals the old node's id at this position, or is already
   listed, stop. Otherwise list it.
2. If the node was carried over from an earlier version rather than built by
   this write, stop.
3. For each entry of the new node, in order, find the old node's entry with the
   same segment. An item entry is listed unless the old entry names the same
   item, or the id is already listed. A child entry recurses, with the old
   entry's child as the old node when the old entry is a child.

After the walk, the schema chunk's id is appended when it differs from the
first parent's `schema_chunk`, or when there is no parent. A commit with no
parent walks against an empty old tree.

Vectors: `update_manifest.added_since`, and `store.commits[*]`, which holds two
consecutive commits with their `payload`, `record`, `id`, `added` and sealed
`uploaded` chunks. Their nonces come from a deterministic stream named
`entropy`: SHA-256 of that label followed by a 4-byte big-endian block
counter, block after block. A write draws one 12-byte nonce per uploaded chunk,
then the 16-byte naming nonce, then the 12-byte payload nonce.
`entropy_start` and `entropy_end` give each commit's byte range in the stream.
Vector: `digests` for `tree_digest`.

## Branches

A branch is stored as a ref whose name is a keyed hash of the branch name.
Source: [seal.go](../seal.go).

```
ref name = HMAC(ref key, branch name)
```

The branch name is its UTF-8 bytes, with no length prefix or normalisation. In
plain mode, the ref name is the branch name's bytes. The `cas_token` on a `Ref`
is opaque to clients. Vectors: `ref_names`, including an empty name, a name
containing `/`, and a non-ASCII name; `store.ref_name`.

## What a reader verifies

Sources: [store.go](../store.go), [seal.go](../seal.go),
[chunkcache.go](../chunkcache.go).

**Commits, whenever one is received.** This covers a branch head, each parent
read while walking history, and every commit in a batched history response.
The reader computes the commit id from the record. It refuses the commit as
tampered when the record's `id` differs from the computed id, or when the
computed id differs from the id requested. A ref names its head commit, so
this check binds a head to the ref. Because the id covers `parents`, a check of
each commit's id also checks the history it claims.

**Before opening a payload.** A `format_version` other than `1` is an
unsupported format. The Go reader also accepts `0`. A `schema_version` below the
reader layout's `min_readable_version` or above its `schema_version` is an
unsupported schema. A reader that reads through the commit's own schema chunk
skips the schema check. The payload is then opened under the commit id.

**Chunks, whenever one is received.** Each chunk is opened under the id it was
requested by, as in [Sealed envelope](#sealed-envelope). Opening authenticates
it, so the reader does not recompute the chunk id from the plaintext.
Recomputing it is possible when the role and key are known, and the Go
implementation does so only to check exports. In plain mode nothing is
authenticated.

**Branch heads.** A reader that remembers the newest head seen on a branch
refuses a head that is a strict ancestor of it. A head on a different line of
history is accepted. The [design document](design.md#the-rollback-check) gives
the limits.

**Not verified.** The Go reader does not check `tree_digest`, that
`payload.branch` matches the ref, the `total_bytes` and `element_count` of
manifest entries, the order of manifest entries, or that received plaintext is
canonical.

The server recomputes the commit id when a commit is stored, and refuses a
record whose non-empty `id` differs ([server/server.go](../server/server.go)).

## Vector coverage

| Rule | Vectors |
|---|---|
| canonical encoding | `canonical` |
| root item, split items, item keys | `layouts[*].items`, over four layouts and three messages |
| segment forms | `segments`, `dates` |
| element order within an item | `layouts[*].items[*].canonical` |
| key derivation | `keys` |
| chunk ids | `chunk_ids` |
| sealed envelope | `seals` |
| manifest nodes | `layouts[*].nodes`, `update_manifest.nodes`, `update_manifest.root` |
| schema chunk | `schema` |
| commit id, payload, sealing | `store.commits[*]` |
| added list | `update_manifest.added_since`, `store.commits[*].added` |
| `tree_digest` | `digests` |
| ref names | `ref_names`, `store.ref_name` |

`layouts[*].tree_digest` is not the commit's `tree_digest`. It is a hash over
the sorted (item key, chunk id) pairs that no commit stores. A commit's
`tree_digest` is the one in `digests`.

These rules have no vector yet:

- Canonical encoding of oneofs set to a zero value, proto3 `optional` fields,
  maps with non-string keys, `float`, `sint`, `fixed` and `bool` fields, NaN
  and `-0.0`, groups, and unknown group records.
- An absent value under the `IDENTITY` or `HASH` transform, a timestamp shard
  key, and an `enum`, `bool`, `bytes` or unsigned identity shard key inside a
  layout.
- Refusals: a date with an invalid month or day, duplicate identities in one
  item, a key that is also a directory, and a node over 65,536 entries.
- Everything under [What a reader verifies](#what-a-reader-verifies): there are
  no vectors for a tampered chunk, a chunk opened under the wrong id, an unknown
  version byte, a short envelope, or a commit whose id does not match its
  record.
- Manifest entries whose segments are prefixes of each other, covered in the
  next section.

## Known divergence

The rule above is that manifest entries are sorted by segment. When the Go
implementation builds a whole tree, it sorts items by full key and takes
segments in that order. The two orders differ when one segment is a prefix of a
sibling and the longer sibling continues with a byte below `/` (0x2F). For keys
`x/a/1` and `x/a-b/1`, the full build writes the node at `x` with entries
`a-b, a`, because `-` (0x2D) sorts before `/`. Rebuilding only changed paths
writes `a, a-b`, so the two builds name the same data differently. A lookup
binary-searches entries by segment and does not find `x/a/1` in the full
build's tree. An implementation should sort entries by segment. Escaped values
are affected, because `%` (0x25) is below `/`.
