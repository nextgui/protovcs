package protovcs_test

// Golden vectors for the Rust protovcs client in rust/.
//
// The Go and Rust clients are kept separately and must write the same bytes:
// a chunk named or sealed differently by one of them is a chunk the other
// cannot find. This test computes, with the Go client, every value a store
// hashes, seals or sends, from fixed keys, nonces and times, and writes them to
// testdata/golden/vectors.json. The Rust tests read that file and must
// reproduce it.
//
// Run with -golden_out=<path> to write the file:
//
//	go test -run TestGoldenVectors -golden_out=$PWD/testdata/golden/vectors.json .
//
// Under Bazel, bazel run //:update_golden does the same, and the test it
// defines fails when the checked-in copy is stale. Without the flag this test
// only checks that the vectors agree with the Go store.

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"math"
	"os"
	"sort"
	"strconv"
	"testing"

	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	protovcs "github.com/nextgui/protovcs"
	"github.com/nextgui/protovcs/memtransport"
	testpb "github.com/nextgui/protovcs/proto/protovcs/testpb"
	pb "github.com/nextgui/protovcs/proto/protovcs/v1"
)

// negativeAmount is a variable so its conversion to uint64 sign-extends.
var negativeAmount int64 = -7

var goldenOut = flag.String("golden_out", "", "where to write the golden vectors")

// hexBytes is bytes written as lowercase hex in JSON.
type hexBytes []byte

func (h hexBytes) MarshalJSON() ([]byte, error) { return json.Marshal(hex.EncodeToString(h)) }

type goldenVectors struct {
	Comment string `json:"comment"`

	RootKey hexBytes `json:"root_key"`
	Keys    struct {
		Name  hexBytes `json:"name"`
		Chunk hexBytes `json:"chunk"`
		Ref   hexBytes `json:"ref"`
	} `json:"keys"`

	ChunkIDs []chunkIDVector `json:"chunk_ids"`
	RefNames []refNameVector `json:"ref_names"`
	Seals    []sealVector    `json:"seals"`

	Segments  []segmentVector  `json:"segments"`
	Dates     []dateVector     `json:"dates"`
	Canonical []canonicalVec   `json:"canonical"`
	Layouts   []layoutVector   `json:"layouts"`
	Update    updateVector     `json:"update_manifest"`
	Schema    schemaVector     `json:"schema"`
	Store     storeVector      `json:"store"`
	Digests   []digestVector   `json:"digests"`
	Entropy   string           `json:"entropy"`
	Messages  []messageVector  `json:"messages"`
	Refs      []sameRefsVector `json:"same_refs"`
}

type chunkIDVector struct {
	Role      string   `json:"role"`
	Key       string   `json:"key"`
	Canonical hexBytes `json:"canonical"`
	Keyring   hexBytes `json:"keyring"`
	Plain     hexBytes `json:"plain"`
}

type refNameVector struct {
	Branch  string   `json:"branch"`
	Keyring hexBytes `json:"keyring"`
	Plain   hexBytes `json:"plain"`
}

type sealVector struct {
	ID        hexBytes `json:"id"`
	Nonce     hexBytes `json:"nonce"`
	Plaintext hexBytes `json:"plaintext"`
	Sealed    hexBytes `json:"sealed"`
	Plain     hexBytes `json:"plain_sealed"`
}

type segmentVector struct {
	Kind  string `json:"kind"`
	Input string `json:"input"`
	Want  string `json:"want"`
}

type dateVector struct {
	Seconds int64 `json:"seconds"`
	Year    int   `json:"year"`
	Month   int   `json:"month"`
	Day     int   `json:"day"`
}

// canonicalVec is a testpb.Store given as arbitrary wire bytes, and the one
// encoding protovcs produces of it.
type canonicalVec struct {
	Name      string   `json:"name"`
	Input     hexBytes `json:"input"`
	Canonical hexBytes `json:"canonical"`
}

// messageVector is a message the vectors below decompose, named so they can
// refer to it.
type messageVector struct {
	Name  string   `json:"name"`
	Bytes hexBytes `json:"bytes"`
}

type itemVector struct {
	Key       string   `json:"key"`
	Canonical hexBytes `json:"canonical"`
	ID        hexBytes `json:"id"`
	Elements  uint32   `json:"elements"`
}

type nodeVector struct {
	Prefix    string   `json:"prefix"`
	Canonical hexBytes `json:"canonical"`
	ID        hexBytes `json:"id"`
}

type layoutVector struct {
	Name    string       `json:"name"`
	Layout  hexBytes     `json:"layout"`
	Message string       `json:"message"`
	Items   []itemVector `json:"items"`
	Nodes   []nodeVector `json:"nodes"`
	Merged  hexBytes     `json:"merged"`
	Tree    hexBytes     `json:"tree_digest"`
}

type updateVector struct {
	Layout  string         `json:"layout"`
	Changes []updateChange `json:"changes"`
	Nodes   []nodeVector   `json:"nodes"`
	Root    hexBytes       `json:"root"`
	Added   []hexBytes     `json:"added_since"`
}

type updateChange struct {
	Key      string   `json:"key"`
	Delete   bool     `json:"delete"`
	ID       hexBytes `json:"id"`
	Bytes    uint64   `json:"bytes"`
	Elements uint32   `json:"elements"`
}

type schemaVector struct {
	Descriptors hexBytes `json:"descriptors"`
	Canonical   hexBytes `json:"canonical"`
	ID          hexBytes `json:"id"`
}

type digestVector struct {
	Layout string   `json:"layout"`
	Node   hexBytes `json:"node_digest"`
}

type sameRefsVector struct {
	A    []refVector `json:"a"`
	B    []refVector `json:"b"`
	Same bool        `json:"same"`
}

type refVector struct {
	Name   hexBytes `json:"name"`
	Commit hexBytes `json:"commit"`
	Token  string   `json:"token"`
}

// storeVector is two commits on one branch, written as a store writes them,
// with every random value drawn from the entropy stream and the clock fixed.
type storeVector struct {
	Branch    string         `json:"branch"`
	RefName   hexBytes       `json:"ref_name"`
	Commits   []commitVector `json:"commits"`
	ReadBack  hexBytes       `json:"read_back"`
	Reachable []hexBytes     `json:"reachable"`
}

type commitVector struct {
	Message      string        `json:"message"`
	Title        string        `json:"title"`
	Description  string        `json:"description"`
	Author       string        `json:"author"`
	Email        string        `json:"email"`
	Client       string        `json:"client"`
	Labels       []labelVector `json:"labels"`
	Details      hexBytes      `json:"details"`
	Seconds      int64         `json:"seconds"`
	Nanos        int32         `json:"nanos"`
	Payload      hexBytes      `json:"payload"`
	Record       hexBytes      `json:"record"`
	ID           hexBytes      `json:"id"`
	Added        []hexBytes    `json:"added"`
	Uploaded     []sealedChunk `json:"uploaded"`
	EntropyStart int           `json:"entropy_start"`
	EntropyEnd   int           `json:"entropy_end"`
}

type labelVector struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

type sealedChunk struct {
	ID     hexBytes `json:"id"`
	Sealed hexBytes `json:"sealed"`
}

// entropy is the stream every random value in the store vector is drawn from:
// SHA-256 of a label and a big-endian block counter, block after block. Both
// clients read it in the order a write draws: one 12-byte nonce per uploaded
// chunk, in the order HasChunks reports them, then the commit's 16-byte naming
// nonce, then the 12-byte nonce sealing its payload.
type entropy struct {
	buf   []byte
	block uint32
	read  int
}

const entropyLabel = "protovcs golden entropy"

func (e *entropy) Read(p []byte) (int, error) {
	for len(e.buf) < len(p) {
		var counter [4]byte
		binary.BigEndian.PutUint32(counter[:], e.block)
		e.block++
		sum := sha256.Sum256(append([]byte(entropyLabel), counter[:]...))
		e.buf = append(e.buf, sum[:]...)
	}
	copy(p, e.buf)
	e.buf = e.buf[len(p):]
	e.read += len(p)
	return len(p), nil
}

func (e *entropy) take(n int) []byte {
	out := make([]byte, n)
	_, _ = e.Read(out)
	return out
}

func goldenRootKey() []byte {
	key := make([]byte, protovcs.RootKeyLen)
	for i := range key {
		key[i] = byte(i*7 + 3)
	}
	return key
}

func goldenLayouts() map[string]*pb.Layout {
	return map[string]*pb.Layout{
		"teststore": {
			Name:          "teststore",
			RootMessage:   "nextgui.protovcs.testpb.Store",
			SchemaVersion: 1,
			Splits: []*pb.Split{
				{
					Field: "records",
					By: []*pb.ShardKey{
						{Field: "group", Transform: pb.Transform_TRANSFORM_IDENTITY},
						{Field: "date", Transform: pb.Transform_TRANSFORM_MONTH},
					},
					Order: []string{"id"},
				},
				{Field: "accounts", By: []*pb.ShardKey{{Field: "id", Transform: pb.Transform_TRANSFORM_IDENTITY}}},
			},
			RootMaxBytes:  65536,
			CommitMessage: "nextgui.protovcs.testpb.Change",
		},
		"hashed": {
			Name:          "hashed",
			RootMessage:   "nextgui.protovcs.testpb.Store",
			SchemaVersion: 2,
			Splits: []*pb.Split{
				{
					Field: "records",
					By: []*pb.ShardKey{
						{Field: "id", Transform: pb.Transform_TRANSFORM_HASH, Buckets: 16},
						{Field: "date", Transform: pb.Transform_TRANSFORM_QUARTER},
					},
				},
				{Field: "accounts", By: []*pb.ShardKey{{Field: "label", Transform: pb.Transform_TRANSFORM_HASH, Buckets: 3}}},
			},
		},
		"yearly": {
			Name:          "yearly",
			RootMessage:   "nextgui.protovcs.testpb.Store",
			SchemaVersion: 1,
			Splits: []*pb.Split{
				{
					Field: "records",
					By: []*pb.ShardKey{
						{Field: "date", Transform: pb.Transform_TRANSFORM_YEAR},
						{Field: "amount", Transform: pb.Transform_TRANSFORM_IDENTITY},
					},
					Order: []string{"score", "id"},
				},
				{Field: "group.members", By: []*pb.ShardKey{{Field: "id", Transform: pb.Transform_TRANSFORM_IDENTITY}}},
			},
		},
		"daily": {
			Name:          "daily",
			RootMessage:   "nextgui.protovcs.testpb.Store",
			SchemaVersion: 1,
			Splits: []*pb.Split{
				{
					Field: "records",
					By: []*pb.ShardKey{
						{Field: "group", Transform: pb.Transform_TRANSFORM_IDENTITY},
						{Field: "date", Transform: pb.Transform_TRANSFORM_DAY},
					},
				},
			},
		},
	}
}

func goldenRecords() []*testpb.Record {
	// "gamma" beside "gamma%": the escaped segment "gamma%25" sorts after
	// "gamma" by segment and before it by whole key.
	groups := []string{"alpha", "beta/slash", "gamma%", "gamma"}
	var out []*testpb.Record
	for i := 0; i < 24; i++ {
		r := &testpb.Record{
			Id:     fmt.Sprintf("rec-%03d", (i*37)%101),
			Group:  groups[i%len(groups)],
			Date:   &testpb.Date{Year: int32(2024 + i%3), Month: int32(1 + (i*5)%12), Day: int32(1 + (i*11)%28)},
			Amount: int64(i%5) - 2,
			Score:  float64(i%7)*1.5e6 - 3.25,
		}
		if i%4 == 0 {
			r.Tags = []string{"t" + strconv.Itoa(i), "a"}
			r.Attributes = map[string]string{"z": "last", "a": "first", strconv.Itoa(i): "n"}
			r.Counters = []int32{int32(i), -1, 300}
		}
		if i%6 == 1 {
			r.Nested = &testpb.Nested{Note: "n" + strconv.Itoa(i), Values: []int64{-5, 1 << 40}}
			r.Payload = []byte{0, 1, 2, byte(i)}
		}
		out = append(out, r)
	}
	return out
}

func goldenStore() *testpb.Store {
	return &testpb.Store{
		Manifest: &testpb.Manifest{SchemaVersion: 1},
		Name:     "golden",
		Revision: 7,
		Records:  goldenRecords(),
		Accounts: []*testpb.Account{{Id: "acc-2", Label: "two"}, {Id: "acc-1", Label: "one"}, {Id: "acc/3", Label: "three"}},
		Group:    &testpb.Group{Members: []*testpb.Account{{Id: "m2", Label: "b"}, {Id: "m1", Label: "a"}}},
	}
}

// goldenStoreChanged is goldenStore with one record changed, one added and
// one account removed: the second commit of the store vector.
func goldenStoreChanged() *testpb.Store {
	s := goldenStore()
	s.Revision = 8
	s.Records[3].Amount = 99
	s.Records = append(s.Records, &testpb.Record{Id: "rec-new", Group: "delta", Date: &testpb.Date{Year: 2027, Month: 2, Day: 3}})
	s.Accounts = s.Accounts[:2]
	return s
}

func mustMarshal(t *testing.T, m proto.Message) []byte {
	t.Helper()
	b, err := proto.MarshalOptions{Deterministic: true}.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func compile(t *testing.T, l *pb.Layout) *protovcs.Compiled {
	t.Helper()
	c, err := protovcs.CompileLayout(l, (&testpb.Store{}).ProtoReflect().Descriptor())
	if err != nil {
		t.Fatalf("CompileLayout(%s): %v", l.GetName(), err)
	}
	return c
}

func TestGoldenVectors(t *testing.T) {
	ctx := context.Background()
	var v goldenVectors
	v.Comment = "Written by TestGoldenVectors in golden_test.go. Do not edit; run bazel run //:update_golden."

	v.RootKey = goldenRootKey()
	kr, err := protovcs.NewKeyring(v.RootKey)
	if err != nil {
		t.Fatal(err)
	}
	plain := protovcs.PlainSealer()
	name, chunk, ref := protovcs.KeyringKeysForTest(kr)
	v.Keys.Name, v.Keys.Chunk, v.Keys.Ref = name, chunk, ref

	// Names, including the separator cases writeNamed exists for.
	for _, c := range []struct {
		role      protovcs.Role
		key       string
		canonical []byte
	}{
		{protovcs.RoleItem, "", nil},
		{protovcs.RoleItem, "records/alpha/2026/01", []byte{8, 1}},
		{protovcs.RoleManifest, "records", []byte("abc")},
		{protovcs.RoleSchema, "", bytes.Repeat([]byte{0xff}, 300)},
		{protovcs.RoleItem, "a", []byte("bc")},
		{protovcs.RoleItem, "ab", []byte("c")},
		{protovcs.RoleItem, string(bytes.Repeat([]byte("k"), 200)), []byte{0}},
	} {
		v.ChunkIDs = append(v.ChunkIDs, chunkIDVector{
			Role: string(c.role), Key: c.key, Canonical: c.canonical,
			Keyring: idBytes(kr.ChunkID(c.role, c.key, c.canonical)),
			Plain:   idBytes(plain.ChunkID(c.role, c.key, c.canonical)),
		})
	}
	for _, b := range []string{"main", "", "feature/x", "ünïcode"} {
		v.RefNames = append(v.RefNames, refNameVector{Branch: b, Keyring: kr.RefName(b), Plain: plain.RefName(b)})
	}

	// Seals with chosen nonces. Each is opened by the real Open, so the
	// framing written here is the framing Seal writes.
	for i, p := range [][]byte{nil, []byte("hello"), bytes.Repeat([]byte{7}, 1000)} {
		id := kr.ChunkID(protovcs.RoleItem, "seal/"+strconv.Itoa(i), p)
		nonce := bytes.Repeat([]byte{byte(0x10 + i)}, 12)
		sealed := protovcs.SealWithNonceForTest(kr, id, nonce, p)
		opened, err := kr.Open(id, sealed)
		if err != nil || !bytes.Equal(opened, p) {
			t.Fatalf("a vector seal does not open: %v", err)
		}
		plainSealed, _ := plain.Seal(id, p)
		v.Seals = append(v.Seals, sealVector{ID: idBytes(id), Nonce: nonce, Plaintext: p, Sealed: sealed, Plain: plainSealed})
	}

	v.Segments = goldenSegments()
	for _, sec := range []int64{0, -1, 86399, 86400, 951782400, 1709164800, -62135596800, 253402300799, 1793966400} {
		d := protovcs.UnixToDateForTest(sec)
		v.Dates = append(v.Dates, dateVector{Seconds: sec, Year: d[0], Month: d[1], Day: d[2]})
	}

	v.Canonical = goldenCanonical(t)

	// Messages the layout vectors decompose.
	messages := map[string]*testpb.Store{
		"store":     goldenStore(),
		"changed":   goldenStoreChanged(),
		"empty":     {},
		"only_root": {Name: "root only", Revision: -1},
	}
	var messageNames []string
	for n := range messages {
		messageNames = append(messageNames, n)
	}
	sort.Strings(messageNames)
	for _, n := range messageNames {
		v.Messages = append(v.Messages, messageVector{Name: n, Bytes: mustMarshal(t, messages[n])})
	}

	layouts := goldenLayouts()
	var layoutNames []string
	for n := range layouts {
		layoutNames = append(layoutNames, n)
	}
	sort.Strings(layoutNames)
	for _, ln := range layoutNames {
		c := compile(t, layouts[ln])
		for _, mn := range []string{"store", "only_root", "empty"} {
			lv := goldenLayout(t, kr, c, ln, mn, messages[mn])
			v.Layouts = append(v.Layouts, lv)
		}
	}

	v.Update = goldenUpdate(t, kr, compile(t, layouts["teststore"]))

	for _, id := range [][]byte{make([]byte, 32), bytes.Repeat([]byte{9}, 32)} {
		root, _ := protovcs.ChunkIDFromBytes(id)
		v.Digests = append(v.Digests, digestVector{Layout: hex.EncodeToString(id), Node: protovcs.NodeDigestForTest(root)})
	}

	v.Refs = goldenSameRefs()

	v.Entropy = entropyLabel
	v.Schema, v.Store = goldenStoreVector(ctx, t, compile(t, layouts["teststore"]))

	if *goldenOut != "" {
		out, err := json.MarshalIndent(v, "", " ")
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(*goldenOut, append(out, '\n'), 0o644); err != nil {
			t.Fatal(err)
		}
	}
}

func idBytes(id protovcs.ChunkID) hexBytes { return id.Bytes() }

func goldenSegments() []segmentVector {
	var out []segmentVector
	for _, d := range [][3]int{{2026, 1, 9}, {999, 12, 31}, {2024, 4, 1}, {2024, 7, 15}, {2024, 10, 30}, {12345, 6, 7}} {
		for _, tr := range []pb.Transform{pb.Transform_TRANSFORM_DAY, pb.Transform_TRANSFORM_MONTH, pb.Transform_TRANSFORM_QUARTER, pb.Transform_TRANSFORM_YEAR} {
			out = append(out, segmentVector{
				Kind:  "date_" + tr.String(),
				Input: fmt.Sprintf("%d-%d-%d", d[0], d[1], d[2]),
				Want:  protovcs.DateSegment(tr, d[0], d[1], d[2]),
			})
		}
	}
	for _, i := range []int64{0, 1, -1, 42, -42, math.MaxInt64, math.MinInt64 + 1} {
		out = append(out, segmentVector{Kind: "int", Input: strconv.FormatInt(i, 10), Want: protovcs.IntSegment(i)})
	}
	for _, u := range []uint64{0, 1, math.MaxUint64} {
		out = append(out, segmentVector{Kind: "uint", Input: strconv.FormatUint(u, 10), Want: protovcs.UintSegment(u)})
	}
	for _, s := range []string{"plain", "a/b", "100%", "tab\there", "del\x7f", "", "ü/ß"} {
		out = append(out, segmentVector{Kind: "string", Input: s, Want: protovcs.StringSegment(s)})
	}
	// Floats order elements by their printed form, which is fmt.Sprint's,
	// because protoreflect.Value.String formats them that way.
	for _, f := range []float64{0, -3.25, 1.5e6, 1e20, 1e21, 123456, 0.0001, 0.00001, 8996996.75, 2.5e-7, math.MaxFloat64} {
		out = append(out, segmentVector{Kind: "float64", Input: strconv.FormatFloat(f, 'g', -1, 64), Want: fmt.Sprint(f)})
	}
	for _, f := range []float32{0.1, 3.4e38, 1e-7, 16777216} {
		out = append(out, segmentVector{Kind: "float32", Input: strconv.FormatFloat(float64(f), 'g', -1, 32), Want: fmt.Sprint(f)})
	}
	out = append(out,
		segmentVector{Kind: "bool", Input: "true", Want: protovcs.BoolSegment(true)},
		segmentVector{Kind: "bool", Input: "false", Want: protovcs.BoolSegment(false)})
	return out
}

// goldenCanonical builds wire encodings that differ from canonical order: an
// unpacked repeated scalar, map entries out of order, fields out of number
// order, and unknown fields between known ones.
func goldenCanonical(t *testing.T) []canonicalVec {
	var out []canonicalVec
	add := func(name string, input []byte) {
		m := &testpb.Store{}
		if err := proto.Unmarshal(input, m); err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		c, err := protovcs.CanonicalBytes(m)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		out = append(out, canonicalVec{Name: name, Input: input, Canonical: c})
	}

	add("store", mustMarshal(t, goldenStore()))
	add("empty", nil)

	// A record with its fields in reverse order, counters unpacked and the map
	// entries unsorted, and unknown fields at 99 and 12 inside it and at 50 at
	// the top level.
	var rec []byte
	rec = protowire.AppendTag(rec, 99, protowire.VarintType)
	rec = protowire.AppendVarint(rec, 5)
	for _, c := range []int32{3, -2, 1} {
		rec = protowire.AppendTag(rec, 10, protowire.VarintType)
		rec = protowire.AppendVarint(rec, uint64(int64(c)))
	}
	rec = protowire.AppendTag(rec, 9, protowire.Fixed64Type)
	rec = protowire.AppendFixed64(rec, math.Float64bits(-0.5))
	for _, kv := range [][2]string{{"y", "2"}, {"b", "1"}, {"y", "3"}} {
		var entry []byte
		entry = protowire.AppendTag(entry, 1, protowire.BytesType)
		entry = protowire.AppendString(entry, kv[0])
		entry = protowire.AppendTag(entry, 2, protowire.BytesType)
		entry = protowire.AppendString(entry, kv[1])
		rec = protowire.AppendTag(rec, 6, protowire.BytesType)
		rec = protowire.AppendBytes(rec, entry)
	}
	rec = protowire.AppendTag(rec, 12, protowire.BytesType)
	rec = protowire.AppendString(rec, "unknown string")
	rec = protowire.AppendTag(rec, 4, protowire.VarintType)
	rec = protowire.AppendVarint(rec, uint64(negativeAmount))
	rec = protowire.AppendTag(rec, 1, protowire.BytesType)
	rec = protowire.AppendString(rec, "id-x")
	rec = protowire.AppendTag(rec, 99, protowire.Fixed32Type)
	rec = protowire.AppendFixed32(rec, 0xdeadbeef)

	var top []byte
	top = protowire.AppendTag(top, 5, protowire.VarintType)
	top = protowire.AppendVarint(top, 1)
	top = protowire.AppendTag(top, 50, protowire.BytesType)
	top = protowire.AppendString(top, "top unknown")
	top = protowire.AppendTag(top, 2, protowire.BytesType)
	top = protowire.AppendBytes(top, rec)
	top = protowire.AppendTag(top, 4, protowire.BytesType)
	top = protowire.AppendString(top, "named")
	top = protowire.AppendTag(top, 1, protowire.BytesType)
	top = protowire.AppendBytes(top, nil)
	add("scrambled", top)

	// Zero values written explicitly, which canonical bytes leave out.
	var zeros []byte
	zeros = protowire.AppendTag(zeros, 5, protowire.VarintType)
	zeros = protowire.AppendVarint(zeros, 0)
	zeros = protowire.AppendTag(zeros, 4, protowire.BytesType)
	zeros = protowire.AppendString(zeros, "")
	add("explicit_zeros", zeros)
	return out
}

func goldenLayout(t *testing.T, s protovcs.Sealer, c *protovcs.Compiled, layoutName, msgName string, m *testpb.Store) layoutVector {
	t.Helper()
	lv := layoutVector{Name: layoutName, Layout: mustMarshal(t, c.Layout()), Message: msgName}
	items, err := c.Materialize(m)
	if err != nil {
		t.Fatalf("%s/%s: Materialize: %v", layoutName, msgName, err)
	}
	var refs []protovcs.ItemRef
	for _, it := range items {
		canonical, err := protovcs.CanonicalBytes(it.Fragment)
		if err != nil {
			t.Fatal(err)
		}
		id := s.ChunkID(protovcs.RoleItem, string(it.Key), canonical)
		n := protovcs.ElementCountForTest(c, it)
		lv.Items = append(lv.Items, itemVector{Key: string(it.Key), Canonical: canonical, ID: idBytes(id), Elements: n})
		refs = append(refs, protovcs.ItemRef{Key: it.Key, ID: id, Bytes: uint64(len(canonical)), Elements: n})
	}
	nodes, err := protovcs.BuildManifest(s, refs)
	if err != nil {
		t.Fatalf("%s/%s: BuildManifest: %v", layoutName, msgName, err)
	}
	for _, n := range nodes {
		lv.Nodes = append(lv.Nodes, nodeVector{Prefix: n.Prefix, Canonical: n.Canonical, ID: idBytes(n.ID)})
	}
	merged, err := c.Merge(items)
	if err != nil {
		t.Fatal(err)
	}
	if lv.Merged, err = protovcs.CanonicalBytes(merged); err != nil {
		t.Fatal(err)
	}
	lv.Tree = protovcs.TreeDigestForTest(refs)
	return lv
}

// goldenUpdate rebuilds the teststore manifest of goldenStore after changing
// one item, deleting one and adding one under a directory that did not exist.
func goldenUpdate(t *testing.T, s protovcs.Sealer, c *protovcs.Compiled) updateVector {
	t.Helper()
	items, err := c.Materialize(goldenStore())
	if err != nil {
		t.Fatal(err)
	}
	var refs []protovcs.ItemRef
	for _, it := range items {
		canonical, _ := protovcs.CanonicalBytes(it.Fragment)
		refs = append(refs, protovcs.ItemRef{Key: it.Key, ID: s.ChunkID(protovcs.RoleItem, string(it.Key), canonical), Bytes: uint64(len(canonical)), Elements: protovcs.ElementCountForTest(c, it)})
	}
	old, err := protovcs.BuildManifest(s, refs)
	if err != nil {
		t.Fatal(err)
	}
	byID := map[protovcs.ChunkID]*pb.ManifestNode{}
	for _, n := range old {
		byID[n.ID] = n.Node
	}
	fetch := func(_ string, id protovcs.ChunkID) (*pb.ManifestNode, error) {
		n, ok := byID[id]
		if !ok {
			return nil, protovcs.ErrNoSuchChunk
		}
		return n, nil
	}

	newID := func(k string) protovcs.ChunkID { return s.ChunkID(protovcs.RoleItem, k, []byte(k)) }
	changes := map[protovcs.ItemKey]*protovcs.ItemRef{
		refs[1].Key:            {Key: refs[1].Key, ID: newID("changed"), Bytes: 11, Elements: 2},
		refs[len(refs)-1].Key:  nil,
		"records/zeta/2030/05": {Key: "records/zeta/2030/05", ID: newID("added"), Bytes: 5, Elements: 1},
		"accounts/acc-9":       {Key: "accounts/acc-9", ID: newID("account"), Bytes: 3, Elements: 1},
	}
	nodes, root, err := protovcs.UpdateManifest(s, old[len(old)-1].ID, changes, fetch)
	if err != nil {
		t.Fatal(err)
	}

	uv := updateVector{Layout: "teststore", Root: idBytes(root)}
	var keys []string
	for k := range changes {
		keys = append(keys, string(k))
	}
	sort.Strings(keys)
	for _, k := range keys {
		r := changes[protovcs.ItemKey(k)]
		if r == nil {
			uv.Changes = append(uv.Changes, updateChange{Key: k, Delete: true})
			continue
		}
		uv.Changes = append(uv.Changes, updateChange{Key: k, ID: idBytes(r.ID), Bytes: r.Bytes, Elements: r.Elements})
	}
	for _, n := range nodes {
		uv.Nodes = append(uv.Nodes, nodeVector{Prefix: n.Prefix, Canonical: n.Canonical, ID: idBytes(n.ID)})
	}
	added, err := protovcs.AddedSince(nodes, root, old[len(old)-1].ID, fetch)
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range added {
		uv.Added = append(uv.Added, idBytes(id))
	}
	return uv
}

func goldenSameRefs() []sameRefsVector {
	r := func(name string, commit byte, token string) refVector {
		return refVector{Name: []byte(name), Commit: bytes.Repeat([]byte{commit}, 32), Token: token}
	}
	cases := []sameRefsVector{
		{A: []refVector{r("a", 1, "1"), r("b", 2, "2")}, B: []refVector{r("b", 2, "2"), r("a", 1, "1")}},
		{A: []refVector{r("a", 1, "1")}, B: []refVector{r("a", 1, "2")}},
		{A: []refVector{r("a", 1, "1")}, B: []refVector{r("a", 3, "1")}},
		{A: []refVector{r("a", 1, "1")}, B: nil},
		{A: nil, B: nil},
	}
	for i := range cases {
		cases[i].Same = protovcs.SameRefs(refStates(cases[i].A), refStates(cases[i].B))
	}
	return cases
}

func refStates(in []refVector) []protovcs.RefState {
	var out []protovcs.RefState
	for _, r := range in {
		id, _ := protovcs.CommitIDFromBytes(r.Commit)
		out = append(out, protovcs.RefState{Name: r.Name, Commit: id, CASToken: r.Token})
	}
	return out
}

// goldenCommit is what a store writes for one version, built step by step
// from the exported parts. goldenStoreVector checks it against a real write.
type goldenCommit struct {
	commit   *pb.Commit
	payload  []byte
	added    []protovcs.ChunkID
	uploaded []protovcs.SealedChunk
	nodes    []protovcs.ManifestChunk
	root     protovcs.ChunkID
}

func buildGoldenCommit(t *testing.T, s *protovcs.Store, kr protovcs.Sealer, rnd *entropy, branch string,
	meta protovcs.CommitMeta, at *timestamppb.Timestamp, m proto.Message, parent *goldenCommit,
	stored map[protovcs.ChunkID]bool) *goldenCommit {

	t.Helper()
	c := s.Layout()
	items, err := c.Materialize(m)
	if err != nil {
		t.Fatal(err)
	}
	pending := map[protovcs.ChunkID][]byte{}
	var refs []protovcs.ItemRef
	for _, it := range items {
		canonical, _ := protovcs.CanonicalBytes(it.Fragment)
		id := kr.ChunkID(protovcs.RoleItem, string(it.Key), canonical)
		pending[id] = canonical
		refs = append(refs, protovcs.ItemRef{Key: it.Key, ID: id, Bytes: uint64(len(canonical)), Elements: protovcs.ElementCountForTest(c, it)})
	}
	nodes, err := protovcs.BuildManifest(kr, refs)
	if err != nil {
		t.Fatal(err)
	}
	for _, n := range nodes {
		pending[n.ID] = n.Canonical
	}
	root := nodes[len(nodes)-1].ID
	schemaID, schemaBytes, err := protovcs.SchemaChunkForTest(s)
	if err != nil {
		t.Fatal(err)
	}
	pending[schemaID] = schemaBytes

	var oldRoot protovcs.ChunkID
	oldNodes := map[protovcs.ChunkID]*pb.ManifestNode{}
	addSchema := true
	if parent != nil {
		oldRoot = parent.root
		for _, n := range parent.nodes {
			oldNodes[n.ID] = n.Node
		}
		addSchema = false
	}
	added, err := protovcs.AddedSince(nodes, root, oldRoot, func(_ string, id protovcs.ChunkID) (*pb.ManifestNode, error) {
		n, ok := oldNodes[id]
		if !ok {
			return nil, protovcs.ErrNoSuchChunk
		}
		return n, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if addSchema {
		added = append(added, schemaID)
	}

	g := &goldenCommit{added: added, nodes: nodes, root: root}
	for _, id := range added {
		if stored[id] {
			continue
		}
		sealed := protovcs.SealWithNonceForTest(kr, id, rnd.take(12), pending[id])
		g.uploaded = append(g.uploaded, protovcs.SealedChunk{ID: id, Sealed: sealed})
		stored[id] = true
	}

	message, err := protovcs.CommitMessageForTest(s, meta)
	if err != nil {
		t.Fatal(err)
	}
	payload := &pb.CommitPayload{
		ManifestRoot: root.Bytes(),
		SchemaChunk:  schemaID.Bytes(),
		TreeDigest:   protovcs.NodeDigestForTest(root),
		Branch:       branch,
		Message:      message,
		Time:         at,
	}
	if g.payload, err = protovcs.CanonicalBytes(payload); err != nil {
		t.Fatal(err)
	}
	commit := &pb.Commit{
		FormatVersion: protovcs.FormatVersion,
		SchemaVersion: c.SchemaVersion(),
		NamingNonce:   rnd.take(16),
	}
	for _, id := range added {
		commit.Added = append(commit.Added, id.Bytes())
	}
	if parent != nil {
		commit.Parents = [][]byte{parent.commit.GetId()}
	}
	name, err := protovcs.CommitName(commit)
	if err != nil {
		t.Fatal(err)
	}
	commit.Payload = protovcs.SealWithNonceForTest(kr, protovcs.ChunkID(name), rnd.take(12), g.payload)
	commit.Id = name.Bytes()
	g.commit = commit
	return g
}

func goldenStoreVector(ctx context.Context, t *testing.T, c *protovcs.Compiled) (schemaVector, storeVector) {
	t.Helper()
	kr, _ := protovcs.NewKeyring(goldenRootKey())
	s, err := protovcs.NewBuilder().WithTransport(memtransport.New()).WithSealer(kr).WithLayout(c).Build()
	if err != nil {
		t.Fatal(err)
	}

	var sv schemaVector
	set, err := protovcs.DescriptorSetForTest(c.Descriptor())
	if err != nil {
		t.Fatal(err)
	}
	sv.Descriptors = mustMarshal(t, set)
	schemaID, schemaBytes, err := protovcs.SchemaChunkForTest(s)
	if err != nil {
		t.Fatal(err)
	}
	sv.Canonical, sv.ID = schemaBytes, idBytes(schemaID)

	branch := "main"
	type spec struct {
		msg     *testpb.Store
		meta    protovcs.CommitMeta
		seconds int64
		nanos   int32
	}
	first := protovcs.Commit("first", "Ada", "ada@example.com")
	first.Client = "golden/1"
	first.Labels = map[string]string{"z": "26", "a": "1"}
	first.Details = &testpb.Change{Source: "golden", RecordsAdded: 24, TouchedGroups: []string{"beta/slash", "alpha"}}
	second := protovcs.Commit("second", "Grace", "grace@example.com")
	second.Description = "one changed, one added, one removed"
	specs := []spec{
		{goldenStore(), first, 1793966400, 500000000},
		{goldenStoreChanged(), second, 1793970000, 0},
	}

	rnd := &entropy{}
	stored := map[protovcs.ChunkID]bool{}
	var built []*goldenCommit
	var parent *goldenCommit
	for _, sp := range specs {
		g := buildGoldenCommit(t, s, kr, rnd, branch, sp.meta, &timestamppb.Timestamp{Seconds: sp.seconds, Nanos: sp.nanos}, sp.msg, parent, stored)
		built = append(built, g)
		parent = g
	}

	// The same two writes through a real store, drawing from the same stream.
	// The clock is not fixed there, so the payloads differ in their time and
	// nothing else.
	checkAgainstRealWrites(ctx, t, c, branch, specs[0].msg, specs[0].meta, specs[1].msg, specs[1].meta, built)

	st := storeVector{Branch: branch, RefName: kr.RefName(branch)}
	offset := 0
	for i, g := range built {
		cv := commitVector{
			Message: []string{"store", "changed"}[i],
			Title:   specs[i].meta.Title, Author: specs[i].meta.Authors[0].Name, Email: specs[i].meta.Authors[0].Email,
			Client: specs[i].meta.Client, Seconds: specs[i].seconds, Nanos: specs[i].nanos,
			Payload: g.payload, Record: mustMarshal(t, g.commit), ID: g.commit.GetId(),
			EntropyStart: offset,
		}
		cv.Description = specs[i].meta.Description
		var keys []string
		for k := range specs[i].meta.Labels {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			cv.Labels = append(cv.Labels, labelVector{Key: k, Value: specs[i].meta.Labels[k]})
		}
		if specs[i].meta.Details != nil {
			cv.Details = mustMarshal(t, specs[i].meta.Details)
		}
		for _, id := range g.added {
			cv.Added = append(cv.Added, idBytes(id))
		}
		for _, u := range g.uploaded {
			cv.Uploaded = append(cv.Uploaded, sealedChunk{ID: idBytes(u.ID), Sealed: u.Sealed})
		}
		offset += 12*len(g.uploaded) + 16 + 12
		cv.EntropyEnd = offset
		st.Commits = append(st.Commits, cv)
	}

	// What reading the head gives back, and what a pack hint names for it.
	tr := memtransport.New()
	for _, g := range built {
		if err := tr.PutChunks(ctx, "", g.uploaded); err != nil {
			t.Fatal(err)
		}
		if _, err := tr.PutCommit(ctx, g.commit, ""); err != nil {
			t.Fatal(err)
		}
	}
	head, _ := protovcs.CommitIDFromBytes(built[len(built)-1].commit.GetId())
	tr.SetRef(kr.RefName(branch), head)
	reader, err := protovcs.NewBuilder().WithTransport(tr).WithSealer(kr).WithLayout(c).Build()
	if err != nil {
		t.Fatal(err)
	}
	got, err := reader.Branch(branch).Read(ctx)
	if err != nil {
		t.Fatalf("a Go store cannot read the vector store: %v", err)
	}
	// A store gives elements back in canonical order, so the comparison is
	// with the message decomposed and reassembled.
	items, err := c.Materialize(goldenStoreChanged())
	if err != nil {
		t.Fatal(err)
	}
	want, err := c.Merge(items)
	if err != nil {
		t.Fatal(err)
	}
	if !proto.Equal(got, want) {
		t.Fatal("the vector store does not read back as the message written")
	}
	if st.ReadBack, err = protovcs.CanonicalBytes(got); err != nil {
		t.Fatal(err)
	}
	reachable, err := reader.ReachableChunks(ctx, built[len(built)-1].commit)
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range reachable {
		st.Reachable = append(st.Reachable, idBytes(id))
	}
	return sv, st
}

// checkAgainstRealWrites writes the two versions through a Go store with
// crypto/rand replaced by the entropy stream, and checks every chunk and
// commit record matches the step-by-step build.
func checkAgainstRealWrites(ctx context.Context, t *testing.T, c *protovcs.Compiled, branch string,
	m1 proto.Message, meta1 protovcs.CommitMeta, m2 proto.Message, meta2 protovcs.CommitMeta, built []*goldenCommit) {

	t.Helper()
	saved := rand.Reader
	rand.Reader = &entropy{}
	defer func() { rand.Reader = saved }()

	kr, _ := protovcs.NewKeyring(goldenRootKey())
	tr := memtransport.New()
	s, err := protovcs.NewBuilder().WithTransport(tr).WithSealer(kr).WithLayout(c).Build()
	if err != nil {
		t.Fatal(err)
	}
	var ids []protovcs.CommitID
	for _, w := range []struct {
		m    proto.Message
		meta protovcs.CommitMeta
	}{{m1, meta1}, {m2, meta2}} {
		id, err := s.Branch(branch).Write(ctx, w.meta, w.m)
		if err != nil {
			t.Fatalf("Write: %v", err)
		}
		ids = append(ids, id)
	}

	for i, g := range built {
		want, _ := protovcs.CommitIDFromBytes(g.commit.GetId())
		if ids[i] != want {
			t.Fatalf("commit %d: the store named it %s and the vector %s", i, ids[i], want)
		}
		commits, err := tr.GetCommits(ctx, ids[i], 1)
		if err != nil {
			t.Fatal(err)
		}
		real := commits[0]
		if !equalBytesLists(real.GetAdded(), g.commit.GetAdded()) || !equalBytesLists(real.GetParents(), g.commit.GetParents()) ||
			!bytes.Equal(real.GetNamingNonce(), g.commit.GetNamingNonce()) || real.GetSchemaVersion() != g.commit.GetSchemaVersion() {
			t.Fatalf("commit %d: the store wrote a different record", i)
		}
		realPayload, err := kr.Open(protovcs.ChunkID(ids[i]), real.GetPayload())
		if err != nil {
			t.Fatal(err)
		}
		p := &pb.CommitPayload{}
		if err := proto.Unmarshal(realPayload, p); err != nil {
			t.Fatal(err)
		}
		wantPayload := &pb.CommitPayload{}
		_ = proto.Unmarshal(g.payload, wantPayload)
		p.Time = wantPayload.GetTime()
		if !proto.Equal(p, wantPayload) {
			t.Fatalf("commit %d: the payloads differ beyond their time", i)
		}
		var ids []protovcs.ChunkID
		for _, u := range g.uploaded {
			ids = append(ids, u.ID)
		}
		got, err := tr.GetChunks(ctx, ids)
		if err != nil {
			t.Fatal(err)
		}
		for j, u := range g.uploaded {
			if !bytes.Equal(got[j].Sealed, u.Sealed) {
				t.Fatalf("commit %d: chunk %s was sealed differently", i, u.ID)
			}
		}
	}
}

func equalBytesLists(a, b [][]byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if !bytes.Equal(a[i], b[i]) {
			return false
		}
	}
	return true
}
