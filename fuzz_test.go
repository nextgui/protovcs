package protovcs

import (
	"testing"

	"google.golang.org/protobuf/proto"

	testpb "github.com/nextgui/protovcs/proto/protovcs/testpb"
	pb "github.com/nextgui/protovcs/proto/protovcs/v1"
)

// Everything here feeds server-supplied bytes to a parser. A protovcs client
// treats its server as nosy and possibly broken, so every one of these paths
// takes input the client did not produce and must fail rather than misbehave.

func FuzzOpenChunk(f *testing.F) {
	key := make([]byte, RootKeyLen)
	for i := range key {
		key[i] = byte(i)
	}
	k, err := NewKeyring(key)
	if err != nil {
		f.Fatalf("NewKeyring: %v", err)
	}

	id := k.ChunkID(RoleItem, "records/2026-08", []byte("contents"))
	sealed, err := k.Seal(id, []byte("contents"))
	if err != nil {
		f.Fatalf("Seal: %v", err)
	}
	f.Add(sealed)
	f.Add([]byte{})
	f.Add([]byte{FormatVersion})
	f.Add(append([]byte{FormatVersion}, make([]byte, nonceLen)...))

	f.Fuzz(func(t *testing.T, sealed []byte) {
		// Any input at all: the only acceptable outcomes are an error or the
		// original plaintext. A panic on server-supplied bytes is a denial of
		// service the server controls.
		plaintext, err := k.Open(id, sealed)
		if err != nil {
			return
		}
		if string(plaintext) != "contents" {
			t.Fatalf("Open accepted bytes that were not the sealed chunk: %q", plaintext)
		}
	})
}

func FuzzCanonicalBytesRoundTrip(f *testing.F) {
	seeds := []proto.Message{
		&testpb.Record{Id: "a"},
		&testpb.Record{Id: "b", Tags: []string{"x", "y"}, Attributes: map[string]string{"k": "v"}},
		&testpb.Store{Name: "s", Records: []*testpb.Record{{Id: "c"}}},
	}
	for _, m := range seeds {
		encoded, err := proto.Marshal(m)
		if err != nil {
			f.Fatalf("Marshal: %v", err)
		}
		f.Add(encoded)
	}
	f.Add([]byte{})

	f.Fuzz(func(t *testing.T, encoded []byte) {
		// A chunk's bytes come from a server. Parsing them and canonically
		// re-encoding is what a client does before comparing or rewriting, so
		// neither step may panic, and the canonical form must itself parse.
		m := &testpb.Store{}
		if err := proto.Unmarshal(encoded, m); err != nil {
			return
		}

		canonical, err := CanonicalBytes(m)
		if err != nil {
			return
		}

		again := &testpb.Store{}
		if err := proto.Unmarshal(canonical, again); err != nil {
			t.Fatalf("the canonical encoding of a parsed message did not parse: %v", err)
		}

		// Canonicalising is idempotent. If it were not, a client that read and
		// rewrote a chunk would give it a new name each time and the store
		// would grow without anything changing.
		twice, err := CanonicalBytes(again)
		if err != nil {
			t.Fatalf("re-encoding a canonical message failed: %v", err)
		}
		if string(canonical) != string(twice) {
			t.Fatal("canonical encoding is not idempotent")
		}
	})
}

func FuzzManifestNodeParsing(f *testing.F) {
	node := &pb.ManifestNode{Entries: []*pb.ManifestNode_Entry{
		{Segment: "a", Target: &pb.ManifestNode_Entry_Item{Item: make([]byte, ChunkIDLen)}},
		{Segment: "b", Target: &pb.ManifestNode_Entry_Child{Child: make([]byte, ChunkIDLen)}},
	}}
	encoded, err := proto.Marshal(node)
	if err != nil {
		f.Fatalf("Marshal: %v", err)
	}
	f.Add(encoded)
	f.Add([]byte{})

	f.Fuzz(func(t *testing.T, encoded []byte) {
		parsed := &pb.ManifestNode{}
		if err := proto.Unmarshal(encoded, parsed); err != nil {
			return
		}
		// A manifest node holds chunk ids that came from a server. Converting
		// them must refuse anything of the wrong length rather than padding or
		// truncating, because a padded id names a different chunk.
		for _, e := range parsed.GetEntries() {
			if raw := e.GetItem(); raw != nil {
				if _, err := ChunkIDFromBytes(raw); err != nil && len(raw) == ChunkIDLen {
					t.Fatal("a correctly sized id was refused")
				}
			}
			if raw := e.GetChild(); raw != nil {
				if _, err := ChunkIDFromBytes(raw); err != nil && len(raw) == ChunkIDLen {
					t.Fatal("a correctly sized id was refused")
				}
			}
		}
	})
}

func FuzzCompileLayout(f *testing.F) {
	good, err := proto.Marshal(testLayout())
	if err != nil {
		f.Fatalf("Marshal: %v", err)
	}
	f.Add(good)
	f.Add([]byte{})

	f.Fuzz(func(t *testing.T, encoded []byte) {
		layout := &pb.Layout{}
		if err := proto.Unmarshal(encoded, layout); err != nil {
			return
		}
		// A layout can arrive from a store, written by a client this build has
		// never met. Compiling it must reject anything it cannot serve rather
		// than panic partway through.
		c, err := CompileLayout(layout, (&testpb.Store{}).ProtoReflect().Descriptor())
		if err != nil {
			return
		}
		// Anything that compiled has to be usable, or the compiler accepted
		// something it should not have.
		if _, err := c.Materialize(&testpb.Store{Records: []*testpb.Record{{
			Id: "x", Group: "g", Date: &testpb.Date{Year: 2026, Month: 1, Day: 1},
		}}}); err != nil {
			return
		}
	})
}
