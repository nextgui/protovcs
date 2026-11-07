package protovcs

import (
	"bytes"
	"testing"

	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"

	testpb "github.com/nextgui/protovcs/proto/protovcs/testpb"
)

// richRecord is a message with something set in every field whose encoding had
// to be decided, so that one round trip covers all of them.
func richRecord() *testpb.Record {
	return &testpb.Record{
		Id:     "rec-1",
		Group:  "group-a",
		Date:   &testpb.Date{Year: 2026, Month: 8, Day: 14},
		Amount: -4200,
		Tags:   []string{"beta", "alpha", "gamma"},
		Attributes: map[string]string{
			"zebra":  "last",
			"apple":  "first",
			"middle": "between",
		},
		Nested:   &testpb.Nested{Note: "a note", Values: []int64{3, 1, 2}},
		Payload:  []byte{0x00, 0xff, 0x10, 0x00},
		Score:    -0.5,
		Counters: []int32{7, 0, -7},
	}
}

func TestCanonicalIsStableAcrossCalls(t *testing.T) {
	// The property the whole design rests on: the same message always produces
	// the same bytes. A map is in there specifically because Go randomises map
	// iteration, so an encoder that forgot to sort would fail this within a few
	// runs rather than reliably.
	m := richRecord()

	first, err := CanonicalBytes(m)
	if err != nil {
		t.Fatalf("CanonicalBytes: %v", err)
	}
	for i := 0; i < 200; i++ {
		again, err := CanonicalBytes(m)
		if err != nil {
			t.Fatalf("CanonicalBytes on call %d: %v", i, err)
		}
		if !bytes.Equal(first, again) {
			t.Fatalf("call %d produced different bytes; the encoding is not deterministic", i)
		}
	}
}

func TestCanonicalIsStableAcrossEqualMessages(t *testing.T) {
	// Two messages built independently, with the map written in a different
	// order, must encode identically. Building the same map twice in a
	// different order is what a second client does.
	a := richRecord()

	b := richRecord()
	b.Attributes = map[string]string{}
	for _, k := range []string{"middle", "zebra", "apple"} {
		b.Attributes[k] = a.Attributes[k]
	}

	ab, err := CanonicalBytes(a)
	if err != nil {
		t.Fatalf("CanonicalBytes(a): %v", err)
	}
	bb, err := CanonicalBytes(b)
	if err != nil {
		t.Fatalf("CanonicalBytes(b): %v", err)
	}
	if !bytes.Equal(ab, bb) {
		t.Fatal("two equal messages encoded differently")
	}
}

func TestCanonicalRoundTrips(t *testing.T) {
	// Canonical bytes are ordinary protobuf. Anything that can be encoded has
	// to come back through a standard parser unchanged, or the encoding is not
	// a protobuf encoding and every other guarantee is void.
	want := richRecord()

	encoded, err := CanonicalBytes(want)
	if err != nil {
		t.Fatalf("CanonicalBytes: %v", err)
	}

	got := &testpb.Record{}
	if err := proto.Unmarshal(encoded, got); err != nil {
		t.Fatalf("the canonical encoding did not parse: %v", err)
	}
	if !proto.Equal(want, got) {
		t.Fatalf("round trip changed the message:\n got %v\nwant %v", got, want)
	}
}

func TestCanonicalOrdersFieldsByNumber(t *testing.T) {
	m := &testpb.Record{Id: "x", Score: 1, Amount: 3}

	encoded, err := CanonicalBytes(m)
	if err != nil {
		t.Fatalf("CanonicalBytes: %v", err)
	}

	var last protoreflect.FieldNumber
	for _, num := range fieldNumbers(t, encoded) {
		if num < last {
			t.Fatalf("field %d came after field %d; fields must ascend", num, last)
		}
		last = num
	}
}

func TestCanonicalPreservesUnknownFields(t *testing.T) {
	// A client on an older schema reads an item, writes it back, and must not
	// drop what it did not understand. This is the test for that, and it is the
	// one that matters most for a store whose data outlives its readers.
	full := richRecord()
	encoded, err := CanonicalBytes(full)
	if err != nil {
		t.Fatalf("CanonicalBytes: %v", err)
	}

	// Nested stands in for an older schema: it shares no field numbers with
	// Record beyond the first, so almost everything lands in unknown fields.
	old := &testpb.Nested{}
	if err := proto.Unmarshal(encoded, old); err != nil {
		t.Fatalf("parsing under the old schema: %v", err)
	}
	if len(old.ProtoReflect().GetUnknown()) == 0 {
		t.Fatal("the fixture produced no unknown fields, so this test proves nothing")
	}

	rewritten, err := CanonicalBytes(old)
	if err != nil {
		t.Fatalf("CanonicalBytes on the old schema: %v", err)
	}

	recovered := &testpb.Record{}
	if err := proto.Unmarshal(rewritten, recovered); err != nil {
		t.Fatalf("parsing what the old client wrote: %v", err)
	}
	if !proto.Equal(full, recovered) {
		t.Fatalf("a client that did not understand the message lost data:\n got %v\nwant %v", recovered, full)
	}
}

func TestCanonicalUnknownFieldsKeepTheirNumbers(t *testing.T) {
	// Unknown fields go at their own numbers rather than after everything the
	// reader understood. Without that, a message that has been through an old
	// client encodes differently from the same message that has not, and the
	// two get different chunk names despite being equal.
	full := richRecord()
	direct, err := CanonicalBytes(full)
	if err != nil {
		t.Fatalf("CanonicalBytes: %v", err)
	}

	old := &testpb.Nested{}
	if err := proto.Unmarshal(direct, old); err != nil {
		t.Fatalf("parsing under the old schema: %v", err)
	}
	viaOldClient, err := CanonicalBytes(old)
	if err != nil {
		t.Fatalf("CanonicalBytes on the old schema: %v", err)
	}

	if !bytes.Equal(direct, viaOldClient) {
		t.Fatal("a message that passed through a client with an older schema encoded differently")
	}
}

func TestCanonicalEmptyMessage(t *testing.T) {
	encoded, err := CanonicalBytes(&testpb.Record{})
	if err != nil {
		t.Fatalf("CanonicalBytes: %v", err)
	}
	if len(encoded) != 0 {
		t.Fatalf("an empty message encoded to %d bytes, want 0", len(encoded))
	}
}

func TestCanonicalNilMessage(t *testing.T) {
	encoded, err := CanonicalBytes(nil)
	if err != nil {
		t.Fatalf("CanonicalBytes(nil): %v", err)
	}
	if encoded != nil {
		t.Fatalf("CanonicalBytes(nil) = %v, want nil", encoded)
	}
}

// fieldNumbers reports the field numbers in an encoded message, in the order
// they appear.
func fieldNumbers(t *testing.T, encoded []byte) []protoreflect.FieldNumber {
	t.Helper()

	var out []protoreflect.FieldNumber
	for len(encoded) > 0 {
		num, typ, n := protowire.ConsumeTag(encoded)
		if n < 0 {
			t.Fatal("malformed tag in the canonical encoding")
		}
		out = append(out, num)
		vl := protowire.ConsumeFieldValue(num, typ, encoded[n:])
		if vl < 0 {
			t.Fatalf("malformed value for field %d", num)
		}
		encoded = encoded[n+vl:]
	}
	return out
}
