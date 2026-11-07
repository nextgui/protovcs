// Package protovcs stores one large protobuf message as a versioned set of
// small encrypted chunks, on a server that cannot read any of them.
package protovcs

import (
	"fmt"
	"math"
	"sort"

	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// CanonicalBytes returns the one encoding of a message that protovcs will ever
// produce.
//
// A chunk is named by a fingerprint of these bytes, so two clients holding the
// same data must produce the same bytes or nothing deduplicates, no cache stays
// valid, and every commit re-uploads a store that did not change. That makes
// the encoding a stored format in its own right, and it is defined here rather
// than borrowed:
//
//   - Fields ascend by number, with unknown fields interleaved at their own
//     numbers rather than appended at the end.
//   - Repeated scalars are always packed, whatever the field says.
//   - Map entries are sorted by key.
//   - Unknown fields are preserved exactly, so a client on an older schema that
//     reads an item and writes it back does not silently drop what it did not
//     understand.
//
// The obvious alternative is proto.MarshalOptions{Deterministic: true}. That
// sorts map entries and nothing more, and its documentation is explicit that
// the output is not stable across releases of the library. An encoding that
// changes when a dependency is upgraded would re-fingerprint every chunk in
// every store, with nothing failing to announce it.
//
// These bytes are ordinary protobuf and any parser reads them. They are simply
// not the bytes proto.Marshal would have chosen.
func CanonicalBytes(m proto.Message) ([]byte, error) {
	if m == nil {
		return nil, nil
	}
	return appendMessage(nil, m.ProtoReflect())
}

// canonicalField is one field to emit, either a populated field of the message
// or a run of bytes from an unknown field.
type canonicalField struct {
	num protoreflect.FieldNumber

	// Set for a field the message understands.
	fd  protoreflect.FieldDescriptor
	val protoreflect.Value

	// Set for an unknown field, holding its tag and value exactly as received.
	raw []byte
}

func appendMessage(b []byte, m protoreflect.Message) ([]byte, error) {
	var fields []canonicalField

	m.Range(func(fd protoreflect.FieldDescriptor, v protoreflect.Value) bool {
		fields = append(fields, canonicalField{num: fd.Number(), fd: fd, val: v})
		return true
	})

	// Unknown fields arrive as the raw bytes they were received in. They are
	// split into records so they can be placed at their own field numbers
	// rather than after everything else, which is what keeps the encoding the
	// same whether or not the reader understood the field.
	unknown := m.GetUnknown()
	for len(unknown) > 0 {
		num, typ, tagLen := protowire.ConsumeTag(unknown)
		if tagLen < 0 {
			return nil, fmt.Errorf("protovcs: unknown field of %s has a malformed tag", m.Descriptor().FullName())
		}
		valLen := protowire.ConsumeFieldValue(num, typ, unknown[tagLen:])
		if valLen < 0 {
			return nil, fmt.Errorf("protovcs: unknown field %d of %s has a malformed value", num, m.Descriptor().FullName())
		}
		total := tagLen + valLen
		fields = append(fields, canonicalField{num: num, raw: unknown[:total]})
		unknown = unknown[total:]
	}

	// Stable, so several records at one number keep the order they arrived in.
	// Repeated unknown fields are the case that depends on it.
	sort.SliceStable(fields, func(i, j int) bool { return fields[i].num < fields[j].num })

	var err error
	for _, f := range fields {
		if f.raw != nil {
			b = append(b, f.raw...)
			continue
		}
		if b, err = appendField(b, f.fd, f.val); err != nil {
			return nil, err
		}
	}
	return b, nil
}

func appendField(b []byte, fd protoreflect.FieldDescriptor, v protoreflect.Value) ([]byte, error) {
	switch {
	case fd.IsMap():
		return appendMap(b, fd, v.Map())
	case fd.IsList():
		return appendList(b, fd, v.List())
	default:
		return appendSingular(b, fd, v)
	}
}

// appendList writes a repeated field, packing whenever the element kind allows
// it.
//
// Always packing, even for a field declared unpacked, is a deliberate departure
// from what protoc would emit. Every parser accepts either form for a packable
// kind, and choosing one unconditionally removes a way for two encoders to
// disagree about bytes that mean the same thing.
func appendList(b []byte, fd protoreflect.FieldDescriptor, list protoreflect.List) ([]byte, error) {
	if list.Len() == 0 {
		return b, nil
	}

	if packable(fd.Kind()) {
		var payload []byte
		var err error
		for i := 0; i < list.Len(); i++ {
			if payload, err = appendScalar(payload, fd.Kind(), list.Get(i)); err != nil {
				return nil, err
			}
		}
		b = protowire.AppendTag(b, fd.Number(), protowire.BytesType)
		return protowire.AppendBytes(b, payload), nil
	}

	var err error
	for i := 0; i < list.Len(); i++ {
		if b, err = appendSingular(b, fd, list.Get(i)); err != nil {
			return nil, err
		}
	}
	return b, nil
}

// appendMap writes a map field as a sorted sequence of entry messages.
//
// Map iteration in Go is random, so an unsorted encoding of the same map would
// differ between two runs of the same binary, let alone two clients.
func appendMap(b []byte, fd protoreflect.FieldDescriptor, m protoreflect.Map) ([]byte, error) {
	if m.Len() == 0 {
		return b, nil
	}

	keyFd := fd.MapKey()
	valFd := fd.MapValue()

	keys := make([]protoreflect.MapKey, 0, m.Len())
	m.Range(func(k protoreflect.MapKey, _ protoreflect.Value) bool {
		keys = append(keys, k)
		return true
	})
	sort.Slice(keys, func(i, j int) bool { return mapKeyLess(keyFd.Kind(), keys[i], keys[j]) })

	for _, k := range keys {
		var entry []byte
		var err error
		if entry, err = appendSingular(entry, keyFd, k.Value()); err != nil {
			return nil, err
		}
		if entry, err = appendSingular(entry, valFd, m.Get(k)); err != nil {
			return nil, err
		}
		b = protowire.AppendTag(b, fd.Number(), protowire.BytesType)
		b = protowire.AppendBytes(b, entry)
	}
	return b, nil
}

func appendSingular(b []byte, fd protoreflect.FieldDescriptor, v protoreflect.Value) ([]byte, error) {
	switch fd.Kind() {
	case protoreflect.MessageKind:
		nested, err := appendMessage(nil, v.Message())
		if err != nil {
			return nil, err
		}
		b = protowire.AppendTag(b, fd.Number(), protowire.BytesType)
		return protowire.AppendBytes(b, nested), nil

	case protoreflect.GroupKind:
		// A group (proto2 group, or an editions DELIMITED field) is framed by a
		// start and an end tag rather than by a length.
		b = protowire.AppendTag(b, fd.Number(), protowire.StartGroupType)
		var err error
		if b, err = appendMessage(b, v.Message()); err != nil {
			return nil, err
		}
		return protowire.AppendTag(b, fd.Number(), protowire.EndGroupType), nil

	case protoreflect.StringKind:
		b = protowire.AppendTag(b, fd.Number(), protowire.BytesType)
		return protowire.AppendString(b, v.String()), nil

	case protoreflect.BytesKind:
		b = protowire.AppendTag(b, fd.Number(), protowire.BytesType)
		return protowire.AppendBytes(b, v.Bytes()), nil

	default:
		b = protowire.AppendTag(b, fd.Number(), wireTypeOf(fd.Kind()))
		return appendScalar(b, fd.Kind(), v)
	}
}

func appendScalar(b []byte, k protoreflect.Kind, v protoreflect.Value) ([]byte, error) {
	switch k {
	case protoreflect.BoolKind:
		return protowire.AppendVarint(b, protowire.EncodeBool(v.Bool())), nil
	case protoreflect.EnumKind:
		return protowire.AppendVarint(b, uint64(v.Enum())), nil
	case protoreflect.Int32Kind, protoreflect.Int64Kind:
		return protowire.AppendVarint(b, uint64(v.Int())), nil
	case protoreflect.Uint32Kind, protoreflect.Uint64Kind:
		return protowire.AppendVarint(b, v.Uint()), nil
	case protoreflect.Sint32Kind:
		return protowire.AppendVarint(b, protowire.EncodeZigZag(v.Int())), nil
	case protoreflect.Sint64Kind:
		return protowire.AppendVarint(b, protowire.EncodeZigZag(v.Int())), nil
	case protoreflect.Sfixed32Kind:
		return protowire.AppendFixed32(b, uint32(v.Int())), nil
	case protoreflect.Fixed32Kind:
		return protowire.AppendFixed32(b, uint32(v.Uint())), nil
	case protoreflect.FloatKind:
		return protowire.AppendFixed32(b, math.Float32bits(float32(v.Float()))), nil
	case protoreflect.Sfixed64Kind:
		return protowire.AppendFixed64(b, uint64(v.Int())), nil
	case protoreflect.Fixed64Kind:
		return protowire.AppendFixed64(b, v.Uint()), nil
	case protoreflect.DoubleKind:
		return protowire.AppendFixed64(b, math.Float64bits(v.Float())), nil
	default:
		return nil, fmt.Errorf("protovcs: cannot canonically encode kind %v", k)
	}
}

func packable(k protoreflect.Kind) bool {
	switch k {
	case protoreflect.MessageKind, protoreflect.GroupKind, protoreflect.StringKind, protoreflect.BytesKind:
		return false
	default:
		return true
	}
}

func wireTypeOf(k protoreflect.Kind) protowire.Type {
	switch k {
	case protoreflect.Fixed32Kind, protoreflect.Sfixed32Kind, protoreflect.FloatKind:
		return protowire.Fixed32Type
	case protoreflect.Fixed64Kind, protoreflect.Sfixed64Kind, protoreflect.DoubleKind:
		return protowire.Fixed64Type
	default:
		return protowire.VarintType
	}
}

func mapKeyLess(k protoreflect.Kind, a, b protoreflect.MapKey) bool {
	switch k {
	case protoreflect.BoolKind:
		return !a.Bool() && b.Bool()
	case protoreflect.Int32Kind, protoreflect.Int64Kind, protoreflect.Sint32Kind,
		protoreflect.Sint64Kind, protoreflect.Sfixed32Kind, protoreflect.Sfixed64Kind:
		return a.Int() < b.Int()
	case protoreflect.Uint32Kind, protoreflect.Uint64Kind, protoreflect.Fixed32Kind, protoreflect.Fixed64Kind:
		return a.Uint() < b.Uint()
	default:
		return a.String() < b.String()
	}
}
