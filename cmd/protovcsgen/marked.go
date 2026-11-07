package main

import (
	"fmt"
	"sort"
	"strings"

	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
)

// checkUnmarked refuses a stored message that can reach, at any depth, a
// message carrying one of the named bool message options set to true.
//
// The options are named rather than imported, so the generator depends on no
// particular options proto. A repository marks its raw-key messages with an
// option of its own and passes that option's full name, and the option's
// descriptor arrives with the other descriptor sets.
//
// It follows every message-typed field: singular, repeated, map values and
// oneof members. The error names each path from the stored message to a marked
// one.
func checkUnmarked(stored protoreflect.MessageDescriptor, files *protoregistry.Files, options []string) error {
	if len(options) == 0 {
		return nil
	}
	numbers := map[protowire.Number]string{}
	for _, name := range options {
		d, err := files.FindDescriptorByName(protoreflect.FullName(name))
		if err != nil {
			return fmt.Errorf("option %s is not in any descriptor set given to this rule", name)
		}
		ext, ok := d.(protoreflect.ExtensionDescriptor)
		if !ok || ext.Kind() != protoreflect.BoolKind || ext.ContainingMessage().FullName() != "google.protobuf.MessageOptions" {
			return fmt.Errorf("%s is not a bool message option", name)
		}
		numbers[ext.Number()] = name
	}

	var paths []string
	walkMarked(stored, string(stored.FullName()), numbers, map[protoreflect.FullName]bool{}, &paths)
	if len(paths) == 0 {
		return nil
	}
	sort.Strings(paths)
	return fmt.Errorf("%s is stored, and reaches %d message(s) marked never to be stored:\n  %s",
		stored.FullName(), len(paths), strings.Join(paths, "\n  "))
}

func walkMarked(md protoreflect.MessageDescriptor, path string, numbers map[protowire.Number]string,
	seen map[protoreflect.FullName]bool, paths *[]string) {
	if seen[md.FullName()] {
		return
	}
	seen[md.FullName()] = true
	fields := md.Fields()
	for i := 0; i < fields.Len(); i++ {
		fd := fields.Get(i)
		target := fd.Message()
		if fd.IsMap() {
			target = fd.MapValue().Message()
		}
		if target == nil {
			continue
		}
		next := path + "." + string(fd.Name())
		if option, ok := markedBy(target, numbers); ok {
			*paths = append(*paths, fmt.Sprintf("%s (%s, %s)", next, target.FullName(), option))
			continue
		}
		walkMarked(target, next, numbers, seen, paths)
	}
}

// markedBy reports which of the options md sets to true.
//
// Read from the encoded options rather than with GetExtension. The options
// were built from descriptor sets this binary did not link the extension for,
// so they hold it as an unknown field.
func markedBy(md protoreflect.MessageDescriptor, numbers map[protowire.Number]string) (string, bool) {
	opts := md.Options()
	if opts == nil {
		return "", false
	}
	raw, err := proto.Marshal(opts)
	if err != nil {
		return "", false
	}
	for len(raw) > 0 {
		num, typ, n := protowire.ConsumeTag(raw)
		if n < 0 {
			return "", false
		}
		raw = raw[n:]
		if name, ok := numbers[num]; ok && typ == protowire.VarintType {
			v, m := protowire.ConsumeVarint(raw)
			if m < 0 {
				return "", false
			}
			if v != 0 {
				return name, true
			}
			raw = raw[m:]
			continue
		}
		m := protowire.ConsumeFieldValue(num, typ, raw)
		if m < 0 {
			return "", false
		}
		raw = raw[m:]
	}
	return "", false
}
