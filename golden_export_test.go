package protovcs

import (
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"

	pb "github.com/nextgui/protovcs/proto/protovcs/v1"
)

// The golden vectors in golden_test.go are an external test, because they write
// stores through memtransport, which imports this package. These expose the
// unexported parts they record.

// KeyringKeysForTest returns the naming, content and ref keys a keyring
// derived from its root key.
func KeyringKeysForTest(s Sealer) (name, chunk, ref []byte) {
	k := s.(*keyring)
	return k.name, k.chunk, k.ref
}

// SealWithNonceForTest seals plaintext as Seal does, with a chosen nonce rather
// than a random one.
func SealWithNonceForTest(s Sealer, id ChunkID, nonce, plaintext []byte) []byte {
	k := s.(*keyring)
	out := append([]byte{FormatVersion}, nonce...)
	return k.aead.Seal(out, nonce, plaintext, id[:])
}

// TreeDigestForTest is treeDigest.
func TreeDigestForTest(refs []ItemRef) []byte { return treeDigest(refs) }

// NodeDigestForTest is nodeDigest.
func NodeDigestForTest(root ChunkID) []byte { return nodeDigest(nil, root) }

// SchemaChunkForTest returns the schema chunk a store writes under.
func SchemaChunkForTest(s *Store) (ChunkID, []byte, error) { return s.schemaChunk() }

// DescriptorSetForTest is descriptorSetFor.
func DescriptorSetForTest(md protoreflect.MessageDescriptor) (*descriptorpb.FileDescriptorSet, error) {
	return descriptorSetFor(md)
}

// UnixToDateForTest is unixToDate.
func UnixToDateForTest(sec int64) [3]int { return unixToDate(sec) }

// ElementCountForTest is elementCount.
func ElementCountForTest(c *Compiled, item Item) uint32 { return elementCount(c, item.Fragment) }

// CommitMessageForTest returns the sealed record of why a change was made, as
// a store builds it.
func CommitMessageForTest(s *Store, meta CommitMeta) (*pb.CommitMessage, error) {
	return s.commitMessage(meta)
}

// GroupFilesForTest describes a message M with a group field "item" at number
// 1, once in proto2 and once in editions with DELIMITED encoding. Item holds an
// int32 x at number 2. The canonical tests and the golden vectors share it.
func GroupFilesForTest() []*descriptorpb.FileDescriptorProto {
	item := func() *descriptorpb.DescriptorProto {
		return &descriptorpb.DescriptorProto{
			Name: proto.String("Item"),
			Field: []*descriptorpb.FieldDescriptorProto{{
				Name: proto.String("x"), Number: proto.Int32(2),
				Label: descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum(),
				Type:  descriptorpb.FieldDescriptorProto_TYPE_INT32.Enum(),
			}},
		}
	}
	return []*descriptorpb.FileDescriptorProto{
		{
			Name: proto.String("group2.proto"), Package: proto.String("group2"), Syntax: proto.String("proto2"),
			MessageType: []*descriptorpb.DescriptorProto{{
				Name: proto.String("M"),
				Field: []*descriptorpb.FieldDescriptorProto{{
					Name: proto.String("item"), Number: proto.Int32(1),
					Label:    descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum(),
					Type:     descriptorpb.FieldDescriptorProto_TYPE_GROUP.Enum(),
					TypeName: proto.String(".group2.M.Item"),
				}},
				NestedType: []*descriptorpb.DescriptorProto{item()},
			}},
		},
		{
			// Editions has no TYPE_GROUP; a DELIMITED message field is one.
			Name: proto.String("delimited.proto"), Package: proto.String("delimited"),
			Syntax: proto.String("editions"), Edition: descriptorpb.Edition_EDITION_2023.Enum(),
			MessageType: []*descriptorpb.DescriptorProto{
				{Name: proto.String("M"), Field: []*descriptorpb.FieldDescriptorProto{{
					Name: proto.String("item"), Number: proto.Int32(1),
					Label:    descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum(),
					Type:     descriptorpb.FieldDescriptorProto_TYPE_MESSAGE.Enum(),
					TypeName: proto.String(".delimited.Item"),
					Options: &descriptorpb.FieldOptions{Features: &descriptorpb.FeatureSet{
						MessageEncoding: descriptorpb.FeatureSet_DELIMITED.Enum(),
					}},
				}}},
				item(),
			},
		},
	}
}
