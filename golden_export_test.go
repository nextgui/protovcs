package protovcs

import (
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
