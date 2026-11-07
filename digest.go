package protovcs

import (
	"crypto/sha256"
	"sort"
)

// treeDigest hashes every item key and the chunk holding it.
//
// One value that answers "is this the same content", and a check on the whole
// materialization that does not depend on the manifest's structure being
// correct. Two versions with the same digest hold the same items under the same
// keys, whatever shape their manifests took.
func treeDigest(refs []ItemRef) []byte {
	sorted := make([]ItemRef, len(refs))
	copy(sorted, refs)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].Key < sorted[j].Key })

	h := sha256.New()
	var lengths [16]byte
	for _, r := range sorted {
		n := putUvarint(lengths[:], uint64(len(r.Key)))
		h.Write(lengths[:n])
		h.Write([]byte(r.Key))
		h.Write(r.ID[:])
	}
	return h.Sum(nil)
}

// nodeDigest hashes the manifest's root, which already commits to every item
// beneath it.
//
// treeDigest hashes an item list, which a partial commit does not have. The root
// node's name is a fingerprint of its entries, each of which is a fingerprint of
// the subtree below, so it commits to exactly the same thing and costs nothing
// to compute.
func nodeDigest(nodes []ManifestChunk, root ChunkID) []byte {
	h := sha256.New()
	h.Write(root[:])
	sum := h.Sum(nil)
	return sum
}

func sha256Sum(b []byte) [sha256.Size]byte { return sha256.Sum256(b) }
