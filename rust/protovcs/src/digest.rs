//! Digests a commit records over its tree. Port of `digest.go`.

use sha2::{Digest, Sha256};

use crate::manifest::ItemRef;
use crate::types::ChunkId;
use crate::wire::put_uvarint;

/// Hashes every item key and the chunk holding it, in key order.
#[doc(hidden)]
pub fn tree_digest(refs: &[ItemRef]) -> Vec<u8> {
    let mut sorted: Vec<&ItemRef> = refs.iter().collect();
    sorted.sort_by(|a, b| a.key.cmp(&b.key));
    let mut h = Sha256::new();
    for r in sorted {
        let mut len = Vec::new();
        put_uvarint(&mut len, r.key.len() as u64);
        h.update(&len);
        h.update(r.key.as_bytes());
        h.update(r.id.as_bytes());
    }
    h.finalize().to_vec()
}

/// Hashes the manifest's root, which already commits to everything beneath it.
/// This is what a commit's payload records.
pub(crate) fn node_digest(root: &ChunkId) -> Vec<u8> {
    Sha256::digest(root.as_bytes()).to_vec()
}

/// [`node_digest`], for the golden vectors.
#[doc(hidden)]
pub fn node_digest_for_test(root: &ChunkId) -> Vec<u8> {
    node_digest(root)
}
