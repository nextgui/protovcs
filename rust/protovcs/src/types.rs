//! Names, roles and versions.

use std::fmt;

use crate::error::{Error, Result};

/// The length of a chunk or commit name in bytes.
pub const CHUNK_ID_LEN: usize = 32;

/// The version of the chunk envelope, the manifest node and the commit
/// record. A chunk of any other version is refused before anything else in it
/// is read.
pub const FORMAT_VERSION: u8 = 1;

/// The version of the canonical encoding rules, which decide every chunk's
/// name. Recorded on every schema chunk.
pub const CANONICAL_VERSION: u32 = 1;

macro_rules! id_type {
    ($(#[$doc:meta])* $name:ident, $what:literal) => {
        $(#[$doc])*
        #[derive(Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
        pub struct $name([u8; CHUNK_ID_LEN]);

        impl $name {
            /// Wraps raw bytes.
            pub const fn new(bytes: [u8; CHUNK_ID_LEN]) -> Self {
                Self(bytes)
            }

            /// Converts a wire value, refusing anything of the wrong length
            /// rather than padding or truncating it.
            ///
            /// # Errors
            ///
            /// When `b` is not [`CHUNK_ID_LEN`] bytes.
            pub fn from_bytes(b: &[u8]) -> Result<Self> {
                let arr: [u8; CHUNK_ID_LEN] = b.try_into().map_err(|_| {
                    Error::invalid(format!(
                        concat!("protovcs: a ", $what, " id is {} bytes, got {}"),
                        CHUNK_ID_LEN,
                        b.len()
                    ))
                })?;
                Ok(Self(arr))
            }

            /// The raw bytes.
            pub fn as_bytes(&self) -> &[u8; CHUNK_ID_LEN] {
                &self.0
            }

            /// A copy as a vector, for putting on the wire.
            pub fn to_vec(&self) -> Vec<u8> {
                self.0.to_vec()
            }

            /// Whether this is the empty id, which nothing has.
            pub fn is_zero(&self) -> bool {
                self.0 == [0; CHUNK_ID_LEN]
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                for b in &self.0 {
                    write!(f, "{b:02x}")?;
                }
                Ok(())
            }
        }

        impl fmt::Debug for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(f, concat!(stringify!($name), "({})"), self)
            }
        }
    };
}

id_type!(
    /// Names one chunk: a fingerprint of its contents under a key only the
    /// client holds. The hexadecimal form is what errors and a path-filing
    /// backend show.
    ChunkId,
    "chunk"
);

id_type!(
    /// Names one commit. The same shape as a chunk id and a different
    /// namespace: a commit is not a chunk.
    CommitId,
    "commit"
);

impl From<CommitId> for ChunkId {
    /// A commit's payload is sealed under the commit's own name.
    fn from(c: CommitId) -> Self {
        Self(c.0)
    }
}

/// Separates the kinds of thing stored as chunks. It goes into the
/// fingerprint, so an item and a manifest node holding the same bytes have
/// different names.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Role {
    /// An item: one fragment of the message.
    Item,
    /// A manifest node.
    Manifest,
    /// The schema a commit was written under.
    Schema,
}

impl Role {
    /// The text that is hashed into a name.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Item => "item",
            Self::Manifest => "manifest",
            Self::Schema => "schema",
        }
    }
}

/// Identifies one item within a version, as a path:
/// `"records/alpha/2026/08"`. Item keys sort byte-wise, and sorting them is
/// what makes a range of them a contiguous walk of the manifest.
pub type ItemKey = String;

/// The key of the root item, which holds every field no layout rule mentions.
/// Empty, so it sorts before every other key.
pub const ROOT_ITEM_KEY: &str = "";
