//! Where sealed chunks are kept between store instances. Port of
//! `chunkcache.go`.

use crate::error::Result;
use crate::transport::SealedChunk;
use crate::types::ChunkId;
use crate::{BoxFuture, MaybeSend, MaybeSync};

/// Keeps sealed chunks where a later store can find them, such as the
/// browser's IndexedDB (the `protovcs_idbcache` crate).
///
/// A store asks it before the server and tells it about every chunk it
/// fetches or uploads. Every chunk is checked when it is opened, so a damaged
/// entry is dropped and fetched again. Every method is best effort: an error is
/// treated as a miss.
pub trait ChunkCache: MaybeSend + MaybeSync {
    /// The chunks it holds among `ids`, in any order.
    fn get<'a>(&'a self, ids: &'a [ChunkId]) -> BoxFuture<'a, Result<Vec<SealedChunk>>>;

    /// Keeps chunks. It may return before they are written.
    fn put<'a>(&'a self, chunks: &'a [SealedChunk]) -> BoxFuture<'a, Result<()>>;

    /// Removes chunks that failed to open.
    fn drop_chunks<'a>(&'a self, ids: &'a [ChunkId]) -> BoxFuture<'a, Result<()>>;
}
