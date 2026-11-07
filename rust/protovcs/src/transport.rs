//! How a client reaches a store. Port of `transport.go`.

use crate::error::{Error, ErrorKind, Result};
use crate::proto::{Commit, Usage};
use crate::types::{ChunkId, CommitId};
use crate::{BoxFuture, MaybeSend, MaybeSync};

/// A branch as the server holds it.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct RefState {
    /// The hashed name.
    pub name: Vec<u8>,
    /// The commit it points at; zero for a deleted branch.
    pub commit: CommitId,
    /// What a writer must return to move it.
    pub cas_token: String,
}

/// Whether two ref sets describe the same branches at the same commits, in
/// any order. The compare-and-swap token counts.
pub fn same_refs(a: &[RefState], b: &[RefState]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let sorted = |r: &[RefState]| {
        let mut v = r.to_vec();
        v.sort_by(|x, y| x.name.cmp(&y.name));
        v
    };
    sorted(a) == sorted(b)
}

/// A chunk on its way to or from a server.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SealedChunk {
    /// Its name.
    pub id: ChunkId,
    /// Its sealed bytes.
    pub sealed: Vec<u8>,
}

/// How a ref move treats a commit that does not descend from the branch head.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RefMove {
    /// Refuse it with [`ErrorKind::NotFastForward`].
    FastForward,
    /// Allow it, orphaning the history in between.
    Force,
}

/// The whole server surface. Everything except `update_ref` is immutable or
/// content-addressed, so it can be retried or reordered freely.
///
/// [`crate::ConnectTransport`] implements it over Connect. Each method's
/// meaning is the Go interface's, in `transport.go`.
pub trait Transport: MaybeSend + MaybeSync {
    /// Every branch and the commit it points at.
    fn list_refs(&self) -> BoxFuture<'_, Result<Vec<RefState>>>;

    /// Calls `deliver` with the store's branches, and again whenever one
    /// moves, until `deliver` fails or the watch breaks. The first call
    /// carries the current state.
    fn watch_refs<'a>(
        &'a self,
        deliver: Box<dyn FnMut(Vec<RefState>) -> Result<()> + Send + 'a>,
    ) -> BoxFuture<'a, Result<()>>;

    /// Moves a branch, or reports [`ErrorKind::RaceLost`]. An empty `expected`
    /// creates it; a zero commit deletes it.
    fn update_ref<'a>(
        &'a self,
        name: &'a [u8],
        commit: CommitId,
        expected: &'a str,
        mode: RefMove,
    ) -> BoxFuture<'a, Result<RefState>>;

    /// Walks back from a commit, at most `depth` records; zero means one.
    fn get_commits(&self, from: CommitId, depth: u32) -> BoxFuture<'_, Result<Vec<Commit>>>;

    /// Records a version without making it reachable.
    fn put_commit<'a>(
        &'a self,
        commit: &'a Commit,
        reservation: &'a str,
    ) -> BoxFuture<'a, Result<CommitId>>;

    /// The chunk ids a client holding `have` might not hold at `want`, and
    /// whether that had to be the full set.
    fn missing_between<'a>(
        &'a self,
        have: &'a [CommitId],
        want: CommitId,
    ) -> BoxFuture<'a, Result<(Vec<ChunkId>, bool)>>;

    /// Reserves quota before anything is uploaded.
    fn begin_write(&self, chunks: u32, bytes: u64) -> BoxFuture<'_, Result<String>>;

    /// Which of these the store does not hold, in the order given.
    fn has_chunks<'a>(&'a self, ids: &'a [ChunkId]) -> BoxFuture<'a, Result<Vec<ChunkId>>>;

    /// Stores chunks. Idempotent.
    fn put_chunks<'a>(
        &'a self,
        reservation: &'a str,
        chunks: &'a [SealedChunk],
    ) -> BoxFuture<'a, Result<()>>;

    /// Fetches chunks.
    fn get_chunks<'a>(&'a self, ids: &'a [ChunkId]) -> BoxFuture<'a, Result<Vec<SealedChunk>>>;

    /// Says which chunks a head reaches, in manifest order, and reports
    /// whether the server kept the hint.
    fn pack_hint<'a>(&'a self, commit: CommitId, ids: &'a [ChunkId])
    -> BoxFuture<'a, Result<bool>>;

    /// What the server is holding for the store. A transport with no server
    /// to ask keeps the default, which reports [`ErrorKind::NoUsage`].
    fn usage(&self) -> BoxFuture<'_, Result<Usage>> {
        Box::pin(async { Err(Error::new(ErrorKind::NoUsage, "")) })
    }
}
