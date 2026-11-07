//! What a store remembers by content. Port of `memo.go`.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};

use futures::FutureExt;
use futures::future::Shared;

use crate::BoxFuture;
use crate::error::{Error, Result};
use crate::proto::Commit;
use crate::types::{ChunkId, CommitId};

/// One fetch other reads wanting the same chunks wait on.
type Fetch = Shared<BoxFuture<'static, std::result::Result<(), Error>>>;

/// Chunk plaintext by chunk name, commits by commit id, and the chunk fetches
/// in progress. Shared by a store and every store derived from it. Both kinds
/// of name are fingerprints, so an entry never needs invalidating.
#[derive(Default)]
pub(crate) struct Memo {
    inner: Mutex<Inner>,
}

#[derive(Default)]
struct Inner {
    chunks: HashMap<ChunkId, Arc<Vec<u8>>>,
    commits: HashMap<CommitId, Commit>,
    fetching: HashMap<ChunkId, Fetch>,
}

impl Memo {
    fn lock(&self) -> std::sync::MutexGuard<'_, Inner> {
        // A panic while holding the lock leaves maps that are still valid.
        self.inner.lock().unwrap_or_else(|e| e.into_inner())
    }

    pub(crate) fn chunk(&self, id: &ChunkId) -> Option<Arc<Vec<u8>>> {
        self.lock().chunks.get(id).cloned()
    }

    pub(crate) fn put_chunks(&self, plaintext: impl IntoIterator<Item = (ChunkId, Vec<u8>)>) {
        let mut inner = self.lock();
        for (id, p) in plaintext {
            inner.chunks.insert(id, Arc::new(p));
        }
    }

    pub(crate) fn commit(&self, id: &CommitId) -> Option<Commit> {
        self.lock().commits.get(id).cloned()
    }

    pub(crate) fn put_commit(&self, id: CommitId, c: Commit) {
        self.lock().commits.insert(id, c);
    }

    /// Makes every chunk named held, fetching what is missing in one request
    /// and waiting for any another read is already fetching.
    ///
    /// `fetch` returns the plaintext of the chunks it is given. A chunk it
    /// leaves out is simply not held; the caller reading it reports that.
    pub(crate) async fn warm<F>(self: &Arc<Self>, ids: &[ChunkId], fetch: F) -> Result<()>
    where
        F: Fn(Vec<ChunkId>) -> BoxFuture<'static, Result<HashMap<ChunkId, Vec<u8>>>>,
    {
        loop {
            let (mine, waits) = {
                let mut inner = self.lock();
                // In the order asked for; `wanted` makes the duplicate check
                // constant time.
                let mut want = Vec::new();
                let mut wanted: HashSet<ChunkId> = HashSet::new();
                let mut waits: Vec<Fetch> = Vec::new();
                for id in ids {
                    if inner.chunks.contains_key(id) {
                        continue;
                    }
                    if let Some(other) = inner.fetching.get(id) {
                        if !waits.iter().any(|w| w.ptr_eq(other)) {
                            waits.push(other.clone());
                        }
                        continue;
                    }
                    if wanted.insert(*id) {
                        want.push(*id);
                    }
                }
                let mine = if want.is_empty() {
                    None
                } else {
                    let memo = Arc::clone(self);
                    let fut = fetch(want.clone());
                    let ids = want.clone();
                    let fetch: BoxFuture<'static, std::result::Result<(), Error>> =
                        Box::pin(async move {
                            let got = fut.await;
                            let mut inner = memo.lock();
                            for id in &ids {
                                inner.fetching.remove(id);
                            }
                            let opened = got?;
                            for (id, p) in opened {
                                inner.chunks.insert(id, Arc::new(p));
                            }
                            Ok(())
                        });
                    let shared: Fetch = fetch.shared();
                    for id in &want {
                        inner.fetching.insert(*id, shared.clone());
                    }
                    Some(shared)
                };
                (mine, waits)
            };

            if let Some(mine) = mine {
                mine.await?;
            }
            // A fetch that failed belongs to another read. Its chunks are
            // asked for again rather than failing this read over it.
            let mut retry = false;
            for w in waits {
                if w.await.is_err() {
                    retry = true;
                }
            }
            if !retry {
                return Ok(());
            }
        }
    }
}
