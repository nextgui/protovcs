//! Keeping the newest head seen on each branch where a later store can find
//! it. Port of `headmarks.go`.

use std::collections::HashMap;
use std::sync::Mutex;

use crate::error::Result;
use crate::types::CommitId;
use crate::{BoxFuture, MaybeSend, MaybeSync};

/// Keeps the newest commit this client has seen on each branch of one store,
/// where a later [`crate::Store`] can find it, such as the browser's
/// IndexedDB.
///
/// [`crate::Branch::head`] refuses a head that is an ancestor of the newest one
/// seen. Without head marks a store remembers only in memory, so a new process
/// or a page reload accepts any head, including one a server rolled back to.
/// With them, the store loads the marks on first use and writes each mark as
/// it advances.
///
/// A record names a commit by its hash, which the server already knows, so it
/// holds nothing secret. It is not protected against local tampering: anything
/// able to change it runs with the page's own access and could read the key
/// instead.
///
/// One `HeadMarks` serves one store. A caller holding several stores gives
/// each its own, because branch names repeat across stores.
pub trait HeadMarks: MaybeSend + MaybeSync {
    /// The recorded commit for each branch.
    fn load(&self) -> BoxFuture<'_, Result<HashMap<String, CommitId>>>;

    /// Records `next` for `branch` when the record holds `old`, where a zero
    /// `old` means there is no record. Returns what the record holds
    /// afterwards: `next` when it swapped, and the record it found otherwise.
    fn compare_and_swap<'a>(
        &'a self,
        branch: &'a str,
        old: CommitId,
        next: CommitId,
    ) -> BoxFuture<'a, Result<CommitId>>;
}

/// How often saving a mark retries after another writer changed the record.
pub(crate) const SAVE_MARK_ATTEMPTS: usize = 4;

/// [`HeadMarks`] held in memory. Stores built over the same one share their
/// marks, as stores in one process would share a file.
#[derive(Debug, Default)]
pub struct MemoryHeadMarks {
    marks: Mutex<HashMap<String, CommitId>>,
}

impl MemoryHeadMarks {
    /// An empty set of marks.
    pub fn new() -> Self {
        Self::default()
    }

    fn marks(&self) -> std::sync::MutexGuard<'_, HashMap<String, CommitId>> {
        self.marks.lock().unwrap_or_else(|e| e.into_inner())
    }
}

impl HeadMarks for MemoryHeadMarks {
    fn load(&self) -> BoxFuture<'_, Result<HashMap<String, CommitId>>> {
        let out = self.marks().clone();
        Box::pin(async move { Ok(out) })
    }

    fn compare_and_swap<'a>(
        &'a self,
        branch: &'a str,
        old: CommitId,
        next: CommitId,
    ) -> BoxFuture<'a, Result<CommitId>> {
        let now = {
            let mut marks = self.marks();
            let held = marks.get(branch).copied().unwrap_or_default();
            if held == old {
                marks.insert(branch.to_string(), next);
                next
            } else {
                held
            }
        };
        Box::pin(async move { Ok(now) })
    }
}
