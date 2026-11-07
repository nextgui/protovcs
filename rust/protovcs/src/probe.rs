//! Named steps inside an operation, for tests that stop or fail one. Port of
//! `probe.go`.

use crate::error::Result;
use crate::{MaybeSend, MaybeSync};

/// A step inside an operation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ProbePoint {
    /// A writer has read the branch and built nothing.
    AfterHead,
    /// Every chunk is stored and the commit does not exist.
    AfterUpload,
    /// The commit is recorded and the branch has not moved.
    AfterCommit,
    /// Immediately before the compare-and-swap.
    BeforeRefUpdate,
}

/// Observes the steps of an operation and may fail one. Returning an error
/// makes the operation fail there, which is how a crash is simulated.
pub trait Probe: MaybeSend + MaybeSync {
    /// Called at `point`.
    ///
    /// # Errors
    ///
    /// Whatever the probe chooses; the operation fails with it.
    fn at(&self, point: ProbePoint) -> Result<()>;
}

impl<F> Probe for F
where
    F: Fn(ProbePoint) -> Result<()> + MaybeSend + MaybeSync,
{
    fn at(&self, point: ProbePoint) -> Result<()> {
        self(point)
    }
}
