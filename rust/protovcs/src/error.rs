//! The one error type the client returns.

use std::fmt;

/// Which way an operation failed. The cases are the Go client's sentinel
/// errors, so a caller can act on the same distinctions: a lost race is
/// retried, a tampered chunk is reported, an old client is told to upgrade.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum ErrorKind {
    /// The store does not hold a chunk.
    NoSuchChunk,
    /// The store does not hold a commit, which garbage collection can remove.
    NoSuchCommit,
    /// A branch does not exist.
    NoSuchRef,
    /// The transport cannot report usage.
    NoUsage,
    /// A ref moved between reading it and writing it.
    RaceLost,
    /// Two elements of one item share the identity their collection declared.
    DuplicateIdentity,
    /// An item outgrew the size its split allows.
    ItemTooLarge,
    /// A branch was asked to move to a commit that does not descend from it.
    NotFastForward,
    /// A chunk or commit does not match the name it was fetched under.
    Tampered,
    /// Written in a format this build does not know.
    UnsupportedFormat,
    /// Written under a schema version this build will not read.
    UnsupportedSchema,
    /// A read outside the checked-out scope.
    OutOfScope,
    /// The server offered an older head than one already seen.
    WentBackwards,
    /// An operation needs a key and the store has none.
    NoKey,
    /// The server ended a call because the store token it was made with
    /// expired. The caller gets a new token and tries again.
    TokenExpired,
    /// The server or the connection failed.
    Transport,
    /// Anything else: a bad layout, a message that does not parse, a caller's
    /// mistake.
    Invalid,
}

impl ErrorKind {
    /// The Go client's text for this kind, which every message begins with.
    fn prefix(self) -> Option<&'static str> {
        Some(match self {
            Self::NoSuchChunk => "protovcs: no such chunk",
            Self::NoSuchCommit => "protovcs: no such commit",
            Self::NoSuchRef => "protovcs: no such ref",
            Self::NoUsage => "protovcs: this transport cannot report usage",
            Self::RaceLost => "protovcs: another writer got there first",
            Self::DuplicateIdentity => "protovcs: two elements of one item share an identity",
            Self::ItemTooLarge => "protovcs: an item is larger than its split allows",
            Self::NotFastForward => "protovcs: that commit does not descend from the branch head",
            Self::Tampered => "protovcs: a chunk does not match the name it was fetched under",
            Self::UnsupportedFormat => "protovcs: written in a format this build does not know",
            Self::UnsupportedSchema => {
                "protovcs: written under a schema version this build does not support"
            }
            Self::OutOfScope => "protovcs: outside the checked-out scope",
            Self::WentBackwards => {
                "protovcs: the server offered an older version than one already seen"
            }
            Self::NoKey => "protovcs: this store was opened without a key",
            Self::TokenExpired => "protovcs: the store token expired",
            Self::Transport | Self::Invalid => return None,
        })
    }
}

/// An error from the client: a kind to match on and a message to show.
#[derive(Clone, Debug)]
pub struct Error {
    kind: ErrorKind,
    detail: String,
}

impl Error {
    /// An error of `kind`, with `detail` saying which thing failed. For a kind
    /// with a fixed text, the message is that text, a colon, and `detail`.
    pub fn new(kind: ErrorKind, detail: impl Into<String>) -> Self {
        Self {
            kind,
            detail: detail.into(),
        }
    }

    /// An [`ErrorKind::Invalid`] error.
    pub fn invalid(detail: impl Into<String>) -> Self {
        Self::new(ErrorKind::Invalid, detail)
    }

    /// Which way the operation failed.
    pub fn kind(&self) -> ErrorKind {
        self.kind
    }

    /// Whether this is an error of `kind`.
    pub fn is(&self, kind: ErrorKind) -> bool {
        self.kind == kind
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match (self.kind.prefix(), self.detail.is_empty()) {
            (Some(p), true) => f.write_str(p),
            (Some(p), false) => write!(f, "{p}: {}", self.detail),
            (None, _) => f.write_str(&self.detail),
        }
    }
}

impl std::error::Error for Error {}

impl From<connectrpc::ConnectError> for Error {
    fn from(e: connectrpc::ConnectError) -> Self {
        Self::new(ErrorKind::Transport, e.to_string())
    }
}

impl From<buffa::DecodeError> for Error {
    fn from(e: buffa::DecodeError) -> Self {
        Self::invalid(format!("protovcs: decoding: {e}"))
    }
}

/// A result with the client's error.
pub type Result<T> = std::result::Result<T, Error>;
