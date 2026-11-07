//! protovcs stores one large protobuf message as a versioned set of small
//! encrypted chunks, on a server that cannot read any of them.
//!
//! This is the Rust client. It is a port of the Go client at the root of the
//! repository, and the two write the same bytes: a chunk one of them names or
//! seals differently is a chunk the other cannot find. The tests hold them
//! together with the golden vectors in `testdata/golden/vectors.json`, which
//! the Go client writes and this crate must reproduce.
//!
//! Read [`Store`] first. A store is opened with a [`Transport`] (how the
//! server is reached; [`ConnectTransport`] speaks Connect over any
//! connect-rust `ClientTransport`), a [`Sealer`] ([`Keyring`] for a store with
//! a key), and a [`Compiled`] layout (how the message is cut into items).
//!
//! Messages are [`DynamicMessage`]s, described by a `DescriptorPool` the caller
//! supplies, where the Go client uses protoreflect. [`Compiled::to_typed`] and
//! [`Compiled::from_typed`] convert to and from generated buffa types.
//!
//! The repository's README describes the Go client, and everything in it
//! holds here.

#![deny(missing_docs)]
// A browser is single-threaded and its caches hold JavaScript values, so the
// store's shared state is not Send there. Arc keeps one type on every target.
#![cfg_attr(target_arch = "wasm32", allow(clippy::arc_with_non_send_sync))]

mod canonical;
mod chunkcache;
mod connect;
mod digest;
mod error;
#[cfg(all(target_arch = "wasm32", feature = "fetch"))]
pub mod fetch;
mod headmarks;
mod layout;
mod manifest;
mod materialize;
mod memo;
mod partial;
mod probe;
mod reflect;
mod rewrite;
mod seal;
mod store;
mod transport;
mod types;
mod version;
mod wire;

pub use buffa_descriptor::{DescriptorPool, DynamicMessage};
pub use canonical::canonical_bytes;
pub use chunkcache::ChunkCache;
pub use connect::{ConnectTransport, MAX_BATCH_BYTES, TokenSource};
#[doc(hidden)]
pub use digest::{node_digest_for_test, tree_digest};
pub use error::{Error, ErrorKind, Result};
pub use headmarks::{HeadMarks, MemoryHeadMarks};
pub use layout::{
    Compiled, ItemSize, bool_segment, date_segment, hash_segment, int_segment, string_segment,
    uint_segment,
};
#[doc(hidden)]
pub use layout::{float_string_for_test, unix_to_date_for_test};
pub use manifest::{
    ItemRef, ManifestChunk, ManifestReader, NodeSource, added_since, build_manifest,
    update_manifest,
};
pub use materialize::Item;
pub use partial::{KeyRange, Partial, Scope};
pub use probe::{Probe, ProbePoint};
pub use rewrite::RewriteResult;
pub use seal::{Entropy, Keyring, PlainSealer, ROOT_KEY_LEN, Sealer, system_entropy, verify_chunk};
pub use store::{
    Author, Branch, Clock, CommitInfo, CommitMeta, DEFAULT_LOG_LIMIT, DEFAULT_MAX_ATTEMPTS,
    LogOptions, Sleep, Store, StoreBuilder, commit_name,
};
pub use transport::{RefMove, RefState, SealedChunk, Transport, same_refs};
pub use types::{
    CANONICAL_VERSION, CHUNK_ID_LEN, ChunkId, CommitId, FORMAT_VERSION, ItemKey, ROOT_ITEM_KEY,
    Role,
};

/// The protovcs protos, generated for Rust.
pub use protovcs_proto::nextgui::protovcs::v1 as proto;

use std::future::Future;
use std::pin::Pin;

/// A boxed future. `Send` everywhere except in a browser, where the futures
/// that reach IndexedDB or `fetch` hold JavaScript values and cannot be.
#[cfg(not(target_arch = "wasm32"))]
pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// A boxed future. `Send` everywhere except in a browser, where the futures
/// that reach IndexedDB or `fetch` hold JavaScript values and cannot be.
#[cfg(target_arch = "wasm32")]
pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + 'a>>;

/// `Send` outside a browser, and nothing inside one. See [`BoxFuture`].
#[cfg(not(target_arch = "wasm32"))]
pub trait MaybeSend: Send {}
#[cfg(not(target_arch = "wasm32"))]
impl<T: Send + ?Sized> MaybeSend for T {}

/// `Send` outside a browser, and nothing inside one. See [`BoxFuture`].
#[cfg(target_arch = "wasm32")]
pub trait MaybeSend {}
#[cfg(target_arch = "wasm32")]
impl<T: ?Sized> MaybeSend for T {}

/// `Sync` outside a browser, and nothing inside one. See [`BoxFuture`].
#[cfg(not(target_arch = "wasm32"))]
pub trait MaybeSync: Sync {}
#[cfg(not(target_arch = "wasm32"))]
impl<T: Sync + ?Sized> MaybeSync for T {}

/// `Sync` outside a browser, and nothing inside one. See [`BoxFuture`].
#[cfg(target_arch = "wasm32")]
pub trait MaybeSync {}
#[cfg(target_arch = "wasm32")]
impl<T: ?Sized> MaybeSync for T {}
