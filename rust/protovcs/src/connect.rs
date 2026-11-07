//! A store reached over Connect. Port of
//! `connecttransport/connecttransport.go`.
//!
//! It works over any connect-rust `ClientTransport`: an in-memory one in
//! tests, `fetch` in a browser, hyper on a server. It decides nothing: every
//! refusal comes from the server, and this turns it into the error a caller
//! acts on.

use std::sync::Arc;

use connectrpc::client::{CallOptions, ClientConfig, ClientTransport};
use connectrpc::{ConnectError, ErrorCode};

use crate::BoxFuture;
use crate::error::{Error, ErrorKind, Result};
use crate::proto::{
    BeginWriteRequest, Commit, GetChunksRequest, GetCommitsRequest, GetUsageRequest,
    HasChunksRequest, ListRefsRequest, MissingBetweenRequest, PackHintRequest, PutChunksRequest,
    PutCommitRequest, Ref, SealedChunk as SealedChunkProto, StoreAdminServiceClient,
    StoreServiceClient, UpdateRefRequest, Usage, WatchRefsRequest,
};
use crate::transport::{RefMove, RefState, SealedChunk, Transport};
use crate::types::{ChunkId, CommitId};

/// How much one `PutChunks` request carries.
pub const MAX_BATCH_BYTES: usize = 4 << 20;

/// Mints the store token sent with every request, as a bearer token. Called
/// once per request, so a token that expires is reminted.
pub type TokenSource = Arc<dyn Fn() -> BoxFuture<'static, Result<String>> + Send + Sync>;

/// A protovcs store reached over Connect. The store id is fixed at
/// construction, so the handle cannot be pointed at another store.
pub struct ConnectTransport<T> {
    client: StoreServiceClient<T>,
    admin: StoreAdminServiceClient<T>,
    store_id: String,
    token: Option<TokenSource>,
}

impl<T> ConnectTransport<T>
where
    T: ClientTransport,
    T::ResponseBody: Unpin,
    <T::ResponseBody as http_body::Body>::Error: std::fmt::Display,
{
    /// A transport for one store, sending requests through `transport` to the
    /// services under `config`'s base URI.
    pub fn new(transport: T, config: ClientConfig, store_id: impl Into<String>) -> Self {
        Self {
            client: StoreServiceClient::new(transport.clone(), config.clone()),
            admin: StoreAdminServiceClient::new(transport, config),
            store_id: store_id.into(),
            token: None,
        }
    }

    /// Attaches a store token to every request.
    pub fn with_token(mut self, token: TokenSource) -> Self {
        self.token = Some(token);
        self
    }

    /// The options every call is made with. Every method goes through here,
    /// so a method added later carries the token without anybody adding it.
    async fn options(&self) -> Result<CallOptions> {
        let opts = CallOptions::default();
        let Some(token) = &self.token else {
            return Ok(opts);
        };
        let t = token()
            .await
            .map_err(|e| Error::new(e.kind(), format!("protovcs: getting a store token: {e}")))?;
        if t.is_empty() {
            return Ok(opts);
        }
        Ok(opts.with_header("authorization", format!("Bearer {t}")))
    }
}

/// Turns a server's refusal into the error a caller matches on.
impl<T> ConnectTransport<T>
where
    T: ClientTransport,
    T::ResponseBody: Unpin,
    <T::ResponseBody as http_body::Body>::Error: std::fmt::Display,
{
    /// Reads one WatchRefs stream until it ends, and reports whether it
    /// delivered anything along with why it ended.
    async fn watch_once(
        &self,
        deliver: &mut (dyn FnMut(Vec<RefState>) -> Result<()> + Send + '_),
    ) -> (bool, Error) {
        let req = WatchRefsRequest {
            store_id: self.store_id.clone(),
            ..Default::default()
        };
        let options = match self.options().await {
            Ok(o) => o,
            Err(e) => return (false, e),
        };
        // Dropping the stream on return, however the watch ends, closes it.
        let mut stream = match self.client.watch_refs_with_options(req, options).await {
            Ok(s) => s,
            Err(e) => return (false, from_connect(e)),
        };
        let mut delivered = false;
        loop {
            let msg = match stream.message().await {
                Ok(Some(msg)) => msg.to_owned_message(),
                Ok(None) => break,
                Err(e) => return (delivered, from_connect(e)),
            };
            let refs = match msg.refs.iter().map(ref_state).collect::<Result<_>>() {
                Ok(r) => r,
                Err(e) => return (delivered, e),
            };
            if let Err(e) = deliver(refs) {
                return (delivered, e);
            }
            delivered = true;
        }
        (
            delivered,
            Error::new(
                ErrorKind::Transport,
                "protovcs: the server ended the ref watch",
            ),
        )
    }
}

fn from_connect(e: ConnectError) -> Error {
    let msg = e.message.clone().unwrap_or_default();
    match e.code {
        ErrorCode::Aborted => Error::new(ErrorKind::RaceLost, msg),
        ErrorCode::NotFound => Error::new(ErrorKind::NoSuchChunk, msg),
        ErrorCode::Unauthenticated => Error::new(ErrorKind::TokenExpired, msg),
        _ => Error::from(e),
    }
}

/// FailedPrecondition from UpdateRef is a non-fast-forward move.
fn update_ref_error(e: ConnectError) -> Error {
    if e.code == ErrorCode::FailedPrecondition {
        return Error::new(
            ErrorKind::NotFastForward,
            e.message.clone().unwrap_or_default(),
        );
    }
    from_connect(e)
}

/// NotFound from GetCommits is a missing commit, not a missing chunk.
fn get_commits_error(e: ConnectError) -> Error {
    if e.code == ErrorCode::NotFound {
        return Error::new(
            ErrorKind::NoSuchCommit,
            e.message.clone().unwrap_or_default(),
        );
    }
    from_connect(e)
}

fn ref_state(r: &Ref) -> Result<RefState> {
    Ok(RefState {
        name: r.name.clone(),
        commit: CommitId::from_bytes(&r.commit)?,
        cas_token: r.cas_token.clone(),
    })
}

fn ids_from(raw: &[Vec<u8>]) -> Result<Vec<ChunkId>> {
    raw.iter().map(|b| ChunkId::from_bytes(b)).collect()
}

impl<T> Transport for ConnectTransport<T>
where
    T: ClientTransport,
    T::ResponseBody: Unpin,
    <T::ResponseBody as http_body::Body>::Error: std::fmt::Display,
{
    fn list_refs(&self) -> BoxFuture<'_, Result<Vec<RefState>>> {
        Box::pin(async move {
            let req = ListRefsRequest {
                store_id: self.store_id.clone(),
                ..Default::default()
            };
            let resp = self
                .client
                .list_refs_with_options(req, self.options().await?)
                .await
                .map_err(from_connect)?
                .into_owned();
            resp.refs.iter().map(ref_state).collect()
        })
    }

    /// The server ends a watch when the token it was opened with expires.
    /// This then opens a new stream, which asks the token source for a token
    /// again. The new stream's first delivery is the current state, so nothing
    /// that moved in between is missed. A stream that ends that way before
    /// delivering anything is not reopened, so a token source that keeps
    /// returning an expired token gets [`ErrorKind::TokenExpired`] rather than
    /// a loop.
    fn watch_refs<'a>(
        &'a self,
        mut deliver: Box<dyn FnMut(Vec<RefState>) -> Result<()> + Send + 'a>,
    ) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            loop {
                let (delivered, err) = self.watch_once(&mut deliver).await;
                if !(delivered && err.is(ErrorKind::TokenExpired)) {
                    return Err(err);
                }
            }
        })
    }

    fn update_ref<'a>(
        &'a self,
        name: &'a [u8],
        commit: CommitId,
        expected: &'a str,
        mode: RefMove,
    ) -> BoxFuture<'a, Result<RefState>> {
        Box::pin(async move {
            let req = UpdateRefRequest {
                store_id: self.store_id.clone(),
                name: name.to_vec(),
                commit: commit.to_vec(),
                expected_cas_token: expected.to_string(),
                delete: commit.is_zero(),
                force: mode == RefMove::Force,
                ..Default::default()
            };
            let resp = self
                .client
                .update_ref_with_options(req, self.options().await?)
                .await
                .map_err(update_ref_error)?
                .into_owned();
            let current = match resp.current.as_option() {
                Some(r) => ref_state(r)?,
                None => RefState::default(),
            };
            // A refused move is a successful call reporting failure, because
            // the caller needs the current state to rebuild against.
            if !resp.updated {
                return Err(Error::new(ErrorKind::RaceLost, ""));
            }
            Ok(current)
        })
    }

    fn get_commits(&self, from: CommitId, depth: u32) -> BoxFuture<'_, Result<Vec<Commit>>> {
        Box::pin(async move {
            let req = GetCommitsRequest {
                store_id: self.store_id.clone(),
                from: from.to_vec(),
                depth,
                ..Default::default()
            };
            Ok(self
                .client
                .get_commits_with_options(req, self.options().await?)
                .await
                .map_err(get_commits_error)?
                .into_owned()
                .commits)
        })
    }

    fn put_commit<'a>(
        &'a self,
        commit: &'a Commit,
        reservation: &'a str,
    ) -> BoxFuture<'a, Result<CommitId>> {
        Box::pin(async move {
            let req = PutCommitRequest {
                store_id: self.store_id.clone(),
                commit: commit.clone().into(),
                reservation: reservation.to_string(),
                ..Default::default()
            };
            let resp = self
                .client
                .put_commit_with_options(req, self.options().await?)
                .await
                .map_err(from_connect)?
                .into_owned();
            CommitId::from_bytes(&resp.id)
        })
    }

    fn missing_between<'a>(
        &'a self,
        have: &'a [CommitId],
        want: CommitId,
    ) -> BoxFuture<'a, Result<(Vec<ChunkId>, bool)>> {
        Box::pin(async move {
            let req = MissingBetweenRequest {
                store_id: self.store_id.clone(),
                have: have.iter().map(CommitId::to_vec).collect(),
                want: want.to_vec(),
                ..Default::default()
            };
            let resp = self
                .client
                .missing_between_with_options(req, self.options().await?)
                .await
                .map_err(from_connect)?
                .into_owned();
            Ok((ids_from(&resp.chunk_ids)?, resp.full))
        })
    }

    fn begin_write(&self, chunks: u32, bytes: u64) -> BoxFuture<'_, Result<String>> {
        Box::pin(async move {
            let req = BeginWriteRequest {
                store_id: self.store_id.clone(),
                estimated_chunks: chunks,
                estimated_bytes: bytes,
                ..Default::default()
            };
            Ok(self
                .client
                .begin_write_with_options(req, self.options().await?)
                .await
                .map_err(from_connect)?
                .into_owned()
                .reservation)
        })
    }

    fn has_chunks<'a>(&'a self, ids: &'a [ChunkId]) -> BoxFuture<'a, Result<Vec<ChunkId>>> {
        Box::pin(async move {
            let req = HasChunksRequest {
                store_id: self.store_id.clone(),
                chunk_ids: ids.iter().map(ChunkId::to_vec).collect(),
                ..Default::default()
            };
            let resp = self
                .client
                .has_chunks_with_options(req, self.options().await?)
                .await
                .map_err(from_connect)?
                .into_owned();
            ids_from(&resp.missing)
        })
    }

    fn put_chunks<'a>(
        &'a self,
        reservation: &'a str,
        chunks: &'a [SealedChunk],
    ) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            // Split into requests that fit. A half-arrived batch can simply be
            // sent again, because a chunk's name determines its bytes.
            let mut batches: Vec<Vec<SealedChunkProto>> = vec![Vec::new()];
            let mut size = 0;
            for c in chunks {
                let last = batches.last_mut().expect("never empty");
                if size + c.sealed.len() > MAX_BATCH_BYTES && !last.is_empty() {
                    batches.push(Vec::new());
                    size = 0;
                }
                batches
                    .last_mut()
                    .expect("never empty")
                    .push(SealedChunkProto {
                        chunk_id: c.id.to_vec(),
                        sealed: c.sealed.clone(),
                        ..Default::default()
                    });
                size += c.sealed.len();
            }
            for batch in batches.into_iter().filter(|b| !b.is_empty()) {
                let req = PutChunksRequest {
                    store_id: self.store_id.clone(),
                    reservation: reservation.to_string(),
                    chunks: batch,
                    ..Default::default()
                };
                self.client
                    .put_chunks_with_options(req, self.options().await?)
                    .await
                    .map_err(from_connect)?;
            }
            Ok(())
        })
    }

    fn get_chunks<'a>(&'a self, ids: &'a [ChunkId]) -> BoxFuture<'a, Result<Vec<SealedChunk>>> {
        Box::pin(async move {
            let req = GetChunksRequest {
                store_id: self.store_id.clone(),
                chunk_ids: ids.iter().map(ChunkId::to_vec).collect(),
                ..Default::default()
            };
            let mut stream = self
                .client
                .get_chunks_with_options(req, self.options().await?)
                .await
                .map_err(from_connect)?;
            let mut out = Vec::with_capacity(ids.len());
            while let Some(msg) = stream.message().await.map_err(from_connect)? {
                let msg = msg.to_owned_message();
                out.push(SealedChunk {
                    id: ChunkId::from_bytes(&msg.chunk_id)?,
                    sealed: msg.sealed,
                });
            }
            // A short answer is damage, not absence.
            if out.len() != ids.len() {
                return Err(Error::new(
                    ErrorKind::NoSuchChunk,
                    format!("asked for {} chunks and received {}", ids.len(), out.len()),
                ));
            }
            Ok(out)
        })
    }

    fn pack_hint<'a>(
        &'a self,
        commit: CommitId,
        ids: &'a [ChunkId],
    ) -> BoxFuture<'a, Result<bool>> {
        Box::pin(async move {
            let req = PackHintRequest {
                store_id: self.store_id.clone(),
                commit: commit.to_vec(),
                chunk_ids: ids.iter().map(ChunkId::to_vec).collect(),
                ..Default::default()
            };
            Ok(self
                .client
                .pack_hint_with_options(req, self.options().await?)
                .await
                .map_err(from_connect)?
                .into_owned()
                .accepted)
        })
    }

    fn usage(&self) -> BoxFuture<'_, Result<Usage>> {
        Box::pin(async move {
            let req = GetUsageRequest {
                store_id: self.store_id.clone(),
                ..Default::default()
            };
            let resp = self
                .admin
                .get_usage_with_options(req, self.options().await?)
                .await
                .map_err(from_connect)?
                .into_owned();
            Ok(resp.usage.as_option().cloned().unwrap_or_default())
        })
    }
}
