//! A protovcs store held in memory, served over Connect.
//!
//! For tests. [`MemServer`] implements `StoreService` with the rules a real
//! server enforces: a commit naming a chunk that is not there is refused, and
//! a ref moves only against the token the writer read. [`MemServer::transport`]
//! reaches it through a connect-rust `ClientTransport` that calls the router
//! in process, so a test exercises the same [`ConnectTransport`] a browser
//! does. It is the Rust counterpart of `memtransport`.
//!
//! It does not enforce quotas.

#![deny(missing_docs)]

use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::sync::{Arc, Mutex};

use connectrpc::client::{ClientConfig, ServiceTransport};
use connectrpc::{
    ConnectError, ConnectRpcService, RequestContext, Response, Router, ServiceRequest,
    ServiceResult, ServiceStream,
};
use futures::StreamExt;
use futures::channel::mpsc;
use protovcs::proto::{
    BeginWriteRequest, BeginWriteResponse, Commit, GetChunksRequest, GetChunksResponse,
    GetCommitsRequest, GetCommitsResponse, HasChunksRequest, HasChunksResponse, ListRefsRequest,
    ListRefsResponse, MissingBetweenRequest, MissingBetweenResponse, PackHintRequest,
    PackHintResponse, PutChunksRequest, PutChunksResponse, PutCommitRequest, PutCommitResponse,
    Ref, StoreService, StoreServiceExt as _, UpdateRefRequest, UpdateRefResponse, WatchRefsRequest,
    WatchRefsResponse,
};
use protovcs::{ChunkId, CommitId, ConnectTransport, SealedChunk, Transport, commit_name};

/// The transport [`MemServer::transport`] returns.
pub type MemTransport = ConnectTransport<ServiceTransport<ConnectRpcService<Router>>>;

#[derive(Default)]
struct State {
    chunks: BTreeMap<ChunkId, Vec<u8>>,
    commits: HashMap<CommitId, Commit>,
    refs: BTreeMap<Vec<u8>, Ref>,
    cas_seq: u64,
    fail_next: Option<ConnectError>,
    watchers: Vec<mpsc::UnboundedSender<()>>,
    fetched: usize,
    uploaded: usize,
    calls: usize,
    last_hint: Option<(Vec<u8>, Vec<Vec<u8>>)>,
}

impl State {
    fn take(&mut self) -> Result<(), ConnectError> {
        match self.fail_next.take() {
            Some(e) => Err(e),
            None => Ok(()),
        }
    }

    fn wake(&mut self) {
        self.watchers.retain(|w| w.unbounded_send(()).is_ok());
    }

    fn refs(&self) -> Vec<Ref> {
        self.refs.values().cloned().collect()
    }

    fn descends_from(&self, start: CommitId, want: CommitId) -> bool {
        let mut seen = HashSet::new();
        let mut queue = VecDeque::from([start]);
        while let Some(id) = queue.pop_front() {
            if id == want {
                return true;
            }
            if !seen.insert(id) {
                continue;
            }
            if let Some(c) = self.commits.get(&id) {
                queue.extend(
                    c.parents
                        .iter()
                        .filter_map(|p| CommitId::from_bytes(p).ok()),
                );
            }
        }
        false
    }
}

/// An in-memory store. Cheap to clone; clones share the store.
#[derive(Clone, Default)]
pub struct MemServer {
    state: Arc<Mutex<State>>,
}

fn bad(e: impl std::fmt::Display) -> ConnectError {
    ConnectError::invalid_argument(e.to_string())
}

impl MemServer {
    /// An empty store.
    pub fn new() -> Self {
        Self::default()
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// A router serving this store.
    pub fn router(&self) -> Router {
        Arc::new(self.clone()).register(Router::new())
    }

    /// A Connect transport reaching this store in process.
    pub fn transport(&self) -> Arc<dyn Transport> {
        Arc::new(self.connect_transport())
    }

    /// [`MemServer::transport`] as its concrete type.
    pub fn connect_transport(&self) -> MemTransport {
        let service = ServiceTransport::new(ConnectRpcService::new(self.router()));
        let config = ClientConfig::new("http://memserver".parse().expect("a valid URI"));
        ConnectTransport::new(service, config, "mem")
    }

    /// Makes the next call fail with `err`.
    pub fn fail_next(&self, err: ConnectError) {
        self.lock().fail_next = Some(err);
    }

    /// Replaces a stored chunk's bytes, as a hostile server would.
    pub fn corrupt(&self, id: ChunkId, sealed: Vec<u8>) {
        self.lock().chunks.insert(id, sealed);
    }

    /// Stores chunks directly, bypassing any client.
    pub fn put_raw_chunks(&self, chunks: impl IntoIterator<Item = SealedChunk>) {
        let mut st = self.lock();
        for c in chunks {
            st.chunks.insert(c.id, c.sealed);
        }
    }

    /// Stores a commit directly, bypassing any client.
    pub fn put_raw_commit(&self, commit: Commit) {
        let id = CommitId::from_bytes(&commit.id).expect("a stored commit has an id");
        self.lock().commits.insert(id, commit);
    }

    /// Points a branch at a commit, bypassing compare-and-swap.
    pub fn set_ref(&self, name: &[u8], commit: CommitId) {
        let mut st = self.lock();
        st.cas_seq += 1;
        let token = st.cas_seq.to_string();
        st.refs.insert(
            name.to_vec(),
            Ref {
                name: name.to_vec(),
                commit: commit.to_vec(),
                cas_token: token,
                ..Default::default()
            },
        );
        st.wake();
    }

    /// Every stored chunk, by name.
    pub fn chunks(&self) -> BTreeMap<ChunkId, Vec<u8>> {
        self.lock().chunks.clone()
    }

    /// One stored commit.
    pub fn commit(&self, id: &CommitId) -> Option<Commit> {
        self.lock().commits.get(id).cloned()
    }

    /// Chunks fetched, chunks uploaded, and chunk calls made.
    pub fn counts(&self) -> (usize, usize, usize) {
        let st = self.lock();
        (st.fetched, st.uploaded, st.calls)
    }

    /// Zeroes [`MemServer::counts`].
    pub fn reset_counts(&self) {
        let mut st = self.lock();
        (st.fetched, st.uploaded, st.calls) = (0, 0, 0);
    }

    /// The last pack hint accepted: the commit and the chunks it named.
    pub fn last_pack_hint(&self) -> Option<(Vec<u8>, Vec<Vec<u8>>)> {
        self.lock().last_hint.clone()
    }
}

// The trait spells each return type as `impl Encodable<...>`; returning the
// message itself is the plainer form.
#[allow(refining_impl_trait)]
impl StoreService for MemServer {
    async fn list_refs(
        &self,
        _ctx: RequestContext,
        _request: ServiceRequest<'_, ListRefsRequest>,
    ) -> ServiceResult<ListRefsResponse> {
        let mut st = self.lock();
        st.take()?;
        Ok(Response::from(ListRefsResponse {
            refs: st.refs(),
            ..Default::default()
        }))
    }

    async fn watch_refs(
        &self,
        _ctx: RequestContext,
        _request: ServiceRequest<'_, WatchRefsRequest>,
    ) -> ServiceResult<ServiceStream<WatchRefsResponse>> {
        let (tx, rx) = mpsc::unbounded();
        let first = {
            let mut st = self.lock();
            st.take()?;
            st.watchers.push(tx);
            st.refs()
        };
        let state = Arc::clone(&self.state);
        let stream = futures::stream::unfold(
            (Some(first), Vec::<Ref>::new(), rx),
            move |(first, mut last, mut rx)| {
                let state = Arc::clone(&state);
                async move {
                    if let Some(refs) = first {
                        last = refs.clone();
                        let msg = WatchRefsResponse {
                            refs,
                            ..Default::default()
                        };
                        return Some((Ok(msg), (None, last, rx)));
                    }
                    loop {
                        rx.next().await?;
                        let now = state.lock().unwrap_or_else(|e| e.into_inner()).refs();
                        if now != last {
                            last = now.clone();
                            let msg = WatchRefsResponse {
                                refs: now,
                                ..Default::default()
                            };
                            return Some((Ok(msg), (None, last, rx)));
                        }
                    }
                }
            },
        );
        Ok(Response::from(
            Box::pin(stream) as ServiceStream<WatchRefsResponse>
        ))
    }

    async fn update_ref(
        &self,
        _ctx: RequestContext,
        request: ServiceRequest<'_, UpdateRefRequest>,
    ) -> ServiceResult<UpdateRefResponse> {
        let req = request.to_owned_message();
        let mut st = self.lock();
        st.take()?;
        let refused = |cur: Option<Ref>| {
            Ok(Response::from(UpdateRefResponse {
                updated: false,
                current: cur.into(),
                ..Default::default()
            }))
        };
        let cur = st.refs.get(&req.name).cloned();
        match (&cur, req.expected_cas_token.as_str()) {
            (None, t) if !t.is_empty() => return refused(None),
            (Some(_), "") => return refused(cur),
            (Some(c), t) if c.cas_token != t => return refused(cur),
            _ => {}
        }
        let commit = CommitId::from_bytes(&req.commit).unwrap_or_default();
        if req.delete || commit.is_zero() {
            st.refs.remove(&req.name);
            st.wake();
            return Ok(Response::from(UpdateRefResponse {
                updated: true,
                ..Default::default()
            }));
        }
        if !st.commits.contains_key(&commit) {
            return Err(ConnectError::internal(format!(
                "protovcs: ref points at commit {commit}, which is not stored"
            )));
        }
        if let Some(c) = &cur {
            let from = CommitId::from_bytes(&c.commit).unwrap_or_default();
            if !req.force && !st.descends_from(commit, from) {
                return Err(ConnectError::failed_precondition(format!(
                    "protovcs: that commit does not descend from the branch head: {commit} does not descend from {from}"
                )));
            }
        }
        st.cas_seq += 1;
        let next = Ref {
            name: req.name.clone(),
            commit: commit.to_vec(),
            cas_token: st.cas_seq.to_string(),
            ..Default::default()
        };
        st.refs.insert(req.name, next.clone());
        st.wake();
        Ok(Response::from(UpdateRefResponse {
            updated: true,
            current: next.into(),
            ..Default::default()
        }))
    }

    async fn get_commits(
        &self,
        _ctx: RequestContext,
        request: ServiceRequest<'_, GetCommitsRequest>,
    ) -> ServiceResult<GetCommitsResponse> {
        let req = request.to_owned_message();
        let mut st = self.lock();
        st.take()?;
        let depth = req.depth.max(1) as usize;
        let mut out = Vec::new();
        let mut next = VecDeque::from([CommitId::from_bytes(&req.from).map_err(bad)?]);
        let mut seen = HashSet::new();
        while out.len() < depth {
            let Some(id) = next.pop_front() else { break };
            if !seen.insert(id) {
                continue;
            }
            let c = st.commits.get(&id).ok_or_else(|| {
                ConnectError::not_found(format!("protovcs: no such commit: {id}"))
            })?;
            for p in &c.parents {
                next.push_back(CommitId::from_bytes(p).map_err(bad)?);
            }
            out.push(c.clone());
        }
        Ok(Response::from(GetCommitsResponse {
            commits: out,
            ..Default::default()
        }))
    }

    async fn put_commit(
        &self,
        _ctx: RequestContext,
        request: ServiceRequest<'_, PutCommitRequest>,
    ) -> ServiceResult<PutCommitResponse> {
        let req = request.to_owned_message();
        let commit = req.commit.as_option().cloned().unwrap_or_default();
        let mut st = self.lock();
        st.take()?;
        for raw in &commit.added {
            let id = ChunkId::from_bytes(raw).map_err(bad)?;
            if !st.chunks.contains_key(&id) {
                return Err(ConnectError::failed_precondition(format!(
                    "protovcs: commit adds chunk {id}, which was never stored"
                )));
            }
        }
        for raw in &commit.parents {
            let pid = CommitId::from_bytes(raw).map_err(bad)?;
            if !st.commits.contains_key(&pid) {
                return Err(ConnectError::failed_precondition(format!(
                    "protovcs: commit names parent {pid}, which is not stored"
                )));
            }
        }
        let id = commit_name(&commit).map_err(bad)?;
        if !commit.id.is_empty() && CommitId::from_bytes(&commit.id).map_err(bad)? != id {
            return Err(ConnectError::invalid_argument(format!(
                "protovcs: commit claims another name than {id}"
            )));
        }
        let mut stored = commit;
        stored.id = id.to_vec();
        st.commits.insert(id, stored);
        Ok(Response::from(PutCommitResponse {
            id: id.to_vec(),
            ..Default::default()
        }))
    }

    async fn missing_between(
        &self,
        _ctx: RequestContext,
        request: ServiceRequest<'_, MissingBetweenRequest>,
    ) -> ServiceResult<MissingBetweenResponse> {
        let req = request.to_owned_message();
        let mut st = self.lock();
        st.take()?;
        let held: HashSet<CommitId> = req
            .have
            .iter()
            .map(|h| CommitId::from_bytes(h).map_err(bad))
            .collect::<Result<_, _>>()?;
        let mut out = Vec::new();
        let mut seen = HashSet::new();
        let mut queue = VecDeque::from([CommitId::from_bytes(&req.want).map_err(bad)?]);
        let mut met_one = held.is_empty();
        while let Some(id) = queue.pop_front() {
            if held.contains(&id) {
                met_one = true;
                continue;
            }
            if !seen.insert(id) {
                continue;
            }
            let c = st.commits.get(&id).ok_or_else(|| {
                ConnectError::not_found(format!("protovcs: no such commit: {id}"))
            })?;
            out.extend(c.added.iter().cloned());
            for p in &c.parents {
                queue.push_back(CommitId::from_bytes(p).map_err(bad)?);
            }
        }
        Ok(Response::from(MissingBetweenResponse {
            chunk_ids: out,
            full: !met_one,
            ..Default::default()
        }))
    }

    async fn begin_write(
        &self,
        _ctx: RequestContext,
        _request: ServiceRequest<'_, BeginWriteRequest>,
    ) -> ServiceResult<BeginWriteResponse> {
        let mut st = self.lock();
        st.take()?;
        st.cas_seq += 1;
        Ok(Response::from(BeginWriteResponse {
            reservation: format!("reservation-{}", st.cas_seq),
            ..Default::default()
        }))
    }

    async fn has_chunks(
        &self,
        _ctx: RequestContext,
        request: ServiceRequest<'_, HasChunksRequest>,
    ) -> ServiceResult<HasChunksResponse> {
        let req = request.to_owned_message();
        let mut st = self.lock();
        st.take()?;
        let mut missing = Vec::new();
        for raw in req.chunk_ids {
            let id = ChunkId::from_bytes(&raw).map_err(bad)?;
            if !st.chunks.contains_key(&id) {
                missing.push(raw);
            }
        }
        Ok(Response::from(HasChunksResponse {
            missing,
            ..Default::default()
        }))
    }

    async fn put_chunks(
        &self,
        _ctx: RequestContext,
        request: ServiceRequest<'_, PutChunksRequest>,
    ) -> ServiceResult<PutChunksResponse> {
        let req = request.to_owned_message();
        let mut st = self.lock();
        st.take()?;
        st.calls += 1;
        for c in req.chunks {
            let id = ChunkId::from_bytes(&c.chunk_id).map_err(bad)?;
            if let std::collections::btree_map::Entry::Vacant(e) = st.chunks.entry(id) {
                e.insert(c.sealed);
                st.uploaded += 1;
            }
        }
        Ok(Response::from(PutChunksResponse::default()))
    }

    async fn get_chunks(
        &self,
        _ctx: RequestContext,
        request: ServiceRequest<'_, GetChunksRequest>,
    ) -> ServiceResult<ServiceStream<GetChunksResponse>> {
        let req = request.to_owned_message();
        let mut st = self.lock();
        st.take()?;
        st.calls += 1;
        let mut out = Vec::with_capacity(req.chunk_ids.len());
        for raw in req.chunk_ids {
            let id = ChunkId::from_bytes(&raw).map_err(bad)?;
            let sealed =
                st.chunks.get(&id).cloned().ok_or_else(|| {
                    ConnectError::not_found(format!("protovcs: no such chunk: {id}"))
                })?;
            st.fetched += 1;
            out.push(Ok(GetChunksResponse {
                chunk_id: raw,
                sealed,
                ..Default::default()
            }));
        }
        Ok(Response::from(
            Box::pin(futures::stream::iter(out)) as ServiceStream<GetChunksResponse>
        ))
    }

    async fn pack_hint(
        &self,
        _ctx: RequestContext,
        request: ServiceRequest<'_, PackHintRequest>,
    ) -> ServiceResult<PackHintResponse> {
        let req = request.to_owned_message();
        let mut st = self.lock();
        st.take()?;
        let head = st.refs.values().any(|r| r.commit == req.commit);
        if head {
            st.last_hint = Some((req.commit, req.chunk_ids));
        }
        Ok(Response::from(PackHintResponse {
            accepted: head,
            ..Default::default()
        }))
    }
}
