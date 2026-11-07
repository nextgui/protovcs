//! One versioned message on a server that cannot read it. Port of
//! `store.go`.

use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use buffa::Message;
use buffa_descriptor::DynamicMessage;
use buffa_descriptor::generated::descriptor::FileDescriptorSet;
use buffa_descriptor::reflect::ReflectMessage;
use buffa_types::google::protobuf::Timestamp;
use sha2::{Digest, Sha256};

use crate::BoxFuture;
use crate::canonical::canonical_bytes;
use crate::chunkcache::ChunkCache;
use crate::digest::node_digest;
use crate::error::{Error, ErrorKind, Result};
use crate::headmarks::{HeadMarks, SAVE_MARK_ATTEMPTS};
use crate::layout::Compiled;
use crate::manifest::{
    ItemRef, ManifestChunk, ManifestReader, NodeSource, added_since, build_manifest,
};
use crate::memo::Memo;
use crate::probe::{Probe, ProbePoint};
use crate::proto::{
    Author as AuthorProto, Commit, CommitMessage, CommitPayload, ManifestNode, Usage,
};
use crate::reflect::canonical_own;
use crate::seal::{Entropy, Sealer, system_entropy};
use crate::transport::{RefMove, RefState, SealedChunk, Transport};
use crate::types::{ChunkId, CommitId, FORMAT_VERSION, ItemKey, Role};

/// How many times [`Branch::change`] rebuilds and retries before giving up.
pub const DEFAULT_MAX_ATTEMPTS: usize = 24;

/// How many commits [`Branch::log`] returns when asked for no number.
pub const DEFAULT_LOG_LIMIT: usize = 100;

/// How many commits one history request asks for at most.
pub(crate) const HISTORY_BATCH: u32 = 1000;

const RETRY_BASE: Duration = Duration::from_millis(2);
const RETRY_CAP: Duration = Duration::from_secs(1);

/// Where a store reads the time a commit records: seconds and nanoseconds
/// since the Unix epoch, in UTC.
pub type Clock = Arc<dyn Fn() -> Timestamp + Send + Sync>;

/// How a store waits between attempts after losing a race. By default it is a
/// timer that needs no particular async runtime; set one to use the
/// application's own.
pub type Sleep = Arc<dyn Fn(Duration) -> BoxFuture<'static, ()> + Send + Sync>;

fn system_sleep() -> Sleep {
    Arc::new(|d| {
        let delay = futures_timer::Delay::new(d);
        // The browser's timer is not Send. A wasm32 page is single-threaded,
        // so the future never leaves the thread it was made on.
        #[cfg(target_arch = "wasm32")]
        let delay = send_wrapper::SendWrapper::new(delay);
        Box::pin(delay)
    })
}

fn system_clock() -> Clock {
    Arc::new(|| {
        #[cfg(target_arch = "wasm32")]
        let ms = js_sys::Date::now();
        #[cfg(not(target_arch = "wasm32"))]
        let ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs_f64() * 1000.0)
            .unwrap_or_default();
        let secs = (ms / 1000.0).floor();
        Timestamp {
            seconds: secs as i64,
            nanos: ((ms - secs * 1000.0) * 1_000_000.0) as i32,
            ..Default::default()
        }
    })
}

pub(crate) struct Inner {
    pub(crate) transport: Arc<dyn Transport>,
    pub(crate) sealer: Arc<dyn Sealer>,
    pub(crate) layout: Arc<Compiled>,
    state: Mutex<State>,
    pub(crate) memo: Arc<Memo>,
    probe: Option<Arc<dyn Probe>>,
    hint: String,
    pub(crate) chunk_cache: Option<Arc<dyn ChunkCache>>,
    clock: Clock,
    sleep: Sleep,
    entropy: Entropy,
    descriptors: Option<FileDescriptorSet>,
    /// Where `high_water` is kept between stores, or none. See
    /// [`HeadMarks`].
    head_marks: Option<Arc<dyn HeadMarks>>,
    /// Whether the marks were loaded; held while loading them.
    marks_loaded: futures::lock::Mutex<bool>,
    /// Held while a mark is saved, so saves of one store do not interleave.
    saving: futures::lock::Mutex<()>,
}

#[derive(Default)]
struct State {
    /// The newest commit seen on each branch.
    high_water: HashMap<String, CommitId>,
    /// What this store last read from or wrote to its head marks.
    stored: HashMap<String, CommitId>,
    /// The last commit hinted.
    hinted: CommitId,
}

/// One versioned message on a server that cannot read it. Cheap to clone;
/// clones share everything.
#[derive(Clone)]
pub struct Store {
    pub(crate) inner: Arc<Inner>,
}

/// Assembles a [`Store`].
#[derive(Default)]
pub struct StoreBuilder {
    transport: Option<Arc<dyn Transport>>,
    sealer: Option<Arc<dyn Sealer>>,
    layout: Option<Arc<Compiled>>,
    probe: Option<Arc<dyn Probe>>,
    hint: String,
    chunk_cache: Option<Arc<dyn ChunkCache>>,
    clock: Option<Clock>,
    sleep: Option<Sleep>,
    entropy: Option<Entropy>,
    descriptors: Option<FileDescriptorSet>,
    head_marks: Option<Arc<dyn HeadMarks>>,
}

impl StoreBuilder {
    /// Sets how the store is reached. Required.
    pub fn transport(mut self, t: Arc<dyn Transport>) -> Self {
        self.transport = Some(t);
        self
    }

    /// Sets how chunks are named and protected. Required.
    pub fn sealer(mut self, s: Arc<dyn Sealer>) -> Self {
        self.sealer = Some(s);
        self
    }

    /// Sets how the message is decomposed. Required.
    pub fn layout(mut self, c: Arc<Compiled>) -> Self {
        self.layout = Some(c);
        self
    }

    /// Attaches a probe. Tests use it; nothing else should.
    pub fn probe(mut self, p: Arc<dyn Probe>) -> Self {
        self.probe = Some(p);
        self
    }

    /// Makes the store send a pack hint after each full read of `branch`'s
    /// head.
    pub fn pack_hints(mut self, branch: impl Into<String>) -> Self {
        self.hint = branch.into();
        self
    }

    /// Makes the store look for chunks in `cache` before the server.
    pub fn chunk_cache(mut self, cache: Arc<dyn ChunkCache>) -> Self {
        self.chunk_cache = Some(cache);
        self
    }

    /// Sets where commit times come from. The default reads the system clock,
    /// or `Date.now()` in a browser.
    pub fn clock(mut self, c: Clock) -> Self {
        self.clock = Some(c);
        self
    }

    /// Sets how the store waits between attempts after losing a race.
    pub fn sleep(mut self, s: Sleep) -> Self {
        self.sleep = Some(s);
        self
    }

    /// Sets where commit nonces and retry jitter come from, for tests that
    /// need them fixed. Only with the `test-util` feature.
    #[cfg(feature = "test-util")]
    pub fn entropy(mut self, e: Entropy) -> Self {
        self.entropy = Some(e);
        self
    }

    /// Sets the descriptors recorded in the schema chunk. By default they are
    /// the root message's file and everything it imports, read from the
    /// layout's pool.
    pub fn schema_descriptors(mut self, set: FileDescriptorSet) -> Self {
        self.descriptors = Some(set);
        self
    }

    /// Makes the store load the newest head seen on each branch from
    /// `marks`, and write there each one as it advances, so the rollback check
    /// in [`Branch::head`] holds across stores. See [`HeadMarks`].
    pub fn head_marks(mut self, marks: Arc<dyn HeadMarks>) -> Self {
        self.head_marks = Some(marks);
        self
    }

    /// Returns the store.
    ///
    /// # Errors
    ///
    /// When the transport, sealer or layout is missing.
    pub fn build(self) -> Result<Store> {
        let transport = self
            .transport
            .ok_or_else(|| Error::invalid("protovcs: a store needs a transport"))?;
        let sealer = self.sealer.ok_or_else(|| {
            Error::invalid("protovcs: a store needs a sealer; use PlainSealer for one with no key")
        })?;
        let layout = self
            .layout
            .ok_or_else(|| Error::invalid("protovcs: a store needs a layout"))?;
        Ok(Store {
            inner: Arc::new(Inner {
                transport,
                sealer,
                layout,
                state: Mutex::default(),
                memo: Arc::new(Memo::default()),
                probe: self.probe,
                hint: self.hint,
                chunk_cache: self.chunk_cache,
                clock: self.clock.unwrap_or_else(system_clock),
                sleep: self.sleep.unwrap_or_else(system_sleep),
                entropy: self.entropy.unwrap_or_else(system_entropy),
                descriptors: self.descriptors,
                head_marks: self.head_marks,
                marks_loaded: futures::lock::Mutex::new(false),
                saving: futures::lock::Mutex::new(()),
            }),
        })
    }
}

/// What a commit records about who made it and why. Sealed with the payload.
#[derive(Clone, Debug, Default)]
pub struct CommitMeta {
    /// One line, for a list of commits.
    pub title: String,
    /// The rest. Usually empty.
    pub description: String,
    /// Who made the change.
    pub authors: Vec<Author>,
    /// The application build that wrote it.
    pub client: String,
    /// Short annotations.
    pub labels: BTreeMap<String, String>,
    /// The application's own record, of the type the layout's
    /// `commit_message` names.
    pub details: Option<DynamicMessage>,
}

impl CommitMeta {
    /// The metadata of a change one person made.
    pub fn new(
        title: impl Into<String>,
        author: impl Into<String>,
        email: impl Into<String>,
    ) -> Self {
        Self {
            title: title.into(),
            authors: vec![Author {
                name: author.into(),
                email: email.into(),
                role: String::new(),
            }],
            ..Default::default()
        }
    }
}

/// Who made a change.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Author {
    /// Their name.
    pub name: String,
    /// Their email address.
    pub email: String,
    /// What they did, when a commit has more than one author.
    pub role: String,
}

/// One entry of a store's history, opened.
#[derive(Clone, Debug, Default)]
pub struct CommitInfo {
    /// The commit's name.
    pub id: CommitId,
    /// Its parents.
    pub parents: Vec<CommitId>,
    /// One line.
    pub title: String,
    /// The rest.
    pub description: String,
    /// Who made it.
    pub authors: Vec<Author>,
    /// The application build that wrote it.
    pub client: String,
    /// Short annotations.
    pub labels: BTreeMap<String, String>,
    /// The writer's clock at the time.
    pub time: Option<Timestamp>,
    /// The branch it was written on.
    pub branch: String,
    /// What the layout said the data meant.
    pub schema_version: u32,
    /// The application's own record, encoded.
    pub details: Vec<u8>,
}

/// Bounds a walk through history.
#[derive(Clone, Debug, Default)]
pub struct LogOptions {
    /// How many commits at most; zero means [`DEFAULT_LOG_LIMIT`].
    pub limit: usize,
    /// Stop at this commit, without including it.
    pub until: CommitId,
}

/// The parts of a commit's payload beyond its tree.
pub(crate) struct VersionSpec {
    pub(crate) branch: String,
    pub(crate) message: CommitMessage,
    pub(crate) at: Timestamp,
}

/// The name of a commit: the hash of its canonical record with the id and
/// payload left out.
///
/// # Errors
///
/// Only if the record cannot be encoded, which a decoded commit always can.
pub fn commit_name(commit: &Commit) -> Result<CommitId> {
    let mut naming = commit.clone();
    naming.id = Vec::new();
    naming.payload = Vec::new();
    let encoded = canonical_own(&naming)?;
    Ok(CommitId::new(Sha256::digest(&encoded).into()))
}

fn verified_name(commit: &Commit) -> Result<CommitId> {
    let name = commit_name(commit)?;
    let claimed = CommitId::from_bytes(&commit.id)?;
    if claimed != name {
        return Err(Error::new(
            ErrorKind::Tampered,
            format!("commit {claimed} is named {name} by its contents"),
        ));
    }
    Ok(name)
}

/// The store's node source: manifest nodes read through its memo and cache.
pub(crate) struct Nodes<'a>(pub(crate) &'a Store);

impl NodeSource for Nodes<'_> {
    fn node<'a>(&'a self, prefix: &'a str, id: ChunkId) -> BoxFuture<'a, Result<ManifestNode>> {
        Box::pin(async move {
            let plaintext = self.0.chunk(id).await?;
            ManifestNode::decode_from_slice(&plaintext).map_err(|e| {
                Error::invalid(format!(
                    "protovcs: manifest node at {prefix:?} did not parse: {e}"
                ))
            })
        })
    }

    fn prefetch<'a>(&'a self, ids: &'a [ChunkId]) -> BoxFuture<'a, Result<()>> {
        Box::pin(self.0.warm(ids))
    }
}

impl Store {
    /// A builder with nothing set.
    pub fn builder() -> StoreBuilder {
        StoreBuilder::default()
    }

    /// A store sharing this one's transport, sealer, memo and cache, under a
    /// different layout and with its own record of heads seen.
    pub(crate) fn derive(&self, layout: Arc<Compiled>) -> Store {
        let i = &self.inner;
        Store {
            inner: Arc::new(Inner {
                transport: Arc::clone(&i.transport),
                sealer: Arc::clone(&i.sealer),
                layout,
                state: Mutex::default(),
                memo: Arc::clone(&i.memo),
                probe: None,
                hint: String::new(),
                chunk_cache: i.chunk_cache.clone(),
                clock: Arc::clone(&i.clock),
                sleep: i.sleep.clone(),
                entropy: Arc::clone(&i.entropy),
                descriptors: None,
                head_marks: None,
                marks_loaded: futures::lock::Mutex::new(false),
                saving: futures::lock::Mutex::new(()),
            }),
        }
    }

    fn state(&self) -> std::sync::MutexGuard<'_, State> {
        self.inner.state.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// The compiled layout this store was opened with.
    pub fn layout(&self) -> &Arc<Compiled> {
        &self.inner.layout
    }

    /// How this store is reached.
    pub fn transport(&self) -> &Arc<dyn Transport> {
        &self.inner.transport
    }

    /// How this store names and protects chunks.
    pub fn sealer(&self) -> &Arc<dyn Sealer> {
        &self.inner.sealer
    }

    /// A handle on one branch. It does not check that the branch exists.
    pub fn branch(&self, name: &str) -> Branch {
        Branch {
            store: self.clone(),
            name: name.to_string(),
            ref_name: self.inner.sealer.ref_name(name),
        }
    }

    /// Where a branch was when this store last heard, without asking.
    pub fn known_head(&self, branch: &str) -> Option<CommitId> {
        self.state().high_water.get(branch).copied()
    }

    pub(crate) fn set_high_water(&self, branch: &str, id: CommitId) {
        self.state().high_water.insert(branch.to_string(), id);
    }

    /// Moves each branch's newest-seen commit to its rewritten version, and
    /// the head marks with them.
    pub(crate) async fn follow_rewrite(&self, mapped: &HashMap<CommitId, CommitId>) {
        let mut moved = Vec::new();
        {
            let mut st = self.state();
            for (name, seen) in st.high_water.iter_mut() {
                if let Some(next) = mapped.get(seen) {
                    *seen = *next;
                    moved.push(name.clone());
                }
            }
        }
        for name in moved {
            self.save_mark(&name).await;
        }
    }

    /// Reads the store's head marks into memory, once.
    ///
    /// A failed load fails the caller and is retried by the next one. Reading
    /// on without the marks would accept any head, which is the gap they
    /// close.
    pub(crate) async fn load_marks(&self) -> Result<()> {
        let Some(marks) = &self.inner.head_marks else {
            return Ok(());
        };
        let mut loaded = self.inner.marks_loaded.lock().await;
        if *loaded {
            return Ok(());
        }
        let found = marks.load().await.map_err(|e| {
            Error::new(
                e.kind(),
                format!("protovcs: loading the newest heads seen: {e}"),
            )
        })?;
        let mut st = self.state();
        for (branch, id) in found {
            st.stored.insert(branch.clone(), id);
            st.high_water.entry(branch).or_insert(id);
        }
        *loaded = true;
        Ok(())
    }

    /// Writes the mark held in memory for `branch` to the store's head marks.
    ///
    /// Best effort: a failed write leaves the mark in memory, and only a later
    /// store misses it. Another store sharing the record may have written a
    /// newer commit in the meantime. The record then keeps it and this store
    /// adopts it, so the record moves only to a descendant, or elsewhere after
    /// a forced move. When the two cannot be compared, the record is left as
    /// it is.
    pub(crate) async fn save_mark(&self, branch: &str) {
        let Some(marks) = &self.inner.head_marks else {
            return;
        };
        if self.load_marks().await.is_err() {
            return;
        }
        let _saving = self.inner.saving.lock().await;
        for _ in 0..SAVE_MARK_ATTEMPTS {
            let (want, old) = {
                let st = self.state();
                (
                    st.high_water.get(branch).copied().unwrap_or_default(),
                    st.stored.get(branch).copied().unwrap_or_default(),
                )
            };
            if want == old {
                return;
            }
            let Ok(now) = marks.compare_and_swap(branch, old, want).await else {
                return;
            };
            self.state().stored.insert(branch.to_string(), now);
            if now == want {
                return;
            }
            if now.is_zero() {
                // The record was cleared, as deleting the database does.
                continue;
            }
            match self.is_ancestor(now, want).await {
                Ok(true) => {
                    if self
                        .branch(branch)
                        .raise_high_water(Some(want), now)
                        .await
                        .is_err()
                    {
                        return;
                    }
                }
                Ok(false) => {}
                Err(_) => return,
            }
        }
    }

    async fn probe_at(&self, point: ProbePoint) -> Result<()> {
        match &self.inner.probe {
            Some(p) => p.at(point),
            None => Ok(()),
        }
    }

    /// Whether `candidate` is reachable from `start`. A commit missing from
    /// the walk is an error, not a no.
    pub(crate) async fn is_ancestor(&self, start: CommitId, candidate: CommitId) -> Result<bool> {
        let mut seen = HashSet::new();
        let mut queue = VecDeque::from([start]);
        let mut depth = 1u32;
        while let Some(id) = queue.pop_front() {
            if !seen.insert(id) {
                continue;
            }
            if id == candidate {
                return Ok(true);
            }
            let (commit, fetched) = self.history_commit(id, depth).await?;
            let Some(commit) = commit else {
                return Err(Error::new(
                    ErrorKind::NoSuchCommit,
                    format!("commit {id}, while walking history"),
                ));
            };
            if fetched {
                depth = (depth * 2).min(HISTORY_BATCH);
            }
            for raw in &commit.parents {
                queue.push_back(CommitId::from_bytes(raw)?);
            }
        }
        Ok(false)
    }

    async fn history_commit(&self, id: CommitId, depth: u32) -> Result<(Option<Commit>, bool)> {
        if let Some(c) = self.inner.memo.commit(&id) {
            return Ok((Some(c), false));
        }
        let commits = self.inner.transport.get_commits(id, depth).await?;
        for c in commits {
            let name = verified_name(&c)?;
            self.inner.memo.put_commit(name, c);
        }
        Ok((self.inner.memo.commit(&id), true))
    }

    /// The message as it was at one commit.
    ///
    /// # Errors
    ///
    /// [`ErrorKind::NoSuchCommit`] for a commit the store no longer holds, and
    /// whatever reading it fails with.
    pub async fn read_at(&self, at: CommitId) -> Result<DynamicMessage> {
        let commit = self.commit_by_id(at).await?;
        self.read_commit(&commit).await
    }

    /// One commit, from the memo when it has been seen.
    ///
    /// # Errors
    ///
    /// [`ErrorKind::NoSuchCommit`], or [`ErrorKind::Tampered`] when the
    /// server sends a different commit.
    pub async fn commit_by_id(&self, id: CommitId) -> Result<Commit> {
        if let Some(c) = self.inner.memo.commit(&id) {
            return Ok(c);
        }
        let mut commits = self.inner.transport.get_commits(id, 1).await?;
        if commits.is_empty() {
            return Err(Error::new(ErrorKind::NoSuchCommit, format!("commit {id}")));
        }
        let c = commits.swap_remove(0);
        let name = verified_name(&c)?;
        if name != id {
            return Err(Error::new(
                ErrorKind::Tampered,
                format!("asked for commit {id} and was sent {name}"),
            ));
        }
        self.inner.memo.put_commit(id, c.clone());
        Ok(c)
    }

    /// The whole message at one commit.
    ///
    /// # Errors
    ///
    /// When the commit cannot be opened or a chunk cannot be fetched.
    pub async fn read_commit(&self, commit: &Commit) -> Result<DynamicMessage> {
        let payload = self.open_payload(commit)?;
        let root = ChunkId::from_bytes(&payload.manifest_root)?;
        let nodes = Nodes(self);
        let refs = ManifestReader::new(root, &nodes).all().await?;
        let ids: Vec<ChunkId> = refs.iter().map(|r| r.id).collect();
        self.warm(&ids).await?;
        let mut plaintexts = Vec::with_capacity(refs.len());
        for r in &refs {
            plaintexts.push(self.chunk(r.id).await?);
        }
        let merged = self.inner.layout.merge_encoded(
            refs.iter()
                .zip(&plaintexts)
                .map(|(r, p)| (r.key.as_str(), p.as_slice())),
        )?;
        self.maybe_hint(commit).await;
        Ok(merged)
    }

    async fn maybe_hint(&self, commit: &Commit) {
        if self.inner.hint.is_empty() {
            return;
        }
        let Ok(id) = CommitId::from_bytes(&commit.id) else {
            return;
        };
        let previous = {
            let mut st = self.state();
            let head = st.high_water.get(&self.inner.hint).copied();
            if head != Some(id) || st.hinted == id {
                return;
            }
            let previous = st.hinted;
            st.hinted = id;
            previous
        };
        match self.reachable_chunks(commit).await {
            Ok(ids) => {
                let _ = self.inner.transport.pack_hint(id, &ids).await;
            }
            Err(_) => {
                let mut st = self.state();
                if st.hinted == id {
                    st.hinted = previous;
                }
            }
        }
    }

    /// Opens a commit's payload, refusing a schema this build will not read.
    pub(crate) fn open_payload(&self, commit: &Commit) -> Result<CommitPayload> {
        let l = self.inner.layout.layout();
        if l.min_readable_version != 0 && commit.schema_version < l.min_readable_version {
            return Err(Error::new(
                ErrorKind::UnsupportedSchema,
                format!(
                    "commit is schema {}, this build reads {} and above",
                    commit.schema_version, l.min_readable_version
                ),
            ));
        }
        if commit.schema_version > self.inner.layout.schema_version() {
            return Err(Error::new(
                ErrorKind::UnsupportedSchema,
                format!(
                    "commit is schema {}, this build knows {}",
                    commit.schema_version,
                    self.inner.layout.schema_version()
                ),
            ));
        }
        self.open_payload_unchecked(commit)
    }

    /// Opens a commit's payload without asking about its schema.
    pub(crate) fn open_payload_unchecked(&self, commit: &Commit) -> Result<CommitPayload> {
        if commit.format_version != 0 && commit.format_version != u32::from(FORMAT_VERSION) {
            return Err(Error::new(
                ErrorKind::UnsupportedFormat,
                format!("commit format {}", commit.format_version),
            ));
        }
        let id = CommitId::from_bytes(&commit.id)?;
        let plaintext = self
            .inner
            .sealer
            .open(&ChunkId::from(id), &commit.payload)?;
        CommitPayload::decode_from_slice(&plaintext).map_err(|e| {
            Error::invalid(format!(
                "protovcs: commit {id} has an unreadable payload: {e}"
            ))
        })
    }

    /// A chunk's plaintext, fetching it when it is not held.
    pub(crate) async fn chunk(&self, id: ChunkId) -> Result<Arc<Vec<u8>>> {
        if let Some(p) = self.inner.memo.chunk(&id) {
            return Ok(p);
        }
        self.warm(&[id]).await?;
        self.inner
            .memo
            .chunk(&id)
            .ok_or_else(|| Error::new(ErrorKind::NoSuchChunk, id.to_string()))
    }

    /// Fetches and opens every chunk not already held, in one request.
    pub(crate) async fn warm(&self, ids: &[ChunkId]) -> Result<()> {
        let store = self.clone();
        self.inner
            .memo
            .warm(ids, move |want| {
                let store = store.clone();
                Box::pin(async move { store.fetch_chunks(&want).await })
            })
            .await
    }

    /// Opens every chunk in `want` that the cache or the server has.
    async fn fetch_chunks(&self, want: &[ChunkId]) -> Result<HashMap<ChunkId, Vec<u8>>> {
        let mut opened = HashMap::with_capacity(want.len());
        let mut missing = want.to_vec();
        if let Some(cache) = &self.inner.chunk_cache {
            let held = cache.get(want).await.unwrap_or_default();
            let mut bad = Vec::new();
            for c in held {
                match self.inner.sealer.open(&c.id, &c.sealed) {
                    Ok(p) => {
                        opened.insert(c.id, p);
                    }
                    Err(_) => bad.push(c.id),
                }
            }
            if !bad.is_empty() {
                let _ = cache.drop_chunks(&bad).await;
            }
            missing.retain(|id| !opened.contains_key(id));
            if missing.is_empty() {
                return Ok(opened);
            }
        }
        let fetched = self.inner.transport.get_chunks(&missing).await?;
        for c in &fetched {
            opened.insert(c.id, self.inner.sealer.open(&c.id, &c.sealed)?);
        }
        self.keep_chunks(&fetched).await;
        Ok(opened)
    }

    async fn keep_chunks(&self, chunks: &[SealedChunk]) {
        if let Some(cache) = &self.inner.chunk_cache
            && !chunks.is_empty()
        {
            let _ = cache.put(chunks).await;
        }
    }

    pub(crate) fn commit_message(&self, meta: &CommitMeta) -> Result<CommitMessage> {
        let details = match &meta.details {
            None => Vec::new(),
            Some(d) => {
                let layout = &self.inner.layout;
                let got = d.message_descriptor().full_name();
                let Some(want) = layout.commit_message() else {
                    return Err(Error::invalid(format!(
                        "protovcs: a commit carries {got} and layout {:?} declares no commit_message",
                        layout.layout().name
                    )));
                };
                let want = layout.pool().message(want).full_name();
                if got != want {
                    return Err(Error::invalid(format!(
                        "protovcs: a commit carries {got} and layout {:?} declares commit_message {want}",
                        layout.layout().name
                    )));
                }
                canonical_bytes(d)?
            }
        };
        Ok(CommitMessage {
            title: meta.title.clone(),
            description: meta.description.clone(),
            authors: meta
                .authors
                .iter()
                .map(|a| AuthorProto {
                    name: a.name.clone(),
                    email: a.email.clone(),
                    role: a.role.clone(),
                    ..Default::default()
                })
                .collect(),
            client: meta.client.clone(),
            labels: meta.labels.clone().into_iter().collect(),
            details,
            ..Default::default()
        })
    }

    pub(crate) fn now(&self) -> Timestamp {
        (self.inner.clock)()
    }

    /// Uploads what a version adds and records the commit, without moving any
    /// branch.
    pub(crate) async fn record_version(
        &self,
        spec: VersionSpec,
        parents: &[Commit],
        nodes: &[ManifestChunk],
        root: ChunkId,
        pending: &mut HashMap<ChunkId, Vec<u8>>,
    ) -> Result<CommitId> {
        let (schema_id, schema_bytes) = self.schema_chunk()?;
        pending.insert(schema_id, schema_bytes);

        let added = self.added_since(nodes, root, parents, schema_id).await?;
        let missing = self.inner.transport.has_chunks(&added).await?;
        let mut batch = Vec::with_capacity(missing.len());
        let mut upload_bytes = 0u64;
        for id in missing {
            let plaintext = pending.get(&id).ok_or_else(|| {
                Error::invalid(format!("protovcs: chunk {id} was added and not built"))
            })?;
            let sealed = self.inner.sealer.seal(&id, plaintext)?;
            upload_bytes += sealed.len() as u64;
            batch.push(SealedChunk { id, sealed });
        }
        let reservation = self
            .inner
            .transport
            .begin_write(batch.len() as u32, upload_bytes)
            .await?;
        if !batch.is_empty() {
            self.inner
                .transport
                .put_chunks(&reservation, &batch)
                .await?;
            self.keep_chunks(&batch).await;
        }
        self.probe_at(ProbePoint::AfterUpload).await?;

        let payload = CommitPayload {
            manifest_root: root.to_vec(),
            schema_chunk: schema_id.to_vec(),
            tree_digest: node_digest(&root),
            branch: spec.branch,
            message: spec.message.into(),
            time: spec.at.into(),
            ..Default::default()
        };
        let payload_bytes = canonical_own(&payload)?;

        let mut nonce = vec![0u8; 16];
        (self.inner.entropy)(&mut nonce)?;
        let mut commit = Commit {
            format_version: u32::from(FORMAT_VERSION),
            schema_version: self.inner.layout.schema_version(),
            added: added.iter().map(ChunkId::to_vec).collect(),
            naming_nonce: nonce,
            parents: parents.iter().map(|p| p.id.clone()).collect(),
            ..Default::default()
        };
        let id = commit_name(&commit)?;
        commit.payload = self.inner.sealer.seal(&ChunkId::from(id), &payload_bytes)?;
        commit.id = id.to_vec();

        let stored = self
            .inner
            .transport
            .put_commit(&commit, &reservation)
            .await?;
        if stored != id {
            return Err(Error::new(
                ErrorKind::Tampered,
                format!("wrote commit {id} and the server recorded {stored}"),
            ));
        }
        self.inner.memo.put_commit(id, commit);
        Ok(id)
    }

    async fn added_since(
        &self,
        nodes: &[ManifestChunk],
        root: ChunkId,
        parents: &[Commit],
        schema_id: ChunkId,
    ) -> Result<Vec<ChunkId>> {
        let (mut old_root, mut old_schema) = (ChunkId::default(), ChunkId::default());
        if let Some(p) = parents.first() {
            let payload = self.open_payload(p)?;
            old_root = ChunkId::from_bytes(&payload.manifest_root)?;
            old_schema = ChunkId::from_bytes(&payload.schema_chunk)?;
        }
        let mut added = added_since(nodes, root, old_root, &Nodes(self)).await?;
        if schema_id != old_schema {
            added.push(schema_id);
        }
        Ok(added)
    }

    /// The schema chunk this store writes under, and its name.
    ///
    /// # Errors
    ///
    /// When the layout's root file is missing from its pool.
    pub fn schema_chunk(&self) -> Result<(ChunkId, Vec<u8>)> {
        let layout = &self.inner.layout;
        let set = match &self.inner.descriptors {
            Some(s) => s.clone(),
            None => descriptor_set_for(layout)?,
        };
        let schema = crate::proto::SchemaChunk {
            layout: layout.layout().clone().into(),
            descriptors: set.into(),
            root_message: layout.root_name().to_string(),
            schema_version: layout.schema_version(),
            canonical_version: crate::types::CANONICAL_VERSION,
            ..Default::default()
        };
        let canonical = canonical_own(&schema)?;
        Ok((
            self.inner.sealer.chunk_id(Role::Schema, "", &canonical),
            canonical,
        ))
    }

    /// The chunk naming a commit's manifest.
    ///
    /// # Errors
    ///
    /// When the payload cannot be opened.
    pub fn manifest_root_of(&self, commit: &Commit) -> Result<ChunkId> {
        ChunkId::from_bytes(&self.open_payload(commit)?.manifest_root)
    }

    /// Every chunk a version needs: its root, its schema, its other manifest
    /// nodes in walk order, and its items in key order.
    ///
    /// # Errors
    ///
    /// When the payload cannot be opened or a node cannot be fetched.
    pub async fn reachable_chunks(&self, commit: &Commit) -> Result<Vec<ChunkId>> {
        let payload = self.open_payload(commit)?;
        let root = ChunkId::from_bytes(&payload.manifest_root)?;
        let schema = ChunkId::from_bytes(&payload.schema_chunk)?;
        let nodes = Nodes(self);
        let (walked, items) = ManifestReader::new(root, &nodes).nodes_and_items().await?;
        let mut out = vec![root, schema];
        out.extend(walked.into_iter().filter(|id| *id != root));
        out.extend(items.into_iter().map(|i| i.id));
        Ok(out)
    }

    /// What the server is holding for this store.
    ///
    /// # Errors
    ///
    /// [`ErrorKind::NoUsage`] when the transport cannot say.
    pub async fn usage(&self) -> Result<Usage> {
        self.inner.transport.usage().await
    }

    /// History from a commit, newest first, following first parents.
    ///
    /// # Errors
    ///
    /// When a commit cannot be fetched or opened, or the history has a cycle.
    pub async fn log_from(&self, start: &Commit, opts: &LogOptions) -> Result<Vec<CommitInfo>> {
        let limit = if opts.limit == 0 {
            DEFAULT_LOG_LIMIT
        } else {
            opts.limit
        };
        let mut out = Vec::new();
        let mut commit = start.clone();
        let mut seen = HashSet::new();
        while out.len() < limit {
            let id = CommitId::from_bytes(&commit.id)?;
            if !opts.until.is_zero() && id == opts.until {
                break;
            }
            if !seen.insert(id) {
                return Err(Error::invalid(format!(
                    "protovcs: commit {id} appears twice in its own history"
                )));
            }
            let info = self.commit_details(&commit)?;
            let parent = info.parents.first().copied();
            out.push(info);
            let Some(parent) = parent else { break };
            commit = self.commit_by_id(parent).await?;
        }
        Ok(out)
    }

    /// One commit's entry, opened.
    ///
    /// # Errors
    ///
    /// When the payload cannot be opened.
    pub fn commit_details(&self, commit: &Commit) -> Result<CommitInfo> {
        let id = CommitId::from_bytes(&commit.id)?;
        let payload = self.open_payload(commit)?;
        let msg = payload.message.as_option().cloned().unwrap_or_default();
        Ok(CommitInfo {
            id,
            parents: commit
                .parents
                .iter()
                .map(|p| CommitId::from_bytes(p))
                .collect::<Result<_>>()?,
            title: msg.title,
            description: msg.description,
            authors: msg
                .authors
                .into_iter()
                .map(|a| Author {
                    name: a.name,
                    email: a.email,
                    role: a.role,
                })
                .collect(),
            client: msg.client,
            labels: msg.labels.into_iter().collect(),
            time: payload.time.as_option().cloned(),
            branch: payload.branch,
            schema_version: commit.schema_version,
            details: msg.details,
        })
    }

    async fn backoff(&self, attempt: usize) {
        let sleep = &self.inner.sleep;
        let window = RETRY_BASE
            .checked_mul(1 << (attempt - 1).min(20))
            .unwrap_or(RETRY_CAP)
            .min(RETRY_CAP);
        let mut b = [0u8; 8];
        let draw = if (self.inner.entropy)(&mut b).is_ok() {
            u64::from_le_bytes(b) % (window.as_nanos() as u64).max(1)
        } else {
            0
        };
        sleep(Duration::from_nanos(draw) + RETRY_BASE).await;
    }
}

/// The root message's file and everything it imports, imports first.
fn descriptor_set_for(layout: &Compiled) -> Result<FileDescriptorSet> {
    let pool = layout.pool();
    let root_file = pool
        .file_containing_symbol(layout.root_name())
        .and_then(|f| f.name.clone())
        .ok_or_else(|| {
            Error::invalid(format!(
                "protovcs: the pool has no file defining {}",
                layout.root_name()
            ))
        })?;
    let mut set = FileDescriptorSet::default();
    let mut seen = HashSet::new();
    fn add(
        pool: &crate::DescriptorPool,
        name: &str,
        seen: &mut HashSet<String>,
        set: &mut FileDescriptorSet,
    ) {
        if !seen.insert(name.to_string()) {
            return;
        }
        let Some(fd) = pool.file_by_name(name) else {
            return;
        };
        for dep in &fd.dependency {
            add(pool, dep, seen, set);
        }
        // Source locations are not part of a schema. The Go client's
        // descriptors never carry them, so neither does this.
        let mut fd = fd.clone();
        fd.source_code_info = Default::default();
        set.file.push(fd);
    }
    add(pool, &root_file, &mut seen, &mut set);
    Ok(set)
}

/// Plaintext waiting to be uploaded, by chunk name.
pub(crate) type Pending = HashMap<ChunkId, Vec<u8>>;

/// One line of history.
#[derive(Clone)]
pub struct Branch {
    pub(crate) store: Store,
    pub(crate) name: String,
    pub(crate) ref_name: Vec<u8>,
}

impl Branch {
    /// The branch's readable name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// The store this branch is in.
    pub fn store(&self) -> &Store {
        &self.store
    }

    /// The commit this branch points at.
    ///
    /// # Errors
    ///
    /// [`ErrorKind::NoSuchRef`] when the branch does not exist, and
    /// [`ErrorKind::WentBackwards`] for a head older than one this store has
    /// already returned.
    pub async fn head(&self) -> Result<(Commit, RefState)> {
        let s = &self.store;
        s.load_marks().await?;
        let refs = s.inner.transport.list_refs().await?;
        let state = refs
            .into_iter()
            .find(|r| r.name == self.ref_name)
            .ok_or_else(|| Error::new(ErrorKind::NoSuchRef, self.name.clone()))?;
        let seen = s.known_head(&self.name);
        let head = s.commit_by_id(state.commit).await?;
        if let Some(seen) = seen
            && seen != state.commit
        {
            let ahead = s.is_ancestor(state.commit, seen).await?;
            let behind = !ahead && s.is_ancestor(seen, state.commit).await?;
            if behind {
                return Err(Error::new(
                    ErrorKind::WentBackwards,
                    format!("{} is behind {seen} on branch {}", state.commit, self.name),
                ));
            }
        }
        self.raise_high_water(seen, state.commit).await?;
        s.save_mark(&self.name).await;
        s.probe_at(ProbePoint::AfterHead).await?;
        Ok((head, state))
    }

    /// Like [`Branch::head`], with a branch that does not exist read as none.
    pub(crate) async fn head_or_none(&self) -> Result<(Option<Commit>, RefState)> {
        match self.head().await {
            Ok((c, s)) => Ok((Some(c), s)),
            Err(e) if e.is(ErrorKind::NoSuchRef) => Ok((None, RefState::default())),
            Err(e) => Err(e),
        }
    }

    async fn raise_high_water(&self, mut seen: Option<CommitId>, head: CommitId) -> Result<()> {
        loop {
            let now = {
                let mut st = self.store.state();
                let now = st.high_water.get(&self.name).copied();
                if now == seen {
                    st.high_water.insert(self.name.clone(), head);
                    return Ok(());
                }
                now
            };
            let Some(now_id) = now else { return Ok(()) };
            if !self.store.is_ancestor(head, now_id).await? {
                return Ok(());
            }
            seen = now;
        }
    }

    /// Calls `moved` every time this branch points somewhere new, until
    /// `moved` fails or the watch breaks. A deleted branch is reported as a
    /// zero state.
    ///
    /// # Errors
    ///
    /// What `moved` returned, or the transport's failure.
    pub async fn watch(&self, mut moved: impl FnMut(RefState) -> Result<()> + Send) -> Result<()> {
        let mut last: Option<RefState> = None;
        let ref_name = self.ref_name.clone();
        self.store
            .inner
            .transport
            .watch_refs(Box::new(move |refs: Vec<RefState>| {
                let now = refs.into_iter().find(|r| r.name == ref_name);
                match now {
                    None => {
                        // Reported once: a branch already reported deleted has
                        // not moved again just because some other branch did.
                        if last.as_ref().is_none_or(|l| l.commit.is_zero()) {
                            return Ok(());
                        }
                        let gone = RefState {
                            name: ref_name.clone(),
                            ..Default::default()
                        };
                        last = Some(gone.clone());
                        moved(gone)
                    }
                    Some(now) => {
                        if let Some(l) = &last
                            && l.commit == now.commit
                            && l.cas_token == now.cas_token
                        {
                            return Ok(());
                        }
                        last = Some(now.clone());
                        moved(now)
                    }
                }
            }))
            .await
    }

    /// The whole message at this branch's head; empty when the branch does
    /// not exist.
    ///
    /// # Errors
    ///
    /// When the head cannot be read.
    pub async fn read(&self) -> Result<DynamicMessage> {
        match self.head_or_none().await? {
            (Some(head), _) => self.store.read_commit(&head).await,
            (None, _) => Ok(self.store.layout().new_message()),
        }
    }

    /// Reads the branch, runs `mutate`, and publishes the result, rebuilding
    /// and retrying when another writer commits first. `mutate` may run more
    /// than once.
    ///
    /// # Errors
    ///
    /// What `mutate` returned, or the last lost race after
    /// [`DEFAULT_MAX_ATTEMPTS`].
    pub async fn change(
        &self,
        meta: &CommitMeta,
        mut mutate: impl FnMut(&mut DynamicMessage) -> Result<()>,
    ) -> Result<CommitId> {
        let mut last_err = None;
        for attempt in 0..DEFAULT_MAX_ATTEMPTS {
            if attempt > 0 {
                self.store.backoff(attempt).await;
            }
            let (head, state) = self.head_or_none().await?;
            let mut current = match &head {
                Some(h) => self.store.read_commit(h).await?,
                None => self.store.layout().new_message(),
            };
            mutate(&mut current)?;
            match self.publish(meta, head.as_ref(), &state, &current).await {
                Ok(id) => return Ok(id),
                Err(e) if e.is(ErrorKind::RaceLost) => last_err = Some(e),
                Err(e) => return Err(e),
            }
        }
        Err(Error::new(
            ErrorKind::RaceLost,
            format!(
                "gave up after {DEFAULT_MAX_ATTEMPTS} attempts on branch {}: {}",
                self.name,
                last_err.map(|e| e.to_string()).unwrap_or_default()
            ),
        ))
    }

    /// Publishes a message as the branch's next version, failing with
    /// [`ErrorKind::RaceLost`] rather than retrying.
    ///
    /// # Errors
    ///
    /// When the message does not materialize, an upload fails, or the race
    /// is lost.
    pub async fn write(&self, meta: &CommitMeta, m: &DynamicMessage) -> Result<CommitId> {
        let (head, state) = self.head_or_none().await?;
        self.publish(meta, head.as_ref(), &state, m).await
    }

    async fn publish(
        &self,
        meta: &CommitMeta,
        parent: Option<&Commit>,
        state: &RefState,
        m: &DynamicMessage,
    ) -> Result<CommitId> {
        let (refs, mut pending) = self.store.item_refs(m)?;
        let parents: Vec<Commit> = parent.into_iter().cloned().collect();
        self.publish_refs(
            meta,
            &parents,
            state,
            &refs,
            &mut pending,
            RefMove::FastForward,
        )
        .await
    }

    pub(crate) async fn publish_refs(
        &self,
        meta: &CommitMeta,
        parents: &[Commit],
        state: &RefState,
        refs: &[ItemRef],
        pending: &mut HashMap<ChunkId, Vec<u8>>,
        mode: RefMove,
    ) -> Result<CommitId> {
        let layout = self.store.layout();
        for r in refs {
            layout.check_item_size(&r.key, r.bytes, r.elements)?;
        }
        let nodes = build_manifest(self.store.sealer().as_ref(), refs)?;
        for n in &nodes {
            pending.insert(n.id, n.canonical.clone());
        }
        let root = nodes
            .last()
            .map(|n| n.id)
            .expect("build_manifest always returns the root node");
        self.publish_built(meta, parents, state, &nodes, root, pending, mode)
            .await
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn publish_built(
        &self,
        meta: &CommitMeta,
        parents: &[Commit],
        state: &RefState,
        nodes: &[ManifestChunk],
        root: ChunkId,
        pending: &mut HashMap<ChunkId, Vec<u8>>,
        mode: RefMove,
    ) -> Result<CommitId> {
        let s = &self.store;
        let message = s.commit_message(meta)?;
        let stored = s
            .record_version(
                VersionSpec {
                    branch: self.name.clone(),
                    message,
                    at: s.now(),
                },
                parents,
                nodes,
                root,
                pending,
            )
            .await?;
        s.probe_at(ProbePoint::AfterCommit).await?;
        s.probe_at(ProbePoint::BeforeRefUpdate).await?;
        s.inner
            .transport
            .update_ref(&self.ref_name, stored, &state.cas_token, mode)
            .await?;
        s.inner.memo.put_chunks(std::mem::take(pending));
        s.set_high_water(&self.name, stored);
        s.save_mark(&self.name).await;
        Ok(stored)
    }

    /// This branch's history, newest first, first parents only. Empty when the
    /// branch was never written.
    ///
    /// # Errors
    ///
    /// When a commit cannot be fetched or opened.
    pub async fn log(&self, opts: &LogOptions) -> Result<Vec<CommitInfo>> {
        match self.head_or_none().await? {
            (Some(head), _) => self.store.log_from(&head, opts).await,
            (None, _) => Ok(Vec::new()),
        }
    }
}

impl Store {
    /// Materializes a message into the items a version names, and the
    /// plaintext of each by chunk id.
    pub(crate) fn item_refs(&self, m: &DynamicMessage) -> Result<(Vec<ItemRef>, Pending)> {
        let layout = self.layout();
        let items = layout.materialize(m)?;
        let mut pending = HashMap::new();
        let mut refs = Vec::with_capacity(items.len());
        for it in items {
            let (r, canonical) = self.item_ref(&it.key, &it.fragment)?;
            pending.insert(r.id, canonical);
            refs.push(r);
        }
        Ok((refs, pending))
    }

    /// The reference and canonical bytes of one item.
    pub(crate) fn item_ref(
        &self,
        key: &ItemKey,
        frag: &DynamicMessage,
    ) -> Result<(ItemRef, Vec<u8>)> {
        let canonical = canonical_bytes(frag)
            .map_err(|e| Error::invalid(format!("protovcs: encoding item {key:?}: {e}")))?;
        let id = self.sealer().chunk_id(Role::Item, key, &canonical);
        Ok((
            ItemRef {
                key: key.clone(),
                id,
                bytes: canonical.len() as u64,
                elements: self.layout().element_count(frag),
            },
            canonical,
        ))
    }
}
