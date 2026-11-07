//! Keeps protovcs chunks in the browser's IndexedDB, so a page load does not
//! download again what an earlier one did.
//!
//! It implements [`protovcs::ChunkCache`] in a browser and compiles to nothing
//! anywhere else. It is the Rust counterpart of the Go package `idbcache`,
//! and the two read and write the same database, so a page that moves from the
//! Go module to the Rust one keeps its cache.
//!
//! # Layout
//!
//! One database, [`DATABASE_NAME`], at version 2, with three object stores:
//!
//! - `"chunks"`, keyed by the chunk id as hex. Each value is
//!   `{s: sealed bytes, n: their length, u: last used, in Unix milliseconds}`,
//!   with an index `"used"` on `u`.
//! - `"meta"`, holding `"total"`: the bytes of sealed chunks held. Every write
//!   that adds or removes a chunk updates it in the same transaction.
//! - `"heads"`, keyed by scope, a NUL byte, and branch name. Each value is the
//!   hex id of the newest commit seen on that branch. Added in version 2; see
//!   [`Cache::head_marks`].
//!
//! A page clears it, on sign-out for example, by deleting the database by
//! name, which clears the head marks with the chunks.
//!
//! Chained requests stay inside one transaction because each `await` resumes
//! in a microtask of the request's success event, while the transaction is
//! still active.

#![cfg(target_arch = "wasm32")]
#![deny(missing_docs)]

use std::cell::RefCell;
use std::rc::Rc;

use futures::FutureExt;
use futures::channel::oneshot;
use js_sys::{Array, Object, Reflect, Uint8Array};
use std::collections::HashMap;

use protovcs::{BoxFuture, ChunkCache, ChunkId, CommitId, Error, HeadMarks, Result, SealedChunk};
use wasm_bindgen::JsCast;
use wasm_bindgen::prelude::*;
use web_sys::{
    IdbCursorWithValue, IdbDatabase, IdbFactory, IdbIndexParameters, IdbKeyRange, IdbObjectStore,
    IdbOpenDbRequest, IdbRequest, IdbTransaction, IdbTransactionMode, IdbVersionChangeEvent,
};

/// The IndexedDB database the cache lives in.
pub const DATABASE_NAME: &str = "protovcs-chunks";

/// How many bytes of sealed chunks a browser keeps before the least recently
/// used are evicted.
pub const DEFAULT_CAP_BYTES: u64 = 100 << 20;

const SCHEMA_VERSION: u32 = 2;
const CHUNK_STORE: &str = "chunks";
const META_STORE: &str = "meta";
const HEAD_STORE: &str = "heads";
const USED_INDEX: &str = "used";
const TOTAL_KEY: &str = "total";

/// How stale a chunk's last-used time may be before a read moves it to now.
const TOUCH_AFTER_MS: f64 = 3_600_000.0;

/// How long a read waits on the cache before treating it as a miss.
const READ_TIMEOUT_MS: i32 = 2_000;

/// How long a background write may take before it is abandoned.
const WRITE_TIMEOUT_MS: i32 = 30_000;

/// How long loading or recording a head mark waits on IndexedDB. A failed load
/// fails the read that needed it, so this is longer than a chunk read's.
const MARK_TIMEOUT_MS: i32 = 10_000;

/// The browser's chunk cache, shared by every store the page opens.
#[derive(Clone)]
pub struct Cache {
    inner: Rc<Inner>,
}

struct Inner {
    name: String,
    cap_bytes: u64,
    /// The open database, or none once another connection asked to delete or
    /// upgrade it, so a deletion in another tab never waits on this one.
    db: RefCell<Option<IdbDatabase>>,
}

fn js_err(what: &str, e: JsValue) -> Error {
    let msg = e
        .dyn_ref::<web_sys::DomException>()
        .map(|d| format!("{}: {}", d.name(), d.message()))
        .or_else(|| e.as_string())
        .unwrap_or_else(|| "the browser gave no reason".into());
    Error::invalid(format!("idbcache: {what}: {msg}"))
}

fn factory() -> Option<IdbFactory> {
    Reflect::get(&js_sys::global(), &JsValue::from_str("indexedDB"))
        .ok()
        .and_then(|v| v.dyn_into::<IdbFactory>().ok())
}

/// Resolves when a request succeeds, or fails with its error.
async fn request(what: &str, req: &IdbRequest) -> Result<JsValue> {
    let (tx, rx) = oneshot::channel::<std::result::Result<JsValue, JsValue>>();
    let tx = Rc::new(RefCell::new(Some(tx)));
    let (ok_tx, err_tx) = (Rc::clone(&tx), tx);
    let ok_req = req.clone();
    let on_ok = Closure::once(move |_: JsValue| {
        if let Some(t) = ok_tx.borrow_mut().take() {
            let _ = t.send(ok_req.result());
        }
    });
    let err_req = req.clone();
    let on_err = Closure::once(move |_: JsValue| {
        if let Some(t) = err_tx.borrow_mut().take() {
            let e = err_req
                .error()
                .ok()
                .flatten()
                .map(JsValue::from)
                .unwrap_or(JsValue::NULL);
            let _ = t.send(Err(e));
        }
    });
    req.set_onsuccess(Some(on_ok.as_ref().unchecked_ref()));
    req.set_onerror(Some(on_err.as_ref().unchecked_ref()));
    let got = rx.await;
    req.set_onsuccess(None);
    req.set_onerror(None);
    drop((on_ok, on_err));
    match got {
        Ok(Ok(v)) => Ok(v),
        Ok(Err(e)) => Err(js_err(what, e)),
        Err(_) => Err(Error::invalid(format!("idbcache: {what}: abandoned"))),
    }
}

/// Resolves when a transaction commits, or fails when it aborts.
async fn complete(what: &str, tx: &IdbTransaction) -> Result<()> {
    let (send, rx) = oneshot::channel::<std::result::Result<(), JsValue>>();
    let send = Rc::new(RefCell::new(Some(send)));
    let (ok, bad) = (Rc::clone(&send), send);
    let on_ok = Closure::once(move |_: JsValue| {
        if let Some(t) = ok.borrow_mut().take() {
            let _ = t.send(Ok(()));
        }
    });
    let err_tx = tx.clone();
    let on_err = Closure::<dyn FnMut(JsValue)>::new(move |_: JsValue| {
        if let Some(t) = bad.borrow_mut().take() {
            let e = err_tx.error().map(JsValue::from).unwrap_or(JsValue::NULL);
            let _ = t.send(Err(e));
        }
    });
    tx.set_oncomplete(Some(on_ok.as_ref().unchecked_ref()));
    tx.set_onerror(Some(on_err.as_ref().unchecked_ref()));
    tx.set_onabort(Some(on_err.as_ref().unchecked_ref()));
    let got = rx.await;
    drop((on_ok, on_err));
    match got {
        Ok(Ok(())) => Ok(()),
        Ok(Err(e)) => Err(js_err(what, e)),
        Err(_) => Err(Error::invalid(format!("idbcache: {what}: abandoned"))),
    }
}

/// Resolves after `ms` milliseconds.
async fn sleep(ms: i32) {
    let promise = js_sys::Promise::new(&mut |resolve, _| {
        let set_timeout = Reflect::get(&js_sys::global(), &JsValue::from_str("setTimeout"))
            .ok()
            .and_then(|f| f.dyn_into::<js_sys::Function>().ok());
        if let Some(f) = set_timeout {
            let _ = f.call2(&JsValue::NULL, &resolve, &JsValue::from(ms));
        }
    });
    let _ = wasm_bindgen_futures::JsFuture::from(promise).await;
}

/// Runs `fut`, giving up after `ms` milliseconds. Giving up does not stop the
/// browser finishing the operation.
async fn within<T>(
    what: &str,
    ms: i32,
    fut: impl std::future::Future<Output = Result<T>>,
) -> Result<T> {
    futures::select! {
        r = fut.fuse() => r,
        () = sleep(ms).fuse() => Err(Error::invalid(format!("idbcache: {what}: timed out"))),
    }
}

fn key(id: &ChunkId) -> JsValue {
    JsValue::from_str(&id.to_string())
}

fn number(v: &JsValue) -> f64 {
    v.as_f64().unwrap_or(0.0)
}

fn now_ms() -> f64 {
    js_sys::Date::now()
}

impl Cache {
    /// A cache over the database called `name`, keeping at most `cap_bytes`
    /// of sealed chunks.
    ///
    /// It does not touch the database: the connection is made on first use.
    ///
    /// # Errors
    ///
    /// When the browser has no IndexedDB, as some private windows do. A page
    /// then runs without a cache.
    pub fn open(name: &str, cap_bytes: u64) -> Result<Self> {
        if factory().is_none() {
            return Err(Error::invalid("idbcache: this browser has no IndexedDB"));
        }
        Ok(Self {
            inner: Rc::new(Inner {
                name: name.to_string(),
                cap_bytes,
                db: RefCell::new(None),
            }),
        })
    }

    /// Deletes the database called `name`, waiting until every other
    /// connection to it has closed.
    ///
    /// # Errors
    ///
    /// When the browser refuses.
    pub async fn delete_database(name: &str) -> Result<()> {
        let f =
            factory().ok_or_else(|| Error::invalid("idbcache: this browser has no IndexedDB"))?;
        let req = f.delete_database(name).map_err(|e| js_err("deleting", e))?;
        request(&format!("deleting {name}"), &req).await.map(|_| ())
    }

    /// The open database, opening it when it is not.
    async fn connect(&self) -> Result<IdbDatabase> {
        if let Some(db) = self.inner.db.borrow().clone() {
            return Ok(db);
        }
        let f =
            factory().ok_or_else(|| Error::invalid("idbcache: this browser has no IndexedDB"))?;
        let what = format!("opening {}", self.inner.name);
        let open: IdbOpenDbRequest = f
            .open_with_u32(&self.inner.name, SCHEMA_VERSION)
            .map_err(|e| js_err(&what, e))?;
        let upgrade_req = open.clone();
        let on_upgrade = Closure::<dyn FnMut(JsValue)>::new(move |event: JsValue| {
            let Ok(db) = upgrade_req
                .result()
                .and_then(|r| r.dyn_into::<IdbDatabase>())
            else {
                return;
            };
            let from = event
                .dyn_ref::<IdbVersionChangeEvent>()
                .map_or(0.0, IdbVersionChangeEvent::old_version);
            if from < 1.0 {
                if let Ok(chunks) = db.create_object_store(CHUNK_STORE) {
                    let _ = chunks.create_index_with_str_and_optional_parameters(
                        USED_INDEX,
                        "u",
                        &IdbIndexParameters::new(),
                    );
                }
                let _ = db.create_object_store(META_STORE);
            }
            if from < 2.0 {
                let _ = db.create_object_store(HEAD_STORE);
            }
        });
        open.set_onupgradeneeded(Some(on_upgrade.as_ref().unchecked_ref()));
        let got = request(&what, &open).await;
        open.set_onupgradeneeded(None);
        drop(on_upgrade);
        let db: IdbDatabase = got?.dyn_into().map_err(|e| js_err(&what, e))?;

        // Kept for the life of the connection.
        let weak = Rc::downgrade(&self.inner);
        let closing = db.clone();
        let on_version_change = Closure::<dyn FnMut(JsValue)>::new(move |_: JsValue| {
            closing.close();
            if let Some(inner) = weak.upgrade() {
                let mut held = inner.db.borrow_mut();
                if held
                    .as_ref()
                    .is_some_and(|d| JsValue::from(d) == JsValue::from(&closing))
                {
                    *held = None;
                }
            }
        });
        db.set_onversionchange(Some(on_version_change.as_ref().unchecked_ref()));
        on_version_change.forget();

        let mut held = self.inner.db.borrow_mut();
        if let Some(other) = held.clone() {
            db.close();
            return Ok(other);
        }
        *held = Some(db.clone());
        Ok(db)
    }

    async fn transaction(
        &self,
        mode: IdbTransactionMode,
    ) -> Result<(IdbTransaction, IdbObjectStore, IdbObjectStore)> {
        let db = self.connect().await?;
        let names = Array::of2(
            &JsValue::from_str(CHUNK_STORE),
            &JsValue::from_str(META_STORE),
        );
        let tx = db
            .transaction_with_str_sequence_and_mode(&names, mode)
            .map_err(|e| {
                // A connection closed since it was read throws here.
                *self.inner.db.borrow_mut() = None;
                js_err("starting a transaction", e)
            })?;
        let chunks = tx
            .object_store(CHUNK_STORE)
            .map_err(|e| js_err("chunks", e))?;
        let meta = tx.object_store(META_STORE).map_err(|e| js_err("meta", e))?;
        Ok((tx, chunks, meta))
    }

    /// A transaction over the heads object store alone.
    async fn heads_transaction(
        &self,
        mode: IdbTransactionMode,
    ) -> Result<(IdbTransaction, IdbObjectStore)> {
        let db = self.connect().await?;
        let tx = db
            .transaction_with_str_and_mode(HEAD_STORE, mode)
            .map_err(|e| {
                *self.inner.db.borrow_mut() = None;
                js_err("starting a transaction", e)
            })?;
        let heads = tx
            .object_store(HEAD_STORE)
            .map_err(|e| js_err("heads", e))?;
        Ok((tx, heads))
    }

    /// The [`HeadMarks`] of one store, kept in this cache's database under
    /// `scope`.
    ///
    /// `scope` names the store. Branch names repeat across stores, so each
    /// store needs a scope of its own. Each record holds a commit id, which
    /// the server already knows. Nothing protects it against local change:
    /// whatever could change it runs with the page's access and could read the
    /// key instead.
    pub fn head_marks(&self, scope: &str) -> IdbHeadMarks {
        IdbHeadMarks {
            cache: self.clone(),
            scope: scope.to_string(),
        }
    }

    /// How many bytes of sealed chunks the cache holds.
    ///
    /// # Errors
    ///
    /// When IndexedDB fails.
    pub async fn bytes(&self) -> Result<u64> {
        let (_, _, meta) = self.transaction(IdbTransactionMode::Readonly).await?;
        let req = meta
            .get(&JsValue::from_str(TOTAL_KEY))
            .map_err(|e| js_err("total", e))?;
        Ok(number(&request("reading the total", &req).await?) as u64)
    }

    async fn get_now(&self, ids: &[ChunkId]) -> Result<Vec<SealedChunk>> {
        let now = now_ms();
        let (_, chunks, _) = self.transaction(IdbTransactionMode::Readonly).await?;
        let mut pending = Vec::with_capacity(ids.len());
        for id in ids {
            let req = chunks
                .get(&key(id))
                .map_err(|e| js_err("reading chunks", e))?;
            pending.push((*id, req));
        }
        let mut out = Vec::new();
        let mut stale = Vec::new();
        for (id, req) in pending {
            let value = request("reading chunks", &req).await?;
            if !value.is_object() {
                continue;
            }
            let sealed = Reflect::get(&value, &JsValue::from_str("s"))
                .ok()
                .map(|s| Uint8Array::new(&s).to_vec())
                .unwrap_or_default();
            let used =
                number(&Reflect::get(&value, &JsValue::from_str("u")).unwrap_or(JsValue::NULL));
            if now - used > TOUCH_AFTER_MS {
                stale.push((id, value));
            }
            out.push(SealedChunk { id, sealed });
        }
        if !stale.is_empty() {
            let me = self.clone();
            wasm_bindgen_futures::spawn_local(async move {
                let _ = within(
                    "marking chunks used",
                    WRITE_TIMEOUT_MS,
                    me.touch(stale, now),
                )
                .await;
            });
        }
        Ok(out)
    }

    async fn touch(&self, entries: Vec<(ChunkId, JsValue)>, now: f64) -> Result<()> {
        let (tx, chunks, _) = self.transaction(IdbTransactionMode::Readwrite).await?;
        for (id, value) in entries {
            let _ = Reflect::set(&value, &JsValue::from_str("u"), &JsValue::from(now));
            chunks
                .put_with_key(&value, &key(&id))
                .map_err(|e| js_err("marking chunks used", e))?;
        }
        complete("marking chunks used", &tx).await
    }

    /// Writes the chunks not already held, and returns the new total.
    async fn put_now(&self, chunks_in: Vec<SealedChunk>, now: f64) -> Result<u64> {
        let (tx, chunks, meta) = self.transaction(IdbTransactionMode::Readwrite).await?;
        let mut added = 0u64;
        for c in &chunks_in {
            let k = key(&c.id);
            let req = chunks
                .count_with_key(&k)
                .map_err(|e| js_err("keeping chunks", e))?;
            if number(&request("keeping chunks", &req).await?) > 0.0 {
                continue;
            }
            let value = Object::new();
            let raw = Uint8Array::from(c.sealed.as_slice());
            let _ = Reflect::set(&value, &JsValue::from_str("s"), &raw);
            let _ = Reflect::set(
                &value,
                &JsValue::from_str("n"),
                &JsValue::from(c.sealed.len() as f64),
            );
            let _ = Reflect::set(&value, &JsValue::from_str("u"), &JsValue::from(now));
            chunks
                .put_with_key(&value, &k)
                .map_err(|e| js_err("keeping chunks", e))?;
            added += c.sealed.len() as u64;
        }
        let total = adjust_total(&meta, added as f64).await?;
        complete("keeping chunks", &tx).await?;
        Ok(total)
    }

    async fn drop_now(&self, ids: &[ChunkId]) -> Result<()> {
        let (tx, chunks, meta) = self.transaction(IdbTransactionMode::Readwrite).await?;
        let mut freed = 0.0;
        for id in ids {
            let k = key(id);
            let req = chunks.get(&k).map_err(|e| js_err("dropping chunks", e))?;
            let value = request("dropping chunks", &req).await?;
            if value.is_object() {
                freed +=
                    number(&Reflect::get(&value, &JsValue::from_str("n")).unwrap_or(JsValue::NULL));
                chunks
                    .delete(&k)
                    .map_err(|e| js_err("dropping chunks", e))?;
            }
        }
        adjust_total(&meta, -freed).await?;
        complete("dropping chunks", &tx).await
    }

    /// Removes the chunks used least recently until the total is at most nine
    /// tenths of the limit.
    async fn evict(&self) -> Result<()> {
        let target = (self.inner.cap_bytes / 10 * 9) as f64;
        let (tx, chunks, meta) = self.transaction(IdbTransactionMode::Readwrite).await?;
        let req = meta
            .get(&JsValue::from_str(TOTAL_KEY))
            .map_err(|e| js_err("evicting", e))?;
        let mut total = number(&request("evicting", &req).await?);
        if total <= target {
            return complete("evicting", &tx).await;
        }
        let index = chunks
            .index(USED_INDEX)
            .map_err(|e| js_err("evicting", e))?;
        let walk = index.open_cursor().map_err(|e| js_err("evicting", e))?;
        loop {
            let cursor = request("evicting", &walk).await?;
            let Ok(cursor) = cursor.dyn_into::<web_sys::IdbCursorWithValue>() else {
                break;
            };
            if total <= target {
                break;
            }
            let value = cursor.value().unwrap_or(JsValue::NULL);
            total -=
                number(&Reflect::get(&value, &JsValue::from_str("n")).unwrap_or(JsValue::NULL));
            cursor.delete().map_err(|e| js_err("evicting", e))?;
            cursor.continue_().map_err(|e| js_err("evicting", e))?;
        }
        meta.put_with_key(
            &JsValue::from(total.max(0.0)),
            &JsValue::from_str(TOTAL_KEY),
        )
        .map_err(|e| js_err("evicting", e))?;
        complete("evicting", &tx).await
    }
}

/// Adds `delta` to the stored total inside the transaction `meta` belongs to,
/// and returns the new total.
async fn adjust_total(meta: &IdbObjectStore, delta: f64) -> Result<u64> {
    let req = meta
        .get(&JsValue::from_str(TOTAL_KEY))
        .map_err(|e| js_err("the total", e))?;
    let total = (number(&request("the total", &req).await?) + delta).max(0.0);
    meta.put_with_key(&JsValue::from(total), &JsValue::from_str(TOTAL_KEY))
        .map_err(|e| js_err("the total", e))?;
    Ok(total as u64)
}

impl ChunkCache for Cache {
    /// Returns the chunks held among `ids`, moving the last-used time of any
    /// not used for an hour.
    fn get<'a>(&'a self, ids: &'a [ChunkId]) -> BoxFuture<'a, Result<Vec<SealedChunk>>> {
        Box::pin(within("reading chunks", READ_TIMEOUT_MS, self.get_now(ids)))
    }

    /// Keeps chunks, returning before they are written. A chunk already held
    /// is left as it is. When the total passes the limit, the least recently
    /// used are evicted down to nine tenths of it.
    fn put<'a>(&'a self, chunks: &'a [SealedChunk]) -> BoxFuture<'a, Result<()>> {
        let me = self.clone();
        let kept = chunks.to_vec();
        let now = now_ms();
        wasm_bindgen_futures::spawn_local(async move {
            let cap = me.inner.cap_bytes;
            let written = within("keeping chunks", WRITE_TIMEOUT_MS, me.put_now(kept, now)).await;
            if matches!(written, Ok(total) if total > cap) {
                let _ = within("evicting", WRITE_TIMEOUT_MS, me.evict()).await;
            }
        });
        Box::pin(async { Ok(()) })
    }

    /// Removes chunks that failed to open.
    fn drop_chunks<'a>(&'a self, ids: &'a [ChunkId]) -> BoxFuture<'a, Result<()>> {
        Box::pin(within(
            "dropping chunks",
            READ_TIMEOUT_MS,
            self.drop_now(ids),
        ))
    }
}

/// One store's head marks in the `"heads"` object store. See
/// [`Cache::head_marks`].
#[derive(Clone)]
pub struct IdbHeadMarks {
    cache: Cache,
    scope: String,
}

impl IdbHeadMarks {
    /// `branch`'s key in the heads object store.
    fn key(&self, branch: &str) -> String {
        format!("{}\0{branch}", self.scope)
    }

    async fn load_now(&self) -> Result<HashMap<String, CommitId>> {
        let prefix = format!("{}\0", self.scope);
        let (_, heads) = self
            .cache
            .heads_transaction(IdbTransactionMode::Readonly)
            .await?;
        // Every key that starts with prefix sorts below scope + "\x01".
        let bounds = IdbKeyRange::bound_with_lower_open_and_upper_open(
            &JsValue::from_str(&prefix),
            &JsValue::from_str(&format!("{}\x01", self.scope)),
            false,
            true,
        )
        .map_err(|e| js_err("loading head marks", e))?;
        let walk = heads
            .open_cursor_with_range(&bounds)
            .map_err(|e| js_err("loading head marks", e))?;
        let mut out = HashMap::new();
        loop {
            let cursor = request("loading head marks", &walk).await?;
            let Ok(cursor) = cursor.dyn_into::<IdbCursorWithValue>() else {
                break;
            };
            let id = cursor
                .value()
                .ok()
                .and_then(|v| v.as_string())
                .map(|v| parse_mark(&v))
                .unwrap_or_default();
            let branch = cursor.key().ok().and_then(|k| k.as_string());
            if let (false, Some(branch)) = (id.is_zero(), branch) {
                out.insert(branch[prefix.len()..].to_string(), id);
            }
            cursor
                .continue_()
                .map_err(|e| js_err("loading head marks", e))?;
        }
        Ok(out)
    }

    /// The read and the write share one transaction, so two tabs cannot both
    /// swap from the same record.
    async fn compare_and_swap_now(
        &self,
        branch: &str,
        old: CommitId,
        next: CommitId,
    ) -> Result<CommitId> {
        let (tx, heads) = self
            .cache
            .heads_transaction(IdbTransactionMode::Readwrite)
            .await?;
        let key = JsValue::from_str(&self.key(branch));
        let req = heads
            .get(&key)
            .map_err(|e| js_err("recording a head mark", e))?;
        let held = request("recording a head mark", &req)
            .await?
            .as_string()
            .map(|v| parse_mark(&v))
            .unwrap_or_default();
        let now = if held == old {
            heads
                .put_with_key(&JsValue::from_str(&next.to_string()), &key)
                .map_err(|e| js_err("recording a head mark", e))?;
            next
        } else {
            held
        };
        complete("recording a head mark", &tx).await?;
        Ok(now)
    }
}

/// Reads a stored commit id. Anything else reads as no record.
fn parse_mark(s: &str) -> CommitId {
    if !s.len().is_multiple_of(2) || !s.is_ascii() {
        return CommitId::default();
    }
    let raw: Option<Vec<u8>> = (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&s[i..i + 2], 16).ok())
        .collect();
    raw.and_then(|b| CommitId::from_bytes(&b).ok())
        .unwrap_or_default()
}

impl HeadMarks for IdbHeadMarks {
    fn load(&self) -> BoxFuture<'_, Result<HashMap<String, CommitId>>> {
        Box::pin(within(
            "loading head marks",
            MARK_TIMEOUT_MS,
            self.load_now(),
        ))
    }

    fn compare_and_swap<'a>(
        &'a self,
        branch: &'a str,
        old: CommitId,
        next: CommitId,
    ) -> BoxFuture<'a, Result<CommitId>> {
        Box::pin(within(
            "recording a head mark",
            MARK_TIMEOUT_MS,
            self.compare_and_swap_now(branch, old, next),
        ))
    }
}
