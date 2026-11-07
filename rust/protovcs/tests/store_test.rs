//! The store's behaviour over the in-memory server. These follow the Go
//! client's tests in `*_test.go`.

mod support;

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use buffa_descriptor::reflect::{ReflectMessage, ReflectMessageMut, Value};
use protovcs::{
    BoxFuture, ChunkCache, ChunkId, CommitId, CommitMeta, DynamicMessage, ErrorKind, HeadMarks,
    Keyring, LogOptions, MemoryHeadMarks, ProbePoint, Result, Scope, SealedChunk, Sealer, Store,
};
use protovcs_memtransport::MemServer;
use support::*;

fn set(m: &mut DynamicMessage, field: &str, v: Value) {
    let pool = Arc::clone(m.pool());
    let fd = pool
        .message(m.message_index())
        .field_by_name(field)
        .unwrap_or_else(|| panic!("no field {field}"));
    m.set(fd, v);
}

fn new(name: &str) -> DynamicMessage {
    let pool = pool();
    let idx = pool.message_index(name).unwrap();
    DynamicMessage::new(pool, idx)
}

fn record(i: usize) -> DynamicMessage {
    let mut date = new("nextgui.protovcs.testpb.Date");
    set(&mut date, "year", Value::I32(2026));
    set(&mut date, "month", Value::I32(1 + (i % 12) as i32));
    set(&mut date, "day", Value::I32(1 + (i % 28) as i32));
    let mut r = new("nextgui.protovcs.testpb.Record");
    set(&mut r, "id", Value::String(format!("rec-{i:04}")));
    set(
        &mut r,
        "group",
        Value::String(["alpha", "beta", "gamma"][i % 3].into()),
    );
    set(&mut r, "date", Value::Message(date));
    set(&mut r, "amount", Value::I64(i as i64));
    r
}

fn account(id: &str) -> DynamicMessage {
    let mut a = new("nextgui.protovcs.testpb.Account");
    set(&mut a, "id", Value::String(id.into()));
    a
}

fn store_message(n: usize) -> DynamicMessage {
    let mut m = new(STORE);
    set(&mut m, "name", Value::String("a store".into()));
    set(&mut m, "revision", Value::I64(3));
    set(
        &mut m,
        "records",
        Value::List((0..n).map(|i| Value::Message(record(i))).collect()),
    );
    set(
        &mut m,
        "accounts",
        Value::List(vec![Value::Message(account("acc-1"))]),
    );
    m
}

fn list_len(m: &DynamicMessage, field: &str) -> usize {
    match m.field_by_number(
        m.message_descriptor()
            .field_by_name(field)
            .unwrap()
            .number(),
    ) {
        Some(Value::List(l)) => l.len(),
        _ => 0,
    }
}

fn int_field(m: &DynamicMessage, field: &str) -> i64 {
    match m.field_by_number(
        m.message_descriptor()
            .field_by_name(field)
            .unwrap()
            .number(),
    ) {
        Some(Value::I64(i)) => *i,
        _ => 0,
    }
}

fn store(server: &MemServer) -> Store {
    Store::builder()
        .transport(server.transport())
        .sealer(Arc::new(Keyring::new(&root_key()).unwrap()))
        .layout(teststore())
        .build()
        .unwrap()
}

fn meta(title: &str) -> CommitMeta {
    CommitMeta::new(title, "test", "test@example.com")
}

#[tokio::test]
async fn a_store_never_written_reads_empty() {
    let s = store(&MemServer::new());
    let got = s.branch("main").read().await.unwrap();
    assert_eq!(list_len(&got, "records"), 0);
    assert!(
        s.branch("main")
            .log(&LogOptions::default())
            .await
            .unwrap()
            .is_empty()
    );
}

#[tokio::test]
async fn write_then_read() {
    let server = MemServer::new();
    let s = store(&server);
    s.branch("main")
        .write(&meta("first"), &store_message(90))
        .await
        .unwrap();

    // A second store has nothing in memory and reads it all from the server.
    let got = store(&server).branch("main").read().await.unwrap();
    assert_eq!(list_len(&got, "records"), 90);
    assert_eq!(list_len(&got, "accounts"), 1);
    assert_eq!(int_field(&got, "revision"), 3);
}

#[tokio::test]
async fn a_stale_write_loses_and_change_retries() {
    let server = MemServer::new();
    let (a, b) = (store(&server), store(&server));
    a.branch("main")
        .write(&meta("first"), &store_message(3))
        .await
        .unwrap();

    // b has seen the first commit; a moves past it before b changes anything.
    b.branch("main").head().await.unwrap();
    a.branch("main")
        .change(&meta("a"), |m| {
            set(m, "revision", Value::I64(10));
            Ok(())
        })
        .await
        .unwrap();

    let mut runs = 0;
    b.branch("main")
        .change(&meta("b"), |m| {
            runs += 1;
            let r = int_field(m, "revision");
            set(m, "revision", Value::I64(r + 1));
            Ok(())
        })
        .await
        .unwrap();
    assert!(runs >= 1);
    let got = a.branch("main").read().await.unwrap();
    assert_eq!(int_field(&got, "revision"), 11);
}

#[tokio::test]
async fn a_write_after_another_writer_moved_the_branch_is_a_lost_race() {
    let server = MemServer::new();
    let a = store(&server);
    a.branch("main")
        .write(&meta("first"), &store_message(3))
        .await
        .unwrap();

    // Hold b just after it reads the head, and let a write meanwhile.
    let a2 = a.clone();
    let held = Arc::new(Mutex::new(false));
    let flag = Arc::clone(&held);
    let b = Store::builder()
        .transport(server.transport())
        .sealer(Arc::new(Keyring::new(&root_key()).unwrap()))
        .layout(teststore())
        .probe(Arc::new(move |p: ProbePoint| -> Result<()> {
            if p == ProbePoint::AfterHead && !*flag.lock().unwrap() {
                *flag.lock().unwrap() = true;
                let a2 = a2.clone();
                std::thread::spawn(move || {
                    tokio::runtime::Runtime::new().unwrap().block_on(async {
                        a2.branch("main")
                            .write(&meta("a"), &store_message(4))
                            .await
                            .unwrap();
                    })
                })
                .join()
                .unwrap();
            }
            Ok(())
        }))
        .build()
        .unwrap();
    let err = b
        .branch("main")
        .write(&meta("b"), &store_message(5))
        .await
        .unwrap_err();
    assert!(err.is(ErrorKind::RaceLost), "{err}");
}

#[tokio::test]
async fn a_failure_after_upload_leaves_the_branch_where_it_was() {
    let server = MemServer::new();
    let s = store(&server);
    let first = s
        .branch("main")
        .write(&meta("first"), &store_message(3))
        .await
        .unwrap();
    let crashing = Store::builder()
        .transport(server.transport())
        .sealer(Arc::new(Keyring::new(&root_key()).unwrap()))
        .layout(teststore())
        .probe(Arc::new(|p: ProbePoint| -> Result<()> {
            if p == ProbePoint::AfterUpload {
                return Err(protovcs::Error::invalid("crash"));
            }
            Ok(())
        }))
        .build()
        .unwrap();
    assert!(
        crashing
            .branch("main")
            .write(&meta("second"), &store_message(9))
            .await
            .is_err()
    );
    let (head, _) = store(&server).branch("main").head().await.unwrap();
    assert_eq!(head.id, first.to_vec());
}

#[tokio::test]
async fn a_partial_checkout_reads_only_its_scope_and_commits_only_its_changes() {
    let server = MemServer::new();
    let s = store(&server);
    s.branch("main")
        .write(&meta("first"), &store_message(60))
        .await
        .unwrap();

    let reader = store(&server);
    server.reset_counts();
    let mut p = reader
        .branch("main")
        .checkout(Scope::field("accounts"))
        .await
        .unwrap();
    let (fetched, _, _) = server.counts();
    assert!(p.get("accounts/acc-1").unwrap().is_some());
    let outside = p.get("records/alpha/2026/01").unwrap_err();
    assert!(outside.is(ErrorKind::OutOfScope));
    assert!(p.require(&Scope::everything()).is_err());
    assert!(
        fetched < 10,
        "a checkout of one field fetched {fetched} chunks"
    );

    server.reset_counts();
    p.put_element("records", record(1000)).await.unwrap();
    p.commit(&meta("one more")).await.unwrap();
    let (_, uploaded, _) = server.counts();
    // The changed item, the nodes above it, and nothing else.
    assert!(uploaded <= 5, "uploaded {uploaded} chunks for one element");

    let got = store(&server).branch("main").read().await.unwrap();
    assert_eq!(list_len(&got, "records"), 61);
}

#[tokio::test]
async fn a_substituted_chunk_is_refused() {
    let server = MemServer::new();
    let s = store(&server);
    s.branch("main")
        .write(&meta("first"), &store_message(6))
        .await
        .unwrap();
    let chunks = server.chunks();
    let mut ids = chunks.keys();
    let (a, b) = (*ids.next().unwrap(), *ids.next().unwrap());
    server.corrupt(a, chunks[&b].clone());
    let err = store(&server).branch("main").read().await.unwrap_err();
    assert!(err.is(ErrorKind::Tampered), "{err}");
}

#[tokio::test]
async fn a_server_serving_an_older_head_is_caught() {
    let server = MemServer::new();
    let s = store(&server);
    let main = s.branch("main");
    let first = main.write(&meta("first"), &store_message(3)).await.unwrap();
    main.write(&meta("second"), &store_message(4))
        .await
        .unwrap();
    main.head().await.unwrap();
    server.set_ref(&s.sealer().ref_name("main"), first);
    let err = main.head().await.unwrap_err();
    assert!(err.is(ErrorKind::WentBackwards), "{err}");
}

#[tokio::test]
async fn two_elements_with_one_identity_are_refused() {
    let s = store(&MemServer::new());
    let mut m = store_message(0);
    set(
        &mut m,
        "records",
        Value::List(vec![Value::Message(record(1)), Value::Message(record(1))]),
    );
    let err = s.branch("main").write(&meta("dup"), &m).await.unwrap_err();
    assert!(err.is(ErrorKind::DuplicateIdentity), "{err}");
}

#[tokio::test]
async fn history_truncation_and_rewrite() {
    let server = MemServer::new();
    let s = store(&server);
    let main = s.branch("main");
    for i in 0..3 {
        main.write(&meta(&format!("v{i}")), &store_message(i + 1))
            .await
            .unwrap();
    }
    let log = main.log(&LogOptions::default()).await.unwrap();
    assert_eq!(
        log.iter().map(|c| c.title.as_str()).collect::<Vec<_>>(),
        ["v2", "v1", "v0"]
    );
    assert_eq!(
        main.log(&LogOptions {
            limit: 0,
            until: log[1].id
        })
        .await
        .unwrap()
        .len(),
        1
    );

    let result = s
        .rewrite(|m| {
            set(m, "name", Value::String("renamed".into()));
            Ok(())
        })
        .await
        .unwrap();
    assert_eq!(result.rewritten().len(), 3);
    assert_eq!(result.branches_moved(), 1);
    let got = main.read().await.unwrap();
    assert_eq!(list_len(&got, "records"), 3);

    main.truncate(&meta("truncated")).await.unwrap();
    assert_eq!(main.log(&LogOptions::default()).await.unwrap().len(), 1);

    let (head, _) = main.head().await.unwrap();
    let (generic, layout) = s.read_generic(&head).await.unwrap();
    assert_eq!(layout.layout().name, "teststore");
    assert_eq!(list_len(&generic, "records"), 3);
}

#[tokio::test]
async fn watch_reports_where_the_branch_is_then_stops_when_asked() {
    let server = MemServer::new();
    let s = store(&server);
    let id = s
        .branch("main")
        .write(&meta("first"), &store_message(1))
        .await
        .unwrap();
    let mut seen = Vec::new();
    let err = s
        .branch("main")
        .watch(|state| {
            seen.push(state.commit);
            Err(protovcs::Error::invalid("stop"))
        })
        .await
        .unwrap_err();
    assert_eq!(err.to_string(), "stop");
    assert_eq!(seen, [id]);
}

/// A chunk cache held in memory.
#[derive(Default)]
struct MemCache(Mutex<BTreeMap<ChunkId, Vec<u8>>>);

impl ChunkCache for MemCache {
    fn get<'a>(&'a self, ids: &'a [ChunkId]) -> BoxFuture<'a, Result<Vec<SealedChunk>>> {
        let held = self.0.lock().unwrap();
        let out = ids
            .iter()
            .filter_map(|id| {
                held.get(id).map(|s| SealedChunk {
                    id: *id,
                    sealed: s.clone(),
                })
            })
            .collect();
        Box::pin(async move { Ok(out) })
    }

    fn put<'a>(&'a self, chunks: &'a [SealedChunk]) -> BoxFuture<'a, Result<()>> {
        let mut held = self.0.lock().unwrap();
        for c in chunks {
            held.insert(c.id, c.sealed.clone());
        }
        Box::pin(async { Ok(()) })
    }

    fn drop_chunks<'a>(&'a self, ids: &'a [ChunkId]) -> BoxFuture<'a, Result<()>> {
        let mut held = self.0.lock().unwrap();
        for id in ids {
            held.remove(id);
        }
        Box::pin(async { Ok(()) })
    }
}

#[tokio::test]
async fn a_chunk_cache_saves_a_second_page_load_and_survives_damage() {
    let server = MemServer::new();
    let cache = Arc::new(MemCache::default());
    let cached = || {
        Store::builder()
            .transport(server.transport())
            .sealer(Arc::new(Keyring::new(&root_key()).unwrap()))
            .layout(teststore())
            .chunk_cache(Arc::clone(&cache) as Arc<dyn ChunkCache>)
            .build()
            .unwrap()
    };
    cached()
        .branch("main")
        .write(&meta("first"), &store_message(30))
        .await
        .unwrap();

    server.reset_counts();
    cached().branch("main").read().await.unwrap();
    assert_eq!(
        server.counts().0,
        0,
        "a read with every chunk cached fetched chunks"
    );

    // A damaged entry is dropped and fetched again.
    let some = *cache.0.lock().unwrap().keys().next().unwrap();
    cache.0.lock().unwrap().insert(some, vec![1, 2, 3]);
    let got = cached().branch("main").read().await.unwrap();
    assert_eq!(list_len(&got, "records"), 30);
    assert_eq!(server.counts().0, 1);
}

#[tokio::test]
async fn usage_is_reported_as_unavailable_by_a_server_without_the_admin_service() {
    let s = store(&MemServer::new());
    assert!(s.usage().await.is_err());
}

fn store_with_marks(server: &MemServer, marks: &Arc<MemoryHeadMarks>) -> Store {
    Store::builder()
        .transport(server.transport())
        .sealer(Arc::new(Keyring::new(&root_key()).unwrap()))
        .layout(teststore())
        .head_marks(Arc::clone(marks) as Arc<dyn HeadMarks>)
        .build()
        .unwrap()
}

fn main_ref() -> Vec<u8> {
    Keyring::new(&root_key()).unwrap().ref_name("main")
}

/// Writes `n` versions to main and returns their commits, oldest first.
async fn write_n(server: &MemServer, n: usize) -> Vec<CommitId> {
    let main = store(server).branch("main");
    let mut heads = Vec::new();
    for i in 1..=n {
        heads.push(
            main.write(&meta(&format!("write {i}")), &store_message(4 * i))
                .await
                .unwrap(),
        );
    }
    heads
}

/// A server serving an old head to a store that has never read the branch,
/// as after a page reload. The marks another store left behind catch it.
#[tokio::test]
async fn a_rollback_is_refused_after_a_reload() {
    let server = MemServer::new();
    let heads = write_n(&server, 3).await;
    let marks = Arc::new(MemoryHeadMarks::new());

    store_with_marks(&server, &marks)
        .branch("main")
        .read()
        .await
        .unwrap();
    server.set_ref(&main_ref(), heads[0]);

    let err = store_with_marks(&server, &marks)
        .branch("main")
        .read()
        .await
        .unwrap_err();
    assert!(err.is(ErrorKind::WentBackwards), "{err}");

    // Without the marks a new store has nothing to compare against.
    store(&server).branch("main").read().await.unwrap();
}

/// A store that last saw an older head reads after another store recorded a
/// newer one. The record keeps the newer head, and the older store adopts it.
#[tokio::test]
async fn an_older_read_does_not_lower_the_mark() {
    let server = MemServer::new();
    let heads = write_n(&server, 3).await;
    let marks = Arc::new(MemoryHeadMarks::new());

    server.set_ref(&main_ref(), heads[0]);
    let behind = store_with_marks(&server, &marks);
    behind.branch("main").read().await.unwrap();
    server.set_ref(&main_ref(), heads[2]);
    store_with_marks(&server, &marks)
        .branch("main")
        .read()
        .await
        .unwrap();

    // The second head is progress for behind, and a rollback for the record.
    server.set_ref(&main_ref(), heads[1]);
    behind.branch("main").read().await.unwrap();
    assert_eq!(behind.known_head("main"), Some(heads[2]));
    assert_eq!(marks.load().await.unwrap().get("main"), Some(&heads[2]));

    let err = store_with_marks(&server, &marks)
        .branch("main")
        .read()
        .await
        .unwrap_err();
    assert!(err.is(ErrorKind::WentBackwards), "{err}");
}

/// A store's own commit reaches the marks, so a reload refuses the head
/// before it.
#[tokio::test]
async fn a_write_is_recorded() {
    let server = MemServer::new();
    let marks = Arc::new(MemoryHeadMarks::new());
    let main = store_with_marks(&server, &marks).branch("main");
    let mut heads = Vec::new();
    for i in 1..=2 {
        heads.push(
            main.write(&meta(&format!("write {i}")), &store_message(4 * i))
                .await
                .unwrap(),
        );
    }

    server.set_ref(&main_ref(), heads[0]);
    let err = store_with_marks(&server, &marks)
        .branch("main")
        .read()
        .await
        .unwrap_err();
    assert!(err.is(ErrorKind::WentBackwards), "{err}");
}

fn record_in(id: &str, group: &str, year: i32) -> DynamicMessage {
    let mut r = record(0);
    set(&mut r, "id", Value::String(id.into()));
    set(&mut r, "group", Value::String(group.into()));
    let mut date = new("nextgui.protovcs.testpb.Date");
    set(&mut date, "year", Value::I32(year));
    set(&mut date, "month", Value::I32(1));
    set(&mut date, "day", Value::I32(1));
    set(&mut r, "date", Value::Message(date));
    r
}

fn with_lists(records: Vec<DynamicMessage>, accounts: Vec<DynamicMessage>) -> DynamicMessage {
    let mut m = new(STORE);
    set(
        &mut m,
        "records",
        Value::List(records.into_iter().map(Value::Message).collect()),
    );
    set(
        &mut m,
        "accounts",
        Value::List(accounts.into_iter().map(Value::Message).collect()),
    );
    m
}

#[tokio::test]
async fn a_field_scope_holds_keys_outside_the_basic_multilingual_plane() {
    let s = store(&MemServer::new());
    let accounts = ["plain", "\u{1F600}", "\u{10FFFF}"].map(account).to_vec();
    s.branch("main")
        .write(&meta("w"), &with_lists(Vec::new(), accounts))
        .await
        .unwrap();
    for scope in [Scope::field("accounts"), Scope::everything()] {
        let p = s.branch("main").checkout(scope.clone()).await.unwrap();
        assert_eq!(p.items().count(), 3, "{scope}");
        assert!(p.get("accounts/\u{1F600}").unwrap().is_some(), "{scope}");
    }
}

#[tokio::test]
async fn a_prefix_scope_covers_whole_segments_only() {
    let s = store(&MemServer::new());
    let records = ["al", "al-x", "alpha", "al/x"]
        .map(|g| record_in(g, g, 2026))
        .to_vec();
    s.branch("main")
        .write(&meta("w"), &with_lists(records, Vec::new()))
        .await
        .unwrap();
    let p = s
        .branch("main")
        .checkout(Scope::prefix("records/al"))
        .await
        .unwrap();
    let keys: Vec<&str> = p.items().map(|i| i.key.as_str()).collect();
    assert_eq!(keys, ["records/al/2026/01"]);
    assert!(!p.scope().holds("records/alpha/2026/01"));
    assert!(!p.scope().holds("records/al-x/2026/01"));
    assert!(p.scope().holds("records/al"));
}

#[tokio::test]
async fn put_refuses_a_directory_key_or_a_key_below_an_item() {
    let s = store(&MemServer::new());
    let main = s.branch("main");
    main.write(&meta("seed"), &store_message(30)).await.unwrap();
    for key in ["records/alpha", "records/alpha/2026/01/x"] {
        let mut p = main.checkout(Scope::nothing()).await.unwrap();
        if p.put(protovcs::Item::new(key, new(STORE))).is_err() {
            continue;
        }
        assert!(
            p.commit(&meta("p")).await.is_err(),
            "a put at {key:?} was committed"
        );
    }
    let mut p = main.checkout(Scope::prefix("records/alpha")).await.unwrap();
    assert!(
        p.put(protovcs::Item::new("records/alpha", new(STORE)))
            .is_err()
    );
    assert_eq!(list_len(&main.read().await.unwrap(), "records"), 30);
}

#[tokio::test]
async fn a_date_outside_four_digit_years_is_refused() {
    let s = store(&MemServer::new());
    for year in [-1, 10000] {
        let err = s
            .branch("main")
            .write(
                &meta("w"),
                &with_lists(vec![record_in("x", "g", year)], Vec::new()),
            )
            .await
            .unwrap_err();
        assert!(err.to_string().contains("0 to 9999"), "{year}: {err}");
    }
}

#[test]
fn int_segments_sort_numerically() {
    let vals = [i64::MIN, i64::MIN + 1, -42, -2, -1, 0, 1, 42, i64::MAX];
    for w in vals.windows(2) {
        let (a, b) = (protovcs::int_segment(w[0]), protovcs::int_segment(w[1]));
        assert_eq!(a.len(), 20);
        assert!(a < b, "{a} sorts at or after {b}");
    }
    assert_eq!(protovcs::int_segment(42), "00000000000000000042");
}

#[tokio::test]
async fn a_deletion_is_reported_once_while_other_branches_move() {
    let server = MemServer::new();
    let s = store(&server);
    let main = s.branch("main");
    main.write(&meta("first"), &store_message(1)).await.unwrap();

    let (tx, mut rx) = futures::channel::mpsc::unbounded();
    let mut calls = 0;
    let watch = main.watch(move |state| {
        calls += 1;
        tx.unbounded_send(state).unwrap();
        if calls == 3 {
            return Err(protovcs::Error::invalid("stop"));
        }
        Ok(())
    });
    let drive = async {
        use futures::StreamExt;
        let first = rx.next().await.unwrap();
        server
            .transport()
            .update_ref(
                &first.name,
                CommitId::default(),
                &first.cas_token,
                protovcs::RefMove::FastForward,
            )
            .await
            .unwrap();
        assert!(rx.next().await.unwrap().commit.is_zero(), "the deletion");
        s.branch("other")
            .write(&meta("other"), &store_message(1))
            .await
            .unwrap();
        // Time for the watch to see the other branch move on its own, before
        // main is written again.
        futures_timer::Delay::new(std::time::Duration::from_millis(100)).await;
        main.write(&meta("again"), &store_message(2)).await.unwrap();
        let third = rx.next().await.unwrap();
        assert!(
            !third.commit.is_zero(),
            "the deletion was reported again when another branch moved"
        );
    };
    let (err, ()) = futures::join!(watch, drive);
    assert_eq!(err.unwrap_err().to_string(), "stop");
}
