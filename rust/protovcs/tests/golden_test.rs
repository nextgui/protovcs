//! The Rust client must reproduce, byte for byte, every vector the Go client
//! wrote to `testdata/golden/vectors.json`.

mod support;

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use buffa::Message;
use buffa_descriptor::generated::descriptor::FileDescriptorSet;
use buffa_types::google::protobuf::Timestamp;
use protovcs::proto::{Commit, ManifestNode, Transform};
use protovcs::{
    ChunkId, CommitId, CommitMeta, ItemRef, Keyring, PlainSealer, RefState, Role, Sealer, Store,
    added_since, build_manifest, canonical_bytes, update_manifest,
};
use protovcs_memtransport::MemServer;
use serde_json::Value;
use support::*;

fn arr(v: &Value) -> &Vec<Value> {
    v.as_array().expect("an array")
}

fn role(s: &str) -> Role {
    match s {
        "item" => Role::Item,
        "manifest" => Role::Manifest,
        _ => Role::Schema,
    }
}

fn keyring() -> Keyring {
    Keyring::new(&root_key()).unwrap()
}

#[test]
fn keys_derive_as_go_derives_them() {
    let k = keyring();
    let (name, chunk, r) = k.derived_keys();
    let v = &vectors()["keys"];
    assert_eq!(name.to_vec(), hex(&v["name"]));
    assert_eq!(chunk.to_vec(), hex(&v["chunk"]));
    assert_eq!(r.to_vec(), hex(&v["ref"]));
}

#[test]
fn chunk_ids_and_ref_names_match() {
    let k = keyring();
    for c in arr(&vectors()["chunk_ids"]) {
        let (r, key, canonical) = (
            role(c["role"].as_str().unwrap()),
            c["key"].as_str().unwrap(),
            hex(&c["canonical"]),
        );
        assert_eq!(
            k.chunk_id(r, key, &canonical).to_vec(),
            hex(&c["keyring"]),
            "{key}"
        );
        assert_eq!(
            PlainSealer.chunk_id(r, key, &canonical).to_vec(),
            hex(&c["plain"]),
            "{key}"
        );
    }
    for r in arr(&vectors()["ref_names"]) {
        let b = r["branch"].as_str().unwrap();
        assert_eq!(k.ref_name(b), hex(&r["keyring"]), "{b}");
        assert_eq!(PlainSealer.ref_name(b), hex(&r["plain"]), "{b}");
    }
}

#[test]
fn seals_match_and_open() {
    let k = keyring();
    for s in arr(&vectors()["seals"]) {
        let id = ChunkId::from_bytes(&hex(&s["id"])).unwrap();
        let nonce: [u8; 12] = hex(&s["nonce"]).try_into().unwrap();
        let plaintext = hex(&s["plaintext"]);
        let sealed = hex(&s["sealed"]);
        assert_eq!(k.seal_with_nonce(&id, &nonce, &plaintext), sealed);
        assert_eq!(k.open(&id, &sealed).unwrap(), plaintext);
        assert_eq!(
            PlainSealer.seal(&id, &plaintext).unwrap(),
            hex(&s["plain_sealed"])
        );
        let mut altered = sealed.clone();
        *altered.last_mut().unwrap() ^= 1;
        assert!(
            k.open(&id, &altered)
                .unwrap_err()
                .is(protovcs::ErrorKind::Tampered)
        );
    }
}

#[test]
fn segments_match() {
    for s in arr(&vectors()["segments"]) {
        let (kind, input, want) = (
            s["kind"].as_str().unwrap(),
            s["input"].as_str().unwrap(),
            s["want"].as_str().unwrap(),
        );
        let got = match kind {
            "int" => protovcs::int_segment(input.parse().unwrap()),
            "uint" => protovcs::uint_segment(input.parse().unwrap()),
            "string" => protovcs::string_segment(input),
            "bool" => protovcs::bool_segment(input == "true"),
            "float64" => protovcs::float_string_for_test(input.parse().unwrap(), None),
            "float32" => {
                let f: f32 = input.parse().unwrap();
                protovcs::float_string_for_test(f64::from(f), Some(f))
            }
            k if k.starts_with("date_") => {
                let t = match &k[5..] {
                    "TRANSFORM_DAY" => Transform::TRANSFORM_DAY,
                    "TRANSFORM_MONTH" => Transform::TRANSFORM_MONTH,
                    "TRANSFORM_QUARTER" => Transform::TRANSFORM_QUARTER,
                    _ => Transform::TRANSFORM_YEAR,
                };
                let p: Vec<i64> = input.split('-').map(|x| x.parse().unwrap()).collect();
                protovcs::date_segment(t, p[0], p[1], p[2])
            }
            k => panic!("unknown segment kind {k}"),
        };
        assert_eq!(got, want, "{kind} {input:?}");
    }
    for d in arr(&vectors()["dates"]) {
        let got = protovcs::unix_to_date_for_test(d["seconds"].as_i64().unwrap());
        let want = (
            d["year"].as_i64().unwrap(),
            d["month"].as_i64().unwrap(),
            d["day"].as_i64().unwrap(),
        );
        assert_eq!(got, want, "{}", d["seconds"]);
    }
}

#[test]
fn canonical_bytes_match() {
    for c in arr(&vectors()["canonical"]) {
        let m = decode(STORE, &hex(&c["input"]));
        assert_eq!(
            canonical_bytes(&m).unwrap(),
            hex(&c["canonical"]),
            "{}",
            c["name"]
        );
    }
}

#[test]
fn layouts_materialize_and_build_manifests_identically() {
    let k = keyring();
    for lv in arr(&vectors()["layouts"]) {
        let what = format!("{}/{}", lv["name"], lv["message"]);
        let c = compile(&hex(&lv["layout"]));
        let m = message(lv["message"].as_str().unwrap());
        let items = c.materialize(&m).unwrap();
        let want_items = lv["items"].as_array().cloned().unwrap_or_default();
        let keys: Vec<&str> = items.iter().map(|i| i.key.as_str()).collect();
        let want_keys: Vec<&str> = want_items
            .iter()
            .map(|i| i["key"].as_str().unwrap())
            .collect();
        assert_eq!(keys, want_keys, "{what}: item keys");

        let mut refs = Vec::new();
        for (it, want) in items.iter().zip(&want_items) {
            let canonical = canonical_bytes(&it.fragment).unwrap();
            assert_eq!(
                canonical,
                hex(&want["canonical"]),
                "{what}: item {}",
                it.key
            );
            let id = k.chunk_id(Role::Item, &it.key, &canonical);
            assert_eq!(id.to_vec(), hex(&want["id"]), "{what}: item {}", it.key);
            refs.push(ItemRef {
                key: it.key.clone(),
                id,
                bytes: canonical.len() as u64,
                elements: want["elements"].as_u64().unwrap() as u32,
            });
        }
        if refs.is_empty() {
            continue;
        }
        let nodes = build_manifest(&k, &refs).unwrap();
        let want_nodes = arr(&lv["nodes"]);
        assert_eq!(nodes.len(), want_nodes.len(), "{what}: node count");
        for (n, want) in nodes.iter().zip(want_nodes) {
            assert_eq!(n.prefix, want["prefix"].as_str().unwrap(), "{what}");
            assert_eq!(
                n.canonical,
                hex(&want["canonical"]),
                "{what}: node {}",
                n.prefix
            );
            assert_eq!(n.id.to_vec(), hex(&want["id"]), "{what}: node {}", n.prefix);
        }
        assert_eq!(
            canonical_bytes(&c.merge(&items).unwrap()).unwrap(),
            hex(&lv["merged"]),
            "{what}: merged"
        );
        assert_eq!(
            protovcs::tree_digest(&refs),
            hex(&lv["tree_digest"]),
            "{what}: tree digest"
        );
    }
}

#[tokio::test]
async fn manifest_updates_and_diffs_match() {
    let k = keyring();
    let lv = arr(&vectors()["layouts"])
        .iter()
        .find(|l| l["name"] == "teststore" && l["message"] == "store")
        .unwrap();
    let old: HashMap<ChunkId, ManifestNode> = arr(&lv["nodes"])
        .iter()
        .map(|n| {
            let canonical = hex(&n["canonical"]);
            (
                ChunkId::from_bytes(&hex(&n["id"])).unwrap(),
                ManifestNode::decode_from_slice(&canonical).unwrap(),
            )
        })
        .collect();
    let old_root = ChunkId::from_bytes(&hex(&arr(&lv["nodes"]).last().unwrap()["id"])).unwrap();

    let uv = &vectors()["update_manifest"];
    let mut changes = BTreeMap::new();
    for c in arr(&uv["changes"]) {
        let key = c["key"].as_str().unwrap().to_string();
        let r = if c["delete"].as_bool().unwrap() {
            None
        } else {
            Some(ItemRef {
                key: key.clone(),
                id: ChunkId::from_bytes(&hex(&c["id"])).unwrap(),
                bytes: c["bytes"].as_u64().unwrap(),
                elements: c["elements"].as_u64().unwrap() as u32,
            })
        };
        changes.insert(key, r);
    }
    let (nodes, root) = update_manifest(&k, old_root, &changes, &old).await.unwrap();
    assert_eq!(root.to_vec(), hex(&uv["root"]));
    let want = arr(&uv["nodes"]);
    assert_eq!(nodes.len(), want.len());
    for (n, w) in nodes.iter().zip(want) {
        assert_eq!(n.prefix, w["prefix"].as_str().unwrap());
        assert_eq!(n.canonical, hex(&w["canonical"]), "node {}", n.prefix);
        assert_eq!(n.id.to_vec(), hex(&w["id"]));
    }
    let added = added_since(&nodes, root, old_root, &old).await.unwrap();
    let want: Vec<Vec<u8>> = arr(&uv["added_since"]).iter().map(hex).collect();
    assert_eq!(added.iter().map(ChunkId::to_vec).collect::<Vec<_>>(), want);
}

#[test]
fn digests_and_ref_comparison_match() {
    for d in arr(&vectors()["digests"]) {
        let root = ChunkId::from_bytes(&hex(&d["layout"])).unwrap();
        assert_eq!(
            protovcs::node_digest_for_test(&root),
            hex(&d["node_digest"])
        );
    }
    let refs = |v: &Value| -> Vec<RefState> {
        v.as_array()
            .map(|a| {
                a.iter()
                    .map(|r| RefState {
                        name: hex(&r["name"]),
                        commit: CommitId::from_bytes(&hex(&r["commit"])).unwrap(),
                        cas_token: r["token"].as_str().unwrap().to_string(),
                    })
                    .collect()
            })
            .unwrap_or_default()
    };
    for c in arr(&vectors()["same_refs"]) {
        assert_eq!(
            protovcs::same_refs(&refs(&c["a"]), &refs(&c["b"])),
            c["same"].as_bool().unwrap()
        );
    }
}

/// The descriptors the Go client records in its schema chunk.
fn go_descriptors() -> FileDescriptorSet {
    FileDescriptorSet::decode_from_slice(&hex(&vectors()["schema"]["descriptors"])).unwrap()
}

fn store_on(server: &MemServer, entropy: protovcs::Entropy, clock: protovcs::Clock) -> Store {
    let sealer = Keyring::with_entropy(&root_key(), Arc::clone(&entropy)).unwrap();
    Store::builder()
        .transport(server.transport())
        .sealer(Arc::new(sealer))
        .layout(teststore())
        .entropy(entropy)
        .clock(clock)
        .schema_descriptors(go_descriptors())
        .build()
        .unwrap()
}

#[test]
fn the_schema_chunk_matches() {
    let server = MemServer::new();
    let (_, clock) = fixed_clock();
    let s = store_on(&server, entropy_stream(), clock);
    let (id, canonical) = s.schema_chunk().unwrap();
    let v = &vectors()["schema"];
    assert_eq!(canonical, hex(&v["canonical"]), "schema chunk bytes");
    assert_eq!(id.to_vec(), hex(&v["id"]));
}

/// Without Go's descriptors supplied, the store records the files it reads
/// from its own pool. Every first-party file matches what Go records.
/// descriptor.proto does not: protobuf-go compiles in its own copy of it, and
/// protox, which compiles the protos here, bundles a different one. Neither
/// client can produce the other's.
#[test]
fn the_schema_descriptors_match_go_except_descriptor_proto() {
    let s = Store::builder()
        .transport(MemServer::new().transport())
        .sealer(Arc::new(keyring()))
        .layout(teststore())
        .build()
        .unwrap();
    let (_, canonical) = s.schema_chunk().unwrap();
    let mine = protovcs::proto::SchemaChunk::decode_from_slice(&canonical).unwrap();
    let mine = mine.descriptors.as_option().unwrap();
    let go = go_descriptors();
    let names =
        |set: &FileDescriptorSet| set.file.iter().map(|f| f.name.clone()).collect::<Vec<_>>();
    assert_eq!(names(mine), names(&go));
    for (a, b) in mine.file.iter().zip(&go.file) {
        if a.name.as_deref() == Some("google/protobuf/descriptor.proto") {
            continue;
        }
        assert_eq!(a, b, "{:?}", a.name);
    }
}

fn meta_of(c: &Value) -> CommitMeta {
    let mut m = CommitMeta::new(
        c["title"].as_str().unwrap(),
        c["author"].as_str().unwrap(),
        c["email"].as_str().unwrap(),
    );
    m.description = c["description"].as_str().unwrap().into();
    m.client = c["client"].as_str().unwrap().into();
    for l in c["labels"].as_array().cloned().unwrap_or_default() {
        m.labels.insert(
            l["key"].as_str().unwrap().into(),
            l["value"].as_str().unwrap().into(),
        );
    }
    let details = hex(&c["details"]);
    if !details.is_empty() {
        m.details = Some(decode("nextgui.protovcs.testpb.Change", &details));
    }
    m
}

/// Two writes through a Rust store, drawing from the Go test's entropy stream
/// with the clock fixed, produce exactly the commits and sealed chunks the Go
/// client produced.
#[tokio::test]
async fn writes_produce_the_go_clients_bytes() {
    let server = MemServer::new();
    let (at, clock) = fixed_clock();
    let s = store_on(&server, entropy_stream(), clock);
    let sv = &vectors()["store"];
    let main = s.branch(sv["branch"].as_str().unwrap());
    assert_eq!(
        main.store().sealer().ref_name(main.name()),
        hex(&sv["ref_name"])
    );

    for c in arr(&sv["commits"]) {
        *at.lock().unwrap() = Timestamp {
            seconds: c["seconds"].as_i64().unwrap(),
            nanos: c["nanos"].as_i64().unwrap() as i32,
            ..Default::default()
        };
        let id = main
            .write(&meta_of(c), &message(c["message"].as_str().unwrap()))
            .await
            .unwrap();
        assert_eq!(id.to_vec(), hex(&c["id"]), "commit {}", c["title"]);

        let got = server.commit(&id).unwrap();
        let want = Commit::decode_from_slice(&hex(&c["record"])).unwrap();
        assert_eq!(got.added, want.added, "added, {}", c["title"]);
        assert_eq!(got.parents, want.parents);
        assert_eq!(got.naming_nonce, want.naming_nonce);
        assert_eq!(
            s.sealer().open(&ChunkId::from(id), &got.payload).unwrap(),
            hex(&c["payload"]),
            "payload plaintext"
        );
        assert_eq!(got.payload, want.payload, "sealed payload");
        assert_eq!(got, want, "the whole commit record");

        let chunks = server.chunks();
        for u in arr(&c["uploaded"]) {
            let cid = ChunkId::from_bytes(&hex(&u["id"])).unwrap();
            assert_eq!(chunks.get(&cid), Some(&hex(&u["sealed"])), "chunk {cid}");
        }
    }
}

/// A Rust store reads a store the Go client wrote, and names the same chunks
/// in a pack hint.
#[tokio::test]
async fn reads_a_store_the_go_client_wrote() {
    let server = MemServer::new();
    let sv = &vectors()["store"];
    let mut head = CommitId::default();
    for c in arr(&sv["commits"]) {
        server.put_raw_chunks(arr(&c["uploaded"]).iter().map(|u| protovcs::SealedChunk {
            id: ChunkId::from_bytes(&hex(&u["id"])).unwrap(),
            sealed: hex(&u["sealed"]),
        }));
        server.put_raw_commit(Commit::decode_from_slice(&hex(&c["record"])).unwrap());
        head = CommitId::from_bytes(&hex(&c["id"])).unwrap();
    }
    server.set_ref(&hex(&sv["ref_name"]), head);

    let s = Store::builder()
        .transport(server.transport())
        .sealer(Arc::new(keyring()))
        .layout(teststore())
        .pack_hints("main")
        .build()
        .unwrap();
    let got = s.branch("main").read().await.unwrap();
    assert_eq!(canonical_bytes(&got).unwrap(), hex(&sv["read_back"]));

    let (commit, _) = s.branch("main").head().await.unwrap();
    let reachable: Vec<Vec<u8>> = s
        .reachable_chunks(&commit)
        .await
        .unwrap()
        .iter()
        .map(ChunkId::to_vec)
        .collect();
    let want: Vec<Vec<u8>> = arr(&sv["reachable"]).iter().map(hex).collect();
    assert_eq!(reachable, want);

    let (hinted, ids) = server
        .last_pack_hint()
        .expect("a hint after a full read of the head");
    assert_eq!(hinted, head.to_vec());
    assert_eq!(ids, want);

    let log = s
        .branch("main")
        .log(&protovcs::LogOptions::default())
        .await
        .unwrap();
    let titles: Vec<&str> = log.iter().map(|i| i.title.as_str()).collect();
    assert_eq!(titles, ["second", "first"]);
    let first = s.commit_by_id(log[1].id).await.unwrap();
    let details = s.open_details(&first, &log[1]).await.unwrap().unwrap();
    assert_eq!(
        details.message_descriptor_name(),
        "nextgui.protovcs.testpb.Change"
    );
}

trait Named {
    fn message_descriptor_name(&self) -> String;
}

impl Named for protovcs::DynamicMessage {
    fn message_descriptor_name(&self) -> String {
        use buffa_descriptor::reflect::ReflectMessage;
        self.message_descriptor().full_name().to_string()
    }
}
