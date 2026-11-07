//! What the integration tests share: the test proto pool, the golden vectors,
//! and the entropy stream they were written with.

#![allow(dead_code)]

use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, OnceLock};

use buffa::Message;
use buffa_types::google::protobuf::Timestamp;
use protovcs::proto::Layout;
use protovcs::{Compiled, DescriptorPool, DynamicMessage, Entropy};
use serde_json::Value;
use sha2::{Digest, Sha256};

/// The repository root, which holds the protos and the vectors. Bazel names
/// the vectors in `PROTOVCS_GOLDEN_VECTORS`; under Cargo the root is two
/// directories above this crate.
fn repo_root() -> PathBuf {
    match std::env::var_os("PROTOVCS_GOLDEN_VECTORS") {
        Some(v) => Path::new(&v)
            .ancestors()
            .nth(3)
            .expect("the vectors are at testdata/golden/vectors.json")
            .to_path_buf(),
        None => Path::new(env!("CARGO_MANIFEST_DIR")).join("../.."),
    }
}

pub const STORE: &str = "nextgui.protovcs.testpb.Store";

pub fn pool() -> Arc<DescriptorPool> {
    static POOL: OnceLock<Arc<DescriptorPool>> = OnceLock::new();
    Arc::clone(POOL.get_or_init(|| {
        Arc::new(DescriptorPool::decode(&testpb_descriptors()).expect("valid descriptors"))
    }))
}

/// The testpb protos and everything they import, compiled from the
/// repository's `proto/` directory.
fn testpb_descriptors() -> Vec<u8> {
    protox::Compiler::new([repo_root()])
        .expect("the repository root is readable")
        .include_imports(true)
        .open_files(["proto/protovcs/testpb/testpb.proto"])
        .expect("testpb compiles")
        .encode_file_descriptor_set()
}

pub fn vectors() -> &'static Value {
    static V: OnceLock<Value> = OnceLock::new();
    V.get_or_init(|| {
        let path = repo_root().join("testdata/golden/vectors.json");
        let text = std::fs::read_to_string(&path)
            .unwrap_or_else(|e| panic!("reading {}: {e}", path.display()));
        serde_json::from_str(&text).expect("the vectors are JSON")
    })
}

pub fn hex(v: &Value) -> Vec<u8> {
    let s = v.as_str().unwrap_or("");
    (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&s[i..i + 2], 16).expect("hex"))
        .collect()
}

pub fn decode(name: &str, bytes: &[u8]) -> DynamicMessage {
    let pool = pool();
    let idx = pool.message_index(name).expect("known message");
    DynamicMessage::decode(pool, idx, bytes).expect("decodes")
}

pub fn message(name: &str) -> DynamicMessage {
    let m = vectors()["messages"]
        .as_array()
        .unwrap()
        .iter()
        .find(|m| m["name"] == name)
        .expect("named message");
    decode(STORE, &hex(&m["bytes"]))
}

pub fn compile(layout_bytes: &[u8]) -> Arc<Compiled> {
    let l = Layout::decode_from_slice(layout_bytes).expect("a layout");
    Arc::new(Compiled::new(l, pool(), STORE).expect("compiles"))
}

/// The teststore layout, as the Go golden test declares it.
pub fn teststore() -> Arc<Compiled> {
    let l = vectors()["layouts"]
        .as_array()
        .unwrap()
        .iter()
        .find(|l| l["name"] == "teststore")
        .unwrap();
    compile(&hex(&l["layout"]))
}

/// The stream the Go test drew every random value from: SHA-256 of a label
/// and a big-endian block counter, block after block.
pub fn entropy_stream() -> Entropy {
    let label = vectors()["entropy"].as_str().unwrap().to_string();
    let state = Mutex::new((Vec::<u8>::new(), 0u32));
    Arc::new(move |out: &mut [u8]| {
        let mut st = state.lock().unwrap();
        while st.0.len() < out.len() {
            let mut h = Sha256::new();
            h.update(label.as_bytes());
            h.update(st.1.to_be_bytes());
            st.1 += 1;
            let block = h.finalize();
            st.0.extend_from_slice(&block);
        }
        out.copy_from_slice(&st.0[..out.len()]);
        st.0.drain(..out.len());
        Ok(())
    })
}

/// A clock a test sets.
pub fn fixed_clock() -> (Arc<Mutex<Timestamp>>, protovcs::Clock) {
    let at = Arc::new(Mutex::new(Timestamp::default()));
    let read = Arc::clone(&at);
    (at, Arc::new(move || read.lock().unwrap().clone()))
}

pub fn root_key() -> Vec<u8> {
    hex(&vectors()["root_key"])
}
