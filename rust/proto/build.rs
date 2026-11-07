//! Generates the protovcs protos from `../../proto`, the copy the Go client
//! is generated from.
//!
//! protox compiles them, so building needs no `protoc`. Bazel names
//! `proto/protovcs/v1/store.proto` in `PROTOVCS_STORE_PROTO`, and the
//! repository root is found from it. Two descriptor sets
//! are written: one with source info, which the code generator turns into doc
//! comments, and one without, which the client compiles in.

use std::path::{Path, PathBuf};

use prost::Message as _;

/// The protos, by the path they are imported by.
const FILES: &[&str] = &[
    "proto/protovcs/v1/annotations.proto",
    "proto/protovcs/v1/layout.proto",
    "proto/protovcs/v1/store.proto",
    "proto/protovcs/v1/storeadminservice.proto",
    "proto/protovcs/v1/storeservice.proto",
    "proto/protovcs/v1/token.proto",
];

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let root = match std::env::var_os("PROTOVCS_STORE_PROTO") {
        Some(store) => PathBuf::from(store)
            .ancestors()
            .nth(4)
            .ok_or("PROTOVCS_STORE_PROTO is not proto/protovcs/v1/store.proto")?
            .to_path_buf(),
        None => PathBuf::from(std::env::var("CARGO_MANIFEST_DIR")?).join("../.."),
    };
    let out = PathBuf::from(std::env::var("OUT_DIR")?);
    for f in FILES {
        println!("cargo:rerun-if-changed={}", root.join(f).display());
    }

    let mut set = protox::Compiler::new([&root])?
        .include_imports(true)
        .include_source_info(true)
        .open_files(FILES)?
        .file_descriptor_set();
    let with_source = out.join("protovcs_with_source.binpb");
    std::fs::write(&with_source, set.encode_to_vec())?;

    for f in &mut set.file {
        f.source_code_info = None;
    }
    std::fs::write(out.join("protovcs.binpb"), set.encode_to_vec())?;

    generate(&with_source, &out)
}

fn generate(descriptors: &Path, out: &Path) -> Result<(), Box<dyn std::error::Error>> {
    connectrpc_build::Config::new()
        .descriptor_set(descriptors)
        .files(FILES)
        .out_dir(out)
        .file_per_package(true)
        .include_file("_connectrpc.rs")
        .compile()?;
    Ok(())
}
