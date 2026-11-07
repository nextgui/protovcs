//! protovcs's protos, generated with buffa and connect-rust from the `proto/`
//! directory at the root of the repository. The client re-exports them as
//! `protovcs::proto`.

#![allow(missing_docs)]

connectrpc::include_generated!();

/// Every proto here and everything they import, as a serialized
/// `FileDescriptorSet` without source info.
pub const DESCRIPTOR_SET: &[u8] = include_bytes!(concat!(env!("OUT_DIR"), "/protovcs.binpb"));
