//! Moving between generated buffa messages and dynamic ones, and the
//! descriptors of protovcs's own protos.
//!
//! The canonical encoder works on dynamic messages, as the Go one works on
//! protoreflect. A generated message is converted by encoding it and decoding
//! the bytes against a pool holding its descriptor.

use std::sync::{Arc, OnceLock};

use buffa::{Message, MessageName};
use buffa_descriptor::{DescriptorPool, DynamicMessage, FieldDescriptor, MessageIndex};

use crate::canonical::canonical_bytes;
use crate::error::{Error, Result};

/// The FileDescriptorSet of protovcs's protos and everything they import,
/// written by the build. See `descriptor_set_file` in `defs.bzl`.
static OWN_DESCRIPTORS: &[u8] = protovcs_proto::DESCRIPTOR_SET;

/// The pool holding protovcs's own messages.
pub(crate) fn own_pool() -> &'static Arc<DescriptorPool> {
    static POOL: OnceLock<Arc<DescriptorPool>> = OnceLock::new();
    POOL.get_or_init(|| {
        Arc::new(
            DescriptorPool::decode(OWN_DESCRIPTORS)
                .expect("the descriptor set compiled into this crate is valid"),
        )
    })
}

/// A generated message as a dynamic one in `pool`.
pub(crate) fn to_dynamic<M: Message + MessageName>(
    pool: &Arc<DescriptorPool>,
    m: &M,
) -> Result<DynamicMessage> {
    let idx = pool
        .message_index(M::FULL_NAME)
        .ok_or_else(|| Error::invalid(format!("protovcs: no descriptor for {}", M::FULL_NAME)))?;
    Ok(DynamicMessage::decode(
        Arc::clone(pool),
        idx,
        &m.encode_to_vec(),
    )?)
}

/// The canonical bytes of one of protovcs's own messages.
pub(crate) fn canonical_own<M: Message + MessageName>(m: &M) -> Result<Vec<u8>> {
    canonical_bytes(&to_dynamic(own_pool(), m)?)
}

/// A dynamic message as a generated one.
pub(crate) fn to_typed<M: Message + Default>(m: &DynamicMessage) -> Result<M> {
    Ok(M::decode_from_slice(&m.encode_to_vec())?)
}

/// Calls `f` with `m` and the descriptor of its field `number`.
///
/// The descriptor is borrowed from the pool, not cloned: a dynamic message
/// accepts only its own pool's descriptors, compared by address.
pub(crate) fn with_field<R>(
    m: &mut DynamicMessage,
    number: u32,
    f: impl FnOnce(&mut DynamicMessage, &FieldDescriptor) -> R,
) -> R {
    let pool = Arc::clone(m.pool());
    let fd = pool
        .message(m.message_index())
        .field(number)
        .expect("compiled field paths name fields of the message they were compiled for");
    f(m, fd)
}

/// An empty message of the type `idx` names.
pub(crate) fn empty(pool: &Arc<DescriptorPool>, idx: MessageIndex) -> DynamicMessage {
    DynamicMessage::new(Arc::clone(pool), idx)
}
