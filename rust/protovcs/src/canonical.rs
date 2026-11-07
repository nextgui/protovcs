//! The one encoding of a message protovcs produces.
//!
//! A chunk is named by a fingerprint of these bytes, so every client must
//! produce exactly the same ones. The rules are the Go client's
//! (`canonical.go`):
//!
//! - Fields ascend by number, with unknown fields at their own numbers.
//! - Repeated scalars are always packed.
//! - Map entries are sorted by key, and each entry writes its key and value
//!   even when they are zero.
//! - Unknown fields are kept, record by record, in the order they arrived.
//!
//! buffa's own encoder is not used for these bytes: its maps iterate in hash
//! order and its unknown fields go last.

use buffa_descriptor::reflect::{MapKey, MapKeyRef, ReflectMessage, ValueRef};
use buffa_descriptor::{DynamicMessage, FieldDescriptor, FieldKind, ScalarType, SingularKind};

use crate::error::{Error, Result};
use crate::wire::{BYTES, FIXED32, FIXED64, VARINT, put_bytes, put_tag, put_uvarint, zigzag};

/// Returns the canonical encoding of a message.
///
/// # Errors
///
/// When a value does not match its field's declared kind.
pub fn canonical_bytes(m: &DynamicMessage) -> Result<Vec<u8>> {
    let mut out = Vec::new();
    append_message(&mut out, m)?;
    Ok(out)
}

pub(crate) fn append_message(out: &mut Vec<u8>, m: &dyn ReflectMessage) -> Result<()> {
    // Each field encoded on its own, then ordered by number. Stable, so
    // several unknown records at one number keep their order.
    let mut fields: Vec<(u32, Vec<u8>)> = Vec::new();
    let mut err = None;
    m.for_each_set(&mut |fd, v| {
        if err.is_some() {
            return;
        }
        let mut b = Vec::new();
        match append_field(&mut b, fd, v) {
            Ok(()) => fields.push((fd.number(), b)),
            Err(e) => err = Some(e),
        }
    });
    if let Some(e) = err {
        return Err(e);
    }
    for uf in m.unknown_fields().iter() {
        let mut b = Vec::new();
        uf.write_to(&mut b);
        fields.push((uf.number, b));
    }
    fields.sort_by_key(|(n, _)| *n);
    for (_, b) in fields {
        out.extend_from_slice(&b);
    }
    Ok(())
}

fn append_field(out: &mut Vec<u8>, fd: &FieldDescriptor, v: ValueRef<'_>) -> Result<()> {
    let number = fd.number();
    match (fd.kind(), v) {
        (FieldKind::Map { key, value }, ValueRef::Map(map)) => {
            let mut entries: Vec<(MapKey, Vec<u8>)> = Vec::new();
            let mut err = None;
            map.for_each(&mut |k, v| {
                if err.is_some() {
                    return;
                }
                let mut entry = Vec::new();
                append_map_key(&mut entry, key, k);
                match append_singular(&mut entry, 2, value, v) {
                    Ok(()) => entries.push((k.to_owned(), entry)),
                    Err(e) => err = Some(e),
                }
            });
            if let Some(e) = err {
                return Err(e);
            }
            entries.sort_by(|a, b| a.0.cmp(&b.0));
            for (_, entry) in entries {
                put_tag(out, number, BYTES);
                put_bytes(out, &entry);
            }
            Ok(())
        }
        (FieldKind::List(kind), ValueRef::List(list)) => {
            if list.is_empty() {
                return Ok(());
            }
            let mut err = None;
            if packable(kind) {
                let mut payload = Vec::new();
                list.for_each(&mut |v| {
                    if err.is_none()
                        && let Err(e) = append_scalar(&mut payload, kind, v)
                    {
                        err = Some(e);
                    }
                });
                put_tag(out, number, BYTES);
                put_bytes(out, &payload);
            } else {
                list.for_each(&mut |v| {
                    if err.is_none()
                        && let Err(e) = append_singular(out, number, kind, v)
                    {
                        err = Some(e);
                    }
                });
            }
            err.map_or(Ok(()), Err)
        }
        (FieldKind::Singular(kind), v) => append_singular(out, number, kind, v),
        (kind, v) => Err(mismatch(kind, &v)),
    }
}

fn append_map_key(out: &mut Vec<u8>, key: ScalarType, k: MapKeyRef<'_>) {
    let v = match k {
        MapKeyRef::Bool(b) => ValueRef::Bool(b),
        MapKeyRef::I32(i) => ValueRef::I32(i),
        MapKeyRef::I64(i) => ValueRef::I64(i),
        MapKeyRef::U32(u) => ValueRef::U32(u),
        MapKeyRef::U64(u) => ValueRef::U64(u),
        MapKeyRef::String(s) => ValueRef::String(s),
    };
    // A map key's kind always matches its value, so this cannot fail.
    let _ = append_singular(out, 1, SingularKind::Scalar(key), v);
}

fn append_singular(
    out: &mut Vec<u8>,
    number: u32,
    kind: SingularKind,
    v: ValueRef<'_>,
) -> Result<()> {
    match (kind, v) {
        // A group is written as a length-delimited message, as the Go client
        // does.
        (SingularKind::Message(_), ValueRef::Message(m)) => {
            let mut nested = Vec::new();
            append_message(&mut nested, &*m)?;
            put_tag(out, number, BYTES);
            put_bytes(out, &nested);
            Ok(())
        }
        (SingularKind::Scalar(ScalarType::String), ValueRef::String(s)) => {
            put_tag(out, number, BYTES);
            put_bytes(out, s.as_bytes());
            Ok(())
        }
        (SingularKind::Scalar(ScalarType::Bytes), ValueRef::Bytes(b)) => {
            put_tag(out, number, BYTES);
            put_bytes(out, b);
            Ok(())
        }
        (kind, v) => {
            put_tag(out, number, wire_type_of(kind));
            append_scalar(out, kind, v)
        }
    }
}

fn append_scalar(out: &mut Vec<u8>, kind: SingularKind, v: ValueRef<'_>) -> Result<()> {
    use ScalarType as T;
    match (kind, &v) {
        (SingularKind::Enum(_), ValueRef::EnumNumber(n)) => put_uvarint(out, i64::from(*n) as u64),
        (SingularKind::Scalar(t), _) => match (t, &v) {
            (T::Bool, ValueRef::Bool(b)) => put_uvarint(out, u64::from(*b)),
            (T::Int32, ValueRef::I32(i)) => put_uvarint(out, i64::from(*i) as u64),
            (T::Int64, ValueRef::I64(i)) => put_uvarint(out, *i as u64),
            (T::Uint32, ValueRef::U32(u)) => put_uvarint(out, u64::from(*u)),
            (T::Uint64, ValueRef::U64(u)) => put_uvarint(out, *u),
            (T::Sint32, ValueRef::I32(i)) => put_uvarint(out, zigzag(i64::from(*i))),
            (T::Sint64, ValueRef::I64(i)) => put_uvarint(out, zigzag(*i)),
            (T::Sfixed32, ValueRef::I32(i)) => out.extend_from_slice(&i.to_le_bytes()),
            (T::Fixed32, ValueRef::U32(u)) => out.extend_from_slice(&u.to_le_bytes()),
            (T::Float, ValueRef::F32(f)) => out.extend_from_slice(&f.to_bits().to_le_bytes()),
            (T::Sfixed64, ValueRef::I64(i)) => out.extend_from_slice(&i.to_le_bytes()),
            (T::Fixed64, ValueRef::U64(u)) => out.extend_from_slice(&u.to_le_bytes()),
            (T::Double, ValueRef::F64(f)) => out.extend_from_slice(&f.to_bits().to_le_bytes()),
            _ => return Err(mismatch(FieldKind::Singular(kind), &v)),
        },
        _ => return Err(mismatch(FieldKind::Singular(kind), &v)),
    }
    Ok(())
}

fn packable(kind: SingularKind) -> bool {
    !matches!(
        kind,
        SingularKind::Message(_)
            | SingularKind::Scalar(ScalarType::String)
            | SingularKind::Scalar(ScalarType::Bytes)
    )
}

fn wire_type_of(kind: SingularKind) -> u32 {
    match kind {
        SingularKind::Scalar(ScalarType::Fixed32 | ScalarType::Sfixed32 | ScalarType::Float) => {
            FIXED32
        }
        SingularKind::Scalar(ScalarType::Fixed64 | ScalarType::Sfixed64 | ScalarType::Double) => {
            FIXED64
        }
        _ => VARINT,
    }
}

fn mismatch(kind: FieldKind, v: &ValueRef<'_>) -> Error {
    Error::invalid(format!(
        "protovcs: cannot canonically encode {v:?} as {kind:?}"
    ))
}
