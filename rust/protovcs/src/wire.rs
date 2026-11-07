//! The few protobuf wire-format operations the canonical encoder needs.

/// Wire type of a varint.
pub(crate) const VARINT: u32 = 0;
/// Wire type of a fixed 64-bit value.
pub(crate) const FIXED64: u32 = 1;
/// Wire type of a length-delimited value.
pub(crate) const BYTES: u32 = 2;
/// Wire type of a fixed 32-bit value.
pub(crate) const FIXED32: u32 = 5;

pub(crate) fn put_uvarint(out: &mut Vec<u8>, mut x: u64) {
    while x >= 0x80 {
        out.push((x as u8) | 0x80);
        x >>= 7;
    }
    out.push(x as u8);
}

pub(crate) fn put_tag(out: &mut Vec<u8>, number: u32, wire_type: u32) {
    put_uvarint(out, (u64::from(number) << 3) | u64::from(wire_type));
}

pub(crate) fn put_bytes(out: &mut Vec<u8>, b: &[u8]) {
    put_uvarint(out, b.len() as u64);
    out.extend_from_slice(b);
}

pub(crate) fn zigzag(v: i64) -> u64 {
    ((v << 1) ^ (v >> 63)) as u64
}

/// Reads a varint from the front of `b`, returning it and its length.
pub(crate) fn read_uvarint(b: &[u8]) -> Option<(u64, usize)> {
    let mut x = 0u64;
    for (i, byte) in b.iter().enumerate().take(10) {
        x |= u64::from(byte & 0x7f) << (7 * i);
        if byte & 0x80 == 0 {
            return Some((x, i + 1));
        }
    }
    None
}

/// Finds the last length-delimited value of field `number` at the top level
/// of an encoded message.
pub(crate) fn find_bytes_field(mut b: &[u8], number: u32) -> Option<&[u8]> {
    let mut found = None;
    while !b.is_empty() {
        let (tag, n) = read_uvarint(b)?;
        b = &b[n..];
        let (num, wt) = ((tag >> 3) as u32, (tag & 7) as u32);
        let len = match wt {
            VARINT => read_uvarint(b)?.1,
            FIXED64 => 8,
            FIXED32 => 4,
            BYTES => {
                let (l, n) = read_uvarint(b)?;
                let end = n.checked_add(usize::try_from(l).ok()?)?;
                if end > b.len() {
                    return None;
                }
                if num == number {
                    found = Some(&b[n..end]);
                }
                end
            }
            _ => return None,
        };
        if len > b.len() {
            return None;
        }
        b = &b[len..];
    }
    found
}
