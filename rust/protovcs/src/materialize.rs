//! Decomposing a message into items and reassembling it. Port of
//! `materialize.go`.

use std::borrow::Cow;
use std::collections::{BTreeMap, HashSet};

use buffa_descriptor::reflect::{ReflectMessage, ReflectMessageMut, Value};
use buffa_descriptor::{DynamicMessage, FieldKind, ScalarType, SingularKind};

use crate::canonical::canonical_bytes;
use crate::error::{Error, ErrorKind, Result};
use crate::layout::{Compiled, CompiledSplit, Step, path_string, scalar_string, shard_value};
use crate::reflect::{empty, with_field};
use crate::types::{ChunkId, ItemKey, ROOT_ITEM_KEY};

/// One fragment of a message, and the unit a store holds. The fragment is a
/// partial instance of the root message, so reassembly is a merge.
#[derive(Clone, Debug)]
pub struct Item {
    /// Where the item is filed.
    pub key: ItemKey,
    /// The part of the message it holds.
    pub fragment: DynamicMessage,
    /// The canonical bytes of the fragment, when a writer has computed them.
    pub canonical: Vec<u8>,
    /// The name those bytes produce, when a writer has computed it.
    pub id: ChunkId,
}

impl Item {
    /// An item holding `fragment` at `key`.
    pub fn new(key: impl Into<ItemKey>, fragment: DynamicMessage) -> Self {
        Self {
            key: key.into(),
            fragment,
            canonical: Vec::new(),
            id: ChunkId::default(),
        }
    }
}

impl Compiled {
    /// Decomposes a message into the items a store holds, in key order.
    ///
    /// Every element of a split field goes to the item its shard key names,
    /// and everything else, unknown fields included, to the root item.
    ///
    /// # Errors
    ///
    /// When `m` is not the root message, a shard key has no value where one is
    /// needed, or two elements of one item share an identity.
    pub fn materialize(&self, m: &DynamicMessage) -> Result<Vec<Item>> {
        self.check_type(m, "")?;
        let mut by_key: BTreeMap<ItemKey, DynamicMessage> = BTreeMap::new();

        let mut root = m.clone();
        for s in &self.splits {
            clear_at(&mut root, &s.path);
        }
        if has_anything(&root) {
            by_key.insert(ROOT_ITEM_KEY.to_string(), root);
        }
        for s in &self.splits {
            self.materialize_split(m, s, &mut by_key)?;
        }
        Ok(by_key
            .into_iter()
            .map(|(k, frag)| Item::new(k, frag))
            .collect())
    }

    fn materialize_split(
        &self,
        root: &DynamicMessage,
        s: &CompiledSplit,
        by_key: &mut BTreeMap<ItemKey, DynamicMessage>,
    ) -> Result<()> {
        let Some(list) = list_at(root, &s.path) else {
            return Ok(());
        };
        let mut touched = HashSet::new();
        for v in list {
            let Value::Message(elem) = v else { continue };
            let key = self.item_key_in(s, elem)?;
            let frag = by_key
                .entry(key.clone())
                .or_insert_with(|| self.new_message());
            list_at_or_create(frag, &s.path).push(Value::Message(elem.clone()));
            touched.insert(key);
        }
        for key in touched {
            let frag = by_key.get_mut(&key).expect("touched keys are in the map");
            let list = list_at_or_create(frag, &s.path);
            if list.len() > 1 {
                sort_list(list, &s.order);
            }
            check_unique(s, &key, list)?;
        }
        Ok(())
    }

    /// The item an element of `field` belongs to. A pure function of the
    /// element.
    ///
    /// # Errors
    ///
    /// When `field` is not split, or a shard key has no value where one is
    /// needed.
    pub fn item_key_for(&self, field: &str, elem: &DynamicMessage) -> Result<ItemKey> {
        let s = self.split_for(field).ok_or_else(|| {
            Error::invalid(format!(
                "protovcs: layout {:?} does not split {field:?}",
                self.layout().name
            ))
        })?;
        self.item_key_in(s, elem)
    }

    fn item_key_in(&self, s: &CompiledSplit, elem: &DynamicMessage) -> Result<ItemKey> {
        let mut parts = Vec::with_capacity(s.by.len() + 1);
        parts.push(s.field.clone());
        for k in &s.by {
            let v = value_at(elem, &k.path);
            let segment = shard_value(self, k, v.as_deref()).map_err(|e| {
                Error::new(e.kind(), format!("protovcs: splitting {:?}: {e}", s.field))
            })?;
            parts.push(segment);
        }
        Ok(parts.join("/"))
    }

    /// Reassembles a message from items, merging them in key order.
    ///
    /// # Errors
    ///
    /// When an item holds a different message type.
    pub fn merge(&self, items: &[Item]) -> Result<DynamicMessage> {
        let mut sorted: Vec<&Item> = items.iter().collect();
        sorted.sort_by(|a, b| a.key.cmp(&b.key));
        let mut out = self.new_message();
        for it in sorted {
            self.check_type(&it.fragment, &it.key)?;
            out.merge(&it.fragment.encode_to_vec())?;
        }
        Ok(out)
    }

    /// Merges item plaintexts, in key order, into one message.
    pub(crate) fn merge_encoded<'a>(
        &self,
        items: impl IntoIterator<Item = (&'a str, &'a [u8])>,
    ) -> Result<DynamicMessage> {
        let mut sorted: Vec<(&str, &[u8])> = items.into_iter().collect();
        sorted.sort_by(|a, b| a.0.cmp(b.0));
        let mut out = self.new_message();
        for (key, b) in sorted {
            out.merge(b).map_err(|e| {
                Error::invalid(format!("protovcs: item {key:?} did not parse: {e}"))
            })?;
        }
        Ok(out)
    }

    /// Decodes one item's plaintext.
    pub(crate) fn decode_item(&self, key: &str, b: &[u8]) -> Result<DynamicMessage> {
        let mut m = self.new_message();
        m.merge(b)
            .map_err(|e| Error::invalid(format!("protovcs: item {key:?} did not parse: {e}")))?;
        Ok(m)
    }

    pub(crate) fn check_type(&self, m: &DynamicMessage, key: &str) -> Result<()> {
        let got = m.message_descriptor().full_name();
        if got != self.root_name() {
            return Err(Error::invalid(if key.is_empty() {
                format!(
                    "protovcs: layout {:?} decomposes {}, given {got}",
                    self.layout().name,
                    self.root_name()
                )
            } else {
                format!(
                    "protovcs: item {key:?} holds {got}, not {}",
                    self.root_name()
                )
            }));
        }
        Ok(())
    }

    /// How many split elements a fragment holds.
    pub(crate) fn element_count(&self, frag: &DynamicMessage) -> u32 {
        self.splits
            .iter()
            .filter_map(|s| list_at(frag, &s.path))
            .map(|l| l.len() as u32)
            .sum()
    }

    /// Appends one element to the field `field` of a fragment.
    pub(crate) fn append_element(
        &self,
        frag: &mut DynamicMessage,
        field: &str,
        elem: DynamicMessage,
    ) -> Result<()> {
        let s = self.split_for(field).ok_or_else(|| {
            Error::invalid(format!(
                "protovcs: layout {:?} does not split {field:?}",
                self.layout().name
            ))
        })?;
        list_at_or_create(frag, &s.path).push(Value::Message(elem));
        Ok(())
    }
}

/// Reads a field path, or `None` when some step is absent.
///
/// A scalar with no presence is always reported present, so a shard key
/// holding zero renders as zero rather than as absent.
pub(crate) fn value_at<'a>(m: &'a DynamicMessage, path: &[Step]) -> Option<Cow<'a, Value>> {
    let mut cur = m;
    for (i, step) in path.iter().enumerate() {
        let stored = cur.field_by_number(step.number);
        if i == path.len() - 1 {
            return match stored {
                Some(v) => Some(Cow::Borrowed(v)),
                None if step.explicit_presence => None,
                None => default_scalar(step.kind).map(Cow::Owned),
            };
        }
        match stored {
            Some(Value::Message(next)) => cur = next,
            _ => return None,
        }
    }
    None
}

fn default_scalar(kind: FieldKind) -> Option<Value> {
    use ScalarType as T;
    Some(match kind {
        FieldKind::Singular(SingularKind::Enum(_)) => Value::EnumNumber(0),
        FieldKind::Singular(SingularKind::Scalar(t)) => match t {
            T::Bool => Value::Bool(false),
            T::Int32 | T::Sint32 | T::Sfixed32 => Value::I32(0),
            T::Int64 | T::Sint64 | T::Sfixed64 => Value::I64(0),
            T::Uint32 | T::Fixed32 => Value::U32(0),
            T::Uint64 | T::Fixed64 => Value::U64(0),
            T::Float => Value::F32(0.0),
            T::Double => Value::F64(0.0),
            T::String => Value::String(String::new()),
            T::Bytes => Value::Bytes(Vec::new()),
        },
        _ => return None,
    })
}

pub(crate) fn list_at<'a>(m: &'a DynamicMessage, path: &[Step]) -> Option<&'a [Value]> {
    let mut cur = m;
    for (i, step) in path.iter().enumerate() {
        let stored = cur.field_by_number(step.number)?;
        if i == path.len() - 1 {
            return match stored {
                Value::List(l) if !l.is_empty() => Some(l),
                _ => None,
            };
        }
        match stored {
            Value::Message(next) => cur = next,
            _ => return None,
        }
    }
    None
}

pub(crate) fn list_at_or_create<'a>(
    m: &'a mut DynamicMessage,
    path: &[Step],
) -> &'a mut Vec<Value> {
    let (step, rest) = path.split_first().expect("a split path is never empty");
    if rest.is_empty() {
        if !matches!(m.field_by_number(step.number), Some(Value::List(_))) {
            with_field(m, step.number, |m, fd| m.set(fd, Value::List(Vec::new())));
        }
        match m.field_by_number_mut(step.number) {
            Some(Value::List(l)) => l,
            _ => unreachable!("the list was set above"),
        }
    } else {
        if !matches!(m.field_by_number(step.number), Some(Value::Message(_))) {
            let idx = step.message().expect("traversed steps are messages");
            let child = empty(m.pool(), idx);
            with_field(m, step.number, |m, fd| m.set(fd, Value::Message(child)));
        }
        match m.field_by_number_mut(step.number) {
            Some(Value::Message(child)) => list_at_or_create(child, rest),
            _ => unreachable!("the message was set above"),
        }
    }
}

fn clear_at(m: &mut DynamicMessage, path: &[Step]) {
    let (step, rest) = path.split_first().expect("a split path is never empty");
    if rest.is_empty() {
        with_field(m, step.number, |m, fd| m.clear(fd));
        return;
    }
    if let Some(Value::Message(child)) = m.field_by_number_mut(step.number) {
        clear_at(child, rest);
    }
}

/// Whether a message has any field or unknown bytes set.
pub(crate) fn has_anything(m: &DynamicMessage) -> bool {
    if !m.unknown_fields().is_empty() {
        return true;
    }
    let mut any = false;
    m.for_each_set(&mut |_, _| any = true);
    any
}

/// Orders an item's elements by the split's order fields, then by their
/// canonical bytes, so the same elements always encode the same way.
fn sort_list(list: &mut Vec<Value>, order: &[Vec<Step>]) {
    let fallback: Vec<Vec<u8>> = list
        .iter()
        .map(|v| match v {
            Value::Message(m) => canonical_bytes(m).unwrap_or_default(),
            _ => Vec::new(),
        })
        .collect();
    let keys: Vec<Vec<Option<String>>> = list
        .iter()
        .map(|v| {
            order
                .iter()
                .map(|path| match v {
                    Value::Message(m) => value_at(m, path).map(|x| scalar_string(&x)),
                    _ => None,
                })
                .collect()
        })
        .collect();
    let mut idx: Vec<usize> = (0..list.len()).collect();
    idx.sort_by(|&a, &b| {
        for (ka, kb) in keys[a].iter().zip(&keys[b]) {
            match (ka, kb) {
                (None, None) => continue,
                (None, Some(_)) => return std::cmp::Ordering::Less,
                (Some(_), None) => return std::cmp::Ordering::Greater,
                (Some(x), Some(y)) if x != y => return x.cmp(y),
                _ => continue,
            }
        }
        fallback[a].cmp(&fallback[b])
    });
    let old = std::mem::take(list);
    let mut slots: Vec<Option<Value>> = old.into_iter().map(Some).collect();
    for i in idx {
        list.push(slots[i].take().expect("each index is taken once"));
    }
}

fn check_unique(s: &CompiledSplit, key: &str, list: &[Value]) -> Result<()> {
    if s.identity.is_empty() || list.len() < 2 {
        return Ok(());
    }
    let mut seen = HashSet::new();
    for v in list {
        let Value::Message(m) = v else { continue };
        let Some(id) = value_at(m, &s.identity) else {
            continue;
        };
        let id = scalar_string(&id);
        if !seen.insert(id.clone()) {
            return Err(Error::new(
                ErrorKind::DuplicateIdentity,
                format!(
                    "item {key:?} holds two elements with {} {id:?}",
                    path_string(&s.identity)
                ),
            ));
        }
    }
    Ok(())
}
