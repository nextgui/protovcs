//! A layout resolved against the message it decomposes. Port of
//! `layout.go`.

use std::collections::{BTreeMap, HashSet};
use std::sync::Arc;

use buffa::{Message, MessageName};
use buffa_descriptor::reflect::{ReflectMessage, Value};
use buffa_descriptor::{
    DescriptorPool, DynamicMessage, FieldKind, MessageIndex, ScalarType, SingularKind,
};
use sha2::{Digest, Sha256};

use crate::error::{Error, ErrorKind, Result};
use crate::manifest::ItemRef;
use crate::partial::Scope;
use crate::proto::{Layout, Op, Query, ShardKey, Transform};
use crate::reflect;
use crate::types::ROOT_ITEM_KEY;
use crate::wire::find_bytes_field;

/// The field number of the `(nextgui.protovcs.v1.identity)` field option.
const IDENTITY_EXTENSION: u32 = 50401;

/// One step of a resolved field path.
#[derive(Clone, Debug)]
pub(crate) struct Step {
    pub(crate) number: u32,
    pub(crate) name: String,
    pub(crate) kind: FieldKind,
    pub(crate) explicit_presence: bool,
}

impl Step {
    pub(crate) fn message(&self) -> Option<MessageIndex> {
        match self.kind {
            FieldKind::Singular(SingularKind::Message(m))
            | FieldKind::List(SingularKind::Message(m)) => Some(m),
            _ => None,
        }
    }

    fn is_list(&self) -> bool {
        matches!(self.kind, FieldKind::List(_))
    }

    fn is_map(&self) -> bool {
        matches!(self.kind, FieldKind::Map { .. })
    }

    fn is_singular_message(&self) -> bool {
        matches!(self.kind, FieldKind::Singular(SingularKind::Message(_)))
    }

    fn kind_name(&self) -> String {
        match self.kind {
            FieldKind::Singular(SingularKind::Scalar(t)) => format!("{t:?}").to_lowercase(),
            FieldKind::Singular(SingularKind::Enum(_)) => "enum".into(),
            FieldKind::Singular(SingularKind::Message(_)) => "message".into(),
            _ => "repeated".into(),
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) struct CompiledShardKey {
    pub(crate) path: Vec<Step>,
    pub(crate) transform: Transform,
    pub(crate) buckets: u32,
}

#[derive(Clone, Debug)]
pub(crate) struct CompiledSplit {
    pub(crate) identity: Vec<Step>,
    pub(crate) field: String,
    pub(crate) path: Vec<Step>,
    pub(crate) by: Vec<CompiledShardKey>,
    pub(crate) order: Vec<Vec<Step>>,
    pub(crate) split_byte_count: u32,
    pub(crate) max_item_bytes: u32,
}

/// A layout resolved against the message it decomposes.
///
/// Every check a layout has to pass happens when it is compiled, so a mistake
/// is reported before a store is opened rather than on the first write.
#[derive(Clone, Debug)]
pub struct Compiled {
    layout: Layout,
    pool: Arc<DescriptorPool>,
    root: MessageIndex,
    pub(crate) splits: Vec<CompiledSplit>,
    commit_message: Option<MessageIndex>,
    unsplit: Vec<String>,
}

/// What a split thinks of one item's size.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ItemSize {
    /// At or under `split_byte_count`, or the layout set none.
    WithinTarget,
    /// Over `split_byte_count` and under any ceiling.
    OverTarget,
    /// Over `max_item_bytes` with more than one element; refused.
    OverCeiling,
    /// Over `max_item_bytes` and holding one element, which nothing divides.
    OneOversizedElement,
}

impl Compiled {
    /// Resolves a layout against the message `root_message` names in `pool`.
    ///
    /// # Errors
    ///
    /// For a layout naming a field that does not exist, a transform that does
    /// not apply to its field, a query no split serves, or a commit message
    /// the root message's files do not reach.
    pub fn new(l: Layout, pool: Arc<DescriptorPool>, root_message: &str) -> Result<Self> {
        let root = pool.message_index(root_message).ok_or_else(|| {
            Error::invalid(format!(
                "protovcs: layout {:?} has no message to resolve against: {root_message} is not in the pool",
                l.name
            ))
        })?;
        if !l.root_message.is_empty() && l.root_message != root_message {
            return Err(Error::invalid(format!(
                "protovcs: layout {:?} is for {}, not {root_message}",
                l.name, l.root_message
            )));
        }

        let mut c = Self {
            layout: l.clone(),
            pool: Arc::clone(&pool),
            root,
            splits: Vec::new(),
            commit_message: None,
            unsplit: Vec::new(),
        };

        let mut seen = HashSet::new();
        for s in &l.splits {
            let cs = c.compile_split(s)?;
            if !seen.insert(cs.field.clone()) {
                return Err(Error::invalid(format!(
                    "protovcs: layout {:?} splits {:?} twice",
                    l.name, cs.field
                )));
            }
            c.splits.push(cs);
        }

        let mut scopes = HashSet::new();
        for s in &l.scopes {
            if s.name.is_empty() {
                return Err(Error::invalid(format!(
                    "protovcs: layout {:?} has a scope with no name",
                    l.name
                )));
            }
            if !scopes.insert(s.name.clone()) {
                return Err(Error::invalid(format!(
                    "protovcs: layout {:?} declares scope {:?} twice",
                    l.name, s.name
                )));
            }
            for inc in &s.include {
                c.resolve_path(root, &inc.field)
                    .map_err(|e| Error::invalid(format!("protovcs: scope {:?}: {e}", s.name)))?;
            }
        }

        for fd in pool.message(root).fields() {
            if matches!(fd.kind(), FieldKind::List(_) | FieldKind::Map { .. })
                && !seen.contains(fd.name())
            {
                c.unsplit.push(fd.name().to_string());
            }
        }

        for q in &l.queries {
            c.check_query(q)?;
        }

        if !l.commit_message.is_empty() {
            c.commit_message = Some(c.resolve_message(&l.commit_message).map_err(|e| {
                Error::invalid(format!(
                    "protovcs: layout {:?} declares commit_message {:?}: {e}",
                    l.name, l.commit_message
                ))
            })?);
        }
        Ok(c)
    }

    /// The layout this was compiled from.
    pub fn layout(&self) -> &Layout {
        &self.layout
    }

    /// The pool the root message is described in.
    pub fn pool(&self) -> &Arc<DescriptorPool> {
        &self.pool
    }

    /// The root message's index in [`Compiled::pool`].
    pub fn root(&self) -> MessageIndex {
        self.root
    }

    /// The root message's full name.
    pub fn root_name(&self) -> &str {
        self.pool.message(self.root).full_name()
    }

    /// What the layout says the data means.
    pub fn schema_version(&self) -> u32 {
        self.layout.schema_version
    }

    /// The repeated fields no rule mentions, which live in the root item.
    pub fn unsplit_collections(&self) -> &[String] {
        &self.unsplit
    }

    /// The message a commit may carry, when the layout declares one.
    pub fn commit_message(&self) -> Option<MessageIndex> {
        self.commit_message
    }

    /// An empty instance of the root message.
    pub fn new_message(&self) -> DynamicMessage {
        reflect::empty(&self.pool, self.root)
    }

    /// Converts a generated message to the dynamic root message.
    ///
    /// # Errors
    ///
    /// When `M` is not the root message or does not decode against the pool.
    pub fn from_typed<M: Message + MessageName>(&self, m: &M) -> Result<DynamicMessage> {
        if M::FULL_NAME != self.root_name() {
            return Err(Error::invalid(format!(
                "protovcs: layout {:?} decomposes {}, given {}",
                self.layout.name,
                self.root_name(),
                M::FULL_NAME
            )));
        }
        reflect::to_dynamic(&self.pool, m)
    }

    /// Converts a dynamic message to a generated one.
    ///
    /// # Errors
    ///
    /// When the bytes do not decode as `M`.
    pub fn to_typed<M: Message + Default>(&self, m: &DynamicMessage) -> Result<M> {
        reflect::to_typed(m)
    }

    /// The scope a caller needs to read one field.
    ///
    /// # Errors
    ///
    /// When the field does not exist.
    pub fn scope_for_field(&self, name: &str) -> Result<Scope> {
        self.resolve_path(self.root, name)
            .map_err(|e| Error::invalid(format!("protovcs: layout {:?}: {e}", self.layout.name)))?;
        if self.split_for(name).is_some() {
            Ok(Scope::field(name))
        } else {
            Ok(Scope::root())
        }
    }

    pub(crate) fn split_for(&self, field: &str) -> Option<&CompiledSplit> {
        self.splits.iter().find(|s| s.field == field)
    }

    fn resolve_message(&self, name: &str) -> std::result::Result<MessageIndex, String> {
        let root_file = self
            .pool
            .file_containing_symbol(self.root_name())
            .and_then(|f| f.name.clone())
            .unwrap_or_default();
        let mut seen = HashSet::new();
        let mut stack = vec![root_file.clone()];
        let target = self
            .pool
            .file_containing_symbol(name)
            .and_then(|f| f.name.clone());
        while let Some(file) = stack.pop() {
            if !seen.insert(file.clone()) {
                continue;
            }
            if Some(&file) == target.as_ref()
                && let Some(idx) = self.pool.message_index(name)
            {
                return Ok(idx);
            }
            if let Some(fd) = self.pool.file_by_name(&file) {
                stack.extend(fd.dependency.iter().cloned());
            }
        }
        Err(format!(
            "no such message among {root_file} and what it imports"
        ))
    }

    /// Turns `"a.b.c"` into the steps that reach it from `md`.
    pub(crate) fn resolve_path(
        &self,
        md: MessageIndex,
        path: &str,
    ) -> std::result::Result<Vec<Step>, String> {
        if path.is_empty() {
            return Err("an empty field path".into());
        }
        let parts: Vec<&str> = path.split('.').collect();
        let mut out = Vec::new();
        let mut cur = Some(md);
        for (i, part) in parts.iter().enumerate() {
            let Some(m) = cur else {
                return Err(format!(
                    "field path {path:?} goes through {:?}, which is not a message",
                    parts[i - 1]
                ));
            };
            let desc = self.pool.message(m);
            let fd = desc.field_by_name(part).ok_or_else(|| {
                format!(
                    "message {} has no field {part:?}, named in path {path:?}",
                    desc.full_name()
                )
            })?;
            let step = Step {
                number: fd.number(),
                name: fd.name().to_string(),
                kind: fd.kind(),
                explicit_presence: fd.presence() != buffa::editions::FieldPresence::Implicit,
            };
            let last = i == parts.len() - 1;
            if !last {
                if step.is_list() || step.is_map() {
                    return Err(format!(
                        "field path {path:?} goes through repeated field {part:?}, which is not one value but one per element.\n\
                         Reassembly here is proto.Merge, which concatenates repeated fields rather than\n\
                         matching them up, so two fragments each holding one {part:?} would merge into two of\n\
                         them rather than one with both halves.\n\
                         Flatten instead: give the inner elements a field of their own on the message,\n\
                         carrying the identity of the {part:?} they belong to, and split that by it.\n\
                         (A property of the reassembly rule, not of the data. Grouping fragments by\n\
                         item key prefix before merging would make this work, given the outer split's\n\
                         shard key identifies one element rather than grouping several.)"
                    ));
                }
                if !step.is_singular_message() {
                    return Err(format!(
                        "field path {path:?} goes through {part:?}, which is not a message"
                    ));
                }
                cur = step.message();
            }
            out.push(step);
        }
        Ok(out)
    }

    fn compile_split(&self, s: &crate::proto::Split) -> Result<CompiledSplit> {
        let path = self
            .resolve_path(self.root, &s.field)
            .map_err(|e| Error::invalid(format!("protovcs: {e}")))?;
        let leaf = path.last().expect("a resolved path is never empty");
        if leaf.is_map() {
            return Err(Error::invalid(format!(
                "protovcs: field {:?} is a map, and splitting maps is not implemented.\n\
                 When it is, the shard key will be the map key — it is unique by\n\
                 construction, which is what a shard key needs.",
                s.field
            )));
        }
        if !leaf.is_list() {
            return Err(Error::invalid(format!(
                "protovcs: field {:?} is not repeated, so it cannot be split; use an extract",
                s.field
            )));
        }
        let Some(elem) = leaf.message() else {
            return Err(Error::invalid(format!(
                "protovcs: field {:?} is a repeated scalar, so it has no shard key to split on",
                s.field
            )));
        };
        let mut cs = CompiledSplit {
            identity: Vec::new(),
            field: s.field.clone(),
            path: path.clone(),
            by: Vec::new(),
            order: Vec::new(),
            split_byte_count: s.split_byte_count,
            max_item_bytes: s.max_item_bytes,
        };
        if cs.max_item_bytes > 0 && cs.split_byte_count > cs.max_item_bytes {
            return Err(Error::invalid(format!(
                "protovcs: split of {:?} aims for {} bytes and refuses above {}, so every item it aims for is one it refuses",
                s.field, cs.split_byte_count, cs.max_item_bytes
            )));
        }
        if s.by.is_empty() {
            return Err(Error::invalid(format!(
                "protovcs: split of {:?} names no shard key",
                s.field
            )));
        }
        for by in &s.by {
            let key_path = self
                .resolve_path(elem, &by.field)
                .map_err(|e| Error::invalid(format!("protovcs: split of {:?}: {e}", s.field)))?;
            self.check_transform(by, key_path.last().expect("non-empty"))
                .map_err(|e| Error::invalid(format!("protovcs: split of {:?}: {e}", s.field)))?;
            cs.by.push(CompiledShardKey {
                path: key_path,
                transform: transform_of(by),
                buckets: by.buckets,
            });
        }

        let identity = self.identity_of(leaf.number, &path)?;
        if !identity.is_empty() {
            let id_path = self
                .resolve_path(elem, &identity)
                .map_err(|e| Error::invalid(format!("protovcs: identity of {:?}: {e}", s.field)))?;
            let last = id_path.last().expect("non-empty");
            if last.is_list() || last.is_map() || last.message().is_some() {
                return Err(Error::invalid(format!(
                    "protovcs: identity of {:?} is {identity:?}, which is not a scalar and so cannot tell two elements apart",
                    s.field
                )));
            }
            cs.identity = id_path;
        }

        for o in &s.order {
            let order_path = self.resolve_path(elem, o).map_err(|e| {
                Error::invalid(format!("protovcs: split of {:?} order: {e}", s.field))
            })?;
            let last = order_path.last().expect("non-empty");
            if last.is_list() || last.is_map() || last.message().is_some() {
                return Err(Error::invalid(format!(
                    "protovcs: split of {:?} cannot order by {o:?}, which is not a scalar",
                    s.field
                )));
            }
            cs.order.push(order_path);
        }
        if cs.order.is_empty() && !cs.identity.is_empty() {
            cs.order.push(cs.identity.clone());
        }
        Ok(cs)
    }

    /// Reads the identity annotation from the repeated field the path ends at.
    fn identity_of(&self, number: u32, path: &[Step]) -> Result<String> {
        let parent = if path.len() > 1 {
            path[path.len() - 2]
                .message()
                .expect("traversed steps are messages")
        } else {
            self.root
        };
        let Some(fd) = self.pool.message(parent).field(number) else {
            return Ok(String::new());
        };
        let Some(opts) = fd.options() else {
            return Ok(String::new());
        };
        let encoded = opts.encode_to_vec();
        Ok(find_bytes_field(&encoded, IDENTITY_EXTENSION)
            .map(|b| String::from_utf8_lossy(b).into_owned())
            .unwrap_or_default())
    }

    fn check_transform(&self, by: &ShardKey, step: &Step) -> std::result::Result<(), String> {
        if step.is_list() || step.is_map() {
            return Err(format!(
                "shard key {:?} is repeated, so it is not one value",
                by.field
            ));
        }
        match transform_of(by) {
            Transform::TRANSFORM_IDENTITY | Transform::TRANSFORM_UNSPECIFIED => {
                if step.message().is_some() {
                    return Err(format!(
                        "shard key {:?} is a message, which has no identity form",
                        by.field
                    ));
                }
                Ok(())
            }
            Transform::TRANSFORM_DAY
            | Transform::TRANSFORM_MONTH
            | Transform::TRANSFORM_QUARTER
            | Transform::TRANSFORM_YEAR => {
                let Some(m) = step.message() else {
                    return Err(format!(
                        "shard key {:?} is {}, and a date transform needs a date or timestamp",
                        by.field,
                        step.kind_name()
                    ));
                };
                if !self.is_date_shaped(m) && !self.is_timestamp(m) {
                    return Err(format!(
                        "shard key {:?} is {}, which is neither a date nor a timestamp",
                        by.field,
                        self.pool.message(m).full_name()
                    ));
                }
                Ok(())
            }
            Transform::TRANSFORM_HASH => {
                if by.buckets == 0 {
                    return Err(format!("shard key {:?} hashes into no buckets", by.field));
                }
                if step.message().is_some() {
                    return Err(format!(
                        "shard key {:?} is a message, which cannot be hashed into buckets",
                        by.field
                    ));
                }
                Ok(())
            }
        }
    }

    pub(crate) fn is_date_shaped(&self, m: MessageIndex) -> bool {
        let md = self.pool.message(m);
        ["year", "month", "day"].iter().all(|n| {
            md.field_by_name(n).is_some_and(|fd| {
                matches!(
                    fd.kind(),
                    FieldKind::Singular(SingularKind::Scalar(ScalarType::Int32))
                )
            })
        })
    }

    pub(crate) fn is_timestamp(&self, m: MessageIndex) -> bool {
        self.pool.message(m).full_name() == "google.protobuf.Timestamp"
    }

    fn check_query(&self, q: &Query) -> Result<()> {
        if q.name.is_empty() {
            return Err(Error::invalid(format!(
                "protovcs: layout {:?} has a query with no name",
                self.layout.name
            )));
        }
        let Some(split) = self.split_for(&q.field) else {
            return Err(Error::invalid(format!(
                "protovcs: query {:?} reads {:?}, which no split covers, so it would scan the whole store",
                q.name, q.field
            )));
        };
        for (i, p) in q.by.iter().enumerate() {
            let Some(want) = split.by.get(i) else {
                return Err(Error::invalid(format!(
                    "protovcs: query {:?} has more conditions than {:?} has shard key components",
                    q.name, q.field
                )));
            };
            let want_path = path_string(&want.path);
            if p.field != want_path {
                return Err(Error::invalid(format!(
                    "protovcs: query {:?} filters on {:?} where {:?}'s shard key has {want_path:?} at position {i}; \
                     reorder the query, or add the field to the split",
                    q.name, p.field, q.field
                )));
            }
            let op = p.op.as_known().unwrap_or(Op::OP_UNSPECIFIED);
            if op == Op::OP_RANGE && want.transform == Transform::TRANSFORM_HASH {
                return Err(Error::invalid(format!(
                    "protovcs: query {:?} asks for a range over {:?}, which is hashed into buckets and has no order",
                    q.name, p.field
                )));
            }
            if op == Op::OP_RANGE && i != q.by.len() - 1 {
                return Err(Error::invalid(format!(
                    "protovcs: query {:?} ranges over {:?} and then filters further; only the last condition may be a range",
                    q.name, p.field
                )));
            }
        }
        Ok(())
    }

    /// How one item stands against its split's two sizes.
    pub fn classify_item(&self, key: &str, bytes: u64, elements: u32) -> ItemSize {
        if key == ROOT_ITEM_KEY {
            let max = self.layout.root_max_bytes;
            if max > 0 && bytes > u64::from(max) {
                return ItemSize::OverCeiling;
            }
            return ItemSize::WithinTarget;
        }
        let Some(split) = self.split_for(field_of(key)) else {
            return ItemSize::WithinTarget;
        };
        if split.max_item_bytes > 0 && bytes > u64::from(split.max_item_bytes) {
            if elements <= 1 {
                return ItemSize::OneOversizedElement;
            }
            return ItemSize::OverCeiling;
        }
        if split.split_byte_count > 0 && bytes > u64::from(split.split_byte_count) {
            return ItemSize::OverTarget;
        }
        ItemSize::WithinTarget
    }

    /// Refuses an item over its split's ceiling.
    ///
    /// # Errors
    ///
    /// [`ErrorKind::ItemTooLarge`], saying which field to add a shard key to.
    pub fn check_item_size(&self, key: &str, bytes: u64, elements: u32) -> Result<()> {
        if self.classify_item(key, bytes, elements) != ItemSize::OverCeiling {
            return Ok(());
        }
        if key == ROOT_ITEM_KEY {
            let hint = if self.unsplit.is_empty() {
                String::new()
            } else {
                format!("\nNo rule mentions: {}", self.unsplit.join(", "))
            };
            return Err(Error::new(
                ErrorKind::ItemTooLarge,
                format!(
                    "the root item is {bytes} bytes and this layout refuses above {}.\n\
                     The root item holds every field no rule mentions, and every reader fetches it.\n\
                     A repeated field that grows is the usual cause: split it, so it is fetched by\n\
                     the readers that want it instead of by all of them.{hint}",
                    self.layout.root_max_bytes
                ),
            ));
        }
        let Some(split) = self.split_for(field_of(key)) else {
            return Ok(());
        };
        Err(Error::new(
            ErrorKind::ItemTooLarge,
            format!(
                "item {key:?} holds {elements} elements in {bytes} bytes, and {:?} refuses above {}; \
                 add a shard key component to {:?} so they divide further",
                split.field, split.max_item_bytes, split.field
            ),
        ))
    }

    /// Every item over its split's target, so a caller can warn.
    pub fn oversized_items(&self, refs: &[ItemRef]) -> Vec<ItemRef> {
        refs.iter()
            .filter(|r| {
                matches!(
                    self.classify_item(&r.key, r.bytes, r.elements),
                    ItemSize::OverTarget | ItemSize::OneOversizedElement
                )
            })
            .cloned()
            .collect()
    }

    pub(crate) fn check_changed_item_sizes(
        &self,
        changes: &BTreeMap<String, Option<ItemRef>>,
    ) -> Result<()> {
        for (key, r) in changes {
            if let Some(r) = r {
                self.check_item_size(key, r.bytes, r.elements)?;
            }
        }
        Ok(())
    }

    /// The identity of a split's elements, or an error naming what to annotate.
    ///
    /// # Errors
    ///
    /// When the field is not split or declares no identity.
    pub fn require_identity(&self, field: &str) -> Result<String> {
        let Some(split) = self.split_for(field) else {
            return Err(Error::invalid(format!(
                "protovcs: layout {:?} does not split {field:?}",
                self.layout.name
            )));
        };
        if split.identity.is_empty() {
            return Err(Error::invalid(format!(
                "protovcs: {field:?} declares no identity, so its elements cannot be told apart across versions.\n\
                 Annotate the field in the proto:\n  repeated Elem {field} = N [(nextgui.protovcs.v1.identity) = \"id\"];"
            )));
        }
        Ok(path_string(&split.identity))
    }

    /// The field that tells one element of a split apart, or empty.
    pub fn identity(&self, field: &str) -> String {
        self.split_for(field)
            .filter(|s| !s.identity.is_empty())
            .map(|s| path_string(&s.identity))
            .unwrap_or_default()
    }
}

fn transform_of(by: &ShardKey) -> Transform {
    by.transform
        .as_known()
        .unwrap_or(Transform::TRANSFORM_UNSPECIFIED)
}

pub(crate) fn path_string(path: &[Step]) -> String {
    path.iter()
        .map(|s| s.name.as_str())
        .collect::<Vec<_>>()
        .join(".")
}

/// The split field an item key belongs to.
pub(crate) fn field_of(key: &str) -> &str {
    key.split_once('/').map_or(key, |(f, _)| f)
}

/// Renders one shard key component as the segment that appears in an item key.
pub(crate) fn shard_value(c: &Compiled, k: &CompiledShardKey, v: Option<&Value>) -> Result<String> {
    let leaf = k.path.last().expect("non-empty");
    match k.transform {
        Transform::TRANSFORM_IDENTITY | Transform::TRANSFORM_UNSPECIFIED => {
            Ok(v.map(scalar_string).unwrap_or_default())
        }
        Transform::TRANSFORM_DAY
        | Transform::TRANSFORM_MONTH
        | Transform::TRANSFORM_QUARTER
        | Transform::TRANSFORM_YEAR => {
            let Some(v) = v else {
                return Err(Error::invalid(format!(
                    "protovcs: {} has no value, so the element belongs to no period",
                    path_string(&k.path)
                )));
            };
            let (y, m, d) = date_parts(c, leaf, v)?;
            Ok(date_segment(k.transform, y, m, d))
        }
        Transform::TRANSFORM_HASH => {
            let raw = v.map(scalar_string).unwrap_or_default();
            let sum = Sha256::digest(raw.as_bytes());
            let head = u64::from_be_bytes(sum[..8].try_into().expect("8 bytes"));
            let bucket = head % u64::from(k.buckets);
            let width = (k.buckets - 1).to_string().len();
            Ok(format!("b{bucket:0width$}"))
        }
    }
}

fn date_parts(c: &Compiled, leaf: &Step, v: &Value) -> Result<(i64, i64, i64)> {
    let Value::Message(m) = v else {
        return Err(Error::invalid(
            "protovcs: a date transform read a non-message value",
        ));
    };
    let get = |name: &str| -> i64 {
        let Some(fd) = m.message_descriptor().field_by_name(name) else {
            return 0;
        };
        match m.field_by_number(fd.number()) {
            Some(Value::I32(i)) => i64::from(*i),
            Some(Value::I64(i)) => *i,
            _ => 0,
        }
    };
    if leaf.message().is_some_and(|i| c.is_timestamp(i)) {
        let (y, mo, d) = unix_to_date(get("seconds"));
        return Ok((y, mo, d));
    }
    let (y, mo, d) = (get("year"), get("month"), get("day"));
    if !(1..=12).contains(&mo) || !(1..=31).contains(&d) {
        return Err(Error::invalid(format!(
            "protovcs: {} holds {}-{:02}-{:02}, which is not a date",
            leaf.name,
            go_pad(y, 4),
            mo,
            d
        )));
    }
    Ok((y, mo, d))
}

/// Reads a timestamp's seconds as a calendar date in UTC.
pub(crate) fn unix_to_date(sec: i64) -> (i64, i64, i64) {
    let days = sec.div_euclid(86_400);
    // Howard Hinnant's civil_from_days.
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    (if m <= 2 { y + 1 } else { y }, m, d)
}

/// Go's `%0Nd`: zero-padded to `width`, with the sign counted in the width.
fn go_pad(v: i64, width: usize) -> String {
    format!("{v:0width$}")
}

/// Formats one segment of an item key from a date, widest part first.
pub fn date_segment(t: Transform, year: i64, month: i64, day: i64) -> String {
    match t {
        Transform::TRANSFORM_YEAR => go_pad(year, 4),
        Transform::TRANSFORM_QUARTER => format!("{}/Q{}", go_pad(year, 4), (month - 1) / 3 + 1),
        Transform::TRANSFORM_MONTH => format!("{}/{:02}", go_pad(year, 4), month),
        _ => format!("{}/{:02}/{:02}", go_pad(year, 4), month, day),
    }
}

/// Formats a string as one segment, escaping what would invent a directory.
pub fn string_segment(v: &str) -> String {
    escape_segment(v.as_bytes())
}

/// Formats a signed integer so that string order matches numeric order.
pub fn int_segment(v: i64) -> String {
    if v < 0 {
        // wrapping_neg matches Go for the one value that has no negation.
        format!("-{:019}", v.wrapping_neg())
    } else {
        format!("0{v:019}")
    }
}

/// Formats an unsigned integer as one segment.
pub fn uint_segment(v: u64) -> String {
    format!("{v:020}")
}

/// Formats a boolean as one segment.
pub fn bool_segment(v: bool) -> String {
    if v { "true" } else { "false" }.into()
}

/// The segment form of a scalar value, which is also how elements are
/// ordered and told apart.
pub(crate) fn scalar_string(v: &Value) -> String {
    match v {
        Value::String(s) => string_segment(s),
        Value::Bytes(b) => escape_segment(b),
        Value::Bool(b) => bool_segment(*b),
        Value::EnumNumber(n) => int_segment(i64::from(*n)),
        Value::I32(i) => int_segment(i64::from(*i)),
        Value::I64(i) => int_segment(*i),
        Value::U32(u) => uint_segment(u64::from(*u)),
        Value::U64(u) => uint_segment(*u),
        Value::F32(f) => go_float(f64::from(*f), Some(*f)),
        Value::F64(f) => go_float(*f, None),
        Value::Message(_) | Value::List(_) | Value::Map(_) => String::new(),
    }
}

/// Formats a float as Go's `fmt.Sprint` does: the shortest digits that read
/// back, in exponent form when the exponent is below -4 or at least 6.
fn go_float(f: f64, as_f32: Option<f32>) -> String {
    if f.is_nan() {
        return "NaN".into();
    }
    if f.is_infinite() {
        return if f > 0.0 { "+Inf" } else { "-Inf" }.into();
    }
    // Rust's `{:e}` prints the shortest round-tripping digits.
    let e = match as_f32 {
        Some(x) => format!("{x:e}"),
        None => format!("{f:e}"),
    };
    let (mantissa, exp) = e.split_once('e').expect("{:e} always has an exponent");
    let exp: i32 = exp.parse().expect("{:e} writes an integer exponent");
    let neg = mantissa.starts_with('-');
    let digits: String = mantissa.chars().filter(char::is_ascii_digit).collect();
    let digits = if digits.trim_end_matches('0').is_empty() {
        "0".to_string()
    } else {
        digits.trim_end_matches('0').to_string()
    };
    let sign = if neg { "-" } else { "" };
    if !(-4..6).contains(&exp) {
        let (first, rest) = digits.split_at(1);
        let frac = if rest.is_empty() {
            String::new()
        } else {
            format!(".{rest}")
        };
        let esign = if exp < 0 { '-' } else { '+' };
        return format!("{sign}{first}{frac}e{esign}{:02}", exp.abs());
    }
    let point = exp + 1;
    let n = digits.len() as i32;
    let body = if point <= 0 {
        format!("0.{}{digits}", "0".repeat((-point) as usize))
    } else if point >= n {
        format!("{digits}{}", "0".repeat((point - n) as usize))
    } else {
        let (a, b) = digits.split_at(point as usize);
        format!("{a}.{b}")
    };
    format!("{sign}{body}")
}

/// Percent-encodes `/`, `%` and control bytes.
fn escape_segment(s: &[u8]) -> String {
    if !s.iter().any(|&c| needs_escape(c)) {
        return String::from_utf8_lossy(s).into_owned();
    }
    let mut out = Vec::with_capacity(s.len() + 8);
    for &c in s {
        if needs_escape(c) {
            out.extend_from_slice(format!("%{c:02X}").as_bytes());
        } else {
            out.push(c);
        }
    }
    String::from_utf8_lossy(&out).into_owned()
}

fn needs_escape(c: u8) -> bool {
    c == b'/' || c == b'%' || c < 0x20 || c == 0x7f
}

/// How a float renders in an item key or an element order, for the golden
/// vectors.
#[doc(hidden)]
pub fn float_string_for_test(f: f64, as_f32: Option<f32>) -> String {
    go_float(f, as_f32)
}

/// [`unix_to_date`], for the golden vectors.
#[doc(hidden)]
pub fn unix_to_date_for_test(sec: i64) -> (i64, i64, i64) {
    unix_to_date(sec)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn floats_format_as_go_prints_them() {
        for (f, want) in [
            (0.0, "0"),
            (-3.25, "-3.25"),
            (1.5e6, "1.5e+06"),
            (1e20, "1e+20"),
            (1e21, "1e+21"),
            (2.5e-7, "2.5e-07"),
            (f64::MAX, "1.7976931348623157e+308"),
            (123456.0, "123456"),
            (0.0001, "0.0001"),
            (0.00001, "1e-05"),
            (8996996.75, "8.99699675e+06"),
        ] {
            assert_eq!(go_float(f, None), want, "{f}");
        }
    }
}
