//! The tree naming every item in a version. Port of
//! `manifest.go`.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};

use crate::error::{Error, ErrorKind, Result};
use crate::proto::ManifestNode;
use crate::proto::manifest_node::Entry;
use crate::proto::manifest_node::entry::Target;
use crate::reflect::canonical_own;
use crate::seal::Sealer;
use crate::types::{ChunkId, ItemKey, ROOT_ITEM_KEY, Role};
use crate::{BoxFuture, MaybeSend, MaybeSync};

/// One item as the manifest records it: where it is and how big.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ItemRef {
    /// The item's key.
    pub key: ItemKey,
    /// The chunk holding it.
    pub id: ChunkId,
    /// Its canonical size.
    pub bytes: u64,
    /// How many split elements it holds.
    pub elements: u32,
}

/// One node of the tree, ready to store.
#[derive(Clone, Debug)]
pub struct ManifestChunk {
    /// The prefix of item keys this node covers. The root covers `""`.
    pub prefix: String,
    /// The node.
    pub node: ManifestNode,
    /// Its canonical bytes.
    pub canonical: Vec<u8>,
    /// Its name.
    pub id: ChunkId,
}

/// The point at which a manifest node is refused.
const MAX_NODE_ENTRIES: usize = 65536;

/// The upper bound of a range that runs to the end of the key space: U+10FFFF,
/// the largest Unicode scalar value, whose UTF-8 is F4 8F BF BF. Keys compare
/// by bytes, so it sorts after every character, including those outside the
/// Basic Multilingual Plane. The Go client uses the same bound.
pub(crate) const LAST_KEY: &str = "\u{10FFFF}";

/// Where a reader gets manifest nodes.
pub trait NodeSource: MaybeSend + MaybeSync {
    /// Opens the node `id`, which covers `prefix`.
    fn node<'a>(&'a self, prefix: &'a str, id: ChunkId) -> BoxFuture<'a, Result<ManifestNode>>;

    /// Fetches many nodes at once ahead of [`NodeSource::node`] asking for
    /// them. A range read then costs one request per level of the tree.
    fn prefetch<'a>(&'a self, ids: &'a [ChunkId]) -> BoxFuture<'a, Result<()>> {
        let _ = ids;
        Box::pin(async { Ok(()) })
    }
}

impl NodeSource for HashMap<ChunkId, ManifestNode> {
    fn node<'a>(&'a self, _prefix: &'a str, id: ChunkId) -> BoxFuture<'a, Result<ManifestNode>> {
        let got = self
            .get(&id)
            .cloned()
            .ok_or_else(|| Error::new(ErrorKind::NoSuchChunk, id.to_string()));
        Box::pin(async move { got })
    }
}

fn segment_at(key: &str, depth: usize) -> &str {
    key.split('/').nth(depth).unwrap_or("")
}

fn depth_of(key: &str) -> usize {
    key.matches('/').count() + 1
}

fn join(prefix: &str, segment: &str) -> String {
    if prefix.is_empty() {
        segment.to_string()
    } else {
        format!("{prefix}/{segment}")
    }
}

fn split_last(prefix: &str) -> (&str, &str) {
    prefix.rsplit_once('/').unwrap_or(("", prefix))
}

/// Whether every key beginning with `prefix` sorts before `from`.
fn prefix_ends_before(prefix: &str, from: &str) -> bool {
    if prefix.len() >= from.len() {
        return prefix < from;
    }
    prefix.as_bytes() < &from.as_bytes()[..prefix.len()]
}

fn item_entry(segment: &str, r: &ItemRef) -> Entry {
    Entry {
        segment: segment.to_string(),
        target: Some(Target::Item(r.id.to_vec())),
        total_bytes: r.bytes,
        element_count: r.elements,
        ..Default::default()
    }
}

fn child_entry(segment: &str, id: &ChunkId, total: u64, elements: u32) -> Entry {
    Entry {
        segment: segment.to_string(),
        target: Some(Target::Child(id.to_vec())),
        total_bytes: total,
        element_count: elements,
        ..Default::default()
    }
}

fn entry_item(e: &Entry) -> Option<&[u8]> {
    match &e.target {
        Some(Target::Item(b)) => Some(b),
        _ => None,
    }
}

fn entry_child(e: &Entry) -> Option<&[u8]> {
    match &e.target {
        Some(Target::Child(b)) => Some(b),
        _ => None,
    }
}

fn find_entry<'a>(node: &'a ManifestNode, segment: &str) -> Option<&'a Entry> {
    if let Ok(i) = node
        .entries
        .binary_search_by(|e| e.segment.as_str().cmp(segment))
    {
        return Some(&node.entries[i]);
    }
    // Versions written before key_cmp sorted a node by whole item key, which
    // puts a segment such as "a-b" before "a" when both have children. The
    // binary search misses there, so a node in that order is searched in full.
    node.entries.iter().find(|e| e.segment == segment)
}

/// Orders item keys one segment at a time, which is the order a node's entries
/// are kept in. Comparing whole keys is not the same: "/" sorts above "-", ".",
/// "%" and others, so "x/a-b/1" would come before "x/a/1" while the segment
/// "a" comes before "a-b".
fn key_cmp(a: &str, b: &str) -> std::cmp::Ordering {
    let rank = |c: u8| if c == b'/' { 0u16 } else { u16::from(c) + 1 };
    a.bytes().map(rank).cmp(b.bytes().map(rank))
}

/// Writes the tree naming every item, deepest nodes first and the root last.
///
/// The tree's shape is a function of the item keys alone, so two clients
/// holding the same items build the same tree.
///
/// # Errors
///
/// When a key appears twice, a key is also a directory, or a node would hold
/// more than 65,536 entries.
pub fn build_manifest(s: &dyn Sealer, items: &[ItemRef]) -> Result<Vec<ManifestChunk>> {
    let mut sorted: Vec<ItemRef> = items.to_vec();
    sorted.sort_by(|a, b| key_cmp(&a.key, &b.key));
    for w in sorted.windows(2) {
        if w[0].key == w[1].key {
            return Err(Error::invalid(format!(
                "protovcs: item {:?} appears twice",
                w[0].key
            )));
        }
    }
    let mut out = Vec::new();
    build_node(s, &sorted, "", 0, &mut out)?;
    Ok(out)
}

fn build_node(
    s: &dyn Sealer,
    items: &[ItemRef],
    prefix: &str,
    depth: usize,
    out: &mut Vec<ManifestChunk>,
) -> Result<ChunkId> {
    let mut node = ManifestNode::default();
    let mut i = 0;
    while i < items.len() {
        let segment = segment_at(&items[i].key, depth);
        let mut j = i;
        while j < items.len() && segment_at(&items[j].key, depth) == segment {
            j += 1;
        }
        let group = &items[i..j];
        i = j;

        if group.len() == 1 && depth_of(&group[0].key) == depth + 1 {
            node.entries.push(item_entry(segment, &group[0]));
            continue;
        }
        if let Some(it) = group.iter().find(|it| depth_of(&it.key) == depth + 1) {
            return Err(Error::invalid(format!(
                "protovcs: item {:?} is also a directory holding other items",
                it.key
            )));
        }
        let child = build_node(s, group, &join(prefix, segment), depth + 1, out)?;
        let total = group.iter().map(|it| it.bytes).sum();
        let elements = group.iter().map(|it| it.elements).sum();
        node.entries
            .push(child_entry(segment, &child, total, elements));
    }
    if node.entries.len() > MAX_NODE_ENTRIES {
        return Err(Error::invalid(format!(
            "protovcs: {} items share the prefix {prefix:?}, which is more than one manifest node holds; \
             add a shard key component so they divide further",
            node.entries.len()
        )));
    }
    let canonical = canonical_own(&node)?;
    let id = s.chunk_id(Role::Manifest, prefix, &canonical);
    out.push(ManifestChunk {
        prefix: prefix.to_string(),
        node,
        canonical,
        id,
    });
    Ok(id)
}

/// Resolves item keys against a stored tree, fetching only the nodes it
/// needs.
pub struct ManifestReader<'a> {
    root: ChunkId,
    source: &'a dyn NodeSource,
}

impl<'a> ManifestReader<'a> {
    /// A reader over the tree whose root is `root`.
    pub fn new(root: ChunkId, source: &'a dyn NodeSource) -> Self {
        Self { root, source }
    }

    /// The chunk holding one item.
    ///
    /// # Errors
    ///
    /// [`ErrorKind::NoSuchChunk`] when there is no such item, or when a node
    /// cannot be fetched.
    pub async fn lookup(&self, key: &str) -> Result<ItemRef> {
        let mut node = self.source.node("", self.root).await?;
        let mut prefix = String::new();
        let missing = || Error::new(ErrorKind::NoSuchChunk, format!("no item {key:?}"));
        for depth in 0.. {
            let segment = segment_at(key, depth);
            let entry = find_entry(&node, segment).ok_or_else(missing)?;
            if let Some(item) = entry_item(entry) {
                if depth_of(key) != depth + 1 {
                    return Err(missing());
                }
                return Ok(ItemRef {
                    key: key.to_string(),
                    id: ChunkId::from_bytes(item)?,
                    bytes: entry.total_bytes,
                    elements: entry.element_count,
                });
            }
            let id = ChunkId::from_bytes(entry_child(entry).unwrap_or_default())?;
            prefix = join(&prefix, segment);
            node = self.source.node(&prefix, id).await?;
        }
        unreachable!("the loop above returns")
    }

    /// Every item whose key falls between `from` and `to` inclusive. Only the
    /// subtrees the range touches are fetched.
    ///
    /// # Errors
    ///
    /// When a node cannot be fetched or does not parse.
    pub async fn range(&self, from: &str, to: &str) -> Result<Vec<ItemRef>> {
        let descend = |key: &str| key <= to && !prefix_ends_before(key, from);
        self.prefetch_levels(&descend).await?;
        let mut out = Vec::new();
        self.walk(String::new(), self.root, &descend, from, to, &mut out)
            .await?;
        Ok(out)
    }

    /// Every item in the version, in key order.
    ///
    /// # Errors
    ///
    /// When a node cannot be fetched or does not parse.
    pub async fn all(&self) -> Result<Vec<ItemRef>> {
        self.range("", LAST_KEY).await
    }

    async fn prefetch_levels(&self, descend: &(dyn Fn(&str) -> bool + Sync)) -> Result<()> {
        let mut level = vec![(String::new(), self.root)];
        while !level.is_empty() {
            let ids: Vec<ChunkId> = level.iter().map(|(_, id)| *id).collect();
            self.source.prefetch(&ids).await?;
            let mut next = Vec::new();
            for (prefix, id) in &level {
                let node = self.source.node(prefix, *id).await?;
                for e in &node.entries {
                    let Some(child) = entry_child(e) else {
                        continue;
                    };
                    let key = join(prefix, &e.segment);
                    if descend(&key) {
                        next.push((key, ChunkId::from_bytes(child)?));
                    }
                }
            }
            level = next;
        }
        Ok(())
    }

    fn walk<'b>(
        &'b self,
        prefix: String,
        id: ChunkId,
        descend: &'b (dyn Fn(&str) -> bool + Sync),
        from: &'b str,
        to: &'b str,
        out: &'b mut Vec<ItemRef>,
    ) -> BoxFuture<'b, Result<()>> {
        Box::pin(async move {
            let node = self.source.node(&prefix, id).await?;
            for e in &node.entries {
                let key = join(&prefix, &e.segment);
                if let Some(item) = entry_item(e) {
                    if key.as_str() >= from && key.as_str() <= to {
                        out.push(ItemRef {
                            key,
                            id: ChunkId::from_bytes(item)?,
                            bytes: e.total_bytes,
                            elements: e.element_count,
                        });
                    }
                    continue;
                }
                if !descend(&key) {
                    continue;
                }
                let child = ChunkId::from_bytes(entry_child(e).unwrap_or_default())?;
                self.walk(key, child, descend, from, to, out).await?;
            }
            Ok(())
        })
    }

    /// Every node id the walk of the whole tree opens, in the order it opens
    /// them, then every item. For [`crate::Store::reachable_chunks`].
    pub(crate) async fn nodes_and_items(&self) -> Result<(Vec<ChunkId>, Vec<ItemRef>)> {
        let mut nodes = Vec::new();
        let mut items = Vec::new();
        self.collect(String::new(), self.root, &mut nodes, &mut items)
            .await?;
        Ok((nodes, items))
    }

    fn collect<'b>(
        &'b self,
        prefix: String,
        id: ChunkId,
        nodes: &'b mut Vec<ChunkId>,
        items: &'b mut Vec<ItemRef>,
    ) -> BoxFuture<'b, Result<()>> {
        Box::pin(async move {
            nodes.push(id);
            let node = self.source.node(&prefix, id).await?;
            for e in &node.entries {
                let key = join(&prefix, &e.segment);
                if let Some(item) = entry_item(e) {
                    items.push(ItemRef {
                        key,
                        id: ChunkId::from_bytes(item)?,
                        bytes: e.total_bytes,
                        elements: e.element_count,
                    });
                    continue;
                }
                let child = ChunkId::from_bytes(entry_child(e).unwrap_or_default())?;
                self.collect(key, child, nodes, items).await?;
            }
            Ok(())
        })
    }
}

/// The chunks a new manifest holds that an old one did not, found by
/// descending both trees together and stopping where a subtree's name
/// matches. A zero `old_root` means there was no old tree.
///
/// # Errors
///
/// When an old node cannot be fetched.
pub async fn added_since(
    new_nodes: &[ManifestChunk],
    new_root: ChunkId,
    old_root: ChunkId,
    old: &dyn NodeSource,
) -> Result<Vec<ChunkId>> {
    let by_id: HashMap<ChunkId, &ManifestNode> =
        new_nodes.iter().map(|n| (n.id, &n.node)).collect();
    let mut seen = HashSet::new();
    added_since_from(&by_id, old, String::new(), new_root, old_root, &mut seen).await
}

/// Descends one subtree of both trees, in entry order as the Go client does.
fn added_since_from<'a>(
    by_id: &'a HashMap<ChunkId, &'a ManifestNode>,
    old: &'a dyn NodeSource,
    prefix: String,
    new_id: ChunkId,
    old_id: ChunkId,
    seen: &'a mut HashSet<ChunkId>,
) -> BoxFuture<'a, Result<Vec<ChunkId>>> {
    Box::pin(async move {
        let mut added = Vec::new();
        if new_id == old_id || !seen.insert(new_id) {
            return Ok(added);
        }
        added.push(new_id);
        let Some(node) = by_id.get(&new_id) else {
            return Ok(added);
        };
        let mut old_entries: HashMap<String, Entry> = HashMap::new();
        if !old_id.is_zero() {
            for e in old.node(&prefix, old_id).await?.entries {
                old_entries.insert(e.segment.clone(), e);
            }
        }
        for e in &node.entries {
            let prev = old_entries.get(&e.segment);
            if let Some(item) = entry_item(e) {
                let id = ChunkId::from_bytes(item)?;
                if let Some(prev_item) = prev.and_then(entry_item)
                    && ChunkId::from_bytes(prev_item)? == id
                {
                    continue;
                }
                if seen.insert(id) {
                    added.push(id);
                }
                continue;
            }
            let child = ChunkId::from_bytes(entry_child(e).unwrap_or_default())?;
            let prev_child = match prev.and_then(entry_child) {
                Some(b) => ChunkId::from_bytes(b)?,
                None => ChunkId::default(),
            };
            let mut sub = added_since_from(
                by_id,
                old,
                join(&prefix, &e.segment),
                child,
                prev_child,
                seen,
            )
            .await?;
            added.append(&mut sub);
        }
        Ok(added)
    })
}

/// Rebuilds only the paths that changed. `None` deletes an item.
///
/// Returns the rebuilt nodes, deepest first, and the new root.
///
/// # Errors
///
/// When a node on a changed path cannot be fetched, a change needs an item to
/// be a directory, or a node would hold too many entries.
pub async fn update_manifest(
    s: &dyn Sealer,
    old_root: ChunkId,
    changes: &BTreeMap<String, Option<ItemRef>>,
    fetch: &dyn NodeSource,
) -> Result<(Vec<ManifestChunk>, ChunkId)> {
    let mut loaded: HashMap<String, ManifestNode> = HashMap::new();
    let mut dirty: BTreeSet<String> = BTreeSet::new();

    async fn load(
        loaded: &mut HashMap<String, ManifestNode>,
        fetch: &dyn NodeSource,
        prefix: &str,
        id: ChunkId,
    ) -> Result<()> {
        if loaded.contains_key(prefix) {
            return Ok(());
        }
        let mut node = if id.is_zero() {
            ManifestNode::default()
        } else {
            fetch.node(prefix, id).await?
        };
        // A node from a version written before key_cmp may be out of segment
        // order, and an update keeps it sorted from here.
        node.entries.sort_by(|a, b| a.segment.cmp(&b.segment));
        loaded.insert(prefix.to_string(), node);
        Ok(())
    }

    load(&mut loaded, fetch, "", old_root).await?;

    for (key, r) in changes {
        let segs: Vec<&str> = key.split('/').collect();
        let mut prefix = String::new();
        for seg in &segs[..segs.len() - 1] {
            let child_prefix = join(&prefix, seg);
            let node = &loaded[&prefix];
            let child_id = match find_entry(node, seg) {
                None => ChunkId::default(),
                Some(e) => match entry_child(e) {
                    Some(b) => ChunkId::from_bytes(b)?,
                    None => {
                        return Err(Error::invalid(format!(
                            "protovcs: item {key:?} needs {child_prefix:?} to be a directory, and it holds an item"
                        )));
                    }
                },
            };
            load(&mut loaded, fetch, &child_prefix, child_id).await?;
            dirty.insert(prefix.clone());
            prefix = child_prefix;
        }
        let leaf = segs[segs.len() - 1];
        dirty.insert(prefix.clone());
        // One entry names one thing, as in build_manifest. Writing or deleting
        // an item where a directory is, or one this change set is creating,
        // would replace or drop every item under it.
        let is_dir = find_entry(&loaded[&prefix], leaf).is_some_and(|e| entry_child(e).is_some())
            || (key != ROOT_ITEM_KEY && loaded.contains_key(&join(&prefix, leaf)));
        if is_dir {
            return Err(Error::invalid(format!(
                "protovcs: item {key:?} is also a directory holding other items"
            )));
        }
        let node = loaded.get_mut(&prefix).expect("loaded above");
        match r {
            None => remove_entry(node, leaf),
            Some(r) => set_entry(node, item_entry(leaf, r)),
        }
    }
    dirty.insert(String::new());

    // Deepest first, and within a depth in reverse order, as Go sorts them.
    let mut prefixes: Vec<String> = dirty.into_iter().collect();
    prefixes.sort_by(|a, b| {
        let (da, db) = (a.matches('/').count(), b.matches('/').count());
        db.cmp(&da).then_with(|| b.cmp(a))
    });

    let mut out = Vec::new();
    let mut root = ChunkId::default();
    for prefix in prefixes {
        let node = loaded[&prefix].clone();
        if node.entries.len() > MAX_NODE_ENTRIES {
            return Err(Error::invalid(format!(
                "protovcs: {} entries share the prefix {prefix:?}, which is more than one manifest node holds; \
                 add a shard key component so they divide further",
                node.entries.len()
            )));
        }
        let canonical = canonical_own(&node)?;
        let id = s.chunk_id(Role::Manifest, &prefix, &canonical);
        let total: u64 = node.entries.iter().map(|e| e.total_bytes).sum();
        let elements: u32 = node.entries.iter().map(|e| e.element_count).sum();
        let empty = node.entries.is_empty();
        out.push(ManifestChunk {
            prefix: prefix.clone(),
            node,
            canonical,
            id,
        });
        if prefix.is_empty() {
            root = id;
            continue;
        }
        let (parent_prefix, segment) = split_last(&prefix);
        let parent = loaded.get_mut(parent_prefix).ok_or_else(|| {
            Error::invalid(format!(
                "protovcs: node {prefix:?} was rebuilt without its parent"
            ))
        })?;
        if empty {
            remove_entry(parent, segment);
        } else {
            set_entry(parent, child_entry(segment, &id, total, elements));
        }
    }
    Ok((out, root))
}

fn set_entry(node: &mut ManifestNode, entry: Entry) {
    match node
        .entries
        .binary_search_by(|e| e.segment.as_str().cmp(&entry.segment))
    {
        Ok(i) => node.entries[i] = entry,
        Err(i) => node.entries.insert(i, entry),
    }
}

fn remove_entry(node: &mut ManifestNode, segment: &str) {
    if let Ok(i) = node
        .entries
        .binary_search_by(|e| e.segment.as_str().cmp(segment))
    {
        node.entries.remove(i);
    }
}
