//! Part of a store, fetched and held. Port of `partial.go`.

use std::collections::{BTreeMap, HashMap};
use std::fmt;

use buffa_descriptor::DynamicMessage;

use crate::error::{Error, ErrorKind, Result};
use crate::manifest::{ItemRef, LAST_KEY, ManifestReader, update_manifest};
use crate::materialize::Item;
use crate::proto::Commit;
use crate::store::{Branch, CommitMeta, Nodes};
use crate::transport::{RefMove, RefState};
use crate::types::{ChunkId, CommitId, ItemKey, ROOT_ITEM_KEY};

/// A run of item keys, inclusive at both ends.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct KeyRange {
    /// The first key.
    pub from: ItemKey,
    /// The last key.
    pub to: ItemKey,
}

/// What a checkout fetched. Inside it an absent item means there is none;
/// outside it a read is refused.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Scope {
    ranges: Vec<KeyRange>,
}

impl Scope {
    /// The scope covering no items, for a caller that only adds.
    pub fn nothing() -> Self {
        Self::default()
    }

    /// The scope covering a whole store.
    pub fn everything() -> Self {
        Self::between("", LAST_KEY)
    }

    /// The scope covering one split field entirely.
    pub fn field(name: &str) -> Self {
        Self::between(name, format!("{name}/{LAST_KEY}"))
    }

    /// The scope covering the root item alone.
    pub fn root() -> Self {
        Self::between(ROOT_ITEM_KEY, ROOT_ITEM_KEY)
    }

    /// Every item key beginning with `prefix`.
    pub fn prefix(prefix: &str) -> Self {
        Self::between(prefix, format!("{prefix}{LAST_KEY}"))
    }

    /// The item keys between `from` and `to` inclusive.
    pub fn between(from: impl Into<ItemKey>, to: impl Into<ItemKey>) -> Self {
        Self {
            ranges: vec![KeyRange {
                from: from.into(),
                to: to.into(),
            }],
        }
    }

    /// Everything any of these cover.
    pub fn union<'a>(scopes: impl IntoIterator<Item = &'a Scope>) -> Self {
        let ranges = scopes
            .into_iter()
            .flat_map(|s| s.ranges.iter().cloned())
            .collect();
        Self { ranges }.normalized()
    }

    /// The ranges, sorted and merged.
    pub fn ranges(&self) -> &[KeyRange] {
        &self.ranges
    }

    /// Whether every key in `other` is inside this scope. Conservative: when
    /// the answer is not clearly yes it is no.
    pub fn covers(&self, other: &Scope) -> bool {
        let mine = self.clone().normalized();
        other
            .clone()
            .normalized()
            .ranges
            .iter()
            .all(|r| mine.ranges.iter().any(|m| m.from <= r.from && r.to <= m.to))
    }

    /// Whether one item key is inside the scope.
    pub fn holds(&self, key: &str) -> bool {
        self.ranges
            .iter()
            .any(|r| r.from.as_str() <= key && key <= r.to.as_str())
    }

    fn normalized(mut self) -> Self {
        if self.ranges.len() < 2 {
            return self;
        }
        self.ranges.sort();
        let mut out: Vec<KeyRange> = Vec::with_capacity(self.ranges.len());
        for r in self.ranges {
            if let Some(last) = out.last_mut()
                && r.from <= last.to
            {
                if r.to > last.to {
                    last.to = r.to;
                }
                continue;
            }
            out.push(r);
        }
        Self { ranges: out }
    }
}

impl fmt::Display for Scope {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let parts: Vec<String> = self
            .ranges
            .iter()
            .map(|r| format!("{:?}..{:?}", r.from, r.to))
            .collect();
        f.write_str(&parts.join(", "))
    }
}

/// Part of a store, fetched and held. Reads inside its scope touch no
/// network; reads outside it fail.
pub struct Partial {
    branch: Branch,
    head: Option<Commit>,
    state: RefState,
    scope: Scope,
    items: BTreeMap<ItemKey, Item>,
    refs: HashMap<ItemKey, ItemRef>,
}

impl Branch {
    /// Fetches exactly the scope named and nothing else.
    ///
    /// # Errors
    ///
    /// When the head or a chunk cannot be read.
    pub async fn checkout(&self, scope: Scope) -> Result<Partial> {
        let (head, state) = self.head_or_none().await?;
        let mut p = Partial {
            branch: self.clone(),
            head,
            state,
            scope: scope.normalized(),
            items: BTreeMap::new(),
            refs: HashMap::new(),
        };
        if p.head.is_some() {
            let scope = p.scope.clone();
            p.load(&scope).await?;
        }
        Ok(p)
    }
}

impl Partial {
    async fn load(&mut self, scope: &Scope) -> Result<()> {
        let store = self.branch.store.clone();
        let head = self.head.as_ref().expect("load is called only with a head");
        let payload = store.open_payload(head)?;
        let root = ChunkId::from_bytes(&payload.manifest_root)?;
        let nodes = Nodes(&store);
        let reader = ManifestReader::new(root, &nodes);
        let mut refs = Vec::new();
        for r in &scope.ranges {
            refs.extend(reader.range(&r.from, &r.to).await?);
        }
        let ids: Vec<ChunkId> = refs
            .iter()
            .filter(|r| !self.items.contains_key(&r.key))
            .map(|r| r.id)
            .collect();
        store.warm(&ids).await?;
        for r in refs {
            if self.items.contains_key(&r.key) {
                continue;
            }
            let plaintext = store.chunk(r.id).await?;
            let frag = store.layout().decode_item(&r.key, &plaintext)?;
            self.items
                .insert(r.key.clone(), Item::new(r.key.clone(), frag));
            self.refs.insert(r.key.clone(), r);
        }
        Ok(())
    }

    /// What this handle covers.
    pub fn scope(&self) -> &Scope {
        &self.scope
    }

    /// Widens the scope, fetching what the new part needs.
    ///
    /// # Errors
    ///
    /// When a chunk cannot be read.
    pub async fn extend(&mut self, more: Scope) -> Result<()> {
        if self.head.is_some() {
            self.load(&more.clone().normalized()).await?;
        }
        self.scope = Scope::union([&self.scope, &more]);
        Ok(())
    }

    /// The items held, in key order.
    pub fn items(&self) -> impl Iterator<Item = &Item> {
        self.items.values()
    }

    /// The part of the message this handle holds. It looks complete and is
    /// not; check [`Partial::scope`] before computing anything over the whole
    /// store.
    ///
    /// # Errors
    ///
    /// When an item holds a different message type.
    pub fn materialized(&self) -> Result<DynamicMessage> {
        let items: Vec<Item> = self.items.values().cloned().collect();
        self.branch.store.layout().merge(&items)
    }

    /// Refuses unless the scope covers `needed`.
    ///
    /// # Errors
    ///
    /// [`ErrorKind::OutOfScope`].
    pub fn require(&self, needed: &Scope) -> Result<()> {
        if !self.scope.covers(needed) {
            return Err(Error::new(
                ErrorKind::OutOfScope,
                format!("holding {}, need {needed}", self.scope),
            ));
        }
        Ok(())
    }

    /// One item: `None` when it is inside the scope and does not exist.
    ///
    /// # Errors
    ///
    /// [`ErrorKind::OutOfScope`] when it was never fetched.
    pub fn get(&self, key: &str) -> Result<Option<&Item>> {
        if !self.scope.holds(key) {
            return Err(Error::new(
                ErrorKind::OutOfScope,
                format!("{key:?}, holding {}", self.scope),
            ));
        }
        Ok(self.items.get(key))
    }

    /// Replaces or adds one item. Allowed outside the scope.
    ///
    /// # Errors
    ///
    /// When the item holds a different message type.
    pub fn put(&mut self, item: Item) -> Result<()> {
        self.branch
            .store
            .layout()
            .check_type(&item.fragment, &item.key)?;
        let key = item.key.clone();
        self.items.insert(key.clone(), item);
        self.scope = Scope::union([&self.scope, &Scope::between(key.clone(), key)]);
        Ok(())
    }

    /// Stages one element into whichever item its shard key names, fetching
    /// that item first when it was not checked out.
    ///
    /// # Errors
    ///
    /// When the field is not split or the item cannot be fetched.
    pub async fn put_element(&mut self, field: &str, elem: DynamicMessage) -> Result<()> {
        let layout = self.branch.store.layout().clone();
        let key = layout.item_key_for(field, &elem)?;
        if !self.items.contains_key(&key) && self.head.is_some() {
            self.load(&Scope::between(key.clone(), key.clone())).await?;
        }
        let mut item = self
            .items
            .get(&key)
            .cloned()
            .unwrap_or_else(|| Item::new(key.clone(), layout.new_message()));
        layout.append_element(&mut item.fragment, field, elem)?;
        self.put(item)
    }

    /// Publishes the changed items as a new version. Every item outside the
    /// scope is carried across by chunk id without being fetched.
    ///
    /// # Errors
    ///
    /// When an item is over its ceiling, a node cannot be fetched, an upload
    /// fails, or the race is lost.
    pub async fn commit(&self, meta: &CommitMeta) -> Result<CommitId> {
        let store = &self.branch.store;
        let layout = store.layout();
        let mut changes: BTreeMap<String, Option<ItemRef>> = BTreeMap::new();
        let mut pending = HashMap::new();
        for it in self.items.values() {
            let (r, canonical) = store.item_ref(&it.key, &it.fragment)?;
            if canonical.is_empty() {
                changes.insert(it.key.clone(), None);
                continue;
            }
            if self.refs.get(&it.key).is_some_and(|was| was.id == r.id) {
                continue;
            }
            pending.insert(r.id, canonical);
            changes.insert(it.key.clone(), Some(r));
        }
        layout.check_changed_item_sizes(&changes)?;

        let old_root = match &self.head {
            Some(h) => store.manifest_root_of(h)?,
            None => ChunkId::default(),
        };
        let (nodes, root) =
            update_manifest(store.sealer().as_ref(), old_root, &changes, &Nodes(store)).await?;
        for n in &nodes {
            pending.insert(n.id, n.canonical.clone());
        }
        let parents: Vec<Commit> = self.head.iter().cloned().collect();
        self.branch
            .publish_built(
                meta,
                &parents,
                &self.state,
                &nodes,
                root,
                &mut pending,
                RefMove::FastForward,
            )
            .await
    }
}
