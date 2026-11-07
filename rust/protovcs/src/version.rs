//! Reading old schemas and moving a store between layouts. Port of
//! `version.go`.

use std::collections::HashMap;
use std::sync::Arc;

use buffa::Message;
use buffa_descriptor::{DescriptorPool, DynamicMessage};

use crate::error::{Error, ErrorKind, Result};
use crate::layout::Compiled;
use crate::proto::{Commit, SchemaChunk};
use crate::store::{Branch, CommitInfo, CommitMeta, Store};
use crate::transport::RefMove;
use crate::types::{CANONICAL_VERSION, ChunkId, CommitId, ItemKey};

impl Store {
    /// The layout and descriptors a commit was written under, read from the
    /// store.
    ///
    /// # Errors
    ///
    /// When the schema chunk cannot be fetched or is inconsistent, or was
    /// written under other canonical rules.
    pub async fn schema_of(&self, commit: &Commit) -> Result<SchemaChunk> {
        let payload = self.open_payload_unchecked(commit)?;
        let id = ChunkId::from_bytes(&payload.schema_chunk)?;
        let plaintext = self.chunk(id).await?;
        let schema = SchemaChunk::decode_from_slice(&plaintext).map_err(|e| {
            Error::invalid(format!(
                "protovcs: commit {} has an unreadable schema chunk: {e}",
                CommitId::from_bytes(&commit.id).unwrap_or_default()
            ))
        })?;
        let layout_root = schema
            .layout
            .as_option()
            .map(|l| l.root_message.as_str())
            .unwrap_or("");
        if layout_root != schema.root_message {
            return Err(Error::invalid(format!(
                "protovcs: schema chunk names {} but its layout is for {layout_root}",
                schema.root_message
            )));
        }
        let v = schema.canonical_version;
        if v != 0 && v != CANONICAL_VERSION {
            return Err(Error::new(
                ErrorKind::UnsupportedFormat,
                format!(
                    "written under canonical encoding rules {v}, this build has {CANONICAL_VERSION}; \
                     every chunk would be renamed and the whole store re-uploaded"
                ),
            ));
        }
        Ok(schema)
    }

    /// Reads a commit under the schema the commit itself carries, for a
    /// version this build has no layout for.
    ///
    /// # Errors
    ///
    /// When the stored descriptors or layout do not resolve, or reading fails.
    pub async fn read_generic(&self, commit: &Commit) -> Result<(DynamicMessage, Arc<Compiled>)> {
        let schema = self.schema_of(commit).await?;
        let pool = stored_pool(&schema)?;
        let layout = Arc::new(
            Compiled::new(
                schema.layout.as_option().cloned().unwrap_or_default(),
                pool,
                &schema.root_message,
            )
            .map_err(|e| {
                Error::invalid(format!("protovcs: the stored layout did not compile: {e}"))
            })?,
        );
        let generic = self.derive(Arc::clone(&layout));
        let msg = generic.read_commit(commit).await?;
        Ok((msg, layout))
    }

    /// A commit's details as a message, decoded with the descriptor its own
    /// schema chunk carries. `None` when it has none.
    ///
    /// # Errors
    ///
    /// When the schema declares no commit message or the details do not parse.
    pub async fn open_details(
        &self,
        commit: &Commit,
        info: &CommitInfo,
    ) -> Result<Option<DynamicMessage>> {
        if info.details.is_empty() {
            return Ok(None);
        }
        let schema = self.schema_of(commit).await?;
        let name = schema
            .layout
            .as_option()
            .map(|l| l.commit_message.clone())
            .unwrap_or_default();
        if name.is_empty() {
            return Err(Error::invalid(format!(
                "protovcs: commit {} carries details and the layout it was written under declares no commit_message",
                info.id
            )));
        }
        let pool = stored_pool(&schema)?;
        let idx = pool.message_index(&name).ok_or_else(|| {
            Error::invalid(format!(
                "protovcs: the stored descriptors do not define {name}"
            ))
        })?;
        let m = DynamicMessage::decode(pool, idx, &info.details).map_err(|e| {
            Error::invalid(format!(
                "protovcs: commit {} has unreadable details: {e}",
                info.id
            ))
        })?;
        Ok(Some(m))
    }
}

fn stored_pool(schema: &SchemaChunk) -> Result<Arc<DescriptorPool>> {
    let set = schema.descriptors.as_option().cloned().unwrap_or_default();
    DescriptorPool::new(set).map(Arc::new).map_err(|e| {
        Error::invalid(format!(
            "protovcs: the stored descriptors did not resolve: {e}"
        ))
    })
}

impl Branch {
    /// Publishes a message assembled by a producer without holding it whole.
    /// The producer yields `(field, element)` pairs in item key order.
    ///
    /// # Errors
    ///
    /// When elements arrive out of order, a field is not split, or the write
    /// fails.
    pub async fn replace(
        &self,
        meta: &CommitMeta,
        produce: impl FnOnce(&mut dyn FnMut(&str, DynamicMessage) -> Result<()>) -> Result<()>,
    ) -> Result<CommitId> {
        let store = self.store().clone();
        let layout = store.layout().clone();
        let (head, state) = self.head_or_none().await?;

        let mut pending: HashMap<ChunkId, Vec<u8>> = HashMap::new();
        let mut refs = Vec::new();
        let mut current: Option<(ItemKey, DynamicMessage)> = None;

        let mut flush = |current: &mut Option<(ItemKey, DynamicMessage)>| -> Result<()> {
            let Some((key, frag)) = current.take() else {
                return Ok(());
            };
            // Elements inside one item arrived in the producer's order, so the
            // item is materialized again to put them in canonical order.
            for it in layout.materialize(&frag)? {
                if it.key != key {
                    continue;
                }
                let (r, canonical) = store.item_ref(&it.key, &it.fragment)?;
                pending.insert(r.id, canonical);
                refs.push(r);
            }
            Ok(())
        };

        let mut yield_elem = |field: &str, elem: DynamicMessage| -> Result<()> {
            let key = layout.item_key_for(field, &elem)?;
            if let Some((cur, _)) = &current {
                if key < *cur {
                    return Err(Error::invalid(format!(
                        "protovcs: Replace was given item {key:?} after {cur:?}; elements must arrive in item key order"
                    )));
                }
                if key != *cur {
                    flush(&mut current)?;
                }
            }
            let (_, frag) = current.get_or_insert_with(|| (key, layout.new_message()));
            layout.append_element(frag, field, elem)
        };
        produce(&mut yield_elem)?;
        flush(&mut current)?;

        let parents: Vec<Commit> = head.into_iter().collect();
        self.publish_refs(
            meta,
            &parents,
            &state,
            &refs,
            &mut pending,
            RefMove::FastForward,
        )
        .await
    }

    /// Rewrites this branch under a different layout.
    ///
    /// # Errors
    ///
    /// When reading or writing fails.
    pub async fn relayout(&self, meta: &CommitMeta, next: Arc<Compiled>) -> Result<CommitId> {
        let current = self.read().await?;
        let rewritten = self.store().derive(next);
        rewritten.branch(self.name()).write(meta, &current).await
    }

    /// Makes the head a new root, dropping its history. Forced, because the
    /// new root descends from nothing.
    ///
    /// # Errors
    ///
    /// When the branch does not exist or the write fails.
    pub async fn truncate(&self, meta: &CommitMeta) -> Result<CommitId> {
        let (head, state) = self.head().await?;
        let current = self.store().read_commit(&head).await?;
        let (refs, mut pending) = self.store().item_refs(&current)?;
        self.publish_refs(meta, &[], &state, &refs, &mut pending, RefMove::Force)
            .await
    }
}
