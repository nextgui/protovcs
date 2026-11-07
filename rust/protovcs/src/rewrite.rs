//! Rewriting every version of a store. Port of `rewrite.go`.

use std::collections::{HashMap, HashSet, VecDeque};

use buffa_descriptor::DynamicMessage;

use crate::canonical::canonical_bytes;
use crate::error::{Error, ErrorKind, Result};
use crate::manifest::build_manifest;
use crate::proto::Commit;
use crate::store::{DEFAULT_MAX_ATTEMPTS, HISTORY_BATCH, Store, VersionSpec};
use crate::transport::{RefMove, RefState};
use crate::types::CommitId;

/// What [`Store::rewrite`] did.
#[derive(Clone, Debug, Default)]
pub struct RewriteResult {
    rewritten: HashMap<CommitId, CommitId>,
    moved: usize,
}

impl RewriteResult {
    /// Each replaced commit and the commit replacing it.
    pub fn rewritten(&self) -> &HashMap<CommitId, CommitId> {
        &self.rewritten
    }

    /// How many branches now point at a rewritten commit.
    pub fn branches_moved(&self) -> usize {
        self.moved
    }
}

impl Store {
    /// Replaces every commit reachable from any branch with one whose message
    /// has been passed through `rewrite`, parents first, then moves every
    /// branch to the rewritten history. `rewrite` must be a pure function of
    /// the message: a lost race reruns it on the commits that landed meanwhile.
    ///
    /// # Errors
    ///
    /// What `rewrite` returned, or the last lost race after
    /// [`DEFAULT_MAX_ATTEMPTS`].
    pub async fn rewrite(
        &self,
        mut rewrite: impl FnMut(&mut DynamicMessage) -> Result<()>,
    ) -> Result<RewriteResult> {
        let mut mapped: HashMap<CommitId, CommitId> = HashMap::new();
        let mut result = RewriteResult::default();
        let mut last_err = None;
        // Loaded first, so the marks it holds follow the rewrite too.
        self.load_marks().await?;
        for _ in 0..DEFAULT_MAX_ATTEMPTS {
            let refs = self.transport().list_refs().await?;
            let heads: Vec<CommitId> = refs.iter().map(|r| r.commit).collect();
            for commit in self.history_of(&heads, &mapped).await? {
                let id = CommitId::from_bytes(&commit.id)?;
                let next = self
                    .rewrite_commit(&commit, &mapped, &mut rewrite)
                    .await
                    .map_err(|e| {
                        Error::new(e.kind(), format!("protovcs: rewriting commit {id}: {e}"))
                    })?;
                mapped.insert(id, next);
                if next != id {
                    mapped.insert(next, next);
                    result.rewritten.insert(id, next);
                }
            }
            match self
                .move_rewritten_refs(&refs, &mapped, &mut result)
                .await?
            {
                None => {
                    self.follow_rewrite(&mapped).await;
                    return Ok(result);
                }
                Some(lost) => last_err = Some(lost),
            }
        }
        Err(Error::new(
            ErrorKind::RaceLost,
            format!(
                "gave up rewriting after {DEFAULT_MAX_ATTEMPTS} attempts: {}",
                last_err.map(|e| e.to_string()).unwrap_or_default()
            ),
        ))
    }

    async fn move_rewritten_refs(
        &self,
        refs: &[RefState],
        mapped: &HashMap<CommitId, CommitId>,
        result: &mut RewriteResult,
    ) -> Result<Option<Error>> {
        for r in refs {
            let next = *mapped.get(&r.commit).ok_or_else(|| {
                Error::invalid(format!(
                    "protovcs: branch head {} was not rewritten",
                    r.commit
                ))
            })?;
            if next == r.commit {
                continue;
            }
            match self
                .transport()
                .update_ref(&r.name, next, &r.cas_token, RefMove::Force)
                .await
            {
                Ok(_) => result.moved += 1,
                Err(e) if e.is(ErrorKind::RaceLost) => return Ok(Some(e)),
                Err(e) => return Err(e),
            }
        }
        Ok(None)
    }

    async fn rewrite_commit(
        &self,
        commit: &Commit,
        mapped: &HashMap<CommitId, CommitId>,
        rewrite: &mut impl FnMut(&mut DynamicMessage) -> Result<()>,
    ) -> Result<CommitId> {
        let id = CommitId::from_bytes(&commit.id)?;
        let payload = self.open_payload(commit)?;
        let mut message = self.read_commit(commit).await?;
        let before = canonical_bytes(&message)?;
        rewrite(&mut message)?;

        let mut parents = Vec::with_capacity(commit.parents.len());
        let mut parents_moved = false;
        for raw in &commit.parents {
            let pid = CommitId::from_bytes(raw)?;
            let next = *mapped.get(&pid).ok_or_else(|| {
                Error::invalid(format!(
                    "protovcs: parent {pid} was not rewritten before its child"
                ))
            })?;
            parents_moved |= next != pid;
            parents.push(self.commit_by_id(next).await?);
        }
        if !parents_moved && canonical_bytes(&message)? == before {
            return Ok(id);
        }

        let (refs, mut pending) = self.item_refs(&message)?;
        for r in &refs {
            self.layout().check_item_size(&r.key, r.bytes, r.elements)?;
        }
        let nodes = build_manifest(self.sealer().as_ref(), &refs)?;
        let root = nodes
            .last()
            .map(|n| n.id)
            .expect("build_manifest always returns the root node");
        for n in &nodes {
            pending.insert(n.id, n.canonical.clone());
        }
        let next = self
            .record_version(
                VersionSpec {
                    branch: payload.branch.clone(),
                    message: payload.message.as_option().cloned().unwrap_or_default(),
                    at: payload.time.as_option().cloned().unwrap_or_default(),
                },
                &parents,
                &nodes,
                root,
                &mut pending,
            )
            .await?;
        self.inner.memo.put_chunks(pending);
        Ok(next)
    }

    /// Every commit reachable from `heads` that `mapped` does not hold,
    /// parents before children.
    async fn history_of(
        &self,
        heads: &[CommitId],
        mapped: &HashMap<CommitId, CommitId>,
    ) -> Result<Vec<Commit>> {
        let mut known: HashMap<CommitId, Commit> = HashMap::new();
        let mut queue: VecDeque<CommitId> = heads.iter().copied().collect();
        while let Some(id) = queue.pop_front() {
            if mapped.contains_key(&id) || known.contains_key(&id) {
                continue;
            }
            let commits = self.transport().get_commits(id, HISTORY_BATCH).await?;
            if commits.is_empty() {
                return Err(Error::new(ErrorKind::NoSuchCommit, format!("commit {id}")));
            }
            for c in commits {
                let cid = crate::store::commit_name(&c)?;
                if CommitId::from_bytes(&c.id)? != cid {
                    return Err(Error::new(
                        ErrorKind::Tampered,
                        format!(
                            "commit {} is named {cid} by its contents",
                            CommitId::from_bytes(&c.id)?
                        ),
                    ));
                }
                if mapped.contains_key(&cid) {
                    continue;
                }
                self.inner.memo.put_commit(cid, c.clone());
                for raw in &c.parents {
                    queue.push_back(CommitId::from_bytes(raw)?);
                }
                known.insert(cid, c);
            }
        }
        parents_first(heads, &known)
    }
}

/// Orders commits so every commit comes after its parents.
fn parents_first(heads: &[CommitId], known: &HashMap<CommitId, Commit>) -> Result<Vec<Commit>> {
    let mut out = Vec::new();
    let mut visiting = HashSet::new();
    let mut done = HashSet::new();
    for head in heads {
        let mut stack = vec![(*head, false)];
        while let Some((id, expanded)) = stack.pop() {
            let Some(c) = known.get(&id) else { continue };
            if done.contains(&id) {
                continue;
            }
            if expanded {
                visiting.remove(&id);
                done.insert(id);
                out.push(c.clone());
                continue;
            }
            if !visiting.insert(id) {
                return Err(Error::invalid(format!(
                    "protovcs: commit {id} is its own ancestor"
                )));
            }
            stack.push((id, true));
            for raw in &c.parents {
                let pid = CommitId::from_bytes(raw)?;
                if visiting.contains(&pid) {
                    return Err(Error::invalid(format!(
                        "protovcs: commit {pid} is its own ancestor"
                    )));
                }
                if !done.contains(&pid) {
                    stack.push((pid, false));
                }
            }
        }
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parents_first_orders_diamond_and_rejects_cycles() {
        let mut a_bytes = [0u8; 32];
        let mut b_bytes = [0u8; 32];
        let mut c_bytes = [0u8; 32];
        let mut d_bytes = [0u8; 32];
        a_bytes[0] = 1;
        b_bytes[0] = 2;
        c_bytes[0] = 3;
        d_bytes[0] = 4;
        let a = CommitId::from_bytes(&a_bytes).unwrap();
        let b = CommitId::from_bytes(&b_bytes).unwrap();
        let c = CommitId::from_bytes(&c_bytes).unwrap();
        let d = CommitId::from_bytes(&d_bytes).unwrap();

        let mut diamond = HashMap::new();
        diamond.insert(
            a,
            Commit {
                id: a_bytes.to_vec(),
                ..Default::default()
            },
        );
        diamond.insert(
            b,
            Commit {
                id: b_bytes.to_vec(),
                parents: vec![a_bytes.to_vec()],
                ..Default::default()
            },
        );
        diamond.insert(
            c,
            Commit {
                id: c_bytes.to_vec(),
                parents: vec![a_bytes.to_vec()],
                ..Default::default()
            },
        );
        diamond.insert(
            d,
            Commit {
                id: d_bytes.to_vec(),
                parents: vec![a_bytes.to_vec(), b_bytes.to_vec(), c_bytes.to_vec()],
                ..Default::default()
            },
        );

        let ordered = parents_first(&[d], &diamond).unwrap();
        assert_eq!(ordered.len(), 4);

        let mut cyclic = HashMap::new();
        cyclic.insert(
            a,
            Commit {
                id: a_bytes.to_vec(),
                parents: vec![b_bytes.to_vec()],
                ..Default::default()
            },
        );
        cyclic.insert(
            b,
            Commit {
                id: b_bytes.to_vec(),
                parents: vec![a_bytes.to_vec()],
                ..Default::default()
            },
        );
        let err = parents_first(&[a], &cyclic).unwrap_err();
        assert!(err.to_string().contains("is its own ancestor"), "{err}");
    }
}
