//! Top-level Pool type, methods, and tests
extern crate rustc_hash;
extern crate slab;
use super::links::TxLinks;
use crate::TxEntry;
use crate::component::edges::Edges;
use crate::component::links::{Relation, TxLinksMap};
use crate::component::sort_key::{AncestorsScoreSortKey, EvictKey};
use crate::error::Reject;
use ckb_logger::{debug, error, trace};
use ckb_types::core::error::OutPointError;
use ckb_types::core::{Cycle, FeeRate};
use ckb_types::packed::OutPoint;
use ckb_types::{
    bytes::Bytes,
    core::TransactionView,
    packed::{Byte32, CellOutput, ProposalShortId},
};
use multi_index_map::MultiIndexMap;
use std::collections::{HashMap, HashSet};
type ConflictEntry = (TxEntry, Reject);

/// Removal reasons are retained until the complete admission has committed.
#[derive(Default)]
pub(crate) struct EntryEvictions {
    pub(crate) replaced: Vec<TxEntry>,
    pub(crate) readers: Vec<TxEntry>,
    pub(crate) capacity: Vec<TxEntry>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Status {
    Pending,
    Gap,
    Proposed,
}

impl std::fmt::Display for Status {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Status::Pending => write!(f, "pending"),
            Status::Gap => write!(f, "gap"),
            Status::Proposed => write!(f, "proposed"),
        }
    }
}

#[derive(Copy, Clone)]
enum EntryOp {
    Add,
    Remove,
}

impl EntryOp {
    /// A transitive relation contributes each endpoint's own weight exactly
    /// once, regardless of how many paths connect them.
    fn apply(self, ancestor: &mut TxEntry, descendant: &mut TxEntry) {
        match self {
            Self::Add => {
                ancestor.add_descendant_weight(descendant);
                descendant.add_ancestor_weight(ancestor);
            }
            Self::Remove => {
                ancestor.sub_descendant_weight(descendant);
                descendant.sub_ancestor_weight(ancestor);
            }
        }
    }
}

#[derive(MultiIndexMap, Clone)]
pub struct PoolEntry {
    #[multi_index(hashed_unique)]
    pub id: ProposalShortId,
    #[multi_index(ordered_non_unique)]
    pub score: AncestorsScoreSortKey,
    #[multi_index(hashed_non_unique)]
    pub status: Status,
    #[multi_index(ordered_non_unique)]
    pub evict_key: EvictKey,
    // other sort key
    pub inner: TxEntry,
}

/// The relation graph owns package membership. Cached weights and sort keys
/// count every reachable entry once, including the entry itself.
pub struct PoolMap {
    /// The pool entries with different kinds of sort strategies
    pub(crate) entries: MultiIndexPoolEntryMap,
    /// All the deps, header_deps, inputs, outputs relationships
    pub(crate) edges: Edges,
    /// All the parent/children relationships
    pub(crate) links: TxLinksMap,
    pub(crate) max_ancestors_count: usize,
    // sum of all tx_pool tx's virtual sizes.
    pub(crate) total_tx_size: usize,
    // sum of all tx_pool tx's cycles.
    pub(crate) total_tx_cycles: Cycle,
    pub(crate) pending_count: usize,
    pub(crate) gap_count: usize,
    pub(crate) proposed_count: usize,
}

impl PoolMap {
    pub fn new(max_ancestors_count: usize) -> Self {
        PoolMap {
            entries: MultiIndexPoolEntryMap::default(),
            edges: Edges::default(),
            links: TxLinksMap::new(),
            max_ancestors_count,
            total_tx_size: 0,
            total_tx_cycles: 0,
            pending_count: 0,
            gap_count: 0,
            proposed_count: 0,
        }
    }

    #[cfg(test)]
    pub(crate) fn header_deps_len(&self) -> usize {
        self.edges.header_deps_len()
    }

    #[cfg(test)]
    pub(crate) fn deps_len(&self) -> usize {
        self.edges.deps_len()
    }

    #[cfg(test)]
    pub(crate) fn inputs_len(&self) -> usize {
        self.edges.inputs_len()
    }

    #[cfg(test)]
    pub(crate) fn size(&self) -> usize {
        self.entries.len()
    }

    #[cfg(test)]
    pub(crate) fn contains_key(&self, id: &ProposalShortId) -> bool {
        self.entries.get_by_id(id).is_some()
    }

    #[cfg(test)]
    pub(crate) fn get_tx(&self, id: &ProposalShortId) -> Option<&TransactionView> {
        self.entries
            .get_by_id(id)
            .map(|entry| entry.inner.transaction())
    }

    #[cfg(test)]
    pub(crate) fn add_proposed(&mut self, entry: TxEntry) -> Result<bool, Reject> {
        self.add_entry(entry, Status::Proposed)
            .map(|(succ, _)| succ)
    }

    pub(crate) fn get_max_update_time(&self) -> u64 {
        self.entries
            .iter()
            .map(|(_, entry)| entry.inner.timestamp)
            .max()
            .unwrap_or(0)
    }

    pub(crate) fn get_by_id(&self, id: &ProposalShortId) -> Option<&PoolEntry> {
        self.entries.get_by_id(id)
    }

    /// Returns the pool entry for a transaction hash.
    pub(crate) fn get_by_tx_hash(&self, tx_hash: &Byte32) -> Option<&PoolEntry> {
        let id = ProposalShortId::from_tx_hash(tx_hash);
        self.get_by_id(&id)
            .filter(|entry| &entry.inner.transaction().hash() == tx_hash)
    }

    fn get_by_id_checked(&self, id: &ProposalShortId) -> &PoolEntry {
        self.get_by_id(id).expect("inconsistent pool")
    }

    pub(crate) fn pending_size(&self) -> usize {
        self.pending_count + self.gap_count
    }

    pub(crate) fn proposed_size(&self) -> usize {
        self.proposed_count
    }

    pub(crate) fn sorted_proposed_iter(&self) -> impl Iterator<Item = &TxEntry> {
        self.score_sorted_iter_by_status(Status::Proposed)
    }

    pub(crate) fn get(&self, id: &ProposalShortId) -> Option<&TxEntry> {
        self.get_by_id(id).map(|entry| &entry.inner)
    }

    pub(crate) fn get_proposed(&self, id: &ProposalShortId) -> Option<&TxEntry> {
        match self.get_by_id(id) {
            Some(entry) if entry.status == Status::Proposed => Some(&entry.inner),
            _ => None,
        }
    }

    pub(crate) fn has_proposed(&self, id: &ProposalShortId) -> bool {
        self.get_proposed(id).is_some()
    }

    /// calculate all ancestors from pool
    pub(crate) fn calc_ancestors(&self, short_id: &ProposalShortId) -> HashSet<ProposalShortId> {
        self.links.calc_ancestors(short_id)
    }

    /// calculate all descendants from pool
    pub(crate) fn calc_descendants(&self, short_id: &ProposalShortId) -> HashSet<ProposalShortId> {
        self.links.calc_descendants(short_id)
    }

    pub(crate) fn get_output_with_data(&self, out_point: &OutPoint) -> Option<(CellOutput, Bytes)> {
        self.get_by_tx_hash(&out_point.tx_hash())
            .and_then(|pool_entry| {
                let entry = &pool_entry.inner;
                entry
                    .transaction()
                    .output_with_data(out_point.index().into())
            })
    }

    /// Insert an entry, deriving its statistics from current membership.
    /// Rejection leaves entries, links, cell edges and totals unchanged.
    ///
    /// ## Returns
    ///
    /// Returns `Reject` when any error happened, otherwise return `Ok((succ, evicts))`
    /// - succ  : means whether the entry is inserted actually into pool,
    /// - evicts: is the evicted transactions before inserting this `TxEntry`,
    ///   Currently, evicts when inserting is only due to referring cell dep will be consumed by this new transaction.
    pub(crate) fn add_entry(
        &mut self,
        entry: TxEntry,
        status: Status,
    ) -> Result<(bool, HashSet<TxEntry>), Reject> {
        self.admit_entry(entry, status, HashSet::new(), usize::MAX)
            .map(|evictions| match evictions {
                Some(evictions) => (true, evictions.readers.into_iter().collect()),
                None => (false, HashSet::new()),
            })
    }

    /// Admit against the surviving graph, including replacement and capacity
    /// removals. `replaced` is the descendant-closed set checked by RBF policy.
    /// All rejection precedes mutation; a duplicate returns `None`.
    pub(crate) fn admit_entry(
        &mut self,
        mut entry: TxEntry,
        status: Status,
        replaced: HashSet<ProposalShortId>,
        max_size: usize,
    ) -> Result<Option<EntryEvictions>, Reject> {
        let tx_short_id = entry.proposal_short_id();
        if self.entries.get_by_id(&tx_short_id).is_some() {
            return Ok(None);
        }
        let (mut links, mut removed) = self.prepare_entry_links(&entry, replaced.clone())?;
        self.edges
            .check_inputs(entry.transaction().input_pts_iter(), &tx_short_id, &removed)?;
        let capacity =
            self.prepare_size_evictions(&entry, status, &mut links, &mut removed, max_size)?;

        // Every removal is descendant-closed and preserves the candidate's
        // producers. No retained path crosses a removed entry.
        let mut evictions = EntryEvictions::default();
        for old in self.remove_entries(removed) {
            let id = old.proposal_short_id();
            if replaced.contains(&id) {
                evictions.replaced.push(old);
            } else if capacity.contains(&id) {
                evictions.capacity.push(old);
            } else {
                evictions.readers.push(old);
            }
        }
        trace!("pool_map.add_{:?} {}", status, entry.transaction().hash());
        entry.reset_statistic_state();
        self.update_relations(&mut entry, &links, EntryOp::Add);
        self.record_entry_edges(&entry);
        for parent in &links.parents {
            self.links.add_child(parent, tx_short_id.clone());
        }
        for child in &links.children {
            self.links.add_parent(child, tx_short_id.clone());
        }
        self.links.add_link(tx_short_id, links);
        self.insert_entry(&entry, status);
        self.track_entry_statics(None, Some(status));
        self.total_tx_size += entry.size;
        self.total_tx_cycles += entry.cycles;
        Ok(Some(evictions))
    }

    /// Change the status of the entry, only used for `gap_rtx` and `proposed_rtx`
    pub(crate) fn set_entry(&mut self, short_id: &ProposalShortId, status: Status) {
        let mut old_status = None;
        self.entries
            .modify_by_id(short_id, |e| {
                old_status = Some(e.status);
                e.status = status;
            })
            .expect("inconsistent pool");
        self.track_entry_statics(old_status, Some(status));
    }

    pub(crate) fn remove_entry(&mut self, id: &ProposalShortId) -> Option<TxEntry> {
        self.entries.remove_by_id(id).map(|entry| {
            debug!(
                "remove entry {} from status: {:?}",
                entry.inner.transaction().hash(),
                entry.status
            );
            // Keep the removed entry's original ancestry for callers that
            // restore a batch in dependency order.
            if let Some(links) = self.remove_entry_links(id) {
                self.update_relations(&mut entry.inner.clone(), &links, EntryOp::Remove);
            }
            self.remove_entry_edges(&entry.inner);
            self.track_entry_statics(Some(entry.status), None);
            self.update_stat_for_remove_tx(entry.inner.size, entry.inner.cycles);
            entry.inner
        })
    }

    pub(crate) fn remove_entry_and_descendants(&mut self, id: &ProposalShortId) -> Vec<TxEntry> {
        let Some(root) = self.get(id).cloned() else {
            return Vec::new();
        };
        let mut removed_ids = self.calc_descendants(id);
        removed_ids.insert(id.clone());
        let ancestors = self.calc_ancestors(id);

        // Every ancestor of the root loses the entire package. Other ancestors
        // can reach only part of it, through a retained parent of a removed tx.
        let other_parents = removed_ids
            .iter()
            .filter_map(|id| self.links.get_parents(id))
            .flatten()
            .filter(|id| !removed_ids.contains(*id) && !ancestors.contains(*id))
            .cloned()
            .collect();
        let other_ancestors = self
            .links
            .calc_relation_ids(other_parents, Relation::Parents);
        for id in other_ancestors.difference(&ancestors) {
            let descendants = self.calc_descendants(id);
            let mut ancestor = self.get_by_id_checked(id).inner.clone();
            for removed in descendants.intersection(&removed_ids) {
                ancestor.sub_descendant_weight(&self.get_by_id_checked(removed).inner);
            }
            self.entries.modify_by_id(id, |stored| {
                stored.evict_key = ancestor.as_evict_key();
                stored.inner = ancestor;
            });
        }
        for id in ancestors {
            self.entries.modify_by_id(&id, |ancestor| {
                ancestor.inner.sub_descendant_package(&root);
                ancestor.evict_key = ancestor.inner.as_evict_key();
            });
        }

        // The removal set is descendant-closed: no retained entry loses an
        // ancestor. Preserve each removed entry's original statistics.
        removed_ids
            .into_iter()
            .filter_map(|id| {
                let entry = self.entries.remove_by_id(&id)?;
                self.remove_entry_links(&id);
                self.remove_entry_edges(&entry.inner);
                self.track_entry_statics(Some(entry.status), None);
                self.update_stat_for_remove_tx(entry.inner.size, entry.inner.cycles);
                Some(entry.inner)
            })
            .collect()
    }

    /// Remove a descendant-closed set, starting at its roots so shared
    /// descendants and their statistics are removed only once.
    fn remove_entries(&mut self, ids: HashSet<ProposalShortId>) -> Vec<TxEntry> {
        let roots: Vec<_> = ids
            .iter()
            .filter(|id| {
                self.links
                    .get_parents(id)
                    .is_none_or(|parents| parents.is_disjoint(&ids))
            })
            .cloned()
            .collect();
        roots
            .iter()
            .flat_map(|id| self.remove_entry_and_descendants(id))
            .collect()
    }

    pub(crate) fn remove_entry_and_descendants_by_tx_hash(
        &mut self,
        tx_hash: &Byte32,
    ) -> Vec<TxEntry> {
        let Some(id) = self.get_by_tx_hash(tx_hash).map(|entry| entry.id.clone()) else {
            return Vec::new();
        };
        self.remove_entry_and_descendants(&id)
    }

    pub(crate) fn remove_entry_by_tx_hash(&mut self, tx_hash: &Byte32) -> Option<TxEntry> {
        let id = self.get_by_tx_hash(tx_hash).map(|entry| entry.id.clone())?;
        self.remove_entry(&id)
    }

    pub(crate) fn resolve_conflict_header_dep(
        &mut self,
        headers: &HashSet<Byte32>,
    ) -> Vec<ConflictEntry> {
        let mut conflicts = Vec::new();

        // invalid header deps
        let mut ids = Vec::new();
        for (tx_id, deps) in self.edges.header_deps.iter() {
            for hash in deps {
                if headers.contains(hash) {
                    ids.push((hash.clone(), tx_id.clone()));
                    break;
                }
            }
        }

        for (blk_hash, id) in ids {
            let entries = self.remove_entry_and_descendants(&id);
            for entry in entries {
                let reject = Reject::Resolve(OutPointError::InvalidHeader(blk_hash.to_owned()));
                conflicts.push((entry, reject));
            }
        }
        conflicts
    }

    pub(crate) fn find_conflict_tx(&self, tx: &TransactionView) -> HashSet<ProposalShortId> {
        tx.input_pts_iter()
            .filter_map(|out_point| self.edges.get_input_ref(&out_point).cloned())
            .collect()
    }

    pub(crate) fn find_conflict_outpoint(&self, tx: &TransactionView) -> Option<OutPoint> {
        tx.input_pts_iter()
            .find_map(|out_point| self.edges.get_input_ref(&out_point).map(|_| out_point))
    }

    pub(crate) fn resolve_conflict(&mut self, tx: &TransactionView) -> Vec<ConflictEntry> {
        let mut conflicts = Vec::new();

        for i in tx.input_pts_iter() {
            if let Some(id) = self.edges.remove_input(&i) {
                let entries = self.remove_entry_and_descendants(&id);
                if !entries.is_empty() {
                    let reject = Reject::Resolve(OutPointError::Dead(i.clone()));
                    let rejects = std::iter::repeat_n(reject, entries.len());
                    conflicts.extend(entries.into_iter().zip(rejects));
                }
            }

            // deps consumed
            if let Some(x) = self.edges.remove_deps(&i) {
                for id in x {
                    let entries = self.remove_entry_and_descendants(&id);
                    if !entries.is_empty() {
                        let reject = Reject::Resolve(OutPointError::Dead(i.clone()));
                        let rejects = std::iter::repeat_n(reject, entries.len());
                        conflicts.extend(entries.into_iter().zip(rejects));
                    }
                }
            }
        }

        conflicts
    }

    pub(crate) fn estimate_fee_rate(
        &self,
        mut target_blocks: usize,
        max_block_bytes: usize,
        max_block_cycles: Cycle,
        min_fee_rate: FeeRate,
    ) -> FeeRate {
        debug_assert!(target_blocks > 0);
        let iter = self.entries.iter_by_score().rev();
        let mut current_block_bytes = 0;
        let mut current_block_cycles = 0;
        for entry in iter {
            current_block_bytes += entry.inner.size;
            current_block_cycles += entry.inner.cycles;
            if current_block_bytes >= max_block_bytes || current_block_cycles >= max_block_cycles {
                target_blocks -= 1;
                if target_blocks == 0 {
                    return entry.inner.fee_rate();
                }
                current_block_bytes = entry.inner.size;
                current_block_cycles = entry.inner.cycles;
            }
        }

        min_fee_rate
    }

    /// Returns at most `limit` proposal short ids for the next block.
    ///
    /// A candidate is proposed together with the ancestors that still need a proposal, so a
    /// descendant never takes a slot while an ancestor is still `Pending` and therefore
    /// uncommittable. When a candidate's whole closure does not fit into the remaining slots,
    /// those slots are filled with the root-first prefix of the closure instead, so long chains
    /// keep advancing instead of leaving the proposal area half empty.
    pub(crate) fn get_proposals(
        &self,
        limit: usize,
        exclusion: &HashSet<ProposalShortId>,
    ) -> HashSet<ProposalShortId> {
        let mut proposals: HashSet<ProposalShortId> = HashSet::with_capacity(limit);
        let mut settled: HashSet<ProposalShortId> = HashSet::new();
        for entry in self.score_sorted_iter_by_status(Status::Pending) {
            if proposals.len() >= limit {
                break;
            }
            let id = entry.proposal_short_id();
            if proposals.contains(&id) {
                continue;
            }
            // An id an uncle already proposed needs no slot of its own, but its ancestors do.
            let needs_slot = !exclusion.contains(&id);
            let ancestors = self.unproposed_ancestors(&id, &proposals, &mut settled, exclusion);
            let needed = ancestors.len() + usize::from(needs_slot);
            let remaining = limit - proposals.len();
            if needed <= remaining {
                if needs_slot {
                    proposals.insert(id);
                }
                proposals.extend(ancestors);
            } else {
                proposals.extend(ancestors.into_iter().take(remaining));
            }
        }
        proposals
    }

    /// Collects the ancestors of `id` that still need a proposal, ordered so that every
    /// ancestor comes before its descendants.
    ///
    /// `Proposed` and `Gap` entries already appeared in a recent block, and an id listed by an
    /// uncle is proposed by that uncle, so none of them is collected. The walk still continues
    /// through them, because an ancestor further up the chain can be `Pending` again.
    ///
    /// `chosen` and `settled` memoise work across the candidates of one call. A `chosen` id is
    /// already part of the proposal set and its whole closure is in that set; a `settled` id is
    /// covered together with all of its ancestors. Stopping at either one keeps a pool that
    /// shares ancestors across many candidates linear instead of walking each closure anew.
    fn unproposed_ancestors(
        &self,
        id: &ProposalShortId,
        chosen: &HashSet<ProposalShortId>,
        settled: &mut HashSet<ProposalShortId>,
        exclusion: &HashSet<ProposalShortId>,
    ) -> Vec<ProposalShortId> {
        let mut ordered: Vec<ProposalShortId> = Vec::new();
        let mut visited: HashSet<ProposalShortId> = HashSet::new();
        // `true` marks the post-order visit that emits the id after its parents.
        let mut stack: Vec<(ProposalShortId, bool)> = Vec::new();
        if let Some(parents) = self.links.get_parents(id) {
            stack.extend(parents.iter().map(|parent| (parent.clone(), false)));
        }
        while let Some((current, expanded)) = stack.pop() {
            if expanded {
                if self.requires_proposal(&current, exclusion) {
                    ordered.push(current);
                } else if self.links.get_parents(&current).is_none_or(|parents| {
                    parents
                        .iter()
                        .all(|parent| chosen.contains(parent) || settled.contains(parent))
                }) {
                    settled.insert(current);
                }
                continue;
            }
            if chosen.contains(&current)
                || settled.contains(&current)
                || !visited.insert(current.clone())
            {
                continue;
            }
            stack.push((current.clone(), true));
            if let Some(parents) = self.links.get_parents(&current) {
                stack.extend(parents.iter().map(|parent| (parent.clone(), false)));
            }
        }
        ordered
    }

    /// Whether `id` still needs a proposal of its own.
    ///
    /// `Proposed` and `Gap` both mean the transaction already appears in the proposal section
    /// of a recent block; `Gap` only adds that it cannot be committed yet.
    fn requires_proposal(
        &self,
        id: &ProposalShortId,
        exclusion: &HashSet<ProposalShortId>,
    ) -> bool {
        !exclusion.contains(id)
            && self
                .get_by_id(id)
                .is_some_and(|entry| entry.status == Status::Pending)
    }

    pub(crate) fn iter(&self) -> impl Iterator<Item = &PoolEntry> {
        self.entries.iter().map(|(_, entry)| entry)
    }

    /// Capacity eviction prefers Pending, then Gap, then Proposed; within a
    /// status it follows the package fee index. Admission excludes entries
    /// already removed or represented by a prospective key.
    pub(crate) fn next_evict_entry(
        &self,
        eligible: impl Fn(&PoolEntry) -> bool,
    ) -> Option<&PoolEntry> {
        [Status::Pending, Status::Gap, Status::Proposed]
            .into_iter()
            .find_map(|status| {
                self.entries
                    .iter_by_evict_key()
                    .find(|entry| entry.status == status && eligible(entry))
            })
    }

    pub(crate) fn clear(&mut self) {
        self.entries = MultiIndexPoolEntryMap::default();
        self.edges.clear();
        self.links.clear();
        self.total_tx_size = 0;
        self.total_tx_cycles = 0;
        self.pending_count = 0;
        self.gap_count = 0;
        self.proposed_count = 0;
    }

    pub(crate) fn score_sorted_iter_by_status(
        &self,
        status: Status,
    ) -> impl Iterator<Item = &TxEntry> {
        self.entries
            .iter_by_score()
            .rev()
            .filter_map(move |entry| (entry.status == status).then_some(&entry.inner))
    }

    pub(crate) fn score_sorted_iter_by_statuses(
        &self,
        statuses: Vec<Status>,
    ) -> impl Iterator<Item = &TxEntry> {
        self.entries
            .iter_by_score()
            .rev()
            .filter_map(move |entry| statuses.contains(&entry.status).then_some(&entry.inner))
    }

    fn remove_entry_links(&mut self, id: &ProposalShortId) -> Option<TxLinks> {
        let links = self.links.remove(id)?;
        for parent in &links.parents {
            self.links.remove_child(parent, id);
        }
        for child in &links.children {
            self.links.remove_parent(child, id);
        }
        Some(links)
    }

    /// Reconcile a vertex insertion/removal against the graph *without* that
    /// vertex. Only ancestor/descendant pairs with no alternative path change.
    /// Update each retained entry's index once, after its complete delta.
    fn update_relations(&mut self, entry: &mut TxEntry, links: &TxLinks, op: EntryOp) {
        let ancestors = self
            .links
            .calc_relation_ids(links.parents.clone(), Relation::Parents);
        let descendants = self
            .links
            .calc_relation_ids(links.children.clone(), Relation::Children);
        let mut ancestors: Vec<_> = ancestors
            .into_iter()
            .map(|id| {
                let entry = self.get_by_id_checked(&id).inner.clone();
                (id, entry)
            })
            .collect();
        for (_, ancestor) in &mut ancestors {
            op.apply(ancestor, entry);
        }
        for id in descendants {
            // Reaching every direct parent proves that every ancestor already
            // has an alternative path. Stop before retraversing their shared
            // ancestry; otherwise retain the full alternative ancestor set.
            let mut remaining_parents = links.parents.clone();
            let mut unchanged = self.links.get_parents(&id).cloned().unwrap_or_default();
            let mut pending: Vec<_> = unchanged.iter().cloned().collect();
            while !remaining_parents.is_empty() {
                let Some(parent) = pending.pop() else {
                    break;
                };
                remaining_parents.remove(&parent);
                if let Some(parents) = self.links.get_parents(&parent) {
                    for parent in parents {
                        if unchanged.insert(parent.clone()) {
                            pending.push(parent.clone());
                        }
                    }
                }
            }
            self.entries.modify_by_id(&id, |descendant| {
                op.apply(entry, &mut descendant.inner);
                if !remaining_parents.is_empty() {
                    for (ancestor_id, ancestor) in &mut ancestors {
                        if !unchanged.contains(ancestor_id) {
                            op.apply(ancestor, &mut descendant.inner);
                        }
                    }
                }
                descendant.score = descendant.inner.as_score_key();
            });
        }
        for (id, ancestor) in ancestors {
            self.entries.modify_by_id(&id, |stored| {
                stored.evict_key = ancestor.as_evict_key();
                stored.inner = ancestor;
            });
        }
    }

    fn record_entry_edges(&mut self, entry: &TxEntry) {
        let id = entry.proposal_short_id();
        // Input uniqueness is checked before any membership mutation.
        for input in entry.transaction().input_pts_iter() {
            self.edges.inputs.insert(input, id.clone());
        }
        for dep in entry.related_dep_out_points() {
            self.edges.insert_deps(dep.clone(), id.clone());
        }
        let header_deps = entry.transaction().header_deps();
        if !header_deps.is_empty() {
            self.edges
                .header_deps
                .insert(id, header_deps.into_iter().collect());
        }
    }

    /// Prepare direct links and the complete eviction set without mutating the
    /// pool. The same resolved dependencies define both links and cell edges.
    fn prepare_entry_links(
        &self,
        entry: &TxEntry,
        mut evicted: HashSet<ProposalShortId>,
    ) -> Result<(TxLinks, HashSet<ProposalShortId>), Reject> {
        let tx = entry.transaction();
        let mut producers = HashSet::new();
        for point in tx
            .input_pts_iter()
            .chain(entry.related_dep_out_points().cloned())
        {
            if let Some(producer) = self.get_by_tx_hash(&point.tx_hash()) {
                if evicted.contains(&producer.id) {
                    return Err(Reject::Resolve(OutPointError::Dead(point)));
                }
                producers.insert(producer.id.clone());
            }
        }
        let mut parents = producers.clone();
        let mut cell_ref_parents = HashSet::new();
        for input in tx.input_pts_iter() {
            if let Some(readers) = self.edges.get_deps_ref(&input) {
                cell_ref_parents.extend(readers.iter().cloned());
                parents.extend(readers.iter().cloned());
            }
        }
        parents.retain(|id| !evicted.contains(id));
        let mut ancestors = self
            .links
            .calc_relation_ids(parents.clone(), Relation::Parents);
        if ancestors.len() >= self.max_ancestors_count {
            // Producer ancestry must survive. Evicting any of it also removes
            // a producer and invalidates a resolved input or dependency.
            let required = self.links.calc_relation_ids(producers, Relation::Parents);
            if required.len() >= self.max_ancestors_count {
                return Err(Reject::ExceededMaximumAncestorsCount);
            }
            // Try removable readers in fee order. Their removal can disconnect
            // upstream ancestors too, so recount instead of subtracting readers.
            for candidate in self.entries.iter_by_evict_key() {
                if !cell_ref_parents.contains(&candidate.id)
                    || required.contains(&candidate.id)
                    || evicted.contains(&candidate.id)
                {
                    continue;
                }
                evicted.insert(candidate.id.clone());
                evicted.extend(self.calc_descendants(&candidate.id));
                parents.retain(|id| !evicted.contains(id));
                ancestors = self
                    .links
                    .calc_relation_ids(parents.clone(), Relation::Parents);
                if ancestors.len() < self.max_ancestors_count {
                    break;
                }
            }
            if ancestors.len() >= self.max_ancestors_count {
                return Err(Reject::ExceededMaximumAncestorsCount);
            }
        }

        let mut children = HashSet::new();
        for output in tx.output_pts() {
            if let Some(readers) = self.edges.get_deps_ref(&output) {
                children.extend(readers.iter().cloned());
            }
            if let Some(spender) = self.edges.get_input_ref(&output) {
                children.insert(spender.clone());
            }
        }
        for dep in entry.related_dep_out_points() {
            if let Some(spender) = self.edges.get_input_ref(dep) {
                children.insert(spender.clone());
            }
        }
        children.retain(|id| !evicted.contains(id));
        let mut descendants = self
            .links
            .calc_relation_ids(children.clone(), Relation::Children);
        // A retained child can still reach a removed branch. Victims include
        // all their descendants, so validate only the surviving closure.
        descendants.retain(|id| !evicted.contains(id));
        if !ancestors.is_disjoint(&descendants) {
            return Err(Reject::Invalidated("cyclic tx-pool dependencies".into()));
        }
        for id in descendants {
            let descendant = self.get_by_id_checked(&id);
            // With no shared ancestors this is the largest possible count.
            // Walk the existing closure only when overlap could decide refusal.
            if descendant
                .inner
                .ancestors_count
                .saturating_add(ancestors.len() + 1)
                > self.max_ancestors_count
                && self.calc_ancestors(&id).union(&ancestors).count() + 2 > self.max_ancestors_count
            {
                return Err(Reject::ExceededMaximumAncestorsCount);
            }
        }
        Ok((TxLinks { parents, children }, evicted))
    }

    /// Simulate size eviction over the prospective membership. Only entries
    /// whose descendant package changes need a temporary eviction key; all
    /// other entries retain their existing ordered index. No pool clone or
    /// speculative live mutation is needed, including on rejection.
    fn prepare_size_evictions(
        &self,
        entry: &TxEntry,
        status: Status,
        links: &mut TxLinks,
        removed: &mut HashSet<ProposalShortId>,
        max_size: usize,
    ) -> Result<HashSet<ProposalShortId>, Reject> {
        let mut size = self.check_stat_for_add_tx(entry, removed)?;
        let mut capacity = HashSet::new();
        if size <= max_size {
            return Ok(capacity);
        }
        let full = || {
            Reject::Full(format!(
                "the fee_rate for this transaction is: {}",
                entry.fee_rate()
            ))
        };
        if entry.size > max_size {
            return Err(full());
        }
        let candidate = entry.proposal_short_id();
        let ancestors = self
            .links
            .calc_relation_ids(links.parents.clone(), Relation::Parents);
        let mut affected = ancestors.clone();
        for id in removed.iter() {
            affected.extend(self.calc_ancestors(id));
        }
        let mut modified = HashMap::new();
        loop {
            let mut children = self
                .links
                .calc_relation_ids(links.children.clone(), Relation::Children);
            children.retain(|id| !removed.contains(id));
            for id in affected.drain() {
                if removed.contains(&id) {
                    continue;
                }
                let mut value = self.get_by_id_checked(&id).inner.clone();
                let mut descendants = self.calc_descendants(&id);
                value.reset_statistic_state();
                if ancestors.contains(&id) {
                    descendants.extend(children.iter().cloned());
                    value.add_descendant_weight(entry);
                }
                for descendant in descendants.difference(removed) {
                    value.add_descendant_weight(&self.get_by_id_checked(descendant).inner);
                }
                modified.insert(id, value.as_evict_key());
            }
            let mut value = entry.clone();
            value.reset_statistic_state();
            for child in children {
                value.add_descendant_weight(&self.get_by_id_checked(&child).inner);
            }
            modified.insert(candidate.clone(), value.as_evict_key());

            let unchanged = self
                .next_evict_entry(|entry| {
                    !removed.contains(&entry.id) && !modified.contains_key(&entry.id)
                })
                .map(|entry| (entry.status, entry.evict_key.clone(), entry.id.clone()));
            let changed = modified
                .iter()
                .map(|(id, key)| {
                    let status = if id == &candidate {
                        status
                    } else {
                        self.get_by_id_checked(id).status
                    };
                    (status, key.clone(), id.clone())
                })
                .min();
            let (_, _, root) = unchanged
                .into_iter()
                .chain(changed)
                .min()
                .ok_or_else(full)?;
            if root == candidate || ancestors.contains(&root) {
                return Err(full());
            }
            let mut victims = self.calc_descendants(&root);
            victims.insert(root);
            victims.retain(|id| !removed.contains(id));
            for id in &victims {
                size -= self.get_by_id_checked(id).inner.size;
                modified.remove(id);
                affected.extend(self.calc_ancestors(id));
            }
            affected.extend(ancestors.iter().cloned());
            capacity.extend(victims.iter().cloned());
            removed.extend(victims);
            if size <= max_size {
                links.children.retain(|id| !removed.contains(id));
                return Ok(capacity);
            }
        }
    }

    fn remove_entry_edges(&mut self, entry: &TxEntry) {
        for i in entry.transaction().input_pts_iter() {
            // release input record
            self.edges.remove_input(&i);
        }
        let id = entry.proposal_short_id();
        for d in entry.related_dep_out_points().cloned() {
            self.edges.delete_txid_by_dep(d, &id);
        }

        self.edges.header_deps.remove(&id);
    }

    fn insert_entry(&mut self, entry: &TxEntry, status: Status) {
        let tx_short_id = entry.proposal_short_id();
        let score = entry.as_score_key();
        let evict_key = entry.as_evict_key();
        self.entries.insert(PoolEntry {
            id: tx_short_id,
            score,
            status,
            inner: entry.clone(),
            evict_key,
        });
    }

    fn track_entry_statics(&mut self, remove: Option<Status>, add: Option<Status>) {
        match remove {
            Some(Status::Pending) => self.pending_count -= 1,
            Some(Status::Gap) => self.gap_count -= 1,
            Some(Status::Proposed) => self.proposed_count -= 1,
            _ => {}
        }
        match add {
            Some(Status::Pending) => self.pending_count += 1,
            Some(Status::Gap) => self.gap_count += 1,
            Some(Status::Proposed) => self.proposed_count += 1,
            _ => {}
        }
        assert_eq!(
            self.pending_count + self.gap_count + self.proposed_count,
            self.entries.len()
        );
        if let Some(metrics) = ckb_metrics::handle() {
            metrics
                .ckb_tx_pool_entry
                .pending
                .set(self.pending_count as i64);
            metrics.ckb_tx_pool_entry.gap.set(self.gap_count as i64);
            metrics
                .ckb_tx_pool_entry
                .proposed
                .set(self.proposed_count as i64);
        }
    }

    fn recompute_total_stat(&self) -> Option<(usize, Cycle)> {
        self.entries.iter().try_fold(
            (0usize, 0 as Cycle),
            |(total_size, total_cycles), (_, entry)| {
                Some((
                    total_size.checked_add(entry.inner.size)?,
                    total_cycles.checked_add(entry.inner.cycles)?,
                ))
            },
        )
    }

    /// Check size and cycle totals before any membership mutation.
    fn check_stat_for_add_tx(
        &self,
        entry: &TxEntry,
        removed: &HashSet<ProposalShortId>,
    ) -> Result<usize, Reject> {
        let mut size = self.total_tx_size;
        let mut cycles = self.total_tx_cycles;
        for id in removed {
            let old = &self.get_by_id_checked(id).inner;
            size -= old.size;
            cycles -= old.cycles;
        }
        let size = size.checked_add(entry.size).ok_or_else(|| {
            Reject::Full(format!(
                "tx-pool total_tx_size {} overflows by add {}",
                size, entry.size
            ))
        })?;
        cycles.checked_add(entry.cycles).ok_or_else(|| {
            Reject::Full(format!(
                "tx-pool total_tx_cycles {} overflows by add {}",
                cycles, entry.cycles
            ))
        })?;
        Ok(size)
    }

    /// Update size and cycles statistics for remove tx
    /// cycles overflow is possible, currently obtaining cycles is not accurate
    fn update_stat_for_remove_tx(&mut self, tx_size: usize, cycles: Cycle) {
        match (
            self.total_tx_size.checked_sub(tx_size),
            self.total_tx_cycles.checked_sub(cycles),
        ) {
            (Some(total_tx_size), Some(total_tx_cycles)) => {
                self.total_tx_size = total_tx_size;
                self.total_tx_cycles = total_tx_cycles;
            }
            _ => {
                if let Some((total_tx_size, total_tx_cycles)) = self.recompute_total_stat() {
                    error!(
                        "tx-pool total stats underflowed when removing size {} cycles {}, recomputed size {} cycles {}",
                        tx_size, cycles, total_tx_size, total_tx_cycles
                    );
                    self.total_tx_size = total_tx_size;
                    self.total_tx_cycles = total_tx_cycles;
                } else {
                    error!(
                        "tx-pool total stats underflowed when removing size {} cycles {}, and recomputing overflowed",
                        tx_size, cycles
                    );
                }
            }
        }
    }
}
