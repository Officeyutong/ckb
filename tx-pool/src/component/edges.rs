use ckb_types::{
    core::tx_pool::Reject,
    packed::{Byte32, OutPoint, ProposalShortId},
};
use std::collections::{HashMap, HashSet, hash_map::Entry};

#[derive(Default, Debug, Clone)]
pub(crate) struct Edges {
    /// input-txid map represent in-pool tx's inputs
    pub(crate) inputs: HashMap<OutPoint, ProposalShortId>,
    /// dep-set<txid> map represent in-pool tx's deps
    pub(crate) deps: HashMap<OutPoint, HashSet<ProposalShortId>>,
    /// dep-set<txid-headers> map represent in-pool tx's header deps
    pub(crate) header_deps: HashMap<ProposalShortId, Vec<Byte32>>,
}

impl Edges {
    #[cfg(test)]
    pub(crate) fn inputs_len(&self) -> usize {
        self.inputs.len()
    }

    #[cfg(test)]
    pub(crate) fn header_deps_len(&self) -> usize {
        self.header_deps.len()
    }

    #[cfg(test)]
    pub(crate) fn deps_len(&self) -> usize {
        self.deps.len()
    }

    pub(crate) fn check_inputs(
        &self,
        inputs: impl Iterator<Item = OutPoint>,
        txid: &ProposalShortId,
        evicted: &HashSet<ProposalShortId>,
    ) -> Result<(), Reject> {
        let mut seen = HashSet::new();
        for out_point in inputs {
            let previous = self
                .inputs
                .get(&out_point)
                .filter(|owner| !evicted.contains(*owner))
                .or_else(|| (!seen.insert(out_point.clone())).then_some(txid));
            if let Some(previous) = previous {
                let msg = format!(
                    "txpool unexpected double-spending out_point: {:?} old_tx: {:?} new_tx: {:?}",
                    out_point, previous, txid
                );
                return Err(Reject::RBFRejected(msg));
            }
        }
        Ok(())
    }

    pub(crate) fn remove_input(&mut self, out_point: &OutPoint) -> Option<ProposalShortId> {
        self.inputs.remove(out_point)
    }

    pub(crate) fn get_input_ref(&self, out_point: &OutPoint) -> Option<&ProposalShortId> {
        self.inputs.get(out_point)
    }

    pub(crate) fn get_deps_ref(&self, out_point: &OutPoint) -> Option<&HashSet<ProposalShortId>> {
        self.deps.get(out_point)
    }

    pub(crate) fn remove_deps(&mut self, out_point: &OutPoint) -> Option<HashSet<ProposalShortId>> {
        self.deps.remove(out_point)
    }

    pub(crate) fn insert_deps(&mut self, out_point: OutPoint, txid: ProposalShortId) {
        self.deps.entry(out_point).or_default().insert(txid);
    }

    pub(crate) fn delete_txid_by_dep(&mut self, out_point: OutPoint, txid: &ProposalShortId) {
        if let Entry::Occupied(mut occupied) = self.deps.entry(out_point) {
            let ids = occupied.get_mut();
            ids.remove(txid);
            if ids.is_empty() {
                occupied.remove();
            }
        }
    }

    pub(crate) fn clear(&mut self) {
        self.inputs.clear();
        self.deps.clear();
        self.header_deps.clear();
    }
}
