extern crate rustc_hash;
extern crate slab;
use crate::component::pool_map::PoolMap;
use ckb_types::core::cell::{CellChecker, CellMetaBuilder, CellProvider, CellStatus};
use ckb_types::packed::{OutPoint, ProposalShortId};
use std::collections::HashSet;

enum Membership<'a> {
    Live,
    IgnoreSpenders,
    AfterRemoval(&'a HashSet<ProposalShortId>),
}

pub(crate) struct PoolCell<'a> {
    pool_map: &'a PoolMap,
    membership: Membership<'a>,
}

impl<'a> PoolCell<'a> {
    pub fn new(pool_map: &'a PoolMap, rbf: bool) -> Self {
        PoolCell {
            pool_map,
            membership: if rbf {
                Membership::IgnoreSpenders
            } else {
                Membership::Live
            },
        }
    }

    pub fn after_removal(pool_map: &'a PoolMap, removed: &'a HashSet<ProposalShortId>) -> Self {
        Self {
            pool_map,
            membership: Membership::AfterRemoval(removed),
        }
    }

    fn is_dead(&self, out_point: &OutPoint) -> bool {
        let spender = self.pool_map.edges.get_input_ref(out_point);
        match self.membership {
            Membership::Live => spender.is_some(),
            Membership::IgnoreSpenders => false,
            Membership::AfterRemoval(removed) => {
                spender.is_some_and(|id| !removed.contains(id))
                    || self
                        .pool_map
                        .get_by_tx_hash(&out_point.tx_hash())
                        .is_some_and(|producer| removed.contains(&producer.id))
            }
        }
    }
}

impl<'a> CellProvider for PoolCell<'a> {
    fn cell(&self, out_point: &OutPoint, _eager_load: bool) -> CellStatus {
        if self.is_dead(out_point) {
            return CellStatus::Dead;
        }
        if let Some((output, data)) = self.pool_map.get_output_with_data(out_point) {
            let cell_meta = CellMetaBuilder::from_cell_output(output, data)
                .out_point(out_point.to_owned())
                .build();
            CellStatus::live_cell(cell_meta)
        } else {
            CellStatus::Unknown
        }
    }
}

impl<'a> CellChecker for PoolCell<'a> {
    fn is_live(&self, out_point: &OutPoint) -> Option<bool> {
        if self.is_dead(out_point) {
            return Some(false);
        }
        if self.pool_map.get_output_with_data(out_point).is_some() {
            return Some(true);
        }
        None
    }
}
