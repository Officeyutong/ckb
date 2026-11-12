use super::{relations::assert_consistent, util::build_tx};
use crate::{
    component::{
        entry::TxEntry,
        pool_map::{PoolMap, Status},
    },
    error::Reject,
};
use ckb_types::{
    core::Capacity,
    packed::{Byte32, ProposalShortId},
};
use std::collections::HashSet;

fn entry(input: u32, size: usize, cycles: u64, fee: u64) -> TxEntry {
    TxEntry::dummy_resolve(
        build_tx(vec![(&Byte32::zero(), input)], 1),
        cycles,
        Capacity::shannons(fee),
        size,
    )
}

fn assert_stat_refusal_preserves_pool(
    pool: &mut PoolMap,
    candidate: TxEntry,
    replaced: HashSet<ProposalShortId>,
    max_size: usize,
) {
    let totals = (pool.total_tx_size, pool.total_tx_cycles);
    let ids = pool
        .iter()
        .map(|entry| entry.id.clone())
        .collect::<HashSet<_>>();
    let actual_size = pool.iter().map(|entry| entry.inner.size).sum();
    let actual_cycles = pool.iter().map(|entry| entry.inner.cycles).sum();
    let candidate_id = candidate.proposal_short_id();
    assert!(matches!(
        pool.admit_entry(candidate, Status::Proposed, replaced, max_size),
        Err(Reject::Full(_))
    ));
    assert_eq!((pool.total_tx_size, pool.total_tx_cycles), totals);
    assert_eq!(
        pool.iter()
            .map(|entry| entry.id.clone())
            .collect::<HashSet<_>>(),
        ids
    );
    assert!(pool.get(&candidate_id).is_none());
    // Undo only the injected counter fault, then independently check all
    // retained graph statistics, edges and ordering indexes.
    pool.total_tx_size = actual_size;
    pool.total_tx_cycles = actual_cycles;
    assert_consistent(pool);
}

#[test]
fn replacement_total_underflow_rejects_before_removing_incumbents() {
    let victims = [entry(100, 300, 100, 1), entry(101, 400, 200, 2)];
    let candidate = TxEntry::dummy_resolve(
        build_tx(vec![(&Byte32::zero(), 100), (&Byte32::zero(), 101)], 1),
        500,
        Capacity::shannons(100),
        500,
    );
    for size_underflow in [true, false] {
        let mut pool = PoolMap::new(10);
        for value in victims.iter().chain([&entry(102, 200, 50, 3)]) {
            assert!(pool.add_proposed(value.clone()).unwrap());
        }
        if size_underflow {
            pool.total_tx_size = 699;
        } else {
            pool.total_tx_cycles = 299;
        }
        assert_stat_refusal_preserves_pool(
            &mut pool,
            candidate.clone(),
            victims.iter().map(TxEntry::proposal_short_id).collect(),
            usize::MAX,
        );
    }
}

#[test]
fn capacity_total_underflow_rejects_before_removing_incumbents() {
    // The candidate's cost would hide the underflow if eviction subtracted
    // from a prospective total that already included the candidate.
    for (size, cycles, limit) in [(499, 1_100, 500), (700, 999, 600)] {
        let mut pool = PoolMap::new(10);
        for value in [entry(100, 500, 1_000, 1), entry(101, 200, 100, 10_000)] {
            assert!(pool.add_proposed(value).unwrap());
        }
        pool.total_tx_size = size;
        pool.total_tx_cycles = cycles;
        assert_stat_refusal_preserves_pool(
            &mut pool,
            entry(102, 400, 800, 1_000),
            HashSet::new(),
            limit,
        );
    }
}

#[test]
fn admission_total_overflow_rejects_without_changing_membership() {
    for size_overflow in [true, false] {
        let mut pool = PoolMap::new(10);
        assert!(pool.add_proposed(entry(100, 300, 100, 1)).unwrap());
        if size_overflow {
            pool.total_tx_size = usize::MAX;
        } else {
            pool.total_tx_cycles = u64::MAX;
        }
        assert_stat_refusal_preserves_pool(
            &mut pool,
            entry(101, 200, 50, 2),
            HashSet::new(),
            usize::MAX,
        );
    }
}
