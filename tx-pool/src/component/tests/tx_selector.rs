use crate::component::tests::util::{MOCK_CYCLES, build_tx};
use crate::component::{
    entry::TxEntry,
    pool_map::{PoolMap, Status},
    tx_selector::TxSelector,
};
use ckb_types::core::{Capacity, Cycle};
use ckb_types::packed::{Byte32, ProposalShortId};

// more than `MAX_CONSECUTIVE_FAILURES`
const FAILURES: u32 = 4001;

fn add_entry(pool: &mut PoolMap, index: u32, fee: u64, size: usize) -> ProposalShortId {
    let tx = build_tx(vec![(&Byte32::zero(), index)], 1);
    let id = tx.proposal_short_id();
    pool.add_entry(
        TxEntry::dummy_resolve(tx, MOCK_CYCLES, Capacity::shannons(fee), size),
        Status::Proposed,
    )
    .unwrap();
    id
}

#[test]
fn test_txs_to_commit_skips_many_entries_with_non_proposed_ancestors() {
    let mut pool = PoolMap::new(10_000);
    // high fee proposed children whose parents are not proposed yet
    for i in 0..FAILURES {
        let parent = build_tx(vec![(&Byte32::zero(), i)], 1);
        let child = build_tx(vec![(&parent.hash(), 0)], 1);
        pool.add_entry(
            TxEntry::dummy_resolve(parent, MOCK_CYCLES, Capacity::shannons(1), 1),
            Status::Gap,
        )
        .unwrap();
        pool.add_entry(
            TxEntry::dummy_resolve(child, MOCK_CYCLES, Capacity::shannons(1_000_000), 1),
            Status::Proposed,
        )
        .unwrap();
    }
    let valid = add_entry(&mut pool, FAILURES, 1, 1);

    let (selected, _, _) = TxSelector::new(&pool).txs_to_commit(usize::MAX, Cycle::MAX);

    assert_eq!(selected.len(), 1);
    assert_eq!(selected[0].proposal_short_id(), valid);
}

#[test]
fn test_txs_to_commit_resets_failures_after_success() {
    let mut pool = PoolMap::new(10_000);
    let size_limit = 1_000;
    // entries are listed by fee rate in descending order
    let first = add_entry(&mut pool, 0, 900_000, 900);
    // 100 bytes left, entries of 200 bytes don't fit
    // two runs of failures, each within the limit but more than the limit in total
    let half = FAILURES / 2 + 1;
    for i in 1..=half {
        add_entry(&mut pool, i, 180_000, 200);
    }
    let second = add_entry(&mut pool, 10_000, 8_000, 10);
    for i in half + 1..=half * 2 {
        add_entry(&mut pool, i, 140_000, 200);
    }
    let third = add_entry(&mut pool, 10_001, 6_000, 10);

    let (selected, size, _) = TxSelector::new(&pool).txs_to_commit(size_limit, Cycle::MAX);

    let selected: Vec<_> = selected.iter().map(|e| e.proposal_short_id()).collect();
    assert_eq!(selected, vec![first, second, third]);
    assert_eq!(size, 920);
}

#[test]
fn test_txs_to_commit_stops_after_consecutive_failures() {
    let mut pool = PoolMap::new(10_000);
    let size_limit = 1_000;
    let first = add_entry(&mut pool, 0, 900_000, 900);
    for i in 1..=FAILURES {
        add_entry(&mut pool, i, 180_000, 200);
    }
    // fits, but is ranked after more than `MAX_CONSECUTIVE_FAILURES` failures
    add_entry(&mut pool, FAILURES + 1, 6_000, 10);

    let (selected, _, _) = TxSelector::new(&pool).txs_to_commit(size_limit, Cycle::MAX);

    assert_eq!(selected.len(), 1);
    assert_eq!(selected[0].proposal_short_id(), first);
}
