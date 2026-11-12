use super::util::build_tx;
use crate::component::{
    entry::TxEntry,
    pool_map::{PoolMap, Status},
    tx_selector::TxSelector,
};
use ckb_types::{core::Capacity, packed::Byte32};

fn late_parent_pool(grandparent_fee: u64) -> (PoolMap, [TxEntry; 3]) {
    let grandparent = build_tx(vec![(&Byte32::zero(), 1)], 1);
    let parent = build_tx(vec![(&grandparent.hash(), 0)], 1);
    let child = build_tx(vec![(&parent.hash(), 0)], 1);
    let entries = [
        (grandparent, grandparent_fee),
        (parent, 1),
        (child, 1_000_000),
    ]
    .map(|(tx, fee)| {
        let size = tx.data().serialized_size_in_block();
        TxEntry::dummy_resolve(tx, 100, Capacity::shannons(fee), size)
    });
    let mut pool = PoolMap::new(1_000);
    for entry in &entries {
        assert!(pool.add_proposed(entry.clone()).unwrap());
    }
    let child_id = entries[2].proposal_short_id();
    assert_eq!(pool.get_proposed(&child_id).unwrap().ancestors_count, 3);

    // Commit the first two transactions, leaving their child in the pool.
    for entry in &entries[..2] {
        assert!(
            pool.remove_entry_by_tx_hash(&entry.transaction().hash())
                .is_some()
        );
    }
    assert_eq!(pool.size(), 1);
    assert!(pool.calc_ancestors(&child_id).is_empty());

    // Detach that block while all three proposals remain in the common chain.
    // Re-add its bodies in their original, valid ancestor order.
    for entry in &entries[..2] {
        assert!(pool.add_proposed(entry.clone()).unwrap());
    }
    let child = pool.get_proposed(&child_id).unwrap();
    assert_eq!(pool.calc_ancestors(&child_id).len(), 2);
    assert_eq!(
        child.ancestors_size,
        entries.iter().map(|entry| entry.size).sum::<usize>()
    );
    assert_eq!(child.ancestors_cycles, 300);
    assert_eq!(child.ancestors_count, 3);
    (pool, entries)
}

fn assert_selection(pool: &PoolMap, bytes_limit: usize, cycles_limit: u64, expected: &[TxEntry]) {
    let (selected, bytes, cycles) = TxSelector::new(pool).txs_to_commit(bytes_limit, cycles_limit);
    assert!(
        bytes <= bytes_limit,
        "selected {bytes} bytes, limit {bytes_limit}"
    );
    assert!(
        cycles <= cycles_limit,
        "selected {cycles} cycles, limit {cycles_limit}"
    );
    assert_eq!(
        selected
            .iter()
            .map(TxEntry::transaction)
            .collect::<Vec<_>>(),
        expected
            .iter()
            .map(TxEntry::transaction)
            .collect::<Vec<_>>()
    );
    assert_eq!(
        selected.iter().map(|entry| entry.size).sum::<usize>(),
        bytes
    );
    assert_eq!(
        selected.iter().map(|entry| entry.cycles).sum::<u64>(),
        cycles
    );
}

#[test]
fn late_parent_package_respects_byte_and_cycle_budgets() {
    // Exercise both the original package and the modified package after its
    // high-fee grandparent has already been selected.
    for grandparent_fee in [1, 2_000_000] {
        let (pool, entries) = late_parent_pool(grandparent_fee);
        let two_bytes = entries[0].size + entries[1].size;
        let all_bytes = entries.iter().map(|entry| entry.size).sum();
        for (bytes, cycles, count) in [
            (two_bytes, u64::MAX, 2),
            (usize::MAX, 200, 2),
            (two_bytes, 200, 2),
            (all_bytes, 300, 3),
            (entries[0].size - 1, u64::MAX, 0),
            (usize::MAX, 99, 0),
        ] {
            assert_selection(&pool, bytes, cycles, &entries[..count]);
        }
    }
}

#[test]
fn late_parent_package_uses_the_remaining_budget() {
    let (mut pool, entries) = late_parent_pool(1);
    let tx = build_tx(vec![(&Byte32::zero(), 2)], 1);
    let size = tx.data().serialized_size_in_block();
    let independent = TxEntry::dummy_resolve(tx, 50, Capacity::shannons(2_000_000), size);
    assert!(pool.add_proposed(independent.clone()).unwrap());
    let expected = [independent, entries[0].clone(), entries[1].clone()];
    assert_selection(
        &pool,
        expected.iter().map(|entry| entry.size).sum(),
        250,
        &expected,
    );
}

#[test]
fn late_parent_package_waits_for_unproposed_ancestors() {
    for status in [Status::Pending, Status::Gap] {
        let (mut pool, entries) = late_parent_pool(1);
        pool.set_entry(&entries[1].proposal_short_id(), status);
        assert_selection(&pool, usize::MAX, u64::MAX, &entries[..1]);
    }
}
