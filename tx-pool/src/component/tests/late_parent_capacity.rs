use super::util::build_tx;
use crate::component::{entry::TxEntry, pool_map::PoolMap, tx_selector::TxSelector};
use ckb_types::{core::Capacity, packed::Byte32};

#[test]
fn late_parent_reorg_package_respects_actual_bytes_and_cycles() {
    let grandparent = build_tx(vec![(&Byte32::zero(), 1)], 1);
    let parent = build_tx(vec![(&grandparent.hash(), 0)], 1);
    let child = build_tx(vec![(&parent.hash(), 0)], 1);
    let entries = [grandparent, parent, child].map(|tx| {
        let size = tx.data().serialized_size_in_block();
        TxEntry::dummy_resolve(tx, 100, Capacity::shannons(1), size)
    });
    let mut child_entry = entries[2].clone();
    child_entry.fee = Capacity::shannons(1_000_000);
    child_entry.reset_statistic_state();
    let mut pool = PoolMap::new(1_000);
    // Before detachment the first two transactions are canonical; only their
    // child is pooled. Re-add the detached bodies in valid ancestor order.
    assert!(pool.add_proposed(child_entry).unwrap());
    assert!(pool.add_proposed(entries[0].clone()).unwrap());
    assert!(pool.add_proposed(entries[1].clone()).unwrap());
    let id = entries[2].proposal_short_id();
    let child = pool.get_proposed(&id).unwrap();
    let actual_bytes: usize = entries.iter().map(|entry| entry.size).sum();
    let cached_bytes = child.ancestors_size;
    let cached_cycles = child.ancestors_cycles;
    assert_eq!(pool.calc_ancestors(&id).len(), 2);
    assert_eq!(cached_bytes, entries[1].size + entries[2].size);
    assert_eq!(cached_cycles, 200);
    assert_eq!(child.ancestors_count, 2);
    assert!(actual_bytes > cached_bytes);
    for (bytes_limit, cycles_limit) in [
        (cached_bytes, u64::MAX),
        (usize::MAX, cached_cycles),
        (cached_bytes, cached_cycles),
    ] {
        let (selected, bytes, cycles) =
            TxSelector::new(&pool).txs_to_commit(bytes_limit, cycles_limit);
        assert_eq!(selected.len(), 2);
        assert!(bytes <= bytes_limit);
        assert!(cycles <= cycles_limit);
        assert_eq!(
            selected.iter().map(|entry| entry.size).sum::<usize>(),
            bytes
        );
        assert_eq!(
            selected.iter().map(|entry| entry.cycles).sum::<u64>(),
            cycles
        );
    }
}
