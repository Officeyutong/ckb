//! Proposal eligibility must match commit eligibility: a descendant may not take a
//! proposal slot while an ancestor of it still needs one, because `TxSelector` refuses to
//! commit it until every ancestor is `Proposed`.

use crate::component::tests::util::{MOCK_CYCLES, build_tx};
use crate::component::{
    entry::TxEntry,
    pool_map::{PoolMap, Status},
    tx_selector::TxSelector,
};
use ckb_types::core::{Capacity, Cycle};
use ckb_types::packed::{Byte32, ProposalShortId};
use std::collections::HashSet;

/// Builds a chain of `depth` transactions where every transaction spends the previous
/// output. With an equal fee rate, the ancestor-weight tie break ranks the deepest
/// transaction first, which is the ordering behind the starvation bug.
fn build_chain(pool: &mut PoolMap, depth: usize, fee: u64, size: usize) -> Vec<ProposalShortId> {
    let mut ids = Vec::with_capacity(depth);
    let mut parent: Option<Byte32> = None;
    for _ in 0..depth {
        let tx = match &parent {
            Some(hash) => build_tx(vec![(hash, 0)], 1),
            None => build_tx(vec![(&Byte32::zero(), 0)], 1),
        };
        parent = Some(tx.hash());
        let id = tx.proposal_short_id();
        pool.add_entry(
            TxEntry::dummy_resolve(tx, MOCK_CYCLES, Capacity::shannons(fee), size),
            Status::Pending,
        )
        .unwrap();
        ids.push(id);
    }
    ids
}

/// Builds one pending root with `children` pending children. Every child ranks above the
/// root because its ancestor weight is larger at an equal fee rate.
fn build_star(
    pool: &mut PoolMap,
    children: usize,
    fee: u64,
    size: usize,
) -> (ProposalShortId, Vec<ProposalShortId>) {
    let root_tx = build_tx(vec![(&Byte32::zero(), 0)], children);
    let root = root_tx.proposal_short_id();
    let root_hash = root_tx.hash();
    pool.add_entry(
        TxEntry::dummy_resolve(root_tx, MOCK_CYCLES, Capacity::shannons(fee), size),
        Status::Pending,
    )
    .unwrap();

    let child_ids = (0..children)
        .map(|index| {
            let tx = build_tx(vec![(&root_hash, index as u32)], 1);
            let id = tx.proposal_short_id();
            pool.add_entry(
                TxEntry::dummy_resolve(tx, MOCK_CYCLES, Capacity::shannons(fee), size),
                Status::Pending,
            )
            .unwrap();
            id
        })
        .collect::<Vec<_>>();
    (root, child_ids)
}

/// Adds an independent root that spends its own outpoint.
fn add_root(pool: &mut PoolMap, out_point_index: u32, fee: u64, size: usize) -> ProposalShortId {
    let tx = build_tx(vec![(&Byte32::zero(), out_point_index)], 1);
    let id = tx.proposal_short_id();
    pool.add_entry(
        TxEntry::dummy_resolve(tx, MOCK_CYCLES, Capacity::shannons(fee), size),
        Status::Pending,
    )
    .unwrap();
    id
}

fn set(ids: &[ProposalShortId]) -> HashSet<ProposalShortId> {
    ids.iter().cloned().collect()
}

#[test]
fn proposals_include_pending_ancestors() {
    let mut pool = PoolMap::new(1_000);
    // ids[2] ranks first, but it cannot be committed before ids[1] and ids[0].
    let ids = build_chain(&mut pool, 3, 1_000, 100);

    let proposals = pool.get_proposals(2, &HashSet::new());

    assert_eq!(
        proposals,
        set(&ids[..2]),
        "the two slots must go to an ancestor-closed prefix, not to the deepest entries"
    );
}

#[test]
fn proposals_fall_back_to_the_ancestor_prefix() {
    let mut pool = PoolMap::new(1_000);
    let ids = build_chain(&mut pool, 4, 1_000, 100);

    // The deepest entry needs three ancestors and does not fit; its prefix still does.
    let proposals = pool.get_proposals(3, &HashSet::new());

    assert_eq!(proposals, set(&ids[..3]));
}

#[test]
fn proposals_keep_the_root_ahead_of_many_descendants() {
    let mut pool = PoolMap::new(1_000);
    let (root, children) = build_star(&mut pool, 300, 1_000, 100);
    assert_eq!(children.len(), 300);

    let proposals = pool.get_proposals(10, &HashSet::new());

    assert_eq!(proposals.len(), 10);
    assert!(
        proposals.contains(&root),
        "the pending root must be proposed so that its children can ever be committed"
    );
}

#[test]
fn proposals_respect_the_limit() {
    let mut pool = PoolMap::new(1_000);
    let (root, _) = build_star(&mut pool, 300, 1_000, 100);

    for limit in [0, 1, 2, 7, 10, 500] {
        let proposals = pool.get_proposals(limit, &HashSet::new());
        assert!(proposals.len() <= limit, "limit {limit} exceeded");
        if limit > 0 {
            assert!(proposals.contains(&root), "limit {limit} dropped the root");
        }
    }
}

#[test]
fn proposals_skip_gap_ancestors() {
    let mut pool = PoolMap::new(1_000);
    let ids = build_chain(&mut pool, 2, 1_000, 100);
    // A `Gap` ancestor was already proposed by a recent block; only the child needs a slot.
    pool.set_entry(&ids[0], Status::Gap);

    let proposals = pool.get_proposals(10, &HashSet::new());

    assert_eq!(proposals, set(&ids[1..]));
}

#[test]
fn proposals_skip_ancestors_proposed_by_an_uncle() {
    let mut pool = PoolMap::new(1_000);
    let ids = build_chain(&mut pool, 2, 1_000, 100);

    // The proposal table sees the union of the main block and uncle proposals, so an uncle
    // proposing the root still lets the main block propose the child.
    let exclusion = set(&ids[..1]);
    let proposals = pool.get_proposals(10, &exclusion);

    assert_eq!(proposals, set(&ids[1..]));
}

#[test]
fn proposals_fill_a_pending_grandparent_behind_an_uncle_ancestor() {
    let mut pool = PoolMap::new(1_000);
    // ids[0] -> ids[1] -> ids[2]; the uncle proposed the middle transaction only.
    let ids = build_chain(&mut pool, 3, 1_000, 100);
    let exclusion = set(&ids[1..2]);

    let proposals = pool.get_proposals(10, &exclusion);

    assert_eq!(proposals, set(&[ids[0].clone(), ids[2].clone()]));
}

#[test]
fn proposals_use_the_remaining_slots_when_closures_do_not_fit() {
    let mut pool = PoolMap::new(1_000);
    // A deep chain ranks above independent roots because its ancestor weight is larger. The
    // leading candidate needs more slots than the template has, yet the slots must be used.
    let chain = build_chain(&mut pool, 10, 1_000, 100);
    add_root(&mut pool, 1, 10, 100);
    add_root(&mut pool, 2, 10, 100);

    let limit = 5;
    let proposals = pool.get_proposals(limit, &HashSet::new());

    assert_eq!(
        proposals.len(),
        limit,
        "the remaining slots must not be left unused"
    );
    assert!(
        chain[..limit].iter().all(|id| proposals.contains(id)),
        "the root-first prefix of the leading chain should fill the slots: {proposals:?}"
    );
}

#[test]
fn proposals_unblock_a_chain_across_templates() {
    let mut pool = PoolMap::new(1_000);
    let ids = build_chain(&mut pool, 5, 1_000, 100);
    // Two proposal slots per template, so the chain advances over several heights.
    let limit = 2;

    let mut height = 0;
    while pool.size() > 0 {
        assert!(height <= ids.len(), "the chain did not drain in time");

        // Propose: the block's proposal section marks entries as `Gap`, which still cannot
        // be committed.
        let proposals = pool.get_proposals(limit, &HashSet::new());
        assert!(
            !proposals.is_empty(),
            "height {height}: the template proposed nothing"
        );
        for id in &proposals {
            pool.set_entry(id, Status::Gap);
        }
        let (committable, _, _) = TxSelector::new(&pool).txs_to_commit(usize::MAX, Cycle::MAX);
        assert!(
            committable.is_empty(),
            "height {height}: a `Gap` transaction was committed"
        );

        // The proposal matures, and the selector must make progress instead of producing a
        // cellbase-only template.
        for id in &proposals {
            pool.set_entry(id, Status::Proposed);
        }
        let (committed, _, _) = TxSelector::new(&pool).txs_to_commit(usize::MAX, Cycle::MAX);
        assert!(
            !committed.is_empty(),
            "height {height}: nothing was committable, the block would be cellbase-only"
        );
        for entry in committed {
            pool.remove_entry(&entry.proposal_short_id());
        }

        height += 1;
    }
}
