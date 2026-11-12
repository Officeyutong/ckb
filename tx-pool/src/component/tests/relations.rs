use super::util::{build_tx, build_tx_with_dep};
use crate::component::{
    entry::TxEntry,
    pool_map::{PoolMap, Status},
    tx_selector::TxSelector,
};
use crate::error::Reject;
use ckb_types::{core::Capacity, packed::Byte32};
use std::collections::{HashMap, HashSet};

fn graph(parents: &[&[usize]]) -> Vec<TxEntry> {
    let mut entries: Vec<TxEntry> = Vec::new();
    for (index, parents) in parents.iter().enumerate() {
        let hashes: Vec<_> = parents
            .iter()
            .map(|&parent| entries[parent].transaction().hash())
            .collect();
        let tx = build_tx_with_dep(
            vec![(&Byte32::zero(), index as u32 + 100)],
            hashes.iter().map(|hash| (hash, 0)).collect(),
            2,
        );
        let size = tx.data().serialized_size_in_block();
        let mut entry = TxEntry::dummy_resolve(
            tx,
            (index as u64 + 1) * 100,
            Capacity::shannons(1 << index),
            size,
        );
        entry.timestamp = index as u64;
        entries.push(entry);
    }
    entries
}

fn closure(start: usize, direct: &[HashSet<usize>]) -> HashSet<usize> {
    let mut result = HashSet::new();
    let mut pending: Vec<_> = direct[start].iter().copied().collect();
    while let Some(index) = pending.pop() {
        if result.insert(index) {
            pending.extend(&direct[index]);
        }
    }
    assert!(!result.contains(&start), "fixture is acyclic");
    result
}

/// Derive relations from retained bodies, independently of links and cached
/// statistics. Check both materialized keys and their ordered indexes.
pub(super) fn assert_consistent(pool: &PoolMap) {
    let entries: Vec<_> = pool.iter().collect();
    let mut parents = vec![HashSet::new(); entries.len()];
    let mut children = parents.clone();
    for (child, entry) in entries.iter().enumerate() {
        let tx = entry.inner.transaction();
        for (parent, candidate) in entries.iter().enumerate() {
            if parent == child {
                continue;
            }
            let hash = candidate.inner.transaction().hash();
            let consumes_output = tx
                .input_pts_iter()
                .chain(entry.inner.related_dep_out_points().cloned())
                .any(|point| point.tx_hash() == hash);
            let consumes_read_cell = tx.input_pts_iter().any(|point| {
                candidate
                    .inner
                    .related_dep_out_points()
                    .any(|dep| dep == &point)
            });
            if consumes_output || consumes_read_cell {
                parents[child].insert(parent);
                children[parent].insert(child);
            }
        }
    }
    let ids = |indices: &HashSet<usize>| {
        indices
            .iter()
            .map(|&index| entries[index].id.clone())
            .collect::<HashSet<_>>()
    };
    for (index, entry) in entries.iter().enumerate() {
        assert_eq!(
            pool.links.get_parents(&entry.id),
            Some(&ids(&parents[index]))
        );
        assert_eq!(
            pool.links.get_children(&entry.id),
            Some(&ids(&children[index]))
        );
        let ancestors = closure(index, &parents);
        let descendants = closure(index, &children);
        assert_eq!(pool.calc_ancestors(&entry.id), ids(&ancestors));
        assert_eq!(pool.calc_descendants(&entry.id), ids(&descendants));
        let totals = |related: &HashSet<usize>| {
            related.iter().copied().chain([index]).fold(
                (0usize, 0u64, 0u64),
                |(size, cycles, fee), member| {
                    let entry = &entries[member].inner;
                    (
                        size + entry.size,
                        cycles + entry.cycles,
                        fee + entry.fee.as_u64(),
                    )
                },
            )
        };
        let inner = &entry.inner;
        assert_eq!(
            inner.ancestors_count,
            ancestors.len() + 1,
            "ancestor count {index}"
        );
        assert_eq!(
            inner.descendants_count,
            descendants.len() + 1,
            "descendant count {index}"
        );
        assert_eq!(
            (
                inner.ancestors_size,
                inner.ancestors_cycles,
                inner.ancestors_fee.as_u64()
            ),
            totals(&ancestors),
            "ancestor weights {index}"
        );
        assert_eq!(
            (
                inner.descendants_size,
                inner.descendants_cycles,
                inner.descendants_fee.as_u64()
            ),
            totals(&descendants),
            "descendant weights {index}"
        );
        assert_eq!(entry.score, inner.as_score_key());
        assert_eq!(entry.evict_key, inner.as_evict_key());
    }
    let mut scores: Vec<_> = entries
        .iter()
        .map(|entry| entry.inner.as_score_key())
        .collect();
    scores.sort();
    assert_eq!(
        pool.entries
            .iter_by_score()
            .map(|entry| entry.score.clone())
            .collect::<Vec<_>>(),
        scores
    );
    let mut eviction: Vec<_> = entries
        .iter()
        .map(|entry| entry.inner.as_evict_key())
        .collect();
    eviction.sort();
    assert_eq!(
        pool.entries
            .iter_by_evict_key()
            .map(|entry| entry.evict_key.clone())
            .collect::<Vec<_>>(),
        eviction
    );
    assert_eq!(
        pool.total_tx_size,
        entries.iter().map(|entry| entry.inner.size).sum::<usize>()
    );
    assert_eq!(
        pool.total_tx_cycles,
        entries.iter().map(|entry| entry.inner.cycles).sum::<u64>()
    );
    let mut inputs = HashMap::new();
    let mut deps: HashMap<_, HashSet<_>> = HashMap::new();
    for entry in &entries {
        for point in entry.inner.transaction().input_pts_iter() {
            assert!(inputs.insert(point, entry.id.clone()).is_none());
        }
        for point in entry.inner.related_dep_out_points() {
            deps.entry(point.clone())
                .or_default()
                .insert(entry.id.clone());
        }
    }
    assert_eq!(pool.edges.inputs, inputs);
    assert_eq!(pool.edges.deps, deps);
}

fn insert(pool: &mut PoolMap, entry: &TxEntry) {
    assert!(pool.add_proposed(entry.clone()).unwrap());
}

#[test]
fn late_parent_updates_both_sides_without_recounting_shared_ancestors() {
    // 0 -> 1 -> 3 -> 4; 0 -> 2 -> 3. Reinsert 1 into an existing diamond.
    let entries = graph(&[&[], &[0], &[0], &[1, 2], &[3]]);
    let mut pool = PoolMap::new(5);
    for &index in &[0, 2, 3, 4, 1] {
        insert(&mut pool, &entries[index]);
    }
    assert_consistent(&pool);
}

#[test]
fn insertion_and_removal_orders_preserve_graph_statistics() {
    let entries = graph(&[&[], &[0], &[0], &[1, 2], &[1, 3]]);
    // Enumerate every insertion order, then remove from each possible starting
    // point. This covers disconnected fragments, bridges and shared paths.
    for a in 0..5 {
        for b in 0..5 {
            for c in 0..5 {
                for d in 0..5 {
                    for e in 0..5 {
                        let order = [a, b, c, d, e];
                        if order.into_iter().collect::<HashSet<_>>().len() != 5 {
                            continue;
                        }
                        for start in 0..5 {
                            let mut pool = PoolMap::new(10);
                            for &index in &order {
                                insert(&mut pool, &entries[index]);
                                assert_consistent(&pool);
                            }
                            for step in 0..5 {
                                let index = (start + step) % 5;
                                pool.remove_entry(&entries[index].proposal_short_id())
                                    .unwrap();
                                assert_consistent(&pool);
                            }
                        }
                    }
                }
            }
        }
    }
}

#[test]
fn every_five_node_dag_preserves_statistics_across_membership_changes() {
    let edges: Vec<_> = (0..5)
        .flat_map(|parent| (parent + 1..5).map(move |child| (parent, child)))
        .collect();
    for mask in 0..1usize << edges.len() {
        let mut parents = vec![Vec::new(); 5];
        for (bit, &(parent, child)) in edges.iter().enumerate() {
            if mask & (1 << bit) != 0 {
                parents[child].push(parent);
            }
        }
        let entries = graph(&parents.iter().map(Vec::as_slice).collect::<Vec<_>>());
        // Derive the removal closure from the fixture's topological order,
        // independently of the pool's links and cached package statistics.
        let mut removed_indices = HashSet::from([2]);
        for (child, parents) in parents.iter().enumerate() {
            if parents
                .iter()
                .any(|parent| removed_indices.contains(parent))
            {
                removed_indices.insert(child);
            }
        }
        let expected_removed: HashSet<_> = removed_indices
            .iter()
            .map(|&index| entries[index].proposal_short_id())
            .collect();
        for order in [[0, 1, 2, 3, 4], [4, 3, 2, 1, 0], [2, 0, 4, 1, 3]] {
            let mut pool = PoolMap::new(5);
            for index in order {
                insert(&mut pool, &entries[index]);
                assert_consistent(&pool);
            }
            let removed = pool.remove_entry_and_descendants(&entries[2].proposal_short_id());
            assert_eq!(removed.len(), expected_removed.len());
            assert_eq!(
                removed
                    .iter()
                    .map(TxEntry::proposal_short_id)
                    .collect::<HashSet<_>>(),
                expected_removed
            );
            assert_consistent(&pool);
            // Returned entries still carry their old statistics. Reinsertion
            // must derive new totals even when children return before parents.
            for entry in removed {
                insert(&mut pool, &entry);
                assert_consistent(&pool);
            }
            for index in [1, 3, 0, 4, 2] {
                pool.remove_entry(&entries[index].proposal_short_id())
                    .unwrap();
                assert_consistent(&pool);
            }
        }
    }
}

#[test]
fn capacity_preflight_matches_materialized_eviction_on_every_four_node_dag() {
    let edges: Vec<_> = (0..4)
        .flat_map(|parent| (parent + 1..4).map(move |child| (parent, child)))
        .collect();
    let ids = |pool: &PoolMap| {
        pool.iter()
            .map(|entry| entry.id.clone())
            .collect::<HashSet<_>>()
    };
    for mask in 0..1usize << edges.len() {
        let mut parents = vec![Vec::new(); 4];
        for (bit, &(parent, child)) in edges.iter().enumerate() {
            if mask & (1 << bit) != 0 {
                parents[child].push(parent);
            }
        }
        let entries = graph(&parents.iter().map(Vec::as_slice).collect::<Vec<_>>());
        for candidate in 0..4 {
            for mixed_status in [false, true] {
                // Include every union of replacement roots, including roots
                // whose descendant packages overlap.
                for replaced_roots in 0..1usize << entries.len() {
                    if replaced_roots & (1 << candidate) != 0 {
                        continue;
                    }
                    for limit in [0, 250, 500, 750, 1_000, usize::MAX] {
                        let status = |index| {
                            if mixed_status {
                                [Status::Pending, Status::Gap, Status::Proposed][index % 3]
                            } else {
                                Status::Proposed
                            }
                        };
                        let mut actual = PoolMap::new(4);
                        let mut reference = PoolMap::new(4);
                        for (index, entry) in entries
                            .iter()
                            .enumerate()
                            .filter(|(index, _)| *index != candidate)
                        {
                            actual.add_entry(entry.clone(), status(index)).unwrap();
                            reference.add_entry(entry.clone(), status(index)).unwrap();
                        }
                        let before = ids(&actual);
                        let candidate_entry = &entries[candidate];
                        let replaced: HashSet<_> = entries
                            .iter()
                            .enumerate()
                            .filter(|(index, _)| replaced_roots & (1 << index) != 0)
                            .flat_map(|(_, entry)| {
                                reference.remove_entry_and_descendants(&entry.proposal_short_id())
                            })
                            .map(|entry| entry.proposal_short_id())
                            .collect();
                        let missing_backing =
                            candidate_entry.related_dep_out_points().any(|point| {
                                actual
                                    .get_by_tx_hash(&point.tx_hash())
                                    .is_some_and(|producer| replaced.contains(&producer.id))
                            });
                        let result = actual.admit_entry(
                            candidate_entry.clone(),
                            status(candidate),
                            replaced.clone(),
                            limit,
                        );
                        if missing_backing {
                            assert!(matches!(result, Err(Reject::Resolve(_))));
                            assert_eq!(ids(&actual), before);
                        } else {
                            reference
                                .add_entry(candidate_entry.clone(), status(candidate))
                                .unwrap();
                            while reference.total_tx_size > limit {
                                let victim = [Status::Pending, Status::Gap, Status::Proposed]
                                    .into_iter()
                                    .find_map(|status| {
                                        reference
                                            .entries
                                            .iter_by_evict_key()
                                            .find(|entry| entry.status == status)
                                            .map(|entry| entry.id.clone())
                                    })
                                    .unwrap();
                                reference.remove_entry_and_descendants(&victim);
                                assert_consistent(&reference);
                            }
                            if reference
                                .get(&candidate_entry.proposal_short_id())
                                .is_some()
                            {
                                let evicted = result.unwrap().unwrap();
                                assert_eq!(
                                    ids(&actual),
                                    ids(&reference),
                                    "mask {mask} candidate {candidate} replaced {replaced_roots:b} limit {limit}"
                                );
                                assert_eq!(
                                    evicted
                                        .replaced
                                        .iter()
                                        .map(TxEntry::proposal_short_id)
                                        .collect::<HashSet<_>>(),
                                    replaced
                                );
                                assert!(evicted.readers.is_empty());
                                assert_eq!(
                                    evicted
                                        .capacity
                                        .iter()
                                        .map(TxEntry::proposal_short_id)
                                        .collect::<HashSet<_>>(),
                                    before
                                        .difference(&ids(&reference))
                                        .filter(|id| !replaced.contains(*id))
                                        .cloned()
                                        .collect()
                                );
                            } else {
                                assert!(matches!(result, Err(Reject::Full(_))));
                                assert_eq!(ids(&actual), before);
                            }
                        }
                        assert_consistent(&actual);
                    }
                }
            }
        }
    }
}

#[test]
fn reorg_reinsertion_keeps_deep_packages_in_dependency_order() {
    let entries = graph(&[&[], &[0], &[1], &[2], &[3]]);
    let mut pool = PoolMap::new(10);
    for entry in &entries {
        insert(&mut pool, entry);
    }
    for entry in &entries[..3] {
        pool.remove_entry(&entry.proposal_short_id()).unwrap();
    }
    for entry in &entries[..3] {
        insert(&mut pool, entry);
    }
    let (selected, _, _) = TxSelector::new(&pool).txs_to_commit(usize::MAX, u64::MAX);
    assert_eq!(
        selected
            .iter()
            .map(TxEntry::proposal_short_id)
            .collect::<Vec<_>>(),
        entries
            .iter()
            .map(TxEntry::proposal_short_id)
            .collect::<Vec<_>>()
    );
    assert_consistent(&pool);
}

#[test]
fn late_parent_cannot_push_descendants_over_the_ancestor_limit() {
    let entries = graph(&[&[], &[0], &[1], &[2]]);
    let mut pool = PoolMap::new(3);
    for &index in &[0, 2, 3] {
        insert(&mut pool, &entries[index]);
    }
    assert_consistent(&pool);
    assert!(matches!(
        pool.add_proposed(entries[1].clone()),
        Err(Reject::ExceededMaximumAncestorsCount)
    ));
    assert_eq!(pool.size(), 3);
    assert_consistent(&pool);
}

#[test]
fn planned_reader_evictions_release_conflicting_inputs() {
    let mut pool = PoolMap::new(1);
    let readers: HashSet<_> = (100..103)
        .map(|input| {
            let tx = build_tx_with_dep(
                vec![(&Byte32::zero(), input)],
                vec![(&Byte32::zero(), 500)],
                1,
            );
            TxEntry::dummy_resolve(tx, 50, Capacity::shannons(1), 200)
        })
        .collect();
    for reader in &readers {
        insert(&mut pool, reader);
    }
    // All three readers must leave to fit the ancestor limit. One also owns
    // another candidate input; that reservation leaves with the reader.
    let tx = build_tx(vec![(&Byte32::zero(), 500), (&Byte32::zero(), 100)], 1);
    let candidate = TxEntry::dummy_resolve(tx, 60, Capacity::shannons(2), 300);
    let (inserted, evicted) = pool.add_entry(candidate, Status::Proposed).unwrap();
    assert!(inserted);
    assert_eq!(evicted, readers);
    assert_eq!(pool.size(), 1);
    assert_eq!(pool.inputs_len(), 2);
    assert_consistent(&pool);
}

fn dep_entry(input: u32, deps: Vec<(&Byte32, u32)>) -> TxEntry {
    let tx = build_tx_with_dep(vec![(&Byte32::zero(), input)], deps, 1);
    let size = tx.data().serialized_size_in_block();
    TxEntry::dummy_resolve(tx, 50, Capacity::shannons(1), size)
}

#[test]
fn reader_eviction_excludes_descendants_reached_through_a_retained_child() {
    // S -> R -> D, P/Q[/T] -> C -> X -> D. R reads a cell spent by late C.
    // Removing R also removes D. X fits exactly with two candidate parents,
    // but three must reject C without committing either planned eviction.
    let entries = graph(&[&[], &[], &[], &[]]);
    for parent_count in [2, 3] {
        let parents: Vec<_> = entries[1..=parent_count]
            .iter()
            .map(|entry| entry.transaction().hash())
            .collect();
        let candidate = dep_entry(500, parents.iter().map(|hash| (hash, 0)).collect());
        let reader = dep_entry(
            104,
            vec![
                (&entries[0].transaction().hash(), 0),
                (&Byte32::zero(), 500),
            ],
        );
        let child = dep_entry(105, vec![(&candidate.transaction().hash(), 0)]);
        let descendant = dep_entry(
            106,
            vec![
                (&reader.transaction().hash(), 0),
                (&child.transaction().hash(), 0),
            ],
        );
        let mut pool = PoolMap::new(4);
        let original: Vec<_> = entries
            .iter()
            .chain([&reader, &child, &descendant])
            .collect();
        for entry in &original {
            insert(&mut pool, entry);
        }
        assert_consistent(&pool);
        let result = pool.add_entry(candidate.clone(), Status::Proposed);
        if parent_count == 2 {
            let (inserted, evicted) = result.unwrap();
            assert!(inserted);
            assert_eq!(evicted, HashSet::from([reader.clone(), descendant.clone()]));
            assert_eq!(pool.size(), original.len() - 1);
            assert_eq!(
                pool.get(&candidate.proposal_short_id())
                    .unwrap()
                    .ancestors_count,
                3
            );
            assert_eq!(
                pool.get(&child.proposal_short_id())
                    .unwrap()
                    .ancestors_count,
                4
            );
        } else {
            assert!(matches!(result, Err(Reject::ExceededMaximumAncestorsCount)));
            assert_eq!(pool.size(), original.len());
            for entry in original {
                assert!(pool.get(&entry.proposal_short_id()).is_some());
            }
            assert!(pool.get(&candidate.proposal_short_id()).is_none());
        }
        assert_consistent(&pool);
    }
}

#[test]
fn reader_eviction_recounts_ancestors_disconnected_from_the_candidate() {
    // A -> B -> R, with R also reading the cell spent by C. C depends on P
    // and either A or B: removing R leaves 3 or 4 ancestors including C.
    let entries = graph(&[&[], &[0], &[]]);
    let reader = dep_entry(
        103,
        vec![
            (&entries[1].transaction().hash(), 0),
            (&Byte32::zero(), 500),
        ],
    );
    for (index, parent) in entries[..2].iter().enumerate() {
        let mut pool = PoolMap::new(3);
        for entry in entries.iter().chain([&reader]) {
            insert(&mut pool, entry);
        }
        let candidate = dep_entry(
            500,
            vec![
                (&entries[2].transaction().hash(), 0),
                (&parent.transaction().hash(), 0),
            ],
        );
        let result = pool.add_entry(candidate.clone(), Status::Proposed);
        if index == 0 {
            let (inserted, evicted) = result.unwrap();
            assert!(inserted);
            assert_eq!(evicted, HashSet::from([reader.clone()]));
            assert_eq!(
                pool.get(&candidate.proposal_short_id())
                    .unwrap()
                    .ancestors_count,
                3
            );
        } else {
            assert!(matches!(result, Err(Reject::ExceededMaximumAncestorsCount)));
            assert!(pool.get(&reader.proposal_short_id()).is_some());
            assert!(pool.get(&candidate.proposal_short_id()).is_none());
        }
        assert_eq!(pool.size(), 4);
        assert_consistent(&pool);
    }
}

#[test]
fn reader_eviction_preserves_candidate_producers_and_their_ancestors() {
    use ckb_types::{
        core::{
            DepType,
            cell::{CellMeta, ResolvedTransaction},
        },
        packed::{CellDep, OutPoint},
        prelude::*,
    };
    use std::sync::Arc;

    for dependency in [None, Some(DepType::Code), Some(DepType::DepGroup)] {
        // R reads K; P depends on R. C spends K and uses P's output, so R
        // cannot be evicted. A separate, more expensive reader of K may leave.
        let reader = dep_entry(100, vec![(&Byte32::zero(), 500)]);
        let producer = dep_entry(101, vec![(&reader.transaction().hash(), 0)]);
        let mut other = dep_entry(102, vec![(&Byte32::zero(), 500)]);
        other.fee = Capacity::shannons(100);
        let output = producer.transaction().hash();
        let tx = if dependency.is_none() {
            build_tx(vec![(&Byte32::zero(), 500), (&output, 0)], 1)
        } else {
            build_tx(vec![(&Byte32::zero(), 500)], 1)
        };
        let group = OutPoint::new(Byte32::zero(), 600);
        let output = OutPoint::new(output, 0);
        let tx = tx
            .as_advanced_builder()
            .set_cell_deps(
                dependency
                    .map(|dep_type| {
                        CellDep::new_builder()
                            .out_point(if dep_type == DepType::DepGroup {
                                group.clone()
                            } else {
                                output.clone()
                            })
                            .dep_type(dep_type)
                            .build()
                    })
                    .into_iter()
                    .collect(),
            )
            .build();
        let size = tx.data().serialized_size_in_block();
        let mut resolved = ResolvedTransaction::dummy_resolve(tx);
        if dependency == Some(DepType::DepGroup) {
            resolved.resolved_cell_deps = vec![CellMeta {
                out_point: output,
                ..Default::default()
            }];
            resolved.resolved_dep_groups = vec![CellMeta {
                out_point: group,
                ..Default::default()
            }];
        }
        let candidate = TxEntry::new(Arc::new(resolved), 50, Capacity::shannons(1), size);
        let mut pool = PoolMap::new(3);
        for entry in [&reader, &producer, &other] {
            insert(&mut pool, entry);
        }
        assert_consistent(&pool);
        let (inserted, evicted) = pool.add_entry(candidate.clone(), Status::Proposed).unwrap();
        assert!(inserted);
        assert_eq!(evicted, HashSet::from([other]));
        assert_eq!(
            pool.get(&candidate.proposal_short_id())
                .unwrap()
                .ancestors_count,
            3
        );
        assert!(pool.get(&reader.proposal_short_id()).is_some());
        assert!(pool.get(&producer.proposal_short_id()).is_some());
        assert_consistent(&pool);
    }
}

#[test]
fn input_rejection_does_not_commit_planned_reader_evictions() {
    let reader = build_tx_with_dep(
        vec![(&Byte32::zero(), 100)],
        vec![(&Byte32::zero(), 500)],
        1,
    );
    let conflict = build_tx(vec![(&Byte32::zero(), 101)], 1);
    let producer = build_tx(vec![(&Byte32::zero(), 102)], 1);
    let producer_hash = producer.hash();
    let mut pool = PoolMap::new(2);
    for tx in [reader, conflict, producer] {
        insert(
            &mut pool,
            &TxEntry::dummy_resolve(tx, 50, Capacity::shannons(1), 200),
        );
    }
    // A fresh input and a retained producer precede the conflict. Neither
    // their indexes nor the planned reader removal may change on rejection.
    let tx = build_tx(
        vec![
            (&producer_hash, 0),
            (&Byte32::zero(), 500),
            (&Byte32::zero(), 101),
        ],
        1,
    );
    let candidate = TxEntry::dummy_resolve(tx, 50, Capacity::shannons(1), 300);
    assert!(matches!(
        pool.add_entry(candidate, Status::Proposed),
        Err(Reject::RBFRejected(_))
    ));
    assert_eq!(pool.size(), 3);
    assert_eq!(pool.inputs_len(), 3);
    assert_consistent(&pool);
}

#[test]
fn late_dependency_reader_precedes_a_retained_spender() {
    let entries = graph(&[&[], &[0]]);
    let tx = build_tx(vec![(&entries[0].transaction().hash(), 0)], 1);
    let spender = TxEntry::dummy_resolve(tx, 50, Capacity::shannons(1), 200);
    let mut pool = PoolMap::new(3);
    for entry in [&entries[0], &spender, &entries[1]] {
        insert(&mut pool, entry);
        assert_consistent(&pool);
    }
    let (selected, _, _) = TxSelector::new(&pool).txs_to_commit(usize::MAX, u64::MAX);
    assert_eq!(
        selected.last().unwrap().proposal_short_id(),
        spender.proposal_short_id()
    );
}

#[test]
fn a_dependency_cycle_is_rejected_without_changing_membership() {
    let spender_tx = build_tx(vec![(&Byte32::zero(), 500)], 1);
    let reader_tx = build_tx_with_dep(
        vec![(&Byte32::zero(), 100)],
        vec![(&Byte32::zero(), 500), (&spender_tx.hash(), 0)],
        1,
    );
    for spender_first in [false, true] {
        let spender = TxEntry::dummy_resolve(spender_tx.clone(), 50, Capacity::shannons(1), 200);
        let reader = TxEntry::dummy_resolve(reader_tx.clone(), 60, Capacity::shannons(2), 300);
        let [first, second] = if spender_first {
            [spender, reader]
        } else {
            [reader, spender]
        };
        let mut pool = PoolMap::new(10);
        insert(&mut pool, &first);
        assert!(matches!(
            pool.add_entry(second, Status::Proposed),
            Err(Reject::Invalidated(_))
        ));
        assert_eq!(pool.size(), 1);
        assert_consistent(&pool);
    }
}

#[test]
fn resolved_dep_group_members_participate_in_parent_links() {
    use ckb_types::{
        core::{
            DepType, TransactionBuilder,
            cell::{CellMeta, ResolvedTransaction},
        },
        packed::{CellDep, CellInput, CellOutput, OutPoint},
        prelude::*,
    };
    use std::sync::Arc;

    let entries = graph(&[&[], &[]]);
    let code = OutPoint::new(entries[0].transaction().hash(), 0);
    let group = OutPoint::new(entries[1].transaction().hash(), 0);
    let tx = TransactionBuilder::default()
        .input(CellInput::new(OutPoint::new(Byte32::zero(), 200), 0))
        .cell_dep(
            CellDep::new_builder()
                .out_point(group.clone())
                .dep_type(DepType::DepGroup)
                .build(),
        )
        .output(CellOutput::default())
        .output_data(ckb_types::bytes::Bytes::new())
        .build();
    let mut resolved = ResolvedTransaction::dummy_resolve(tx);
    resolved.resolved_cell_deps = vec![CellMeta {
        out_point: code,
        ..Default::default()
    }];
    resolved.resolved_dep_groups = vec![CellMeta {
        out_point: group,
        ..Default::default()
    }];
    let reader = TxEntry::new(Arc::new(resolved), 70, Capacity::shannons(4), 300);
    for reader_first in [false, true] {
        let mut pool = PoolMap::new(3);
        if reader_first {
            insert(&mut pool, &reader);
        }
        for entry in &entries {
            insert(&mut pool, entry);
        }
        if !reader_first {
            insert(&mut pool, &reader);
        }
        assert_consistent(&pool);
        assert_eq!(
            pool.get(&reader.proposal_short_id())
                .unwrap()
                .ancestors_count,
            3
        );
    }
}

#[test]
fn duplicate_candidate_inputs_are_rejected_before_any_mutation() {
    let tx = build_tx(vec![(&Byte32::zero(), 100), (&Byte32::zero(), 100)], 1);
    let candidate = TxEntry::dummy_resolve(tx, 50, Capacity::shannons(1), 200);
    let mut pool = PoolMap::new(10);
    assert!(matches!(
        pool.add_entry(candidate, Status::Proposed),
        Err(Reject::RBFRejected(_))
    ));
    assert_eq!(pool.size(), 0);
    assert_eq!(pool.inputs_len(), 0);
    assert_eq!(pool.links.inner.len(), 0);
    assert_consistent(&pool);
}
