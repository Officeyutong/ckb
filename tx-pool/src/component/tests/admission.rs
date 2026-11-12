use super::{
    chunk::service,
    relations::assert_consistent,
    util::{build_tx, build_tx_with_dep},
};
use crate::{TxEntry, callback::Callbacks, error::Reject, process::TxStatus};
use ckb_types::{
    core::{Capacity, DepType, FeeRate, cell::CellMeta},
    packed::{CellDep, OutPoint},
    prelude::*,
};
use std::{
    collections::HashSet,
    sync::{Arc, Mutex},
};

fn entry(input: u32, deps: &[&TxEntry], size: usize, fee: u64) -> TxEntry {
    let hashes: Vec<_> = deps
        .iter()
        .map(|entry| entry.transaction().hash())
        .collect();
    TxEntry::dummy_resolve(
        build_tx_with_dep(
            vec![(&funding().transaction().hash(), input)],
            hashes.iter().map(|hash| (hash, 0)).collect(),
            1,
        ),
        1,
        Capacity::shannons(fee),
        size,
    )
}

fn funding() -> TxEntry {
    TxEntry::dummy_resolve(
        build_tx(vec![], 400),
        1,
        Capacity::shannons(1_000_000_000),
        100,
    )
}

#[tokio::test]
async fn ancestor_rejected_replacement_preserves_membership_and_emits_no_callbacks() {
    rejected_replacement_preserves_membership(false).await;
}

#[tokio::test]
async fn capacity_rejected_replacement_preserves_membership_and_emits_no_callbacks() {
    rejected_replacement_preserves_membership(true).await;
}

async fn rejected_replacement_preserves_membership(capacity: bool) {
    let mut service = service();
    let calls = Arc::new(Mutex::new(Vec::new()));
    let mut callbacks = Callbacks::new();
    let pending_calls = Arc::clone(&calls);
    callbacks.register_pending(Box::new(move |entry| {
        pending_calls
            .lock()
            .unwrap()
            .push(entry.transaction().hash());
    }));
    let reject_calls = Arc::clone(&calls);
    callbacks.register_reject(Box::new(move |_, entry, _| {
        reject_calls
            .lock()
            .unwrap()
            .push(entry.transaction().hash());
    }));
    service.callbacks = Arc::new(callbacks);
    let funding = funding();
    let original = entry(10, &[], 100, 1);
    let cheap = entry(11, &[], 100, 0);
    let root = entry(12, &[], 200, 5);
    let parent = entry(13, &[&root], 200, 5);
    let candidate = entry(10, &[&parent], 600, 10_000);
    let originals = [&funding, &original, &cheap, &root, &parent];
    let tip = {
        let mut pool = service.tx_pool.write().await;
        pool.config.min_rbf_rate = FeeRate::from_u64(1);
        pool.config.max_tx_pool_size = if capacity { 1_000 } else { usize::MAX };
        pool.pool_map.max_ancestors_count = if capacity { 5 } else { 3 };
        for entry in originals {
            pool.add_pending(entry.clone()).unwrap();
        }
        pool.snapshot().tip_hash()
    };
    let (result, _) = service.submit_entry(tip, candidate, TxStatus::Fresh).await;
    if capacity {
        assert!(matches!(result, Err(Reject::Full(_))));
    } else {
        assert!(matches!(result, Err(Reject::ExceededMaximumAncestorsCount)));
    }
    let pool = service.tx_pool.read().await;
    assert_eq!(
        pool.pool_map
            .iter()
            .map(|entry| entry.id.clone())
            .collect::<HashSet<_>>(),
        originals
            .iter()
            .map(|entry| entry.proposal_short_id())
            .collect()
    );
    assert_eq!(pool.pool_map.total_tx_size, 700);
    assert_eq!(pool.conflicts_cache.len(), 0);
    assert_eq!(pool.conflicts_outputs_cache.len(), 0);
    assert!(calls.lock().unwrap().is_empty());
    assert_consistent(&pool.pool_map);
}

#[tokio::test]
async fn accepted_replacement_and_capacity_eviction_publish_only_committed_membership() {
    let mut service = service();
    let funding = funding();
    let original = entry(70, &[], 100, 1);
    let child = entry(71, &[&original], 100, 1);
    let cheap = entry(72, &[], 200, 1);
    let candidate = entry(70, &[&funding], 350, 10_000);
    let candidate_id = candidate.proposal_short_id();
    let replaced: HashSet<_> = [&original, &child]
        .into_iter()
        .map(TxEntry::proposal_short_id)
        .collect();
    let cheap_id = cheap.proposal_short_id();
    let calls = Arc::new(Mutex::new(Vec::new()));
    let pending_calls = Arc::clone(&calls);
    let reject_calls = Arc::clone(&calls);
    let mut callbacks = Callbacks::new();
    callbacks.register_pending(Box::new(move |entry| {
        pending_calls
            .lock()
            .unwrap()
            .push(entry.proposal_short_id());
    }));
    callbacks.register_reject(Box::new(move |pool, entry, reject| {
        assert!(pool.pool_map.get(&candidate_id).is_some());
        assert_eq!(pool.pool_map.size(), 2);
        assert_eq!(pool.pool_map.total_tx_size, 450);
        assert_consistent(&pool.pool_map);
        let id = entry.proposal_short_id();
        if replaced.contains(&id) {
            assert!(matches!(reject, Reject::RBFRejected(_)));
        } else {
            assert_eq!(id, cheap_id);
            assert!(matches!(reject, Reject::Full(_)));
        }
        reject_calls.lock().unwrap().push(id);
    }));
    service.callbacks = Arc::new(callbacks);
    let tip = {
        let mut pool = service.tx_pool.write().await;
        pool.config.min_rbf_rate = FeeRate::from_u64(1);
        pool.config.max_tx_pool_size = 500;
        for entry in [&funding, &original, &child, &cheap] {
            pool.add_pending(entry.clone()).unwrap();
        }
        pool.snapshot().tip_hash()
    };
    let (result, _) = service
        .submit_entry(tip, candidate.clone(), TxStatus::Fresh)
        .await;
    assert!(result.is_ok(), "{result:?}");
    let pool = service.tx_pool.read().await;
    let calls = calls.lock().unwrap();
    assert_eq!(calls.len(), 4);
    assert_eq!(calls.last(), Some(&candidate.proposal_short_id()));
    assert_eq!(
        calls.iter().cloned().collect::<HashSet<_>>(),
        [&original, &child, &cheap, &candidate]
            .into_iter()
            .map(TxEntry::proposal_short_id)
            .collect()
    );
    assert_eq!(pool.conflicts_cache.len(), 2);
    assert_consistent(&pool.pool_map);
}

#[tokio::test]
async fn accepted_spender_reports_reader_package_evictions_after_commit() {
    let mut service = service();
    let funding = funding();
    let reader = entry(80, &[&funding], 100, 1);
    let retained_reader = entry(81, &[&funding], 100, 10);
    let child = entry(82, &[&reader], 100, 1);
    let candidate = TxEntry::dummy_resolve(
        build_tx(vec![(&funding.transaction().hash(), 0)], 1),
        1,
        Capacity::shannons(10_000),
        100,
    );
    let candidate_id = candidate.proposal_short_id();
    let reason = format!("invalidated by tx {}", candidate.transaction().hash());
    let calls = Arc::new(Mutex::new(Vec::new()));
    let recorded = Arc::clone(&calls);
    let mut callbacks = Callbacks::new();
    callbacks.register_reject(Box::new(move |pool, entry, reject| {
        assert!(pool.pool_map.get(&candidate_id).is_some());
        assert_eq!(pool.pool_map.size(), 3);
        assert!(matches!(reject, Reject::Invalidated(message) if message == reason));
        assert_consistent(&pool.pool_map);
        recorded.lock().unwrap().push(entry.proposal_short_id());
    }));
    service.callbacks = Arc::new(callbacks);
    let tip = {
        let mut pool = service.tx_pool.write().await;
        pool.pool_map.max_ancestors_count = 3;
        for entry in [&funding, &reader, &retained_reader, &child] {
            pool.add_pending(entry.clone()).unwrap();
        }
        pool.snapshot().tip_hash()
    };
    let (result, _) = service.submit_entry(tip, candidate, TxStatus::Fresh).await;
    assert!(result.is_ok(), "{result:?}");
    let calls = calls.lock().unwrap();
    assert_eq!(calls.len(), 2);
    assert_eq!(
        calls.iter().cloned().collect::<HashSet<_>>(),
        [&reader, &child]
            .into_iter()
            .map(TxEntry::proposal_short_id)
            .collect()
    );
}

#[tokio::test]
async fn replacement_counts_shared_victims_once_at_the_limit() {
    let service = service();
    let mut pool = service.tx_pool.write().await;
    pool.config.min_rbf_rate = FeeRate::from_u64(1);
    let left = entry(20, &[], 100, 1);
    let right = entry(21, &[], 100, 1);
    let mut child = entry(22, &[&left, &right], 100, 1);
    for entry in [&left, &right, &child] {
        pool.add_pending(entry.clone()).unwrap();
    }
    for index in 0..97 {
        child = entry(index + 23, &[&child], 100, 1);
        pool.add_pending(child.clone()).unwrap();
    }
    assert_eq!(pool.pool_map.size(), 100);
    let candidate = TxEntry::dummy_resolve(
        build_tx(
            vec![
                (&funding().transaction().hash(), 20),
                (&funding().transaction().hash(), 21),
            ],
            1,
        ),
        1,
        Capacity::shannons(10_000),
        200,
    );
    assert!(pool.check_rbf(pool.snapshot(), &candidate).is_ok());
    let extra = entry(120, &[&child], 100, 1);
    pool.add_pending(extra).unwrap();
    assert!(matches!(
        pool.check_rbf(pool.snapshot(), &candidate),
        Err(Reject::RBFRejected(_))
    ));
    assert_eq!(pool.pool_map.size(), 101);
    assert_consistent(&pool.pool_map);
}

#[tokio::test]
async fn replacement_preserves_expanded_dep_group_backing() {
    let service = service();
    let original = entry(30, &[], 100, 1);
    let funding = funding();
    let group = OutPoint::new(funding.transaction().hash(), 300);
    let tx = build_tx(vec![(&funding.transaction().hash(), 30)], 1)
        .as_advanced_builder()
        .cell_dep(
            CellDep::new_builder()
                .out_point(group.clone())
                .dep_type(DepType::DepGroup)
                .build(),
        )
        .build();
    let mut resolved = ckb_types::core::cell::ResolvedTransaction::dummy_resolve(tx);
    resolved.resolved_cell_deps = vec![CellMeta {
        out_point: OutPoint::new(original.transaction().hash(), 0),
        ..Default::default()
    }];
    resolved.resolved_dep_groups = vec![CellMeta {
        out_point: group,
        ..Default::default()
    }];
    let candidate = TxEntry::new(Arc::new(resolved), 1, Capacity::shannons(10_000), 200);
    let tip = {
        let mut pool = service.tx_pool.write().await;
        pool.config.min_rbf_rate = FeeRate::from_u64(1);
        pool.add_pending(funding).unwrap();
        pool.add_pending(original.clone()).unwrap();
        pool.snapshot().tip_hash()
    };
    let (result, _) = service.submit_entry(tip, candidate, TxStatus::Fresh).await;
    assert!(matches!(
        result,
        Err(Reject::RBFRejected(_)) | Err(Reject::Resolve(_))
    ));
    let pool = service.tx_pool.read().await;
    assert_eq!(pool.pool_map.size(), 2);
    assert!(pool.pool_map.get(&original.proposal_short_id()).is_some());
    assert_consistent(&pool.pool_map);
}

#[tokio::test]
async fn expiry_removes_fresh_descendants_once_and_keeps_unrelated_members() {
    let service = service();
    let clock = ckb_systemtime::faketime();
    clock.set_faketime(10_000);
    let mut left = entry(40, &[], 100, 1);
    let mut right = entry(41, &[], 100, 1);
    left.timestamp = 1;
    right.timestamp = 1;
    let child = entry(42, &[&left, &right], 100, 1);
    let survivor = entry(43, &[], 100, 1);
    let calls = Arc::new(Mutex::new(Vec::new()));
    let recorded = Arc::clone(&calls);
    let mut callbacks = Callbacks::new();
    callbacks.register_reject(Box::new(move |_, entry, reject| {
        assert!(matches!(reject, Reject::Expiry(_)));
        recorded.lock().unwrap().push(entry.proposal_short_id());
    }));
    let mut pool = service.tx_pool.write().await;
    pool.expiry = 5_000;
    for entry in [&left, &right, &child, &survivor] {
        pool.add_pending(entry.clone()).unwrap();
    }
    pool.remove_expired(&callbacks);
    assert_eq!(pool.pool_map.size(), 1);
    assert!(pool.pool_map.get(&survivor.proposal_short_id()).is_some());
    let calls = calls.lock().unwrap();
    assert_eq!(calls.len(), 3);
    assert_eq!(
        calls.iter().cloned().collect::<HashSet<_>>(),
        [&left, &right, &child]
            .into_iter()
            .map(TxEntry::proposal_short_id)
            .collect()
    );
    assert_consistent(&pool.pool_map);
}

#[tokio::test]
async fn submission_rechecks_backing_changed_during_verification_at_the_same_tip() {
    for spent in [false, true] {
        let service = service();
        let funding = funding();
        let producer = entry(50, &[], 100, 1);
        let candidate = entry(51, &[&producer], 100, 1);
        let tip = {
            let mut pool = service.tx_pool.write().await;
            pool.add_pending(funding).unwrap();
            pool.add_pending(producer.clone()).unwrap();
            // Resolution completed against live backing. Change only pool
            // membership before submitting that already resolved transaction.
            assert!(
                pool.check_rtx_from_pool(&candidate.rtx, &HashSet::new())
                    .is_ok()
            );
            if spent {
                let spender = TxEntry::dummy_resolve(
                    build_tx(vec![(&producer.transaction().hash(), 0)], 1),
                    1,
                    Capacity::shannons(1),
                    100,
                );
                pool.add_pending(spender).unwrap();
            } else {
                pool.remove_tx_by_hash(&producer.transaction().hash());
            }
            pool.snapshot().tip_hash()
        };
        let before = service
            .tx_pool
            .read()
            .await
            .pool_map
            .iter()
            .map(|entry| entry.id.clone())
            .collect::<HashSet<_>>();
        let (result, snapshot) = service
            .submit_entry(tip.clone(), candidate, TxStatus::Fresh)
            .await;
        assert_eq!(snapshot.tip_hash(), tip);
        assert!(matches!(result, Err(Reject::Resolve(_))));
        let pool = service.tx_pool.read().await;
        assert_eq!(
            pool.pool_map
                .iter()
                .map(|entry| entry.id.clone())
                .collect::<HashSet<_>>(),
            before
        );
        assert_consistent(&pool.pool_map);
    }
}

#[tokio::test]
async fn detached_proposal_changes_status_without_replacing_entry_owners() {
    let service = service();
    let root = entry(60, &[], 100, 1);
    let child = entry(61, &[&root], 100, 1);
    let other = entry(62, &[], 100, 1);
    let mut pool = service.tx_pool.write().await;
    for entry in [&root, &child, &other] {
        pool.add_proposed(entry.clone()).unwrap();
    }
    let before: Vec<_> = pool
        .pool_map
        .iter()
        .map(|entry| {
            (
                entry.id.clone(),
                Arc::clone(&entry.inner.rtx),
                entry.score.clone(),
                entry.evict_key.clone(),
            )
        })
        .collect();
    pool.remove_by_detached_proposal([root.proposal_short_id()].iter());
    assert_eq!(pool.pool_map.pending_size(), 2);
    assert_eq!(pool.pool_map.proposed_size(), 1);
    for (id, rtx, score, evict_key) in before {
        let entry = pool.pool_map.get_by_id(&id).unwrap();
        assert!(Arc::ptr_eq(&entry.inner.rtx, &rtx));
        assert_eq!(entry.score, score);
        assert_eq!(entry.evict_key, evict_key);
    }
    assert_consistent(&pool.pool_map);
}
