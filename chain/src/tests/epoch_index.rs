use crate::ChainServiceScope;
use ckb_chain_spec::consensus::ConsensusBuilder;
use ckb_shared::{Shared, SharedBuilder};
use ckb_store::ChainStore;
use ckb_test_chain_utils::{MockStore, create_cellbase, dao_data};
use ckb_types::core::{BlockBuilder, BlockView, HeaderView};
use ckb_types::{bytes::Bytes, prelude::*, utilities::merkle_mountain_range::ChainRootMMR};
use ckb_verification::HeaderVerifier;
use ckb_verification_traits::{Switch, Verifier};
use std::sync::Arc;

fn next_block(
    shared: &Shared,
    mock_store: &MockStore,
    parent: &HeaderView,
    nonce: u128,
) -> BlockView {
    let epoch = shared
        .consensus()
        .next_epoch_ext(parent, &shared.store().borrow_as_data_loader())
        .unwrap()
        .epoch();
    let cellbase = create_cellbase(mock_store, shared.consensus(), parent);
    let dao = dao_data(
        shared.consensus(),
        parent,
        std::slice::from_ref(&cellbase),
        mock_store,
        false,
    );
    let mut ancestors = vec![parent.clone()];
    while ancestors.last().unwrap().number() > 0 {
        let header = shared
            .store()
            .get_block_header(&ancestors.last().unwrap().parent_hash())
            .unwrap();
        ancestors.push(header);
    }
    let txn = mock_store.store().begin_transaction();
    let mut mmr = ChainRootMMR::new(0, &txn);
    for header in ancestors.iter().rev() {
        mmr.push(header.digest()).unwrap();
    }
    let extension = Bytes::copy_from_slice(mmr.get_root().unwrap().calc_mmr_hash().as_slice());
    let block = BlockBuilder::default()
        .parent_hash(parent.hash())
        .number(parent.number() + 1)
        .timestamp(parent.timestamp() + 1)
        .epoch(epoch.number_with_fraction(parent.number() + 1))
        .compact_target(epoch.compact_target())
        .nonce(nonce)
        .dao(dao)
        .transaction(cellbase)
        .extension(Some(extension.into()))
        .build();
    HeaderVerifier::new(shared.snapshot().as_ref(), shared.consensus())
        .verify(&block.header())
        .unwrap();
    mock_store.insert_block(&block, &epoch);
    block
}

fn build_chain() -> (ChainServiceScope, Shared, Vec<BlockView>) {
    let mut consensus = ConsensusBuilder::default()
        .permanent_difficulty_in_dummy(true)
        .epoch_duration_target(32)
        .build();
    consensus.genesis_epoch_ext.set_length(4);
    let (shared, mut pack) = SharedBuilder::with_temp_db()
        .consensus(consensus)
        .build()
        .unwrap();
    let scope = ChainServiceScope::new(pack.take_chain_services_builder());
    let genesis = shared.consensus().genesis_block().header();
    let mock_store = MockStore::new(&genesis, shared.store());
    let mut parent = genesis;
    let mut blocks = Vec::new();
    for _ in 0..9 {
        let block = next_block(&shared, &mock_store, &parent, 0);
        scope
            .chain_controller()
            .blocking_process_block_with_switch(Arc::new(block.clone()), Switch::NONE)
            .unwrap();
        parent = block.header();
        blocks.push(block);
    }
    assert_eq!(blocks[3].epoch().number(), 1);
    assert_eq!(blocks[7].epoch().number(), 2);
    (scope, shared, blocks)
}

#[test]
fn epoch_index_survives_losing_fork_and_tracks_reorg() {
    let (scope, shared, blocks) = build_chain();
    let store = shared.store();
    let canonical_hash = store.get_epoch_index(1).unwrap();
    let canonical_epoch = store.get_epoch_ext(&canonical_hash).unwrap();
    let parent = blocks[1].header();
    let mock_store = MockStore::new(&shared.consensus().genesis_block().header(), store);
    for block in blocks.iter().take(2) {
        let epoch_hash = store.get_block_epoch_index(&block.hash()).unwrap();
        mock_store.insert_block(block, &store.get_epoch_ext(&epoch_hash).unwrap());
    }
    let mut parent = parent;
    let mut fork = Vec::new();

    // Competing tail and epoch head go through the chain controller. Neither wins.
    for _ in 0..2 {
        let block = next_block(&shared, &mock_store, &parent, 1);
        scope
            .chain_controller()
            .blocking_process_block_with_switch(Arc::new(block.clone()), Switch::NONE)
            .unwrap();
        parent = block.header();
        fork.push(block);
        assert_eq!(store.get_epoch_index(1), Some(canonical_hash.clone()));
        assert_eq!(
            store.get_epoch_ext(&canonical_hash),
            Some(canonical_epoch.clone())
        );
        assert_eq!(shared.snapshot().tip_header().hash(), blocks[8].hash());
    }
    let fork_epoch_hash = store.get_block_epoch_index(&fork[1].hash()).unwrap();
    assert_ne!(fork_epoch_hash, canonical_hash);
    let fork_epoch = store.get_epoch_ext(&fork_epoch_hash).unwrap();

    // A non-head side block also leaves the shared index unchanged.
    let block = next_block(&shared, &mock_store, &parent, 1);
    scope
        .chain_controller()
        .blocking_process_block_with_switch(Arc::new(block.clone()), Switch::NONE)
        .unwrap();
    parent = block.header();
    assert_eq!(store.get_epoch_index(1), Some(canonical_hash.clone()));

    // Extend the stored fork until it wins, including previously stored epoch heads.
    while parent.number() <= blocks[8].number() {
        let block = next_block(&shared, &mock_store, &parent, 1);
        scope
            .chain_controller()
            .blocking_process_block_with_switch(Arc::new(block.clone()), Switch::NONE)
            .unwrap();
        parent = block.header();
    }
    assert_eq!(shared.snapshot().tip_header().hash(), parent.hash());
    assert_eq!(store.get_epoch_index(1), Some(fork_epoch_hash.clone()));
    assert_eq!(store.get_epoch_ext(&fork_epoch_hash), Some(fork_epoch));
    assert_eq!(store.get_epoch_ext(&canonical_hash), Some(canonical_epoch));
    assert_eq!(
        store.get_epoch_index(2),
        store.get_block_epoch_index(&parent.hash())
    );
}

#[test]
fn epoch_index_is_removed_when_truncating_across_epoch_heads() {
    let (scope, shared, blocks) = build_chain();
    let store = shared.store();
    let genesis_index = store.get_epoch_index(0).unwrap();
    let epoch_one_index = store.get_epoch_index(1).unwrap();
    let epoch_two_index = store.get_epoch_index(2).unwrap();
    let epoch_two = store.get_epoch_ext(&epoch_two_index).unwrap();

    // Removing a non-head block must preserve the current epoch index.
    scope.chain_controller().truncate(blocks[7].hash()).unwrap();
    assert_eq!(store.get_epoch_index(2), Some(epoch_two_index.clone()));

    scope.chain_controller().truncate(blocks[4].hash()).unwrap();
    assert_eq!(store.get_epoch_index(2), None);
    assert_eq!(store.get_epoch_index(1), Some(epoch_one_index));
    assert_eq!(store.get_epoch_ext(&epoch_two_index), Some(epoch_two));

    scope.chain_controller().truncate(blocks[1].hash()).unwrap();
    assert_eq!(store.get_epoch_index(1), None);
    assert_eq!(store.get_epoch_index(0), Some(genesis_index));
}
