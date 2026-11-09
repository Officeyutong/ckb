use crate::relayer::MAX_RELAY_TXS_NUM_PER_BATCH;
use crate::relayer::Relayer;
use crate::relayer::tests::helper::{MockProtocolContext, build_chain, new_transaction};
use crate::relayer::transaction_hashes_process::TransactionHashesProcess;
use crate::relayer::transactions_process::{TransactionsProcess, dedup_transactions};
use crate::{Status, StatusCode};
use ckb_network::{PeerIndex, SupportProtocols};
use ckb_types::{core::TransactionBuilder, packed, prelude::*};
use std::sync::Arc;

fn dummy_tx(nonce: u64) -> ckb_types::core::TransactionView {
    // The transaction hash excludes witnesses, so vary a raw field to get a
    // distinct hash for each body.
    TransactionBuilder::default().version(nonce as u32).build()
}

fn relay_transactions(
    transactions: impl IntoIterator<Item = (ckb_types::core::TransactionView, u64)>,
) -> packed::RelayTransactions {
    let transactions = transactions
        .into_iter()
        .map(|(tx, cycles)| {
            packed::RelayTransaction::new_builder()
                .cycles(cycles)
                .transaction(tx.data())
                .build()
        })
        .collect();
    packed::RelayTransactions::new_builder()
        .transactions(
            packed::RelayTransactionVec::new_builder()
                .set(transactions)
                .build(),
        )
        .build()
}

#[test]
fn dedup_keeps_first_body_per_hash() {
    let tx = dummy_tx(0);
    let other = dummy_tx(1);
    let tx_hash = tx.hash();
    let other_hash = other.hash();
    assert_ne!(tx_hash, other_hash);

    let deduped = dedup_transactions(vec![(tx.clone(), 1), (tx.clone(), 1), (other, 2), (tx, 3)]);

    assert_eq!(deduped.len(), 2);
    assert_eq!(deduped[0].0.hash(), tx_hash);
    assert_eq!(deduped[0].1, 1);
    assert_eq!(deduped[1].0.hash(), other_hash);
    assert_eq!(deduped[1].1, 2);
}

#[test]
fn dedup_preserves_distinct_bodies() {
    let bodies: Vec<_> = (0..8).map(|i| (dummy_tx(i), i)).collect();

    let deduped = dedup_transactions(bodies);

    assert_eq!(deduped.len(), 8);
}

#[test]
fn duplicate_bodies_are_deduplicated_before_cycle_validation() {
    let (_chain, relayer, always_success_out_point) = build_chain(5);
    let peer: PeerIndex = 1.into();
    let tx = new_transaction(&relayer, 1, &always_success_out_point);
    let tx_hash = tx.hash();

    relayer.shared.state().peers().relay_connected(peer);
    assert!(
        relayer
            .shared
            .state()
            .add_ask_for_txs(peer, vec![tx_hash.clone()])
            .is_ok()
    );
    assert_eq!(
        relayer.shared.state().pop_ask_for_txs().get(&peer),
        Some(&vec![tx_hash.clone()])
    );

    let max_block_cycles = relayer.shared.shared().consensus().max_block_cycles();
    let message = relay_transactions([(tx.clone(), max_block_cycles), (tx, max_block_cycles + 1)]);
    let nc = Arc::new(MockProtocolContext::new(SupportProtocols::RelayV3));
    let process = TransactionsProcess::new(
        message.as_reader(),
        &relayer,
        Arc::<MockProtocolContext>::clone(&nc),
        peer,
    );

    assert_eq!(process.execute(), Status::ok());
    assert_eq!(nc.banned_peers_len(), 0);
    assert!(relayer.shared.state().already_known_tx(&tx_hash));
}

#[test]
fn oversized_response_is_rejected_before_processing() {
    let (_chain, relayer, _always_success_out_point) = build_chain(1);
    let peer: PeerIndex = 1.into();
    let relay_tx = packed::RelayTransaction::new_builder()
        .cycles(0u64)
        .transaction(dummy_tx(0).data())
        .build();
    let message = packed::RelayTransactions::new_builder()
        .transactions(
            packed::RelayTransactionVec::new_builder()
                .set(vec![relay_tx; MAX_RELAY_TXS_NUM_PER_BATCH + 1])
                .build(),
        )
        .build();
    let nc = Arc::new(MockProtocolContext::new(SupportProtocols::RelayV3));
    let process = TransactionsProcess::new(message.as_reader(), &relayer, nc, peer);

    assert_eq!(
        process.execute().code(),
        StatusCode::ProtocolMessageIsMalformed
    );
}

fn receive_tx_body(relayer: &Relayer, peer: PeerIndex, tx: ckb_types::core::TransactionView) {
    let max_block_cycles = relayer.shared.shared().consensus().max_block_cycles();
    let message = relay_transactions([(tx, max_block_cycles)]);
    let nc = Arc::new(MockProtocolContext::new(SupportProtocols::RelayV3));
    let process = TransactionsProcess::new(message.as_reader(), relayer, nc, peer);
    assert_eq!(process.execute(), Status::ok());
}

fn receive_tx_announcement(relayer: &Relayer, peer: PeerIndex, tx_hash: packed::Byte32) {
    let message = packed::RelayTransactionHashes::new_builder()
        .tx_hashes(vec![tx_hash])
        .build();
    let process = TransactionHashesProcess::new(message.as_reader(), relayer, peer);
    assert!(process.execute().is_ok());
}

// The responder's body is received and requested before `announcer` announces the tx
fn receive_body_before_announcement() -> (
    ckb_chain::ChainServiceScope,
    Relayer,
    PeerIndex,
    packed::Byte32,
) {
    let (chain, relayer, always_success_out_point) = build_chain(5);
    let (responder, announcer) = (PeerIndex::new(1), PeerIndex::new(2));
    let tx = new_transaction(&relayer, 1, &always_success_out_point);
    let tx_hash = tx.hash();
    let state = relayer.shared.state();
    state.peers().relay_connected(responder);
    state.peers().relay_connected(announcer);

    assert!(
        state
            .add_ask_for_txs(responder, vec![tx_hash.clone()])
            .is_ok()
    );
    assert_eq!(
        state.pop_ask_for_txs().get(&responder),
        Some(&vec![tx_hash.clone()])
    );
    receive_tx_body(&relayer, responder, tx);
    assert!(state.already_known_tx(&tx_hash));

    // The tx is already known, so the announcement is not queued as a request
    receive_tx_announcement(&relayer, announcer, tx_hash.clone());
    assert!(state.unknown_tx_hashes().get_priority(&tx_hash).is_none());
    (chain, relayer, announcer, tx_hash)
}

#[test]
fn announcement_while_body_is_verified_is_kept_for_refetch() {
    let (_chain, relayer, announcer, tx_hash) = receive_body_before_announcement();
    let state = relayer.shared.state();

    // The responder is banned for a malformed body, also when it was the last connected peer and
    // the announcer connected afterwards
    state.refetch_tx(&tx_hash);
    assert!(!state.already_known_tx(&tx_hash));
    std::thread::sleep(std::time::Duration::from_millis(1));
    assert_eq!(
        state.pop_ask_for_txs().get(&announcer),
        Some(&vec![tx_hash])
    );
}

#[test]
fn verified_tx_is_no_longer_tracked_for_refetch() {
    let (_chain, relayer, announcer, tx_hash) = receive_body_before_announcement();
    let state = relayer.shared.state();

    // The relayer stops tracking the tx when it receives the `Ok` or `Reject` result
    state.remove_tx_fallback_peers(&tx_hash);
    receive_tx_announcement(&relayer, PeerIndex::new(3), tx_hash.clone());
    state.refetch_tx(&tx_hash);
    std::thread::sleep(std::time::Duration::from_millis(1));
    assert!(!state.pop_ask_for_txs().contains_key(&announcer));
}
