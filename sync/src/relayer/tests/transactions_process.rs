use crate::relayer::MAX_RELAY_TXS_NUM_PER_BATCH;
use crate::relayer::tests::helper::{MockProtocolContext, build_chain, new_transaction};
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
