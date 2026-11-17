use crate::StatusCode;
use crate::relayer::get_block_transactions_process::GetBlockTransactionsProcess;
use crate::relayer::tests::helper::{MockProtocolContext, build_chain};
use crate::relayer::{MAX_RELAY_TXS_BYTES_PER_BATCH, MAX_RELAY_TXS_NUM_PER_BATCH};
use ckb_network::{PeerIndex, SupportProtocols};
use ckb_store::ChainStore;
use ckb_types::packed;
use ckb_types::prelude::*;
use std::sync::Arc;

#[test]
fn test_reject_duplicate_transaction_indexes() {
    let (_chain, relayer, _) = build_chain(5);

    let tip_hash = relayer.shared.active_chain().tip_hash();
    let tip_block = relayer.shared.store().get_block(&tip_hash).unwrap();
    let repeated_tx = tip_block.transactions().first().unwrap().data();
    let repeat_count = MAX_RELAY_TXS_BYTES_PER_BATCH / repeated_tx.total_size() + 1;
    assert!(repeat_count <= MAX_RELAY_TXS_NUM_PER_BATCH);

    let content = packed::GetBlockTransactions::new_builder()
        .block_hash(tip_hash)
        .indexes(vec![0u32; repeat_count])
        .build();
    let mock_protocol_context = MockProtocolContext::new(SupportProtocols::RelayV3);
    let nc = Arc::new(mock_protocol_context);
    let peer_index: PeerIndex = 1.into();
    let process = GetBlockTransactionsProcess::new(
        content.as_reader(),
        &relayer,
        Arc::<MockProtocolContext>::clone(&nc),
        peer_index,
    );

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    // Duplicate indexes are rejected with RequestDuplicate before any response is built
    assert_eq!(
        rt.block_on(process.execute()),
        StatusCode::RequestDuplicate.into(),
    );
}

#[test]
fn test_reject_duplicate_uncle_indexes() {
    let (_chain, relayer, _) = build_chain(5);

    let tip_hash = relayer.shared.active_chain().tip_hash();
    let content = packed::GetBlockTransactions::new_builder()
        .block_hash(tip_hash)
        .uncle_indexes(vec![0u32, 0u32])
        .build();
    let mock_protocol_context = MockProtocolContext::new(SupportProtocols::RelayV3);
    let nc = Arc::new(mock_protocol_context);
    let peer_index: PeerIndex = 1.into();
    let process = GetBlockTransactionsProcess::new(
        content.as_reader(),
        &relayer,
        Arc::<MockProtocolContext>::clone(&nc),
        peer_index,
    );

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    assert_eq!(
        rt.block_on(process.execute()),
        StatusCode::RequestDuplicate.into(),
    );
}

#[test]
fn test_respond_requested_transactions_and_skip_out_of_range_indexes() {
    let (_chain, relayer, _) = build_chain(5);

    let tip_hash = relayer.shared.active_chain().tip_hash();
    let tip_block = relayer.shared.store().get_block(&tip_hash).unwrap();
    let txs_len = tip_block.transactions().len() as u32;

    // Out-of-range indexes are skipped, in-range ones are answered in request order
    let content = packed::GetBlockTransactions::new_builder()
        .block_hash(tip_hash.clone())
        .indexes(vec![txs_len, 0u32, txs_len + 1_000])
        .build();
    let mock_protocol_context = MockProtocolContext::new(SupportProtocols::RelayV3);
    let nc = Arc::new(mock_protocol_context);
    let peer_index: PeerIndex = 1.into();
    let process = GetBlockTransactionsProcess::new(
        content.as_reader(),
        &relayer,
        Arc::<MockProtocolContext>::clone(&nc),
        peer_index,
    );

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    assert!(rt.block_on(process.execute()).is_ok());

    let expected = packed::BlockTransactions::new_builder()
        .block_hash(tip_hash)
        .transactions(vec![tip_block.transaction(0).unwrap().data()])
        .build();
    let message = packed::RelayMessage::new_builder().set(expected).build();
    assert_eq!(nc.sent_messages_len(), 1);
    assert!(nc.has_sent(
        SupportProtocols::RelayV3.protocol_id(),
        peer_index,
        message.as_bytes()
    ));
}
