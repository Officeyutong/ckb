use crate::tests::{RpcTestRequest, RpcTestSuite, setup};
use ckb_jsonrpc_types::{TransactionAndWitnessProof, TransactionProof};
use ckb_store::ChainStore;
use ckb_test_chain_utils::always_success_consensus;
use ckb_types::{core::BlockView, prelude::*, utilities::CBMT};
use serde_json::Value;

// JSON-RPC standard error code for invalid params.
const INVALID_PARAMS_CODE: i64 = -32602;

fn rpc(suite: &RpcTestSuite, method: &str, param: Value) -> (Value, Value) {
    let response = suite.rpc(&RpcTestRequest {
        id: 42,
        jsonrpc: "2.0".to_string(),
        method: method.to_string(),
        params: vec![param],
    });
    (response.result, response.error)
}

fn transaction_proof(block: &BlockView) -> Value {
    serde_json::to_value(TransactionProof {
        block_hash: block.hash().into(),
        witnesses_root: block.calc_witnesses_root().into(),
        proof: CBMT::build_merkle_proof(block.tx_hashes(), &[0])
            .unwrap()
            .into(),
    })
    .unwrap()
}

fn transaction_and_witness_proof(block: &BlockView) -> Value {
    serde_json::to_value(TransactionAndWitnessProof {
        block_hash: block.hash().into(),
        transactions_proof: CBMT::build_merkle_proof(block.tx_hashes(), &[0])
            .unwrap()
            .into(),
        witnesses_proof: CBMT::build_merkle_proof(block.tx_witness_hashes(), &[0])
            .unwrap()
            .into(),
    })
    .unwrap()
}

// The test suite stores a fork block, a sibling of the tip block, which is not on the main
// chain. A proof that is valid for it must not be accepted as a main chain inclusion proof.
#[test]
fn test_verify_proofs_reject_non_main_chain_block() {
    let suite = setup(always_success_consensus());
    let snapshot = suite.shared.snapshot();
    let tip_block = snapshot.get_block(&snapshot.tip_hash()).unwrap();
    // Same construction as the fork block inserted by `setup_rpc_test_suite`
    let fork_block = tip_block
        .as_advanced_builder()
        .header(
            tip_block
                .header()
                .as_advanced_builder()
                .timestamp(tip_block.header().timestamp() + 1)
                .build(),
        )
        .build();
    assert!(snapshot.get_block(&fork_block.hash()).is_some());
    assert!(!snapshot.is_main_chain(&fork_block.hash()));

    let expected_tx_hashes: Vec<ckb_types::H256> = vec![tip_block.tx_hashes()[0].unpack()];
    let expected_tx_hashes = serde_json::to_value(expected_tx_hashes).unwrap();
    for (method, proof) in [
        (
            "verify_transaction_proof",
            transaction_proof as fn(&BlockView) -> Value,
        ),
        (
            "verify_transaction_and_witness_proof",
            transaction_and_witness_proof,
        ),
    ] {
        let (result, error) = rpc(&suite, method, proof(&tip_block));
        assert_eq!(
            result, expected_tx_hashes,
            "{method} must accept a main chain proof, error {error:?}"
        );

        let (result, error) = rpc(&suite, method, proof(&fork_block));
        assert!(
            result.is_null(),
            "{method} must reject a non-main chain proof, got {result:?}"
        );
        assert_eq!(
            error["code"].as_i64(),
            Some(INVALID_PARAMS_CODE),
            "{method}: expected InvalidParams error, got {error:?}"
        );
    }
}
