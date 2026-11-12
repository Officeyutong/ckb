use crate::util::transaction::{relay_tx, send_tx};
use crate::utils::{build_compact_block, wait_until};
use crate::{Net, Node, Spec};
use ckb_network::SupportProtocols;
use ckb_types::{
    bytes::Bytes,
    core::{Capacity, Cycle, ScriptHashType, TransactionView, capacity_bytes},
    packed::{
        BlockProposal, Byte32, CellDep, CellInput, CellOutput, OutPoint, RelayMessage,
        RelayMessageUnion, RelayTransactionHashes, Script,
    },
    prelude::*,
};
use std::time::Duration;

const PROGRAM: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../script/testdata/current_cycles"
);

const DIRECT_ORPHAN_CYCLES: u64 = 1_000_000_000;

pub struct NetworkVerificationTimeBudget;

impl Spec for NetworkVerificationTimeBudget {
    crate::setup!(num_nodes: 1);

    fn modify_app_config(&self, config: &mut ckb_app_config::CKBAppConfig) {
        config.network.connect_outbound_interval_secs = 0;
        config.tx_pool.max_tx_verify_cycles = DIRECT_ORPHAN_CYCLES;
    }

    fn modify_chain_spec(&self, spec: &mut ckb_chain_spec::ChainSpec) {
        spec.genesis.system_cells.push(ckb_chain_spec::SystemCell {
            create_type_id: false,
            capacity: None,
            file: ckb_resource::Resource::file_system(PROGRAM.into()),
        });
    }

    fn run(&self, nodes: &mut Vec<Node>) {
        let node = &mut nodes[0];
        let (funding, tx, cycles) = fund_budget_workload(node);
        // Validate the dry run under the production cap, then restart with the
        // test cap. The budget is process-local and is never node configuration.
        node.stop();
        node.start_with_env(&[("CKB_TEST_MAX_TX_VERIFY_TIME_MS", "1")]);
        let rpc = node.rpc_client();
        rpc.remove_transaction(funding.hash());
        node.submit_transaction(&funding);
        let pool_before = rpc.get_raw_tx_pool(None);
        for _ in 0..2 {
            let error = rpc
                .inner()
                .test_tx_pool_accept(tx.data().into(), Some("passthrough".into()))
                .unwrap_err();
            let error: jsonrpc_core::Error = serde_json::from_str(&error.to_string())
                .expect("the refusal must be a JSON-RPC error");
            assert_eq!(error.code, jsonrpc_core::ErrorCode::ServerError(-1113));
            assert_eq!(rpc.get_raw_tx_pool(None), pool_before);
            assert_eq!(
                rpc.get_transaction(tx.hash()).tx_status.status,
                ckb_jsonrpc_types::Status::Unknown,
                "a dry-run refusal must not become a persistent rejection"
            );
        }
        let mut net = Net::new(
            self.name(),
            node.consensus(),
            vec![SupportProtocols::RelayV3],
        );
        net.connect(node);
        relay_tx(&net, node, tx.clone(), cycles);

        assert_network_refusal(&net, node, &tx.hash());

        // Exercise the real proposal response, which carries no declared cycles
        // or peer metadata into the verification queue.
        let block = node
            .new_block_builder(None, None, None)
            .proposal(tx.proposal_short_id())
            .build();
        net.send(node, SupportProtocols::RelayV3, build_compact_block(&block));
        assert!(net.should_receive(node, |data| {
            RelayMessage::from_slice(data).is_ok_and(|message| {
                matches!(
                    message.to_enum(),
                    RelayMessageUnion::GetBlockProposal(request)
                        if request.proposals().into_iter().any(|id| id == tx.proposal_short_id())
                )
            })
        }));
        assert!(wait_until(10, || node.get_tip_block() == block));
        let response = RelayMessage::new_builder()
            .set(
                BlockProposal::new_builder()
                    .transactions(vec![tx.data()])
                    .build(),
            )
            .build();
        net.send(node, SupportProtocols::RelayV3, response.as_bytes());
        assert_network_refusal(&net, node, &tx.hash());

        // Fulfil that fresh relay request while the parent is missing. Restoring
        // the parent must enforce the same budget without a worker control channel.
        assert!(rpc.remove_transaction(funding.hash()));
        send_tx(&net, node, tx.clone(), cycles);
        assert!(wait_until(10, || rpc.tx_pool_info().orphan.value() == 1));
        node.submit_transaction(&funding);
        assert!(wait_until(10, || rpc.tx_pool_info().orphan.value() == 0));
        assert_network_refusal(&net, node, &tx.hash());

        assert_eq!(node.submit_transaction(&tx), tx.hash());
        let submitted = rpc.get_transaction(tx.hash());
        assert_eq!(submitted.cycles.map(|cycles| cycles.value()), Some(cycles));
        assert_eq!(
            submitted.tx_status.status,
            ckb_jsonrpc_types::Status::Pending
        );
        // The compact block already proposed the transaction; mine the gap
        // before waiting for it in the commit section.
        node.mine(node.consensus().tx_proposal_window().closest() - 1);
        node.mine_with_blocking(|template| {
            !template
                .transactions
                .iter()
                .any(|entry| entry.hash == tx.hash().into())
        });
        assert_eq!(
            rpc.get_transaction(tx.hash()).tx_status.status,
            ckb_jsonrpc_types::Status::Committed
        );
    }
}

// Prepare a valid workload that takes the direct orphan route and exceeds the
// test-only time cap. The dry run uses the production cap to resolve its parent.
fn fund_budget_workload(node: &Node) -> (TransactionView, TransactionView, Cycle) {
    node.mine_until_out_bootstrap_period();
    // Four cellbase rewards fund the 64 output cells.
    node.mine(4);
    let tip = node.get_tip_block_number();
    let rpc = node.rpc_client();
    let code_hash = CellOutput::calc_data_hash(&Bytes::from(std::fs::read(PROGRAM).unwrap()));
    let genesis = &node.consensus().genesis_block().transactions()[0];
    let index = genesis
        .outputs_data()
        .into_iter()
        .position(|data| CellOutput::calc_data_hash(&data.raw_data()) == code_hash)
        .expect("the workload is a genesis system cell");
    let dep = CellDep::new_builder()
        .out_point(OutPoint::new(genesis.hash(), index as u32))
        .build();

    // Distinct groups each execute the existing 4,096-syscall fixture.
    // Together they comfortably exceed 1 ms while remaining consensus-valid.
    let outputs = (0u8..64)
        .map(|group| {
            CellOutput::new_builder()
                .capacity(capacity_bytes!(100))
                .lock(node.always_success_script())
                .type_(Some(
                    Script::new_builder()
                        .code_hash(code_hash.clone())
                        .hash_type(ScriptHashType::Data1)
                        .args(Bytes::from(vec![group]))
                        .build(),
                ))
                .build()
        })
        .collect();
    let funding = node
        .new_transaction_spend_tip_cellbase()
        .as_advanced_builder()
        .set_inputs(
            (tip - 3..=tip)
                .map(|height| {
                    let cellbase = node.get_block_by_number(height).transactions()[0].hash();
                    CellInput::new(OutPoint::new(cellbase, 0), 0)
                })
                .collect(),
        )
        .set_outputs(vec![
            CellOutput::new_builder()
                .capacity(capacity_bytes!(6_500))
                .lock(node.always_success_script())
                .build(),
        ])
        .build();
    node.submit_transaction(&funding);
    let tx = funding
        .as_advanced_builder()
        .set_inputs(vec![CellInput::new(OutPoint::new(funding.hash(), 0), 0)])
        .cell_dep(dep)
        .set_outputs(outputs)
        .set_outputs_data(vec![Bytes::new().pack(); 64])
        .build();
    // A successful dry run reports cycles and fees without admitting the tx.
    let pool_before = rpc.get_raw_tx_pool(None);
    let completed = rpc
        .inner()
        .test_tx_pool_accept(tx.data().into(), Some("passthrough".into()))
        .expect("the workload fits the production CPU cap");
    assert_eq!(completed.fee.value(), capacity_bytes!(100).as_u64());
    assert_eq!(rpc.get_raw_tx_pool(None), pool_before);
    assert_eq!(
        rpc.get_transaction(tx.hash()).tx_status.status,
        ckb_jsonrpc_types::Status::Unknown
    );
    let cycles = completed.cycles.value();
    assert!(
        cycles <= DIRECT_ORPHAN_CYCLES,
        "the orphan must take the direct verification path"
    );
    (funding, tx, cycles)
}

fn assert_network_refusal(net: &Net, node: &Node, tx_hash: &Byte32) {
    let rpc = node.rpc_client();
    let announcement = RelayMessage::new_builder()
        .set(
            RelayTransactionHashes::new_builder()
                .tx_hashes(vec![tx_hash.clone()])
                .build(),
        )
        .build();
    // A fresh request for the same hash acknowledges removal of the known
    // marker after refusal. An empty queue alone could precede delivery.
    assert!(
        wait_until(15, || {
            net.send(node, SupportProtocols::RelayV3, announcement.as_bytes());
            net.receive_timeout(node, Duration::from_millis(200))
                .ok()
                .and_then(|(_, _, data)| RelayMessage::from_slice(&data).ok())
                .is_some_and(|message| {
                    matches!(
                        message.to_enum(),
                        RelayMessageUnion::GetRelayTransactions(request)
                            if request.tx_hashes().into_iter().any(|hash| hash == *tx_hash)
                    )
                })
        }),
        "a time-limited transaction must be requestable again"
    );
    assert_eq!(
        rpc.get_transaction(tx_hash.clone()).tx_status.status,
        ckb_jsonrpc_types::Status::Unknown,
        "time exhaustion must not become a persistent rejection"
    );
    assert_eq!(rpc.get_peers().len(), 1);
    assert!(rpc.get_banned_addresses().is_empty());
}
