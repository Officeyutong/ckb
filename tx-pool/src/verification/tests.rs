use super::*;
use ckb_chain_spec::consensus::ConsensusBuilder;
use ckb_db::RocksDB;
use ckb_script::{ScriptVersion, TransactionScriptsVerifier};
use ckb_store::ChainDB;
use ckb_types::{
    H256, U256,
    bytes::Bytes,
    core::{
        Capacity, ScriptHashType, TransactionBuilder, capacity_bytes,
        cell::{CellMetaBuilder, ResolvedTransaction},
        hardfork::HardForks,
    },
    packed::{CellDep, CellInput, CellOutput, OutPoint, Script},
    prelude::*,
};
use tempfile::TempDir;

#[test]
fn time_budget_refusal_is_retryable_and_not_a_persistent_rejection() {
    let reject = Reject::ExcessiveVerifyTime;
    assert!(!reject.is_malformed_tx());
    assert!(!reject.should_recorded());
    assert!(reject.is_allowed_relay());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn network_budget_refusal_does_not_prevent_unbudgeted_verification() {
    let rtx = program_transaction(
        ScriptVersion::V0,
        &[include_bytes!("../../../script/testdata/always_success")],
    );
    let snapshot = snapshot(Arc::new(ConsensusBuilder::default().build()));
    let tx_env = Arc::new(TxVerifyEnv::new_submit(snapshot.tip_header()));
    let (_commands, mut command_rx) = watch::channel(ChunkCommand::Resume);

    // Queued relay/proposal and directly processed orphans share the same budget.
    for commands in [Some(&mut command_rx), None] {
        let refused = verify_rtx(
            Arc::clone(&snapshot),
            Arc::clone(&rtx),
            Arc::clone(&tx_env),
            &None,
            u64::MAX,
            commands,
            Some(Duration::ZERO),
        )
        .await;
        assert!(matches!(refused, Err(Reject::ExcessiveVerifyTime)));
    }
    let local = verify_rtx(
        Arc::clone(&snapshot),
        Arc::clone(&rtx),
        Arc::clone(&tx_env),
        &None,
        u64::MAX,
        None,
        None,
    )
    .await
    .expect("RPC verification has no network time budget");
    let queued_local = verify_rtx(
        Arc::clone(&snapshot),
        Arc::clone(&rtx),
        Arc::clone(&tx_env),
        &None,
        u64::MAX,
        Some(&mut command_rx),
        None,
    )
    .await
    .expect("queued local submission has no network time budget");
    let network = verify_rtx(
        snapshot,
        rtx,
        tx_env,
        &None,
        u64::MAX,
        Some(&mut command_rx),
        Some(Duration::from_secs(2)),
    )
    .await
    .expect("a later network attempt can verify normally");
    assert!(local.cycles > 0);
    assert_eq!(local, queued_local);
    assert_eq!(local, network);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dao_lock_size_is_checked_before_every_script_verification_route() {
    use ckb_verification::TransactionError;

    let snapshot = snapshot(Arc::new(ConsensusBuilder::default().build()));
    let dao = Script::new_builder()
        .code_hash(snapshot.consensus().dao_type_hash())
        .hash_type(ScriptHashType::Type)
        .build();
    let input = CellOutput::new_builder()
        .capacity(capacity_bytes!(500))
        .type_(Some(dao))
        .build();
    let output = input
        .clone()
        .as_builder()
        .lock(Script::new_builder().args(Bytes::from_static(&[1])).build())
        .build();
    let rtx = Arc::new(ResolvedTransaction {
        transaction: TransactionBuilder::default()
            .input(CellInput::default())
            .output(output)
            .output_data(Bytes::from_static(&[1; 8]))
            .build(),
        resolved_inputs: vec![
            CellMetaBuilder::from_cell_output(input, Bytes::from_static(&[0; 8])).build(),
        ],
        resolved_cell_deps: Vec::new(),
        resolved_dep_groups: Vec::new(),
    });
    let env = Arc::new(TxVerifyEnv::new_submit(snapshot.tip_header()));
    let (_sender, mut commands) = watch::channel(ChunkCommand::Resume);
    // Invalid scripts and an exhausted budget must not hide the cheap DAO error.
    // Cover RPC, queued local, queued network and direct orphan verification.
    for limit in [None, Some(Duration::ZERO)] {
        for command_rx in [None, Some(&mut commands)] {
            let error = verify_rtx(
                Arc::clone(&snapshot),
                Arc::clone(&rtx),
                Arc::clone(&env),
                &None,
                u64::MAX,
                command_rx,
                limit,
            )
            .await
            .unwrap_err();
            let Reject::Verification(error) = error else {
                panic!("DAO rejection must precede script execution: {error}");
            };
            ckb_error::assert_error_eq!(error, TransactionError::DaoLockSizeMismatch { index: 0 });
        }
    }
}

pub(super) fn snapshot(consensus: Arc<ckb_chain_spec::consensus::Consensus>) -> Arc<Snapshot> {
    let tmp = TempDir::new().unwrap();
    let store = ChainDB::new(RocksDB::open_in(&tmp, 1), Default::default());
    Arc::new(Snapshot::new(
        consensus.genesis_block().header(),
        U256::zero(),
        Default::default(),
        store.get_snapshot(),
        Default::default(),
        consensus,
    ))
}

pub(super) fn program_transaction(
    version: ScriptVersion,
    programs: &[&'static [u8]],
) -> Arc<ResolvedTransaction> {
    let lock = Script::new_builder()
        .code_hash(CellOutput::calc_data_hash(&Bytes::from_static(programs[0])))
        .hash_type(version.data_hash_type())
        .build();
    transaction(lock, programs)
}

fn transaction(lock: Script, programs: &[&'static [u8]]) -> Arc<ResolvedTransaction> {
    let input = OutPoint::new(H256([1; 32]).into(), 0);
    let cells: Vec<_> = programs
        .iter()
        .enumerate()
        .map(|(index, program)| {
            CellMetaBuilder::from_cell_output(CellOutput::default(), Bytes::from_static(program))
                .out_point(OutPoint::new(H256([2; 32]).into(), index as u32))
                .build()
        })
        .collect();
    let tx = TransactionBuilder::default()
        .input(CellInput::new(input.clone(), 0))
        .cell_deps(cells.iter().map(|cell| {
            CellDep::new_builder()
                .out_point(cell.out_point.clone())
                .build()
        }))
        .output(
            CellOutput::new_builder()
                .capacity(capacity_bytes!(100))
                .lock(lock.clone())
                .build(),
        )
        .output_data(Bytes::new())
        .build();
    Arc::new(ResolvedTransaction {
        transaction: tx,
        resolved_cell_deps: cells,
        resolved_inputs: vec![
            CellMetaBuilder::from_cell_output(
                CellOutput::new_builder()
                    .capacity(capacity_bytes!(200))
                    .lock(lock)
                    .build(),
                Bytes::new(),
            )
            .out_point(input)
            .build(),
        ],
        resolved_dep_groups: vec![],
    })
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn budgeted_verification_preserves_vm_versions_cycles_and_errors() {
    let snapshot = snapshot(Arc::new(
        ConsensusBuilder::default()
            .hardfork_switch(HardForks::new_dev())
            .build(),
    ));
    let env = Arc::new(TxVerifyEnv::new_submit(snapshot.tip_header()));
    let (_commands, mut commands) = watch::channel(ChunkCommand::Resume);
    for version in [ScriptVersion::V0, ScriptVersion::V1, ScriptVersion::V2] {
        for program in [
            include_bytes!("../../../script/testdata/always_success").as_slice(),
            include_bytes!("../../../script/testdata/always_failure").as_slice(),
        ] {
            let rtx = program_transaction(version, &[program]);
            let verifier = TransactionScriptsVerifier::new(
                rtx,
                snapshot.as_data_loader(),
                snapshot.cloned_consensus(),
                Arc::clone(&env),
            );
            let expected = verifier.verify(u64::MAX);
            let mut runner = execution::VmRunner {
                command: &mut commands,
                remaining: Some(Duration::from_secs(2)),
            };
            let actual = verifier.verify_with_runner(u64::MAX, &mut runner).await;
            match expected {
                Ok(cycles) => {
                    assert_eq!(actual.unwrap(), Some(cycles));
                    ckb_error::assert_error_eq!(
                        verifier
                            .verify_with_runner(cycles - 1, &mut runner)
                            .await
                            .unwrap_err(),
                        verifier.verify(cycles - 1).unwrap_err(),
                    );
                }
                Err(error) => ckb_error::assert_error_eq!(actual.unwrap_err(), error),
            }
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn real_spawned_vm_cannot_outlive_its_budget() {
    let snapshot = snapshot(Arc::new(
        ConsensusBuilder::default()
            .hardfork_switch(HardForks::new_dev())
            .build(),
    ));
    let rtx = program_transaction(
        ScriptVersion::V2,
        &[
            include_bytes!("../../../script/testdata/spawn_caller_exec"),
            include_bytes!("../../../script/testdata/infinite_loop"),
        ],
    );
    let env = Arc::new(TxVerifyEnv::new_submit(snapshot.tip_header()));
    let verifier = TransactionScriptsVerifier::new(
        rtx,
        snapshot.as_data_loader(),
        snapshot.cloned_consensus(),
        env,
    );
    let (_commands, mut commands) = watch::channel(ChunkCommand::Resume);
    let mut runner = execution::VmRunner {
        command: &mut commands,
        remaining: Some(Duration::from_millis(20)),
    };
    let result = tokio::time::timeout(
        Duration::from_secs(3),
        verifier.verify_with_runner(u64::MAX, &mut runner),
    )
    .await
    .unwrap();
    assert!(result.unwrap().is_none());
    assert_eq!(runner.remaining, Some(Duration::ZERO));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn type_id_and_cached_cycles_skip_vm_time_without_skipping_validation() {
    let snapshot = snapshot(Arc::new(ConsensusBuilder::default().build()));
    let type_id = Script::new_builder()
        .code_hash(ckb_chain_spec::consensus::TYPE_ID_CODE_HASH)
        .hash_type(ScriptHashType::Type)
        .args(Bytes::from_static(&[1; 32]))
        .build();
    let rtx = transaction(type_id, &[]);
    let env = Arc::new(TxVerifyEnv::new_submit(snapshot.tip_header()));
    let verifier = ContextualTransactionVerifier::new(
        Arc::clone(&rtx),
        snapshot.cloned_consensus(),
        snapshot.as_data_loader(),
        Arc::clone(&env),
    );
    let expected = verifier.verify(u64::MAX, false).unwrap();
    let (_commands, mut commands) = watch::channel(ChunkCommand::Resume);
    let mut runner = execution::VmRunner {
        command: &mut commands,
        remaining: Some(Duration::ZERO),
    };
    assert_eq!(
        verifier
            .verify_with_runner(u64::MAX, &mut runner)
            .await
            .unwrap(),
        Some(expected)
    );

    // Use an ordinary VM script here: a cache miss must exhaust the zero budget.
    let rtx = program_transaction(
        ScriptVersion::V0,
        &[include_bytes!("../../../script/testdata/always_success")],
    );
    let expected = ContextualTransactionVerifier::new(
        Arc::clone(&rtx),
        snapshot.cloned_consensus(),
        snapshot.as_data_loader(),
        Arc::clone(&env),
    )
    .verify(u64::MAX, false)
    .unwrap();
    let cached = ContextualTransactionVerifier::new_with_cached_script_cycles(
        Arc::clone(&rtx),
        snapshot.cloned_consensus(),
        snapshot.as_data_loader(),
        Arc::clone(&env),
        Some(expected.cycles),
    );
    assert_eq!(
        cached
            .verify_with_runner(expected.cycles, &mut runner)
            .await
            .unwrap(),
        Some(expected)
    );
    ckb_error::assert_error_eq!(
        cached
            .verify_with_runner(expected.cycles - 1, &mut runner)
            .await
            .unwrap_err(),
        ckb_script::ScriptError::ExceededMaximumCycles(expected.cycles - 1).unknown_source(),
    );
    let mut invalid = (*rtx).clone();
    invalid.resolved_inputs[0].cell_output = invalid.resolved_inputs[0]
        .cell_output
        .clone()
        .as_builder()
        .capacity(capacity_bytes!(99))
        .build();
    let invalid = ContextualTransactionVerifier::new_with_cached_script_cycles(
        Arc::new(invalid),
        snapshot.cloned_consensus(),
        snapshot.as_data_loader(),
        env,
        Some(expected.cycles),
    );
    ckb_error::assert_error_eq!(
        invalid
            .verify_with_runner(expected.cycles, &mut runner)
            .await
            .unwrap_err(),
        ckb_verification::TransactionError::OutputsSumOverflow {
            inputs_sum: capacity_bytes!(99),
            outputs_sum: capacity_bytes!(100)
        },
    );
}
