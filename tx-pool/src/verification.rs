//! Canonical verification and CPU budgets for network transactions and RPC dry runs.

pub(crate) mod calibration;
mod cpu_clock;
mod execution;

#[cfg(test)]
mod tests;

use crate::error::Reject;
use ckb_script::{ChunkCommand, ScriptError, TransactionScriptError};
use ckb_snapshot::Snapshot;
use ckb_store::ChainStore;
use ckb_store::data_loader_wrapper::AsDataLoader;
use ckb_types::core::{Cycle, cell::ResolvedTransaction};
use ckb_verification::{
    ContextualTransactionVerifier, DaoScriptSizeVerifier, TxVerifyEnv,
    cache::{CachedScriptCycles, Completed},
};
use std::{sync::Arc, time::Duration};
use tokio::{sync::watch, task::block_in_place};

fn classify_verification_error(error: ckb_error::Error) -> Reject {
    let strict_only = error
        .root_cause()
        .downcast_ref::<TransactionScriptError>()
        .is_some_and(|script_error| {
            matches!(
                script_error.script_error(),
                ScriptError::VMInternalError(vm_error) if vm_error.is_raised_by_strict()
            )
        });
    if strict_only {
        Reject::PoolPolicy(error)
    } else {
        Reject::Verification(error)
    }
}

pub(crate) async fn verify_rtx(
    snapshot: Arc<Snapshot>,
    rtx: Arc<ResolvedTransaction>,
    tx_env: Arc<TxVerifyEnv>,
    cache_entry: &Option<CachedScriptCycles>,
    max_tx_verify_cycles: Cycle,
    command_rx: Option<&mut watch::Receiver<ChunkCommand>>,
    cpu_time_limit: Option<Duration>,
) -> Result<Completed, Reject> {
    let consensus = snapshot.cloned_consensus();
    let data_loader = snapshot.as_data_loader();

    DaoScriptSizeVerifier::new(
        Arc::clone(&rtx),
        Arc::clone(&consensus),
        snapshot.borrow_as_data_loader(),
    )
    .verify()
    .map_err(Reject::Verification)?;
    let new_verifier = || {
        ContextualTransactionVerifier::new_with_cached_script_cycles(
            rtx,
            consensus,
            data_loader,
            tx_env,
            cache_entry.map(|entry| entry.cycles),
        )
        .with_validation_mode(ckb_script::ValidationMode::Strict)
    };

    match (command_rx, cpu_time_limit) {
        // RPC submissions keep the whole synchronous check off the async worker.
        (None, None) => block_in_place(|| new_verifier().verify(max_tx_verify_cycles, false))
            .map_err(classify_verification_error),
        (command_rx, cpu_time_limit) => {
            let verifier = new_verifier();
            // RPC dry runs and direct orphans have no worker control channel.
            let mut resume_channel;
            let command_rx = match command_rx {
                Some(command_rx) => command_rx,
                None => {
                    resume_channel = watch::channel(ChunkCommand::Resume);
                    &mut resume_channel.1
                }
            };
            verifier
                .verify_with_runner(
                    max_tx_verify_cycles,
                    &mut execution::VmRunner {
                        command: command_rx,
                        remaining: cpu_time_limit,
                    },
                )
                .await
                .map_err(classify_verification_error)?
                .ok_or_else(|| {
                    if let Some(metrics) = ckb_metrics::handle() {
                        metrics.ckb_tx_pool_vm_cpu_budget_refusals_total.inc();
                    }
                    Reject::ExcessiveVerifyTime
                })
        }
    }
}
