//! Canonical verification entry point and local CPU budgets for network transactions.

pub(crate) mod calibration;
mod cpu_clock;
mod execution;

#[cfg(test)]
mod tests;

use crate::error::Reject;
use ckb_script::ChunkCommand;
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

    let new_verifier = || {
        DaoScriptSizeVerifier::new(
            Arc::clone(&rtx),
            Arc::clone(&consensus),
            snapshot.borrow_as_data_loader(),
        )
        .verify()?;
        Ok::<_, ckb_error::Error>(
            ContextualTransactionVerifier::new_with_cached_script_cycles(
                rtx,
                consensus,
                data_loader,
                tx_env,
                cache_entry.map(|entry| entry.cycles),
            ),
        )
    };

    match (command_rx, cpu_time_limit) {
        // RPC submissions keep the whole synchronous check off the async worker.
        (None, None) => block_in_place(|| new_verifier()?.verify(max_tx_verify_cycles, false))
            .map_err(Reject::Verification),
        (command_rx, cpu_time_limit) => {
            let verifier = new_verifier().map_err(Reject::Verification)?;
            // Direct network orphans have a budget but no worker control channel.
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
                .map_err(Reject::Verification)?
                .ok_or_else(|| {
                    if let Some(metrics) = ckb_metrics::handle() {
                        metrics.ckb_tx_pool_vm_cpu_budget_refusals_total.inc();
                    }
                    Reject::ExcessiveVerifyTime
                })
        }
    }
}
