# ckb-tx-pool

This crate is a component of [ckb](https://github.com/nervosnetwork/ckb).

CKB Tx-pool stores transactions, which is designed for CKB Two Step Transaction Confirmation mechanism

## Verification CPU budget

Relay, proposal-response, network-orphan verification and `test_tx_pool_accept`
share one CPU budget per transaction, across script groups and resumes. The dry-run
RPC has no declared cycles and uses the full CPU cap. Local transaction submission,
recovery and canonical block verification keep their existing cycle limits.

When `test_tx_pool_accept` exhausts its CPU budget, it returns
`PoolRejectedTransactionByVerifyTimeLimit (-1113)`. This is a retryable resource
refusal, not consensus invalidity. Neither successful nor refused dry runs admit
the transaction, relay it or persist a rejection.

Only CPU time measured on the execution thread spends the budget. Queueing,
suspension and time spent descheduled or blocked do not. A wall timer requests a
cooperative checkpoint; after joining that slice, verification resumes if its CPU
receipt leaves budget. A refusal joins the VM work first, remains retryable and
does not ban the peer or enter the success or persistent rejection caches.

The clock uses the POSIX thread CPU clock where available and `GetThreadTimes`
on Windows. Availability is checked at startup. Without a usable clock, the node
logs that the CPU budget is disabled and retains cycle-bounded verification. If
only calibration fails, the fixed CPU cap remains active. A later clock failure
interrupts the affected verification instead of granting more unmeasured work.

Calibration runs three fixed-workload samples on each VM version and uses the
median per version, retaining the slowest rate and loading cost. The policy adds
16-fold slack and caps the budget at `MIN_BLOCK_INTERVAL` (currently 8 seconds).
These are internal admission rules, not consensus rules or configuration knobs.
The fixed workload cannot characterize every script or host; the slack is a
conservative policy choice, not a measured bound on arbitrary workloads.

Startup logs report the rate, minimum allowance, mode and calibration cost. With
metrics enabled, inspect:

- `ckb_tx_pool_vm_calibration`, with one active `mode`: `calibrated`, `cap_only` or `unavailable`.
- `ckb_tx_pool_vm_calibration_seconds` and `ckb_tx_pool_vm_cycles_per_ms`.
- `ckb_tx_pool_vm_cpu_budget_refusals_total`.
- `ckb_tx_pool_vm_cpu_clock_failures_total`.

Integration tests explicitly enable the root `test` feature and can set
`CKB_TEST_MAX_TX_VERIFY_TIME_MS` in the child node's environment. Building the
integration harness or the whole workspace does not enable that feature on the
node. Production binaries ignore the variable, and old node configurations
continue to load without new fields.
