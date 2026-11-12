# ckb-tx-pool

This crate is a component of [ckb](https://github.com/nervosnetwork/ckb).

CKB Tx-pool stores transactions, which is designed for CKB Two Step Transaction Confirmation mechanism

## Membership and package statistics

`PoolMap` derives parent links from inputs, resolved cell dependencies (including
dep-group members), and the ordering of dependency readers before cell spenders.
Its ancestor and descendant statistics count each reachable transaction once,
including itself. Inserting a late parent or removing a bridge updates both sides
of the changed relations while preserving alternative paths. Removing a whole
descendant package updates only the ancestors that remain in the pool.

Admission checks limits, cycles in the relation graph, and input conflicts against
the surviving graph before changing membership. Replacement, reader, and capacity
evictions form one descendant-closed removal set. They preserve the candidate's
input and resolved dependency producers and their ancestors. Ancestor limits also
cover retained descendants of a late parent. Capacity preflight recalculates only
affected eviction keys, retaining the existing status and fee priorities without
cloning or tentatively changing the pool. A rejected admission leaves membership,
indexes, conflict history, and callbacks unchanged.

Submission rechecks resolved backing under the pool write lock even when the chain
tip has not changed during verification. Replacement counts shared victims once
and protects expanded dep-group members. Successful admission publishes removal
and acceptance callbacks only after membership commits. Expiry removes complete
descendant packages; proposal detachment changes status without removing entries.
Reorg restoration enforces capacity after reinserting the detached batch.
Submission derives the final proposal status at commit and uses that same status
for the block-assembler notification after releasing the pool lock. Verification
and membership share the Pending/Gap/Proposed states; callers do not carry a
second status into admission or publish an acceptance notification separately.
A blocked or cancelled notification does not retain the pool lock or roll back
committed membership; notification on this path remains best-effort.

Package selection uses the maintained statistics for capacity and dependency
ordering. A prepared package keeps unselected entries and their short IDs in
ancestor order; the selection's membership set also excludes them from subsequent
package updates. Only remaining-capacity failures count toward the consecutive
failure limit, and successful selection resets it. Unproposed ancestors do not
consume that limit.
Admission and reorg trimming share the same capacity-eviction priority. The
[relation tests](src/component/tests/relations.rs) independently reconstruct the
graph from transaction bodies and check statistics, indexes, and rejection paths.
They also compare capacity preflight with materialized eviction across all
four-node DAGs, replacement closures, proposal statuses, and size limits. The
[admission tests](src/component/tests/admission.rs) cover submission failures,
backing changes during verification, and post-commit callback state.

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
