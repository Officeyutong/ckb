# ckb-tx-pool

This crate is a component of [ckb](https://github.com/nervosnetwork/ckb).

CKB Tx-pool stores transactions, which is designed for CKB Two Step Transaction Confirmation mechanism

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

Package selection uses the maintained statistics for capacity and dependency ordering. The
[relation tests](src/component/tests/relations.rs) independently reconstruct the
graph from transaction bodies and check statistics, indexes, and rejection paths.
They also compare capacity preflight with materialized eviction across all
four-node DAGs, replacement closures, proposal statuses, and size limits. The
[admission tests](src/component/tests/admission.rs) cover submission failures,
backing changes during verification, and post-commit callback state.
