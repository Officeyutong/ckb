# ckb-tx-pool

This crate is a component of [ckb](https://github.com/nervosnetwork/ckb).

CKB Tx-pool stores transactions, which is designed for CKB Two Step Transaction Confirmation mechanism

`PoolMap` derives parent links from inputs, resolved cell dependencies (including
dep-group members), and the ordering of dependency readers before cell spenders.
Its ancestor and descendant statistics count each reachable transaction once,
including itself. Inserting a late parent or removing a bridge updates both sides
of the changed relations while preserving alternative paths. Removing a whole
descendant package updates only the ancestors that remain in the pool.

Admission checks limits, cycles in the relation graph, and input conflicts before
changing membership, including any planned reader evictions. Reader evictions
preserve the candidate's producers and their ancestors. Ancestor limits apply to
the surviving graph, including retained descendants of a late parent. Package
selection uses these maintained statistics for capacity and dependency ordering. The
[relation tests](src/component/tests/relations.rs) independently reconstruct the
graph from transaction bodies and check statistics, indexes, and rejection paths.
