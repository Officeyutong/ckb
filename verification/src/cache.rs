//! TX verification cache

use ckb_script::TransactionState;
use ckb_types::{
    core::{Capacity, Cycle, EntryCompleted, TransactionView},
    packed::Byte32,
};
use std::collections::HashMap;
use std::sync::Arc;

/// An opaque transaction verification cache key derived from a witness transaction hash.
///
/// The private field and the absence of a `Byte32` conversion ensure callers can only create a
/// key from a [`TransactionView`].
///
/// The key carries no epoch, script version or hardfork context on purpose; see
/// [`TxVerificationCache`] for the reasoning and for the residual risk that a future
/// semantic-changing hardfork has to account for.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct VerifyCacheKey(Byte32);

impl From<&TransactionView> for VerifyCacheKey {
    fn from(tx: &TransactionView) -> Self {
        Self(tx.witness_hash())
    }
}

/// TX verification lru cache
///
/// # The cache is not bound to the VM or hardfork context, and that is deliberate
///
/// Entries are keyed by the witness hash only (see [`VerifyCacheKey`]) and store nothing but a
/// script cycle count, so a hit replays that count without executing the script, even when the
/// current epoch would select a different VM version for the same transaction.
///
/// This is a reviewed decision, not an oversight. A hardfork that changes script results is
/// scheduled at a known epoch, so only a node that stays up across the activation can carry an
/// entry from before it. CKB2021 and CKB2023 shipped together with explicit invalidation: the
/// cache was reset when a hardfork feature that may change script results became enabled. After
/// both forks activated, historical chain data was reviewed and confirmed to be unaffected by
/// that shortcut, and the transition machinery was removed as cleanup.
///
/// The residual exposure is recorded here on purpose: a node that stays up across a *future*
/// fork which changes script results can serve a pre-fork entry for a transaction that the new
/// VM rejects, while a node without that entry re-executes the script and rejects the block
/// that contains it. Any such fork must bring the invalidation back as part of its rollout, and
/// must not assume that the cache is cold at the activation epoch.
pub type TxVerificationCache = lru::LruCache<VerifyCacheKey, CachedScriptCycles>;

/// Verification cache entries fetched for a batch of transactions.
pub type FetchedTxVerificationCache = HashMap<VerifyCacheKey, CachedScriptCycles>;

/// Lookup entries in a transaction verification cache by witness transaction hash.
pub trait TxVerificationCacheLookup {
    /// Returns the cached verification result for `key`.
    fn get_by_wtx_hash(&self, key: &VerifyCacheKey) -> Option<&CachedScriptCycles>;
}

impl TxVerificationCacheLookup for TxVerificationCache {
    fn get_by_wtx_hash(&self, key: &VerifyCacheKey) -> Option<&CachedScriptCycles> {
        self.peek(key)
    }
}

impl TxVerificationCacheLookup for FetchedTxVerificationCache {
    fn get_by_wtx_hash(&self, key: &VerifyCacheKey) -> Option<&CachedScriptCycles> {
        self.get(key)
    }
}

const CACHE_SIZE: usize = 1000 * 30;

/// Initialize cache
pub fn init_cache() -> TxVerificationCache {
    lru::LruCache::new(CACHE_SIZE)
}

/// Cached result of successful transaction script verification.
///
/// It records the cycles only; no VM version, epoch or hardfork marker is stored alongside it.
/// See [`TxVerificationCache`] for why that is intentional.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CachedScriptCycles {
    /// Cached transaction script cycles.
    pub cycles: Cycle,
}

impl CachedScriptCycles {
    /// Creates cached script cycles.
    pub fn new(cycles: Cycle) -> CachedScriptCycles {
        Self { cycles }
    }
}

/// Suspended state
#[derive(Clone, Debug)]
pub struct Suspended {
    /// Cached tx fee
    pub fee: Capacity,
    /// State
    pub state: Arc<TransactionState>,
}

/// Completed contextual transaction verification.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Completed {
    /// Verified transaction script cycles.
    pub cycles: Cycle,
    /// Calculated transaction fee.
    pub fee: Capacity,
}

impl From<Completed> for EntryCompleted {
    fn from(value: Completed) -> Self {
        EntryCompleted {
            cycles: value.cycles,
            fee: value.fee,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ckb_types::{bytes::Bytes, core::TransactionBuilder};

    #[test]
    fn cache_key_distinguishes_transactions_with_different_witnesses() {
        let tx = TransactionBuilder::default().build();
        let cousin = tx.as_advanced_builder().witness(Bytes::new()).build();

        assert_eq!(tx.hash(), cousin.hash());
        assert_ne!(tx.witness_hash(), cousin.witness_hash());

        let cached_cycles = CachedScriptCycles::new(42);
        let tx_key = VerifyCacheKey::from(&tx);
        let cousin_key = VerifyCacheKey::from(&cousin);

        let mut cache = init_cache();
        cache.put(tx_key.clone(), cached_cycles);
        assert_eq!(cache.get_by_wtx_hash(&tx_key), Some(&cached_cycles));
        assert_eq!(cache.get_by_wtx_hash(&cousin_key), None);

        let fetched_cache = FetchedTxVerificationCache::from([(tx_key.clone(), cached_cycles)]);
        assert_eq!(fetched_cache.get_by_wtx_hash(&tx_key), Some(&cached_cycles));
        assert_eq!(fetched_cache.get_by_wtx_hash(&cousin_key), None);
    }
}
