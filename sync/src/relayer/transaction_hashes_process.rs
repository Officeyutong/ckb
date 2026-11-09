use crate::relayer::{MAX_RELAY_TXS_NUM_PER_BATCH, Relayer};
use crate::{Status, StatusCode};
use ckb_network::PeerIndex;
use ckb_types::{packed, prelude::*};

pub struct TransactionHashesProcess<'a> {
    message: packed::RelayTransactionHashesReader<'a>,
    relayer: &'a Relayer,
    peer: PeerIndex,
}

impl<'a> TransactionHashesProcess<'a> {
    pub fn new(
        message: packed::RelayTransactionHashesReader<'a>,
        relayer: &'a Relayer,
        peer: PeerIndex,
    ) -> Self {
        TransactionHashesProcess {
            message,
            relayer,
            peer,
        }
    }

    pub fn execute(self) -> Status {
        let state = self.relayer.shared().state();
        {
            let relay_transaction_hashes = self.message;
            if relay_transaction_hashes.tx_hashes().len() > MAX_RELAY_TXS_NUM_PER_BATCH {
                return StatusCode::ProtocolMessageIsMalformed.with_context(format!(
                    "TxHashes count({}) > MAX_RELAY_TXS_NUM_PER_BATCH({})",
                    relay_transaction_hashes.tx_hashes().len(),
                    MAX_RELAY_TXS_NUM_PER_BATCH,
                ));
            }
        }

        let (known_tx_hashes, tx_hashes): (Vec<_>, Vec<_>) = {
            let mut tx_filter = state.tx_filter();
            tx_filter.remove_expired();
            self.message
                .tx_hashes()
                .iter()
                .map(|x| x.to_entity())
                .collect::<std::collections::HashSet<_>>()
                .into_iter()
                .partition(|tx_hash| tx_filter.contains(tx_hash))
        };

        // A known tx may still be under verification, keep this peer in case its body is discarded
        state.add_tx_fallback_peer(known_tx_hashes.into_iter(), self.peer);
        state.add_ask_for_txs(self.peer, tx_hashes)
    }
}
