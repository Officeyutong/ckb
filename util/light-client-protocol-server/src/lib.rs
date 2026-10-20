//! Server-side implementation for CKB light client protocol.
//!
//! TODO(light-client) More documentation.

use std::sync::Arc;

use ckb_logger::{debug, error, info, trace, warn};
use ckb_network::{CKBProtocolContext, CKBProtocolHandler, PeerIndex, async_trait, bytes::Bytes};
use ckb_shared::{Shared, Snapshot};
use ckb_store::ChainStore;
use ckb_types::{core, packed, prelude::*};

use crate::prelude::*;

mod components;
mod constant;
mod prelude;
mod status;

#[cfg(test)]
mod tests;

pub use status::{Status, StatusCode};

/// Light client protocol handler.
pub struct LightClientProtocol {
    /// Sync shared state.
    pub shared: Shared,
}

impl LightClientProtocol {
    /// Create a new light client protocol handler.
    pub fn new(shared: Shared) -> Self {
        Self { shared }
    }
}

/// The parts of a block needed to build a [`packed::VerifiableHeader`],
/// excluding the parent chain root which depends on the MMR.
pub(crate) struct VerifiableHeaderParts {
    pub(crate) header: core::HeaderView,
    pub(crate) uncles_hash: packed::Byte32,
    pub(crate) extension: Option<packed::Bytes>,
}

impl VerifiableHeaderParts {
    pub(crate) fn load(snapshot: &Snapshot, hash: &packed::Byte32) -> Option<Self> {
        let (header, uncles_hash, extension) =
            snapshot.get_block_header_with_uncles_and_extension(hash)?;
        Some(Self {
            header,
            uncles_hash,
            extension,
        })
    }

    pub(crate) fn to_verifiable_header(
        &self,
        parent_chain_root: packed::HeaderDigest,
    ) -> packed::VerifiableHeader {
        packed::VerifiableHeader::new_builder()
            .header(self.header.data())
            .uncles_hash(self.uncles_hash.clone())
            .extension(Pack::pack(&self.extension))
            .parent_chain_root(parent_chain_root)
            .build()
    }
}

#[async_trait]
impl CKBProtocolHandler for LightClientProtocol {
    async fn init(&mut self, _nc: Arc<dyn CKBProtocolContext + Sync>) {}

    async fn connected(
        &mut self,
        _nc: Arc<dyn CKBProtocolContext + Sync>,
        peer: PeerIndex,
        version: &str,
    ) {
        info!("LightClient({}).connected peer={}", version, peer);
    }

    async fn disconnected(&mut self, _nc: Arc<dyn CKBProtocolContext + Sync>, peer: PeerIndex) {
        info!("LightClient.disconnected peer={}", peer);
    }

    async fn received(
        &mut self,
        nc: Arc<dyn CKBProtocolContext + Sync>,
        peer: PeerIndex,
        data: Bytes,
    ) {
        trace!("LightClient.received peer={}", peer);

        let msg = match packed::LightClientMessageReader::from_slice(&data) {
            Ok(msg) => msg.to_enum(),
            _ => {
                warn!(
                    "LightClient.received a malformed message from Peer({})",
                    peer
                );
                nc.ban_peer(
                    peer,
                    constant::BAD_MESSAGE_BAN_TIME,
                    String::from("send us a malformed message"),
                );
                return;
            }
        };

        let item_name = msg.item_name();
        let status = self.try_process(&nc, peer, msg).await;
        if let Some(ban_time) = status.should_ban() {
            error!(
                "process {} from {}; ban {:?} since result is {}",
                item_name, peer, ban_time, status
            );
            nc.ban_peer(peer, ban_time, status.to_string());
        } else if status.should_warn() {
            warn!("process {} from {}; result is {}", item_name, peer, status);
        } else if !status.is_ok() {
            debug!("process {} from {}; result is {}", item_name, peer, status);
        }
    }
}

impl LightClientProtocol {
    async fn try_process(
        &mut self,
        nc: &Arc<dyn CKBProtocolContext + Sync>,
        peer_index: PeerIndex,
        message: packed::LightClientMessageUnionReader<'_>,
    ) -> Status {
        match message {
            packed::LightClientMessageUnionReader::GetLastState(reader) => {
                components::GetLastStateProcess::new(reader, self, peer_index, nc)
                    .execute()
                    .await
            }
            packed::LightClientMessageUnionReader::GetLastStateProof(reader) => {
                components::GetLastStateProofProcess::new(reader, self, peer_index, nc)
                    .execute()
                    .await
            }
            packed::LightClientMessageUnionReader::GetBlocksProof(reader) => {
                components::GetBlocksProofProcess::new(reader, self, peer_index, nc)
                    .execute()
                    .await
            }
            packed::LightClientMessageUnionReader::GetTransactionsProof(reader) => {
                components::GetTransactionsProofProcess::new(reader, self, peer_index, nc)
                    .execute()
                    .await
            }
            _ => StatusCode::UnexpectedProtocolMessage.into(),
        }
    }

    pub(crate) fn get_verifiable_tip_header(&self) -> Result<packed::VerifiableHeader, String> {
        let snapshot = self.shared.snapshot();

        let tip_hash = snapshot.tip_hash();
        let tip_header_parts = VerifiableHeaderParts::load(&snapshot, &tip_hash)
            .expect("checked: tip block should be existed");
        let parent_chain_root = if tip_header_parts.header.is_genesis() {
            Default::default()
        } else {
            let mmr = snapshot.chain_root_mmr(tip_header_parts.header.number() - 1);
            match mmr.get_root() {
                Ok(root) => root,
                Err(err) => {
                    let errmsg = format!("failed to generate a root since {err:?}");
                    return Err(errmsg);
                }
            }
        };

        let tip_header = tip_header_parts.to_verifiable_header(parent_chain_root);

        Ok(tip_header)
    }

    pub(crate) async fn reply_tip_state<T>(
        &self,
        peer: PeerIndex,
        nc: &Arc<dyn CKBProtocolContext + Sync>,
    ) -> Status
    where
        T: Entity,
        <T as Entity>::Builder: ProverMessageBuilder,
        <<T as Entity>::Builder as Builder>::Entity: Into<packed::LightClientMessageUnion>,
    {
        let tip_header = match self.get_verifiable_tip_header() {
            Ok(tip_state) => tip_state,
            Err(errmsg) => {
                return StatusCode::InternalError.with_context(errmsg);
            }
        };
        let content = T::new_builder().set_last_header(tip_header).build();
        let message = packed::LightClientMessage::new_builder()
            .set(content)
            .build();
        nc.reply(peer, &message).await;
        Status::ok()
    }

    pub(crate) async fn reply_proof<T>(
        &self,
        peer: PeerIndex,
        nc: &Arc<dyn CKBProtocolContext + Sync>,
        last_header_parts: &VerifiableHeaderParts,
        items_positions: Vec<u64>,
        proved_items: <<T as Entity>::Builder as ProverMessageBuilder>::ProvedItems,
        missing_items: <<T as Entity>::Builder as ProverMessageBuilder>::MissingItems,
    ) -> Status
    where
        T: Entity,
        <T as Entity>::Builder: ProverMessageBuilder,
        <<T as Entity>::Builder as Builder>::Entity: Into<packed::LightClientMessageUnion>,
    {
        let (parent_chain_root, proof) = if last_header_parts.header.is_genesis() {
            (Default::default(), Default::default())
        } else {
            let snapshot = self.shared.snapshot();
            let mmr = snapshot.chain_root_mmr(last_header_parts.header.number() - 1);
            let parent_chain_root = match mmr.get_root() {
                Ok(root) => root,
                Err(err) => {
                    let errmsg = format!("failed to generate a root since {err:?}");
                    return StatusCode::InternalError.with_context(errmsg);
                }
            };
            let proof = if items_positions.is_empty() {
                Default::default()
            } else {
                match mmr.gen_proof(items_positions) {
                    Ok(proof) => proof.proof_items().to_owned(),
                    Err(err) => {
                        let errmsg = format!("failed to generate a proof since {err:?}");
                        return StatusCode::InternalError.with_context(errmsg);
                    }
                }
            };
            (parent_chain_root, proof)
        };
        let verifiable_last_header = last_header_parts.to_verifiable_header(parent_chain_root);
        let content = T::new_builder()
            .set_last_header(verifiable_last_header)
            .set_proof(proof.into())
            .set_proved_items(proved_items)
            .set_missing_items(missing_items)
            .build();
        let message = packed::LightClientMessage::new_builder()
            .set(content)
            .build();
        nc.reply(peer, &message).await;
        Status::ok()
    }
}
