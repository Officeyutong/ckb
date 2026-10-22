use crate::utils::{build_relay_tx_hashes, build_relay_txs, wait_until};
use crate::{Net, Node, Spec};
use ckb_app_config::BlockAssemblerConfig;
use ckb_chain_spec::{OUTPUT_INDEX_SECP256K1_BLAKE160_SIGHASH_ALL, build_genesis_type_id_script};
use ckb_crypto::secp::{Generator, Privkey};
use ckb_hash::{blake2b_256, new_blake2b};
use ckb_jsonrpc_types::JsonBytes;
use ckb_network::SupportProtocols;
use ckb_types::{
    H256,
    bytes::Bytes,
    core::{Capacity, DepType, ScriptHashType, TransactionBuilder, TransactionView},
    packed::{
        Byte32, CellDep, CellInput, CellOutput, OutPoint, RelayMessage, RelayMessageUnionReader,
        WitnessArgs,
    },
    prelude::*,
};

/// A requested peer answers with the right raw tx but an invalid witness. The node marks `tx.hash()`
/// (which excludes witnesses) as known on receipt, so after banning the peer it must forget the hash
/// and refetch the tx from other announcers, otherwise the valid witness variant is suppressed.
pub struct RelayInvalidWitnessVariant {
    privkey: Privkey,
    lock_arg: Bytes,
}

impl Default for RelayInvalidWitnessVariant {
    fn default() -> Self {
        let privkey = Generator::non_crypto_safe_prng(363).gen_privkey();
        let pubkey_data = privkey.pubkey().expect("Get pubkey failed").serialize();
        let lock_arg = Bytes::from(blake2b_256(pubkey_data)[0..20].to_vec());
        RelayInvalidWitnessVariant { privkey, lock_arg }
    }
}

fn secp_code_hash() -> H256 {
    build_genesis_type_id_script(OUTPUT_INDEX_SECP256K1_BLAKE160_SIGHASH_ALL)
        .calc_script_hash()
        .into()
}

impl RelayInvalidWitnessVariant {
    // Spend the tip cellbase (paid to our secp lock) and sign it; returns (valid, invalid) variants
    // sharing the same `tx.hash()` and differing only in witness.
    fn build_variants(&self, node: &Node) -> (TransactionView, TransactionView) {
        let cell_dep = CellDep::new_builder()
            .out_point(OutPoint::new(node.dep_group_tx_hash(), 0))
            .dep_type(DepType::DepGroup)
            .build();
        let cellbase_hash = node.get_tip_block().transactions()[0].hash();
        let output = CellOutput::new_builder()
            .capacity(Capacity::shannons(100_0000_0000))
            .lock(node.always_success_script())
            .build();
        let tx = TransactionBuilder::default()
            .cell_dep(cell_dep)
            .input(CellInput::new(OutPoint::new(cellbase_hash, 0), 0))
            .output(output)
            .output_data(Bytes::new())
            .build();

        let placeholder = WitnessArgs::new_builder()
            .lock(Some(Bytes::from(vec![0u8; 65])))
            .build();
        let witness_len = placeholder.as_slice().len() as u64;
        let message = {
            let mut hasher = new_blake2b();
            hasher.update(tx.hash().as_slice());
            hasher.update(&witness_len.to_le_bytes());
            hasher.update(placeholder.as_slice());
            let mut buf = [0u8; 32];
            hasher.finalize(&mut buf);
            H256::from(buf)
        };
        let sign = |msg: &H256| {
            let sig = self.privkey.sign_recoverable(msg).expect("sign");
            placeholder
                .clone()
                .as_builder()
                .lock(Some(Bytes::from(sig.serialize())))
                .build()
                .as_bytes()
        };
        let valid = tx.as_advanced_builder().witness(sign(&message)).build();
        // Well-formed signature over the wrong message: fails the secp lock script.
        let invalid = tx
            .as_advanced_builder()
            .witness(sign(&H256::from([1u8; 32])))
            .build();
        assert_eq!(valid.hash(), invalid.hash());
        assert_ne!(valid.witness_hash(), invalid.witness_hash());
        (valid, invalid)
    }
}

fn wait_get_relay_txs_containing(net: &Net, node: &Node, hash: &Byte32) -> bool {
    let hash = hash.clone();
    net.should_receive(node, move |data| {
        RelayMessage::from_slice(data)
            .ok()
            .and_then(|message| match message.as_reader().to_enum() {
                RelayMessageUnionReader::GetRelayTransactions(reader) => {
                    Some(reader.tx_hashes().iter().any(|h| h.to_entity() == hash))
                }
                _ => None,
            })
            .unwrap_or(false)
    })
}

fn in_pool(node: &Node, tx: &TransactionView) -> bool {
    node.rpc_client()
        .get_transaction(tx.hash())
        .transaction
        .is_some()
}

impl Spec for RelayInvalidWitnessVariant {
    fn run(&self, nodes: &mut Vec<Node>) {
        let node = &nodes[0];
        node.mine(20);
        let (valid, invalid) = self.build_variants(node);
        node.mine(1);
        let (bundled, _) = self.build_variants(node);
        node.mine(3);
        let cycles = |tx: &TransactionView| -> u64 {
            node.rpc_client()
                .estimate_cycles(tx.data().into())
                .cycles
                .into()
        };
        let new_net = || {
            let mut net = Net::new(
                self.name(),
                node.consensus(),
                vec![SupportProtocols::Sync, SupportProtocols::RelayV3],
            );
            net.connect(node);
            net
        };
        let attacker = new_net();
        let honest = new_net();
        let hashes = [valid.hash(), bundled.hash()];

        // The attacker announces first and becomes the requested peer, the honest peer is a fallback.
        attacker.send(
            node,
            SupportProtocols::RelayV3,
            build_relay_tx_hashes(&hashes),
        );
        assert!(
            wait_get_relay_txs_containing(&attacker, node, &valid.hash()),
            "node should request the txs from the attacker"
        );
        honest.send(
            node,
            SupportProtocols::RelayV3,
            build_relay_tx_hashes(&hashes),
        );

        // The attacker answers with a genuine tx and the invalid witness variant of the other one.
        attacker.send(
            node,
            SupportProtocols::RelayV3,
            build_relay_txs(&[
                (bundled.clone(), cycles(&bundled)),
                (invalid.clone(), cycles(&valid)),
            ]),
        );
        assert!(
            wait_until(20, || !node.rpc_client().get_banned_addresses().is_empty()),
            "attacker should be banned"
        );
        assert!(!in_pool(node, &invalid));

        // The node should forget the discarded body and refetch it from the honest peer.
        assert!(
            wait_get_relay_txs_containing(&honest, node, &valid.hash()),
            "node should refetch the valid witness variant from the honest peer"
        );
        honest.send(
            node,
            SupportProtocols::RelayV3,
            build_relay_txs(&[
                (valid.clone(), cycles(&valid)),
                (bundled.clone(), cycles(&bundled)),
            ]),
        );
        assert!(
            wait_until(20, || in_pool(node, &valid) && in_pool(node, &bundled)),
            "both txs should enter the pool"
        );
    }

    fn modify_app_config(&self, config: &mut ckb_app_config::CKBAppConfig) {
        config.block_assembler = Some(BlockAssemblerConfig {
            code_hash: secp_code_hash(),
            hash_type: ScriptHashType::Type.into(),
            args: JsonBytes::from_bytes(self.lock_arg.clone()),
            message: Default::default(),
            use_binary_version_as_message_prefix: false,
            binary_version: "TEST".to_string(),
            update_interval_millis: 0,
            notify: vec![],
            notify_scripts: vec![],
            notify_timeout_millis: 800,
            notify_auth_token: None,
        });
    }
}
