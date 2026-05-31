#[derive(Debug, Clone, Default)]
pub(super) struct LocalGrantReceiptAuthority;

impl LocalGrantReceiptAuthority {
    fn node_key_id(storage_node: StorageNodeId) -> StorageNodeKeyId {
        StorageNodeKeyId::from_raw(storage_node.raw())
    }

    fn grant_id(segment_id: SegmentId) -> GrantId {
        GrantId::from_raw(segment_id.raw())
    }

    fn nonce(segment_id: SegmentId, write_intent: WriteIntentId) -> GrantNonce {
        GrantNonce::from_raw(segment_id.raw() ^ write_intent.raw().rotate_left(17))
    }

    fn verify_expected_proof(
        expected: crate::provider::ProofTag,
        proof: crate::provider::ProofTag,
    ) -> Result<()> {
        if proof != expected {
            return Err(StorageError::conflict("proof verification failed"));
        }
        Ok(())
    }

    fn verify_grant_proof_and_hash(grant: &WriteGrant) -> Result<crate::provider::GrantHash> {
        let (hash, expected_proof) = deterministic_test_grant_hash_and_proof(grant.key_id, grant);
        Self::verify_expected_proof(expected_proof, grant.proof)?;
        Ok(hash)
    }

    fn verify_receipt_proof(receipt: &SegmentWriteReceipt) -> Result<()> {
        Self::verify_expected_proof(
            deterministic_test_proof_for_receipt(receipt.node_key_id, receipt),
            receipt.proof,
        )
    }

    fn verify_reference_proof(evidence: &ReferenceEvidence) -> Result<()> {
        Self::verify_expected_proof(
            deterministic_test_proof_for_reference(evidence.node_key_id, evidence),
            evidence.proof,
        )
    }

    fn verify_not_expired(expires_at: LogicalDeadline) -> Result<()> {
        if expires_at.raw() < LOCAL_GRANT_EPOCH.raw() {
            return Err(StorageError::unavailable("write grant expired"));
        }
        Ok(())
    }

    fn verify_receipt_matches_grant(
        &self,
        grant: &WriteGrant,
        receipt: &SegmentWriteReceipt,
    ) -> Result<VerifiedSegmentReceipt> {
        let grant_hash = self.verify_write_grant_and_hash(
            grant,
            receipt.storage_node,
            receipt.segment_id,
            receipt.bytes,
        )?;
        let verified = self.verify_segment_receipt(receipt)?;
        if receipt.grant_id != grant.grant_id
            || receipt.grant_hash != grant_hash
            || receipt.tenant != grant.tenant
            || receipt.principal != grant.principal
            || receipt.owner != grant.owner
            || receipt.intent != grant.intent
            || receipt.write_intent != grant.write_intent
            || receipt.segment_id != grant.segment_id
            || receipt.storage_node != grant.storage_node
            || receipt.bytes != grant.max_bytes
            || receipt.durability != grant.durability
            || receipt.receipt_epoch != grant.grant_epoch
            || receipt.expires_at != grant.expires_at
            || receipt.node_key_id != grant.key_id
        {
            return Err(StorageError::conflict(
                "segment receipt does not match write grant",
            ));
        }
        Ok(verified)
    }

    fn create_segment_receipt_after_verified_write(
        &self,
        grant: &WriteGrant,
        grant_hash: crate::provider::GrantHash,
        commit: SegmentReplicaCommit,
        storage_node_incarnation: ServerIncarnation,
    ) -> Result<SegmentWriteReceipt> {
        if commit.descriptor.segment_id != grant.segment_id
            || commit.placement.segment_id != grant.segment_id
        {
            return Err(StorageError::conflict(
                "receipt commit does not match granted segment",
            ));
        }
        if commit.descriptor.bytes != grant.max_bytes || commit.placement.bytes != grant.max_bytes {
            return Err(StorageError::conflict(
                "receipt byte count does not match granted byte count",
            ));
        }
        let mut receipt = SegmentWriteReceipt {
            tenant: grant.tenant,
            grant_id: grant.grant_id,
            grant_hash,
            principal: grant.principal,
            owner: grant.owner,
            storage_node: grant.storage_node,
            storage_node_incarnation,
            segment_id: grant.segment_id,
            write_intent: grant.write_intent,
            intent: grant.intent,
            bytes: grant.max_bytes,
            integrity: commit.descriptor.integrity,
            durability: grant.durability,
            lifecycle: SegmentReceiptLifecycle::DurablePendingMetadata,
            receipt_epoch: grant.grant_epoch,
            expires_at: grant.expires_at,
            node_key_id: Self::node_key_id(grant.storage_node),
            proof_scheme: ProofScheme::DeterministicTestMacV1,
            proof: crate::provider::ProofTag::ZERO,
            descriptor: commit.descriptor,
            placement: commit.placement,
        };
        receipt.proof = deterministic_test_proof_for_receipt(receipt.node_key_id, &receipt);
        Ok(receipt)
    }

    fn verify_write_grant_and_hash(
        &self,
        grant: &WriteGrant,
        storage_node: StorageNodeId,
        segment_id: SegmentId,
        bytes: u64,
    ) -> Result<crate::provider::GrantHash> {
        Self::verify_not_expired(grant.expires_at)?;
        if grant.grant_epoch != LOCAL_GRANT_EPOCH {
            return Err(StorageError::conflict(
                "write grant epoch does not match verifier epoch",
            ));
        }
        if grant.owner != grant.intent.owner() {
            return Err(StorageError::conflict(
                "write grant owner does not match intent",
            ));
        }
        if grant.max_bytes == 0 {
            return Err(StorageError::invalid_argument(
                "write grant must authorize at least one byte",
            ));
        }
        if grant.proof_scheme != ProofScheme::DeterministicTestMacV1 {
            return Err(StorageError::unsupported(
                "local grant verifier supports only deterministic test proofs",
            ));
        }
        if grant.key_id != Self::node_key_id(grant.storage_node) {
            return Err(StorageError::conflict(
                "grant key does not match storage node",
            ));
        }
        let grant_hash = Self::verify_grant_proof_and_hash(grant)?;
        if grant.storage_node != storage_node {
            return Err(StorageError::conflict(
                "write grant storage node does not match request",
            ));
        }
        if grant.segment_id != segment_id {
            return Err(StorageError::conflict(
                "write grant segment ID does not match request",
            ));
        }
        if bytes != grant.max_bytes {
            return Err(StorageError::invalid_argument(
                "write grant byte count does not match granted segment length",
            ));
        }
        Ok(grant_hash)
    }
}

impl GrantReceiptAuthority for LocalGrantReceiptAuthority {
    fn issue_write_grant(&self, request: WriteGrantRequest) -> Result<WriteGrant> {
        if request.max_bytes == 0 {
            return Err(StorageError::invalid_argument(
                "write grant must authorize at least one byte",
            ));
        }
        Self::verify_not_expired(request.expires_at)?;
        let owner = request.intent.owner();
        let key_id = Self::node_key_id(request.storage_node);
        let mut grant = WriteGrant {
            tenant: request.tenant,
            principal: request.principal,
            grant_id: Self::grant_id(request.segment_id),
            nonce: Self::nonce(request.segment_id, request.write_intent),
            grant_epoch: LOCAL_GRANT_EPOCH,
            expires_at: request.expires_at,
            owner,
            intent: request.intent,
            write_intent: request.write_intent,
            segment_id: request.segment_id,
            storage_node: request.storage_node,
            max_bytes: request.max_bytes,
            payload_integrity: request.payload_integrity,
            durability: request.durability,
            key_id,
            proof_scheme: ProofScheme::DeterministicTestMacV1,
            proof: crate::provider::ProofTag::ZERO,
        };
        grant.proof = deterministic_test_proof_for_grant(key_id, &grant);
        Ok(grant)
    }

    fn verify_write_grant(
        &self,
        grant: &WriteGrant,
        storage_node: StorageNodeId,
        segment_id: SegmentId,
        bytes: u64,
    ) -> Result<()> {
        self.verify_write_grant_and_hash(grant, storage_node, segment_id, bytes)
            .map(|_| ())
    }

    fn create_segment_receipt(
        &self,
        grant: &WriteGrant,
        commit: SegmentReplicaCommit,
        storage_node_incarnation: ServerIncarnation,
    ) -> Result<SegmentWriteReceipt> {
        let grant_hash = self.verify_write_grant_and_hash(
            grant,
            commit.placement.storage_node,
            commit.descriptor.segment_id,
            commit.descriptor.bytes,
        )?;
        self.create_segment_receipt_after_verified_write(
            grant,
            grant_hash,
            commit,
            storage_node_incarnation,
        )
    }

    fn verify_segment_receipt(
        &self,
        receipt: &SegmentWriteReceipt,
    ) -> Result<VerifiedSegmentReceipt> {
        Self::verify_not_expired(receipt.expires_at)?;
        if receipt.receipt_epoch != LOCAL_GRANT_EPOCH {
            return Err(StorageError::conflict(
                "receipt epoch does not match verifier epoch",
            ));
        }
        if receipt.owner != receipt.intent.owner() {
            return Err(StorageError::conflict(
                "receipt owner does not match intent",
            ));
        }
        if receipt.bytes == 0 {
            return Err(StorageError::invalid_argument(
                "receipt must describe at least one byte",
            ));
        }
        if receipt.proof_scheme != ProofScheme::DeterministicTestMacV1 {
            return Err(StorageError::unsupported(
                "local receipt verifier supports only deterministic test proofs",
            ));
        }
        if receipt.node_key_id != Self::node_key_id(receipt.storage_node) {
            return Err(StorageError::conflict(
                "receipt key does not match storage node",
            ));
        }
        Self::verify_receipt_proof(receipt)?;
        if receipt.lifecycle != SegmentReceiptLifecycle::DurablePendingMetadata {
            return Err(StorageError::conflict(
                "receipt is not durable-pending-metadata",
            ));
        }
        if receipt.segment_id != receipt.descriptor.segment_id
            || receipt.segment_id != receipt.placement.segment_id
        {
            return Err(StorageError::conflict(
                "receipt segment IDs do not match descriptor and placement",
            ));
        }
        if receipt.storage_node != receipt.placement.storage_node {
            return Err(StorageError::conflict(
                "receipt storage node does not match placement",
            ));
        }
        if receipt.bytes != receipt.descriptor.bytes || receipt.bytes != receipt.placement.bytes {
            return Err(StorageError::conflict(
                "receipt byte count does not match descriptor and placement",
            ));
        }
        if receipt.integrity != receipt.descriptor.integrity {
            return Err(StorageError::conflict(
                "receipt payload integrity does not match descriptor",
            ));
        }
        Ok(VerifiedSegmentReceipt {
            receipt: receipt.clone(),
            descriptor: receipt.descriptor.clone(),
        })
    }

    fn create_reference_evidence(
        &self,
        receipt: &SegmentWriteReceipt,
        metadata_commit: CommitSeq,
    ) -> Result<ReferenceEvidence> {
        self.verify_segment_receipt(receipt)?;
        let mut evidence = ReferenceEvidence {
            tenant: receipt.tenant,
            principal: receipt.principal,
            owner: receipt.owner,
            grant_id: receipt.grant_id,
            segment_id: receipt.segment_id,
            storage_node: receipt.storage_node,
            metadata_commit,
            receipt_epoch: receipt.receipt_epoch,
            node_key_id: receipt.node_key_id,
            proof_scheme: ProofScheme::DeterministicTestMacV1,
            proof: crate::provider::ProofTag::ZERO,
        };
        evidence.proof = deterministic_test_proof_for_reference(evidence.node_key_id, &evidence);
        Ok(evidence)
    }

    fn verify_reference_evidence(
        &self,
        evidence: &ReferenceEvidence,
        segment_id: SegmentId,
        storage_node: StorageNodeId,
    ) -> Result<()> {
        if evidence.proof_scheme != ProofScheme::DeterministicTestMacV1 {
            return Err(StorageError::unsupported(
                "local reference verifier supports only deterministic test proofs",
            ));
        }
        if evidence.segment_id != segment_id {
            return Err(StorageError::conflict(
                "reference evidence segment ID does not match request",
            ));
        }
        if evidence.storage_node != storage_node {
            return Err(StorageError::conflict(
                "reference evidence storage node does not match request",
            ));
        }
        if evidence.node_key_id != Self::node_key_id(storage_node) {
            return Err(StorageError::conflict(
                "reference evidence key does not match storage node",
            ));
        }
        Self::verify_reference_proof(evidence)
    }
}
