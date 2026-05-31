use super::*;

fn config() -> LocalStoreConfig {
    LocalStoreConfig {
        shard_count: 2,
        block_size: 4096,
        file_root_blocks: 8,
        metadata_fanout: 2,
        metadata_leaf_blocks: 1024,
        storage_node: StorageNodeId::from_raw(77),
        observability_event_capacity: 1024,
    }
}

fn device_request() -> MetadataCreateDeviceRequest {
    MetadataCreateDeviceRequest {
        spec: DeviceSpec {
            logical_blocks: 16,
            block_size: 4096,
        },
        name: Some("root".to_string()),
    }
}

fn metadata_leaf(node_id: u128, start: u64, blocks: u64) -> MetadataNode {
    MetadataNode {
        node_id: MetadataNodeId::from_raw(node_id),
        covered_range: crate::api::BlockRange::new(
            BlockIndex::from_raw(start),
            BlockCount::from_raw(blocks),
        ),
        kind: MetadataNodeKind::Leaf {
            entries: Vec::new(),
            run_extents: Vec::new(),
        },
    }
}

#[test]
fn txn_metadata_publish_conflicts_only_on_touched_shards() {
    for mode in [
        MetadataTxnMode::Serial,
        MetadataTxnMode::Sharded { shard_count: 8 },
    ] {
        let metadata = TxnBlockMetadataPlane::new(config(), mode).unwrap();
        let head = metadata.create_device(device_request()).unwrap();
        let shard_zero = metadata_leaf(10_001, 0, 8);
        let shard_one = metadata_leaf(10_002, 8, 8);
        let stale_zero = metadata_leaf(10_003, 0, 8);
        metadata
            .persist_metadata_node(MetadataNodeWrite::new(shard_zero.clone(), Vec::new()))
            .unwrap();
        metadata
            .persist_metadata_node(MetadataNodeWrite::new(shard_one.clone(), Vec::new()))
            .unwrap();
        metadata
            .persist_metadata_node(MetadataNodeWrite::new(stale_zero.clone(), Vec::new()))
            .unwrap();

        metadata
            .publish_commit_group(CommitGroupIntent {
                owner: MappingOwner::BlockDevice(head.device_id),
                fence: MetadataFence::DeviceGeneration(head.generation),
                updates: vec![RootUpdate::BlockShard(ShardRootUpdate {
                    shard_id: ShardId::from_raw(0),
                    old_root: head.shard_roots[0],
                    new_root: shard_zero.node_id,
                })],
            })
            .unwrap();

        metadata
            .publish_commit_group(CommitGroupIntent {
                owner: MappingOwner::BlockDevice(head.device_id),
                fence: MetadataFence::DeviceGeneration(head.generation),
                updates: vec![RootUpdate::BlockShard(ShardRootUpdate {
                    shard_id: ShardId::from_raw(1),
                    old_root: head.shard_roots[1],
                    new_root: shard_one.node_id,
                })],
            })
            .unwrap();

        let merged = metadata.get_head(head.device_id).unwrap();
        assert_eq!(merged.shard_roots[0], shard_zero.node_id);
        assert_eq!(merged.shard_roots[1], shard_one.node_id);

        let stale = metadata.publish_commit_group(CommitGroupIntent {
            owner: MappingOwner::BlockDevice(head.device_id),
            fence: MetadataFence::DeviceGeneration(head.generation),
            updates: vec![RootUpdate::BlockShard(ShardRootUpdate {
                shard_id: ShardId::from_raw(0),
                old_root: head.shard_roots[0],
                new_root: stale_zero.node_id,
            })],
        });
        assert!(matches!(stale, Err(StorageError::Conflict { .. })));
        assert_eq!(metadata.get_head(head.device_id).unwrap(), merged);
    }
}

#[test]
fn txn_block_coordinator_preserves_block_write_read_semantics() {
    for mode in [
        MetadataTxnMode::Serial,
        MetadataTxnMode::Sharded { shard_count: 8 },
    ] {
        let cfg = config();
        let store =
            TxnBlockCoordinator::with_storage_nodes(cfg, vec![cfg.storage_node], mode).unwrap();
        let device_id = store
            .create_device(CreateDeviceRequest {
                spec: DeviceSpec {
                    logical_blocks: 16,
                    block_size: 4096,
                },
                name: None,
            })
            .unwrap();
        store
            .write_device_with_integrity(
                device_id,
                0,
                &[9; 4096],
                WriteDurability::Acknowledged,
                PayloadIntegrity::Verified,
            )
            .unwrap();
        store
            .write_device_with_integrity(
                device_id,
                8 * 4096,
                &[3; 4096],
                WriteDurability::Acknowledged,
                PayloadIntegrity::Verified,
            )
            .unwrap();

        let mut buf = vec![0; 9 * 4096];
        store
            .read_device_with_verification(
                device_id,
                ByteRange::new(0, 9 * 4096),
                &mut buf,
                ReadVerification::Default,
            )
            .unwrap();
        assert_eq!(&buf[0..4096], &[9; 4096]);
        assert_eq!(&buf[4096..8 * 4096], vec![0; 7 * 4096].as_slice());
        assert_eq!(&buf[8 * 4096..9 * 4096], &[3; 4096]);
    }
}

#[test]
fn txn_metadata_profile_is_empty_when_disabled_and_records_publish_when_enabled() {
    let metadata =
        TxnBlockMetadataPlane::new(config(), MetadataTxnMode::Sharded { shard_count: 8 }).unwrap();
    let head = metadata.create_device(device_request()).unwrap();
    let node = metadata_leaf(11_001, 0, 8);
    metadata
        .persist_metadata_node(MetadataNodeWrite::new(node.clone(), Vec::new()))
        .unwrap();
    assert!(metadata.drain_profiles(100).unwrap().is_empty());

    metadata.enable_profiling(16).unwrap();
    metadata
        .publish_commit_group(CommitGroupIntent {
            owner: MappingOwner::BlockDevice(head.device_id),
            fence: MetadataFence::DeviceGeneration(head.generation),
            updates: vec![RootUpdate::BlockShard(ShardRootUpdate {
                shard_id: ShardId::from_raw(0),
                old_root: head.shard_roots[0],
                new_root: node.node_id,
            })],
        })
        .unwrap();
    let profiles = metadata.drain_profiles(100).unwrap();
    assert_eq!(profiles.len(), 1);
    assert_eq!(
        profiles[0].phase,
        MetadataTxnProfilePhase::PublishBlockCommit
    );
    assert_eq!(profiles[0].conflict_count, 0);
    assert!(profiles[0].read_key_count >= 1);
    assert!(profiles[0].write_key_count >= 1);
}

#[test]
fn txn_block_write_profile_is_empty_when_disabled_and_records_successful_writes() {
    let cfg = config();
    let store = TxnBlockCoordinator::with_storage_nodes(
        cfg,
        vec![cfg.storage_node, StorageNodeId::from_raw(88)],
        MetadataTxnMode::Sharded { shard_count: 8 },
    )
    .unwrap();
    let device_id = store
        .create_device(CreateDeviceRequest {
            spec: DeviceSpec {
                logical_blocks: 16,
                block_size: 4096,
            },
            name: None,
        })
        .unwrap();
    store
        .write_device_with_integrity(
            device_id,
            0,
            &[9; 4096],
            WriteDurability::Acknowledged,
            PayloadIntegrity::Verified,
        )
        .unwrap();
    assert!(store.drain_block_write_profiles(100).unwrap().is_empty());

    store.enable_block_write_profiling(16).unwrap();
    store
        .write_device_with_integrity(
            device_id,
            4096,
            &[7; 4096],
            WriteDurability::Acknowledged,
            PayloadIntegrity::Verified,
        )
        .unwrap();
    let profiles = store.drain_block_write_profiles(100).unwrap();
    assert_eq!(profiles.len(), 1);
    let profile = &profiles[0];
    assert_eq!(profile.touched_shard_count, 1);
    assert_eq!(profile.segment_count, 1);
    assert_eq!(profile.storage_node_count, 2);
    assert!(profile.total_nanos > 0);
    assert!(profile.segment_write_nanos > 0);
    assert!(profile.storage_node_ids_nanos > 0);
    assert!(profile.placement_select_nanos > 0);
    assert!(profile.segment_id_alloc_nanos > 0);
    assert!(profile.grant_issue_nanos > 0);
    assert!(profile.storage_node_transport_dispatch_nanos > 0);
    assert!(profile.grant_verify_nanos > 0);
    assert!(profile.catalog_duplicate_probe_nanos > 0);
    assert!(profile.catalog_reserve_nanos > 0);
    assert!(profile.catalog_begin_nanos > 0);
    assert!(profile.segment_store_write_nanos > 0);
    assert!(profile.checksum_integrity_nanos > 0);
    assert!(profile.segment_store_insert_nanos > 0);
    assert!(profile.segment_sync_nanos > 0);
    assert!(profile.receipt_create_nanos > 0);
    assert!(profile.receipt_verify_nanos > 0);
    assert!(profile.catalog_commit_nanos > 0);
    assert!(profile.tree_path_copy_nanos > 0);
    assert!(profile.metadata_publish_call_nanos > 0);
    assert!(profile.mark_referenced_nanos > 0);
    assert!(profile.mark_reference_evidence_nanos > 0);
    assert!(profile.mark_reference_transport_dispatch_nanos > 0);
    assert!(profile.mark_reference_verify_nanos > 0);
    assert!(profile.mark_reference_catalog_nanos > 0);
}

#[test]
fn txn_block_write_profile_reports_multi_shard_write_shape() {
    let cfg = config();
    let store = TxnBlockCoordinator::with_storage_nodes(
        cfg,
        vec![cfg.storage_node],
        MetadataTxnMode::Serial,
    )
    .unwrap();
    let device_id = store
        .create_device(CreateDeviceRequest {
            spec: DeviceSpec {
                logical_blocks: 16,
                block_size: 4096,
            },
            name: None,
        })
        .unwrap();
    store.enable_block_write_profiling(16).unwrap();
    store
        .write_device_with_integrity(
            device_id,
            7 * 4096,
            &[5; 8192],
            WriteDurability::Acknowledged,
            PayloadIntegrity::Unchecked,
        )
        .unwrap();
    let profiles = store.drain_block_write_profiles(100).unwrap();
    assert_eq!(profiles.len(), 1);
    assert_eq!(profiles[0].touched_shard_count, 2);
    assert_eq!(profiles[0].segment_count, 2);
    assert!(profiles[0].payload_copy_nanos > 0);
}

#[test]
fn txn_metadata_multi_shard_failure_exposes_no_partial_update() {
    let metadata =
        TxnBlockMetadataPlane::new(config(), MetadataTxnMode::Sharded { shard_count: 8 }).unwrap();
    let head = metadata.create_device(device_request()).unwrap();
    let shard_zero = metadata_leaf(12_001, 0, 8);
    let stale_zero = metadata_leaf(12_002, 0, 8);
    let shard_one = metadata_leaf(12_003, 8, 8);
    for node in [&shard_zero, &stale_zero, &shard_one] {
        metadata
            .persist_metadata_node(MetadataNodeWrite::new(node.clone(), Vec::new()))
            .unwrap();
    }

    metadata
        .publish_commit_group(CommitGroupIntent {
            owner: MappingOwner::BlockDevice(head.device_id),
            fence: MetadataFence::DeviceGeneration(head.generation),
            updates: vec![RootUpdate::BlockShard(ShardRootUpdate {
                shard_id: ShardId::from_raw(0),
                old_root: head.shard_roots[0],
                new_root: shard_zero.node_id,
            })],
        })
        .unwrap();

    let failed = metadata.publish_commit_group(CommitGroupIntent {
        owner: MappingOwner::BlockDevice(head.device_id),
        fence: MetadataFence::DeviceGeneration(head.generation),
        updates: vec![
            RootUpdate::BlockShard(ShardRootUpdate {
                shard_id: ShardId::from_raw(0),
                old_root: head.shard_roots[0],
                new_root: stale_zero.node_id,
            }),
            RootUpdate::BlockShard(ShardRootUpdate {
                shard_id: ShardId::from_raw(1),
                old_root: head.shard_roots[1],
                new_root: shard_one.node_id,
            }),
        ],
    });
    assert!(matches!(failed, Err(StorageError::Conflict { .. })));
    let after = metadata.get_head(head.device_id).unwrap();
    assert_eq!(after.shard_roots[0], shard_zero.node_id);
    assert_eq!(after.shard_roots[1], head.shard_roots[1]);
}

#[test]
fn txn_block_fork_restore_delete_and_gc_roots_preserve_block_contents() {
    let cfg = config();
    let store = TxnBlockCoordinator::with_storage_nodes(
        cfg,
        vec![cfg.storage_node],
        MetadataTxnMode::Sharded { shard_count: 8 },
    )
    .unwrap();
    let device_id = store
        .create_device(CreateDeviceRequest {
            spec: DeviceSpec {
                logical_blocks: 16,
                block_size: 4096,
            },
            name: None,
        })
        .unwrap();
    store
        .write_device_with_integrity(
            device_id,
            0,
            &[1; 4096],
            WriteDurability::Acknowledged,
            PayloadIntegrity::Verified,
        )
        .unwrap();
    let checkpoint = store.checkpoint(device_id).unwrap();
    store
        .write_device_with_integrity(
            device_id,
            0,
            &[2; 4096],
            WriteDurability::Acknowledged,
            PayloadIntegrity::Verified,
        )
        .unwrap();

    let fork = store
        .fork_device(
            device_id,
            ForkRequest {
                target: None,
                name: None,
            },
        )
        .unwrap();
    store
        .write_device_with_integrity(
            device_id,
            0,
            &[3; 4096],
            WriteDurability::Acknowledged,
            PayloadIntegrity::Verified,
        )
        .unwrap();
    let restored = store
        .restore_device(device_id, RestorePoint::Checkpoint(checkpoint))
        .unwrap();

    let mut source = vec![0; 4096];
    let mut forked = vec![0; 4096];
    let mut restored_bytes = vec![0; 4096];
    store
        .read_device_with_verification(
            device_id,
            ByteRange::new(0, 4096),
            &mut source,
            ReadVerification::Default,
        )
        .unwrap();
    store
        .read_device_with_verification(
            fork,
            ByteRange::new(0, 4096),
            &mut forked,
            ReadVerification::Default,
        )
        .unwrap();
    store
        .read_device_with_verification(
            restored,
            ByteRange::new(0, 4096),
            &mut restored_bytes,
            ReadVerification::Default,
        )
        .unwrap();
    assert_eq!(source, vec![3; 4096]);
    assert_eq!(forked, vec![2; 4096]);
    assert_eq!(restored_bytes, vec![1; 4096]);

    let delete = store.delete_device(device_id).unwrap();
    assert_eq!(delete.device_id, device_id);
    assert!(
        store
            .read_device_with_verification(
                device_id,
                ByteRange::new(0, 4096),
                &mut source,
                ReadVerification::Default,
            )
            .is_err()
    );
    let roots = store
        .roots_for_gc(RetentionPolicy::retain_deleted_devices())
        .unwrap();
    assert!(!roots.is_empty());
}
