use criterion::{BatchSize, BenchmarkId, Criterion, criterion_group, criterion_main};
use std::hint::black_box;
use toy_cow_block_storage::api::BlockRange;
use toy_cow_block_storage::id::{BlockCount, BlockIndex, MetadataNodeId, SegmentId};
use toy_cow_block_storage::local::{
    InMemoryMetadataPlane, InMemorySegmentStore, LocalObjectStore, LocalStoreConfig,
};
use toy_cow_block_storage::object::{LeafEntry, MetadataNode, MetadataNodeKind, SegmentDescriptor};
use toy_cow_block_storage::provider::{
    MetadataCreateDeviceRequest, MetadataPlane, RetentionPolicy, SegmentReservation, SegmentStore,
};
use toy_cow_block_storage::sim::SeededRng;
use toy_cow_block_storage::{
    AppendLease, AppendLeaseId, BlockClient, BlockDevice, BlockRequest, ByteRange, DeviceId,
    DeviceSpec, FileId, FileVersion, ForkRequest, KeyspaceId, NativeFile, NativeKeyspaceClient,
    NativeRequest, RestorePoint, WriteDurability, WriterEpoch,
};

fn bench_byte_range_validation(c: &mut Criterion) {
    let spec = DeviceSpec {
        logical_blocks: 1024 * 1024,
        block_size: 4096,
    };
    let range = ByteRange::new(128 * 4096, 64 * 4096);

    c.bench_function("byte_range_validation", |b| {
        b.iter(|| black_box(range).validate_for_device(black_box(&spec)))
    });
}

fn bench_block_request_validation(c: &mut Criterion) {
    let spec = DeviceSpec {
        logical_blocks: 1024 * 1024,
        block_size: 4096,
    };
    let request = BlockRequest::Write {
        device_id: DeviceId::from_raw(7),
        offset: 128 * 4096,
        bytes: vec![0; 64 * 4096],
        durability: WriteDurability::Acknowledged,
    };

    c.bench_function("block_request_validation", |b| {
        b.iter(|| black_box(&request).validate_for_existing_device(black_box(&spec)))
    });
}

fn bench_seeded_rng(c: &mut Criterion) {
    c.bench_function("seeded_rng_next_u64", |b| {
        b.iter(|| {
            let mut rng = SeededRng::new(black_box(42));
            let mut acc = 0;
            for _ in 0..1024 {
                acc ^= rng.next_u64();
            }
            black_box(acc)
        })
    });
}

fn bench_block_range_helpers(c: &mut Criterion) {
    let range = BlockRange::new(BlockIndex::from_raw(10), BlockCount::from_raw(1024));
    let other = BlockRange::new(BlockIndex::from_raw(512), BlockCount::from_raw(64));

    c.bench_function("block_range_helpers", |b| {
        b.iter(|| {
            let range = black_box(range);
            let other = black_box(other);
            black_box(range.end_exclusive()).unwrap();
            black_box(range.contains_range(other)).unwrap();
            black_box(range.overlaps(other)).unwrap();
            black_box(range.split_at(BlockIndex::from_raw(512))).unwrap()
        })
    });
}

fn bench_metadata_leaf_validation(c: &mut Criterion) {
    let segments = vec![
        SegmentDescriptor {
            segment_id: SegmentId::from_raw(1),
            blocks: BlockCount::from_raw(128),
            bytes: 128 * 4096,
            checksum: None,
        },
        SegmentDescriptor {
            segment_id: SegmentId::from_raw(2),
            blocks: BlockCount::from_raw(128),
            bytes: 128 * 4096,
            checksum: None,
        },
    ];
    let node = MetadataNode {
        node_id: MetadataNodeId::from_raw(1),
        covered_range: BlockRange::new(BlockIndex::from_raw(0), BlockCount::from_raw(256)),
        kind: MetadataNodeKind::Leaf {
            entries: vec![
                LeafEntry {
                    logical_start: BlockIndex::from_raw(0),
                    blocks: BlockCount::from_raw(64),
                    segment_id: SegmentId::from_raw(1),
                    segment_offset: BlockIndex::from_raw(0),
                },
                LeafEntry {
                    logical_start: BlockIndex::from_raw(128),
                    blocks: BlockCount::from_raw(64),
                    segment_id: SegmentId::from_raw(2),
                    segment_offset: BlockIndex::from_raw(0),
                },
            ],
        },
    };

    c.bench_function("metadata_leaf_validation", |b| {
        b.iter(|| black_box(&node).validate(black_box(&segments)))
    });
}

fn bench_in_memory_metadata_node_lookup(c: &mut Criterion) {
    let metadata = InMemoryMetadataPlane::new(LocalStoreConfig::default()).unwrap();
    let node = MetadataNode {
        node_id: MetadataNodeId::from_raw(99),
        covered_range: BlockRange::new(BlockIndex::from_raw(0), BlockCount::from_raw(128)),
        kind: MetadataNodeKind::Leaf {
            entries: Vec::new(),
        },
    };
    metadata.persist_metadata_node(node.clone()).unwrap();

    c.bench_function("in_memory_metadata_node_lookup", |b| {
        b.iter(|| metadata.get_metadata_node(black_box(node.node_id)))
    });
}

fn bench_in_memory_segment_read(c: &mut Criterion) {
    let store = InMemorySegmentStore::new(LocalStoreConfig::default()).unwrap();
    let reservation = SegmentReservation {
        segment_id: SegmentId::from_raw(42),
        bytes: 4096,
    };
    store.write_segment(&reservation, &[7; 4096]).unwrap();
    store.sync_segment(reservation.segment_id).unwrap();
    let mut buf = vec![0; 4096];

    c.bench_function("in_memory_segment_read", |b| {
        b.iter(|| {
            store
                .read_segment(
                    black_box(reservation.segment_id),
                    black_box(ByteRange::new(0, 4096)),
                    black_box(&mut buf),
                )
                .unwrap();
            black_box(buf[0])
        })
    });
}

fn bench_local_empty_device_read(c: &mut Criterion) {
    let store = LocalObjectStore::new();
    let head = store
        .metadata()
        .create_device(MetadataCreateDeviceRequest {
            spec: DeviceSpec {
                logical_blocks: 1024 * 1024,
                block_size: 4096,
            },
            name: None,
        })
        .unwrap();
    let mut buf = vec![0; 64 * 4096];

    c.bench_function("local_empty_device_read", |b| {
        b.iter(|| {
            store
                .read_device(
                    black_box(head.device_id),
                    black_box(ByteRange::new(128 * 4096, 64 * 4096)),
                    black_box(&mut buf),
                )
                .unwrap();
            black_box(buf[0])
        })
    });
}

fn bench_local_single_shard_write(c: &mut Criterion) {
    c.bench_function("local_single_shard_write", |b| {
        b.iter_batched(
            || {
                let store = LocalObjectStore::new();
                let head = store
                    .metadata()
                    .create_device(MetadataCreateDeviceRequest {
                        spec: DeviceSpec {
                            logical_blocks: 1024,
                            block_size: 4096,
                        },
                        name: None,
                    })
                    .unwrap();
                (store, head.device_id, vec![3; 4096])
            },
            |(store, device_id, bytes)| {
                store
                    .write_device(
                        black_box(device_id),
                        black_box(0),
                        black_box(&bytes),
                        WriteDurability::Acknowledged,
                    )
                    .unwrap()
            },
            BatchSize::SmallInput,
        )
    });
}

fn bench_local_single_shard_write_by_tree_depth(c: &mut Criterion) {
    let mut group = c.benchmark_group("local_single_shard_write_tree_depth");
    for leaf_blocks in [1024, 64, 8, 1] {
        group.bench_with_input(
            BenchmarkId::from_parameter(leaf_blocks),
            &leaf_blocks,
            |b, &leaf_blocks| {
                b.iter_batched(
                    || {
                        let store = LocalObjectStore::with_config(LocalStoreConfig {
                            shard_count: 1,
                            block_size: 4096,
                            file_root_blocks: 1024,
                            metadata_fanout: 4,
                            metadata_leaf_blocks: leaf_blocks,
                            storage_node: toy_cow_block_storage::StorageNodeId::from_raw(1),
                        })
                        .unwrap();
                        let head = store
                            .metadata()
                            .create_device(MetadataCreateDeviceRequest {
                                spec: DeviceSpec {
                                    logical_blocks: 1024,
                                    block_size: 4096,
                                },
                                name: None,
                            })
                            .unwrap();
                        (store, head.device_id, vec![5; 4096])
                    },
                    |(store, device_id, bytes)| {
                        store
                            .write_device(
                                black_box(device_id),
                                black_box(512 * 4096),
                                black_box(&bytes),
                                WriteDurability::Acknowledged,
                            )
                            .unwrap()
                    },
                    BatchSize::SmallInput,
                )
            },
        );
    }
    group.finish();
}

fn bench_local_multi_shard_atomic_write(c: &mut Criterion) {
    c.bench_function("local_multi_shard_atomic_write", |b| {
        b.iter_batched(
            || {
                let store = LocalObjectStore::with_config(LocalStoreConfig {
                    shard_count: 4,
                    block_size: 4096,
                    file_root_blocks: 128,
                    metadata_fanout: 4,
                    metadata_leaf_blocks: 8,
                    storage_node: toy_cow_block_storage::StorageNodeId::from_raw(1),
                })
                .unwrap();
                let server =
                    std::sync::Arc::new(toy_cow_block_storage::LocalBlockServer::new(store));
                let client = toy_cow_block_storage::LocalBlockClient::new(
                    toy_cow_block_storage::InProcessBlockTransport::new(server),
                );
                let device_id = client
                    .create_device(toy_cow_block_storage::CreateDeviceRequest {
                        spec: DeviceSpec {
                            logical_blocks: 64,
                            block_size: 4096,
                        },
                        name: None,
                    })
                    .unwrap();
                (client.open_device(device_id).unwrap(), vec![11; 64 * 4096])
            },
            |(device, bytes)| device.write_at(black_box(0), black_box(&bytes)).unwrap(),
            BatchSize::SmallInput,
        )
    });
}

fn bench_local_read_by_mapping_count(c: &mut Criterion) {
    let mut group = c.benchmark_group("local_read_mapping_count");
    for mapping_count in [1_u64, 8, 32] {
        group.bench_with_input(
            BenchmarkId::from_parameter(mapping_count),
            &mapping_count,
            |b, &mapping_count| {
                let store = LocalObjectStore::with_config(LocalStoreConfig {
                    shard_count: 4,
                    block_size: 4096,
                    file_root_blocks: 128,
                    metadata_fanout: 4,
                    metadata_leaf_blocks: 4,
                    storage_node: toy_cow_block_storage::StorageNodeId::from_raw(1),
                })
                .unwrap();
                let server =
                    std::sync::Arc::new(toy_cow_block_storage::LocalBlockServer::new(store));
                let client = toy_cow_block_storage::LocalBlockClient::new(
                    toy_cow_block_storage::InProcessBlockTransport::new(server),
                );
                let device_id = client
                    .create_device(toy_cow_block_storage::CreateDeviceRequest {
                        spec: DeviceSpec {
                            logical_blocks: 128,
                            block_size: 4096,
                        },
                        name: None,
                    })
                    .unwrap();
                let device = client.open_device(device_id).unwrap();
                for index in 0..mapping_count {
                    let block = index * (128 / mapping_count);
                    device.write_at(block * 4096, &[index as u8; 4096]).unwrap();
                }
                let mut buf = vec![0; 128 * 4096];
                b.iter(|| {
                    device.read_at(black_box(0), black_box(&mut buf)).unwrap();
                    black_box(buf[0])
                })
            },
        );
    }
    group.finish();
}

fn bench_local_native_append(c: &mut Criterion) {
    c.bench_function("local_native_append", |b| {
        b.iter_batched(
            || {
                let store = LocalObjectStore::new();
                let keyspace = store
                    .metadata()
                    .create_keyspace(
                        toy_cow_block_storage::provider::MetadataCreateKeyspaceRequest {
                            request: toy_cow_block_storage::CreateKeyspaceRequest { name: None },
                        },
                    )
                    .unwrap();
                let head = store
                    .metadata()
                    .create_file(toy_cow_block_storage::provider::MetadataCreateFileRequest {
                        keyspace_id: keyspace.keyspace_id,
                        request: toy_cow_block_storage::CreateFileRequest {
                            spec: toy_cow_block_storage::FileSpec { name: None },
                        },
                    })
                    .unwrap();
                let lease = store
                    .acquire_append_lease(keyspace.keyspace_id, head.file_id)
                    .unwrap();
                (store, lease, vec![4; 4096])
            },
            |(store, lease, bytes)| {
                store
                    .append_file(
                        black_box(lease),
                        black_box(&bytes),
                        WriteDurability::Acknowledged,
                    )
                    .unwrap()
            },
            BatchSize::SmallInput,
        )
    });
}

fn bench_local_native_stale_lease_rejection(c: &mut Criterion) {
    let store = LocalObjectStore::new();
    let server = std::sync::Arc::new(toy_cow_block_storage::LocalNativeServer::new(store));
    let client = toy_cow_block_storage::LocalNativeClient::new(
        toy_cow_block_storage::InProcessNativeTransport::new(server),
    );
    let keyspace_id = client
        .create_keyspace(toy_cow_block_storage::CreateKeyspaceRequest { name: None })
        .unwrap();
    let file_id = client
        .create_file(
            keyspace_id,
            toy_cow_block_storage::CreateFileRequest {
                spec: toy_cow_block_storage::FileSpec { name: None },
            },
        )
        .unwrap();
    let file = client.open_file(keyspace_id, file_id).unwrap();
    let stale = file.acquire_append().unwrap();
    let _fresh = file.acquire_append().unwrap();
    let bytes = vec![1; 4096];

    c.bench_function("local_native_stale_lease_rejection", |b| {
        b.iter(|| {
            black_box(
                file.append_with_lease(black_box(stale.clone()), black_box(&bytes))
                    .is_err(),
            )
        })
    });
}

fn bench_local_fork_vs_device_size(c: &mut Criterion) {
    let mut group = c.benchmark_group("local_fork_device_size");
    for logical_blocks in [1024, 1024 * 1024, 16 * 1024 * 1024] {
        group.bench_with_input(
            BenchmarkId::from_parameter(logical_blocks),
            &logical_blocks,
            |b, &logical_blocks| {
                b.iter_batched(
                    || {
                        let store = LocalObjectStore::with_config(LocalStoreConfig {
                            shard_count: 8,
                            block_size: 4096,
                            file_root_blocks: 1024,
                            metadata_fanout: 4,
                            metadata_leaf_blocks: logical_blocks,
                            storage_node: toy_cow_block_storage::StorageNodeId::from_raw(1),
                        })
                        .unwrap();
                        let server = std::sync::Arc::new(
                            toy_cow_block_storage::LocalBlockServer::new(store),
                        );
                        let client = toy_cow_block_storage::LocalBlockClient::new(
                            toy_cow_block_storage::InProcessBlockTransport::new(server),
                        );
                        let device_id = client
                            .create_device(toy_cow_block_storage::CreateDeviceRequest {
                                spec: DeviceSpec {
                                    logical_blocks,
                                    block_size: 4096,
                                },
                                name: None,
                            })
                            .unwrap();
                        client.open_device(device_id).unwrap()
                    },
                    |device| {
                        device
                            .fork(ForkRequest {
                                target: None,
                                name: None,
                            })
                            .unwrap()
                    },
                    BatchSize::SmallInput,
                )
            },
        );
    }
    group.finish();
}

fn bench_local_checkpoint_restore(c: &mut Criterion) {
    c.bench_function("local_checkpoint_restore", |b| {
        b.iter_batched(
            || {
                let store = LocalObjectStore::with_config(LocalStoreConfig {
                    shard_count: 4,
                    block_size: 4096,
                    file_root_blocks: 1024,
                    metadata_fanout: 4,
                    metadata_leaf_blocks: 16,
                    storage_node: toy_cow_block_storage::StorageNodeId::from_raw(1),
                })
                .unwrap();
                let server = std::sync::Arc::new(toy_cow_block_storage::LocalBlockServer::new(
                    store.clone(),
                ));
                let client = toy_cow_block_storage::LocalBlockClient::new(
                    toy_cow_block_storage::InProcessBlockTransport::new(server),
                );
                let device_id = client
                    .create_device(toy_cow_block_storage::CreateDeviceRequest {
                        spec: DeviceSpec {
                            logical_blocks: 1024,
                            block_size: 4096,
                        },
                        name: None,
                    })
                    .unwrap();
                let device = client.open_device(device_id).unwrap();
                for block in (0..128).step_by(8) {
                    device.write_at(block * 4096, &[7; 8 * 4096]).unwrap();
                }
                let checkpoint = store.metadata().checkpoint(device_id).unwrap();
                for block in (256..384).step_by(8) {
                    device.write_at(block * 4096, &[9; 8 * 4096]).unwrap();
                }
                (device, checkpoint)
            },
            |(device, checkpoint)| {
                device
                    .restore(black_box(RestorePoint::Checkpoint(checkpoint)))
                    .unwrap()
            },
            BatchSize::SmallInput,
        )
    });
}

fn bench_local_native_keyspace_checkpoint_restore(c: &mut Criterion) {
    c.bench_function("local_native_keyspace_checkpoint_restore", |b| {
        b.iter_batched(
            || {
                let store = LocalObjectStore::with_config(LocalStoreConfig {
                    shard_count: 1,
                    block_size: 4096,
                    file_root_blocks: 1024,
                    metadata_fanout: 4,
                    metadata_leaf_blocks: 16,
                    storage_node: toy_cow_block_storage::StorageNodeId::from_raw(1),
                })
                .unwrap();
                let server = std::sync::Arc::new(toy_cow_block_storage::LocalNativeServer::new(
                    store.clone(),
                ));
                let client = toy_cow_block_storage::LocalNativeClient::new(
                    toy_cow_block_storage::InProcessNativeTransport::new(server),
                );
                let keyspace_id = client
                    .create_keyspace(toy_cow_block_storage::CreateKeyspaceRequest { name: None })
                    .unwrap();
                for file_index in 0..8 {
                    let file_id = client
                        .create_file(
                            keyspace_id,
                            toy_cow_block_storage::CreateFileRequest {
                                spec: toy_cow_block_storage::FileSpec {
                                    name: Some(format!("file-{file_index}")),
                                },
                            },
                        )
                        .unwrap();
                    let file = client.open_file(keyspace_id, file_id).unwrap();
                    file.append_with_lease(file.acquire_append().unwrap(), &[7; 4096])
                        .unwrap();
                }
                let checkpoint = client.checkpoint_keyspace(keyspace_id).unwrap();
                let file_id = client
                    .create_file(
                        keyspace_id,
                        toy_cow_block_storage::CreateFileRequest {
                            spec: toy_cow_block_storage::FileSpec { name: None },
                        },
                    )
                    .unwrap();
                let file = client.open_file(keyspace_id, file_id).unwrap();
                file.append_with_lease(file.acquire_append().unwrap(), &[9; 4096])
                    .unwrap();
                (client, keyspace_id, checkpoint)
            },
            |(client, keyspace_id, checkpoint)| {
                client
                    .restore_keyspace(
                        black_box(keyspace_id),
                        black_box(RestorePoint::Checkpoint(checkpoint)),
                    )
                    .unwrap()
            },
            BatchSize::SmallInput,
        )
    });
}

fn bench_roots_for_gc_with_deleted_retention(c: &mut Criterion) {
    let store = LocalObjectStore::with_config(LocalStoreConfig {
        shard_count: 8,
        block_size: 4096,
        file_root_blocks: 1024,
        metadata_fanout: 4,
        metadata_leaf_blocks: 16,
        storage_node: toy_cow_block_storage::StorageNodeId::from_raw(1),
    })
    .unwrap();
    let server = std::sync::Arc::new(toy_cow_block_storage::LocalBlockServer::new(store.clone()));
    let client = toy_cow_block_storage::LocalBlockClient::new(
        toy_cow_block_storage::InProcessBlockTransport::new(server),
    );
    for _ in 0..32 {
        let device_id = client
            .create_device(toy_cow_block_storage::CreateDeviceRequest {
                spec: DeviceSpec {
                    logical_blocks: 1024,
                    block_size: 4096,
                },
                name: None,
            })
            .unwrap();
        let device = client.open_device(device_id).unwrap();
        device.write_at(0, &[7; 4096]).unwrap();
        store.metadata().checkpoint(device_id).unwrap();
        device.write_at(4096, &[9; 4096]).unwrap();
        device.delete().unwrap();
    }

    c.bench_function("roots_for_gc_deleted_retention", |b| {
        b.iter(|| {
            store
                .metadata()
                .roots_for_gc(black_box(RetentionPolicy::retain_deleted_devices()))
                .unwrap()
        })
    });
}

fn bench_metadata_gc_mark_traversal(c: &mut Criterion) {
    let store = LocalObjectStore::with_config(LocalStoreConfig {
        shard_count: 8,
        block_size: 4096,
        file_root_blocks: 1024,
        metadata_fanout: 4,
        metadata_leaf_blocks: 8,
        storage_node: toy_cow_block_storage::StorageNodeId::from_raw(1),
    })
    .unwrap();
    let server = std::sync::Arc::new(toy_cow_block_storage::LocalBlockServer::new(store.clone()));
    let client = toy_cow_block_storage::LocalBlockClient::new(
        toy_cow_block_storage::InProcessBlockTransport::new(server),
    );
    for device_index in 0..16 {
        let device_id = client
            .create_device(toy_cow_block_storage::CreateDeviceRequest {
                spec: DeviceSpec {
                    logical_blocks: 1024,
                    block_size: 4096,
                },
                name: None,
            })
            .unwrap();
        let device = client.open_device(device_id).unwrap();
        for block in (device_index..128).step_by(16) {
            device.write_at(block * 4096, &[7; 4096]).unwrap();
        }
        store.metadata().checkpoint(device_id).unwrap();
        if device_index % 3 == 0 {
            device.delete().unwrap();
        }
    }

    c.bench_function("metadata_gc_mark_traversal", |b| {
        b.iter(|| {
            store
                .mark_reachable_for_gc(black_box(RetentionPolicy::retain_deleted_devices()))
                .unwrap()
        })
    });
}

fn bench_native_append_validation(c: &mut Criterion) {
    let keyspace_id = KeyspaceId::from_raw(5);
    let file_id = FileId::from_raw(9);
    let request = NativeRequest::Append {
        keyspace_id,
        file_id,
        lease: AppendLease {
            keyspace_id,
            file_id,
            lease_id: AppendLeaseId::from_raw(7),
            writer_epoch: WriterEpoch::from_raw(3),
            base_version: FileVersion::from_raw(2),
        },
        bytes: vec![0; 64 * 4096],
        durability: WriteDurability::Acknowledged,
    };

    c.bench_function("native_append_validation", |b| {
        b.iter(|| black_box(&request).validate_for_existing_file())
    });
}

criterion_group! {
    name = regression;
    config = Criterion::default().noise_threshold(0.05);
    targets = bench_byte_range_validation, bench_block_request_validation, bench_native_append_validation, bench_block_range_helpers, bench_metadata_leaf_validation, bench_in_memory_metadata_node_lookup, bench_in_memory_segment_read, bench_local_empty_device_read, bench_local_read_by_mapping_count, bench_local_single_shard_write, bench_local_single_shard_write_by_tree_depth, bench_local_multi_shard_atomic_write, bench_local_native_append, bench_local_native_stale_lease_rejection, bench_local_fork_vs_device_size, bench_local_checkpoint_restore, bench_local_native_keyspace_checkpoint_restore, bench_roots_for_gc_with_deleted_retention, bench_metadata_gc_mark_traversal, bench_seeded_rng
}
criterion_main!(regression);
