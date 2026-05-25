use criterion::{BatchSize, BenchmarkId, Criterion, criterion_group, criterion_main};
use rusqlite::Connection;
use std::{
    fs,
    hint::black_box,
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
    time::{Duration, Instant},
};
use toy_cow_block_storage::api::BlockRange;
use toy_cow_block_storage::id::{BlockCount, BlockIndex, MetadataNodeId, SegmentId, StorageNodeId};
use toy_cow_block_storage::local::{
    DurableDataLogPolicy, DurableObjectStore, InMemoryMetadataPlane, InMemorySegmentStore,
    LocalObjectStore, LocalStoreConfig, MaintenanceMode, MaintenancePolicy,
};
use toy_cow_block_storage::object::{LeafEntry, MetadataNode, MetadataNodeKind, SegmentDescriptor};
use toy_cow_block_storage::provider::{
    MetadataCreateDeviceRequest, MetadataCreateFileRequest, MetadataCreateKeyspaceRequest,
    MetadataPlane, MetadataSnapshotKeyspaceRequest, RetentionPolicy, SegmentReservation,
    SegmentStore,
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

fn bench_local_multi_node_placement(c: &mut Criterion) {
    let mut group = c.benchmark_group("local_multi_node_placement");
    group.bench_function("block_write_4k_round_robin_3_nodes", |b| {
        b.iter_batched(
            || {
                let config = LocalStoreConfig::default();
                let store = LocalObjectStore::with_storage_nodes(
                    config,
                    vec![
                        config.storage_node,
                        StorageNodeId::from_raw(2),
                        StorageNodeId::from_raw(3),
                    ],
                )
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
                (store, head.device_id, vec![3; 4096])
            },
            |(store, device_id, bytes)| {
                for block in 0..3 {
                    store
                        .write_device(
                            black_box(device_id),
                            black_box(block * 4096),
                            black_box(&bytes),
                            WriteDurability::Acknowledged,
                        )
                        .unwrap();
                }
            },
            BatchSize::SmallInput,
        )
    });
    group.bench_function("read_12k_fanout_3_nodes", |b| {
        b.iter_batched(
            || {
                let config = LocalStoreConfig::default();
                let store = LocalObjectStore::with_storage_nodes(
                    config,
                    vec![
                        config.storage_node,
                        StorageNodeId::from_raw(2),
                        StorageNodeId::from_raw(3),
                    ],
                )
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
                for block in 0..3 {
                    store
                        .write_device(
                            head.device_id,
                            block * 4096,
                            &vec![(block + 1) as u8; 4096],
                            WriteDurability::Acknowledged,
                        )
                        .unwrap();
                }
                (store, head.device_id, vec![0; 3 * 4096])
            },
            |(store, device_id, mut buf)| {
                store
                    .read_device(
                        black_box(device_id),
                        black_box(ByteRange::new(0, 3 * 4096)),
                        black_box(&mut buf),
                    )
                    .unwrap();
                black_box(buf[0])
            },
            BatchSize::SmallInput,
        )
    });
    group.finish();
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

fn bench_local_native_large_append(c: &mut Criterion) {
    let mut group = c.benchmark_group("local_native_large_append");
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(1));
    const LARGE_BYTES: usize = 32 * 1024 * 1024;
    const CHUNK_BYTES: usize = 4096;
    const LARGE_BLOCKS: u64 = (LARGE_BYTES / CHUNK_BYTES) as u64;
    const REGRESSION_SMALL_APPENDS: u64 = 1024;

    let large_config = LocalStoreConfig {
        shard_count: 1,
        block_size: 4096,
        file_root_blocks: LARGE_BLOCKS,
        metadata_fanout: 4,
        metadata_leaf_blocks: LARGE_BLOCKS,
        storage_node: toy_cow_block_storage::StorageNodeId::from_raw(1),
    };

    group.bench_function("normal_single_32mib_append", |b| {
        b.iter_batched(
            || {
                let store = LocalObjectStore::with_config(large_config).unwrap();
                let keyspace = store
                    .metadata()
                    .create_keyspace(MetadataCreateKeyspaceRequest {
                        request: toy_cow_block_storage::CreateKeyspaceRequest { name: None },
                    })
                    .unwrap();
                let file = store
                    .metadata()
                    .create_file(MetadataCreateFileRequest {
                        keyspace_id: keyspace.keyspace_id,
                        request: toy_cow_block_storage::CreateFileRequest {
                            spec: toy_cow_block_storage::FileSpec { name: None },
                        },
                    })
                    .unwrap();
                let lease = store
                    .acquire_append_lease(keyspace.keyspace_id, file.file_id)
                    .unwrap();
                (store, lease, vec![7; LARGE_BYTES])
            },
            |(store, lease, payload)| {
                store
                    .append_file(lease, black_box(&payload), WriteDurability::Acknowledged)
                    .unwrap()
            },
            BatchSize::SmallInput,
        )
    });

    group.bench_function("normal_1024x4k_appends", |b| {
        b.iter_batched(
            || {
                let store = LocalObjectStore::with_config(large_config).unwrap();
                let keyspace = store
                    .metadata()
                    .create_keyspace(MetadataCreateKeyspaceRequest {
                        request: toy_cow_block_storage::CreateKeyspaceRequest { name: None },
                    })
                    .unwrap();
                let file = store
                    .metadata()
                    .create_file(MetadataCreateFileRequest {
                        keyspace_id: keyspace.keyspace_id,
                        request: toy_cow_block_storage::CreateFileRequest {
                            spec: toy_cow_block_storage::FileSpec { name: None },
                        },
                    })
                    .unwrap();
                (
                    store,
                    keyspace.keyspace_id,
                    file.file_id,
                    vec![5; CHUNK_BYTES],
                )
            },
            |(store, keyspace_id, file_id, chunk)| {
                for _ in 0..REGRESSION_SMALL_APPENDS {
                    let lease = store.acquire_append_lease(keyspace_id, file_id).unwrap();
                    store
                        .append_file(lease, black_box(&chunk), WriteDurability::Acknowledged)
                        .unwrap();
                }
            },
            BatchSize::SmallInput,
        )
    });

    group.finish();
}

fn bench_local_native_write_at(c: &mut Criterion) {
    c.bench_function("local_native_write_at", |b| {
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
                (store, keyspace.keyspace_id, head.file_id, vec![4; 4096])
            },
            |(store, keyspace_id, file_id, bytes)| {
                store
                    .write_file_at(
                        black_box(keyspace_id),
                        black_box(file_id),
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

fn bench_native_write_validation(c: &mut Criterion) {
    let request = NativeRequest::Write {
        keyspace_id: KeyspaceId::from_raw(5),
        file_id: FileId::from_raw(9),
        offset: 128,
        bytes: vec![0; 64 * 4096],
        durability: WriteDurability::Acknowledged,
    };

    c.bench_function("native_write_validation", |b| {
        b.iter(|| black_box(&request).validate_for_existing_file())
    });
}

const NATIVE_KEYSPACE_SCALING_SIZES: [usize; 3] = [1, 1_000, 100_000];

fn native_scaling_config() -> LocalStoreConfig {
    LocalStoreConfig {
        shard_count: 1,
        block_size: 4096,
        file_root_blocks: 1_000_000,
        metadata_fanout: 2,
        metadata_leaf_blocks: 1_000_000,
        storage_node: toy_cow_block_storage::StorageNodeId::from_raw(1),
    }
}

fn create_native_keyspace(store: &LocalObjectStore) -> KeyspaceId {
    store
        .metadata()
        .create_keyspace(MetadataCreateKeyspaceRequest {
            request: toy_cow_block_storage::CreateKeyspaceRequest { name: None },
        })
        .unwrap()
        .keyspace_id
}

fn create_native_file(store: &LocalObjectStore, keyspace_id: KeyspaceId) -> FileId {
    store
        .metadata()
        .create_file(MetadataCreateFileRequest {
            keyspace_id,
            request: toy_cow_block_storage::CreateFileRequest {
                spec: toy_cow_block_storage::FileSpec { name: None },
            },
        })
        .unwrap()
        .file_id
}

fn seed_native_keyspace(file_count: usize) -> (LocalObjectStore, KeyspaceId, Vec<FileId>) {
    let store = LocalObjectStore::with_config(native_scaling_config()).unwrap();
    let keyspace_id = create_native_keyspace(&store);
    let mut files = Vec::with_capacity(file_count);
    for _ in 0..file_count {
        files.push(create_native_file(&store, keyspace_id));
    }
    (store, keyspace_id, files)
}

fn bench_native_keyspace_scaling(c: &mut Criterion) {
    let mut group = c.benchmark_group("native_keyspace_scaling");
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(2));

    for file_count in NATIVE_KEYSPACE_SCALING_SIZES {
        let (store, keyspace_id, files) = seed_native_keyspace(file_count);
        let target = files[file_count / 2];
        let alternate = files[(file_count / 2 + 1) % file_count];
        let payload = vec![3; 4096];
        store
            .write_file_at(
                keyspace_id,
                target,
                0,
                &payload,
                WriteDurability::Acknowledged,
            )
            .unwrap();

        group.bench_with_input(
            BenchmarkId::new("file_info", file_count),
            &file_count,
            |b, _| {
                b.iter(|| {
                    store
                        .metadata()
                        .get_file_info(black_box(keyspace_id), black_box(target))
                        .unwrap()
                })
            },
        );

        let mut read_buf = vec![0; 4096];
        group.bench_with_input(
            BenchmarkId::new("read_4k", file_count),
            &file_count,
            |b, _| {
                b.iter(|| {
                    store
                        .read_file(
                            black_box(keyspace_id),
                            black_box(target),
                            black_box(ByteRange::new(0, 4096)),
                            black_box(&mut read_buf),
                        )
                        .unwrap();
                    black_box(read_buf[0])
                })
            },
        );

        group.bench_with_input(
            BenchmarkId::new("write_at_4k", file_count),
            &file_count,
            |b, _| {
                b.iter(|| {
                    store
                        .write_file_at(
                            black_box(keyspace_id),
                            black_box(target),
                            black_box(0),
                            black_box(&payload),
                            WriteDurability::Acknowledged,
                        )
                        .unwrap()
                })
            },
        );

        group.bench_with_input(
            BenchmarkId::new("append_1b_with_lease", file_count),
            &file_count,
            |b, _| {
                b.iter(|| {
                    let lease = store
                        .acquire_append_lease(black_box(keyspace_id), black_box(alternate))
                        .unwrap();
                    store
                        .append_file(lease, black_box(&[9]), WriteDurability::Acknowledged)
                        .unwrap()
                })
            },
        );

        let stale = store.acquire_append_lease(keyspace_id, target).unwrap();
        let _fresh = store.acquire_append_lease(keyspace_id, target).unwrap();
        group.bench_with_input(
            BenchmarkId::new("stale_lease_rejection", file_count),
            &file_count,
            |b, _| {
                b.iter(|| {
                    black_box(
                        store
                            .append_file(
                                black_box(stale.clone()),
                                black_box(&[1]),
                                WriteDurability::Acknowledged,
                            )
                            .is_err(),
                    )
                })
            },
        );

        group.bench_with_input(
            BenchmarkId::new("create_file", file_count),
            &file_count,
            |b, _| b.iter(|| create_native_file(&store, black_box(keyspace_id))),
        );
    }

    group.finish();
}

fn bench_native_alignment_paths(c: &mut Criterion) {
    let mut group = c.benchmark_group("native_alignment");
    group.sample_size(20);
    let (store, keyspace_id, files) = seed_native_keyspace(1);
    let file_id = files[0];
    let aligned = vec![1; 4096];
    let unaligned = vec![2; 17];
    store
        .write_file_at(
            keyspace_id,
            file_id,
            0,
            &aligned,
            WriteDurability::Acknowledged,
        )
        .unwrap();

    group.bench_function("write_aligned_4k", |b| {
        b.iter(|| {
            store
                .write_file_at(
                    black_box(keyspace_id),
                    black_box(file_id),
                    black_box(0),
                    black_box(&aligned),
                    WriteDurability::Acknowledged,
                )
                .unwrap()
        })
    });

    group.bench_function("write_unaligned_partial_block", |b| {
        b.iter(|| {
            store
                .write_file_at(
                    black_box(keyspace_id),
                    black_box(file_id),
                    black_box(1),
                    black_box(&unaligned),
                    WriteDurability::Acknowledged,
                )
                .unwrap()
        })
    });

    group.bench_function("append_aligned_4k", |b| {
        b.iter_batched(
            || {
                let (store, keyspace_id, files) = seed_native_keyspace(1);
                let lease = store.acquire_append_lease(keyspace_id, files[0]).unwrap();
                (store, lease, aligned.clone())
            },
            |(store, lease, payload)| {
                store
                    .append_file(lease, black_box(&payload), WriteDurability::Acknowledged)
                    .unwrap()
            },
            BatchSize::SmallInput,
        )
    });

    group.bench_function("append_unaligned_17b", |b| {
        b.iter_batched(
            || {
                let (store, keyspace_id, files) = seed_native_keyspace(1);
                let lease = store.acquire_append_lease(keyspace_id, files[0]).unwrap();
                (store, lease, unaligned.clone())
            },
            |(store, lease, payload)| {
                store
                    .append_file(lease, black_box(&payload), WriteDurability::Acknowledged)
                    .unwrap()
            },
            BatchSize::SmallInput,
        )
    });

    let mut aligned_read = vec![0; 4096];
    group.bench_function("read_aligned_4k", |b| {
        b.iter(|| {
            store
                .read_file(
                    black_box(keyspace_id),
                    black_box(file_id),
                    black_box(ByteRange::new(0, 4096)),
                    black_box(&mut aligned_read),
                )
                .unwrap();
            black_box(aligned_read[0])
        })
    });

    let mut unaligned_read = vec![0; 17];
    group.bench_function("read_unaligned_17b", |b| {
        b.iter(|| {
            store
                .read_file(
                    black_box(keyspace_id),
                    black_box(file_id),
                    black_box(ByteRange::new(1, 17)),
                    black_box(&mut unaligned_read),
                )
                .unwrap();
            black_box(unaligned_read[0])
        })
    });

    group.finish();
}

fn bench_native_snapshot_restore_root_copy(c: &mut Criterion) {
    let mut group = c.benchmark_group("native_snapshot_restore_root_copy");
    group.sample_size(10);
    group.warm_up_time(Duration::from_millis(10));
    group.measurement_time(Duration::from_millis(10));
    for file_count in NATIVE_KEYSPACE_SCALING_SIZES {
        let (checkpoint_store, checkpoint_keyspace, _checkpoint_files) =
            seed_native_keyspace(file_count);
        let (snapshot_store, snapshot_keyspace, _snapshot_files) = seed_native_keyspace(file_count);
        let (restore_store, restore_keyspace, _restore_files) = seed_native_keyspace(file_count);
        let restore_checkpoint = restore_store
            .metadata()
            .checkpoint_keyspace(restore_keyspace)
            .unwrap();
        group.bench_with_input(
            BenchmarkId::new("checkpoint", file_count),
            &file_count,
            |b, _| {
                b.iter(|| {
                    checkpoint_store
                        .metadata()
                        .checkpoint_keyspace(black_box(checkpoint_keyspace))
                        .unwrap()
                })
            },
        );
        group.bench_with_input(
            BenchmarkId::new("snapshot", file_count),
            &file_count,
            |b, _| {
                b.iter(|| {
                    snapshot_store
                        .metadata()
                        .snapshot_keyspace(MetadataSnapshotKeyspaceRequest {
                            source: black_box(snapshot_keyspace),
                            target: None,
                            name: None,
                        })
                        .unwrap()
                })
            },
        );
        group.bench_with_input(
            BenchmarkId::new("restore_checkpoint", file_count),
            &file_count,
            |b, _| {
                b.iter(|| {
                    restore_store
                        .metadata()
                        .restore_keyspace(
                            black_box(restore_keyspace),
                            black_box(RestorePoint::Checkpoint(restore_checkpoint)),
                        )
                        .unwrap()
                })
            },
        );
    }
    group.finish();
}

fn bench_native_concurrent_batches(c: &mut Criterion) {
    let mut group = c.benchmark_group("native_concurrent_batches");
    group.sample_size(10);
    let thread_count = 4;
    let (store, keyspace_id, files) = seed_native_keyspace(thread_count);
    let conflict_file = files[0];
    let payload = vec![4; 4096];

    group.bench_function("independent_writes_4_threads", |b| {
        b.iter(|| {
            std::thread::scope(|scope| {
                for file_id in files.iter().copied() {
                    let store = store.clone();
                    let payload = &payload;
                    scope.spawn(move || {
                        store
                            .write_file_at(
                                keyspace_id,
                                file_id,
                                0,
                                payload,
                                WriteDurability::Acknowledged,
                            )
                            .unwrap();
                    });
                }
            })
        })
    });

    group.bench_function("conflicting_writes_4_threads", |b| {
        b.iter(|| {
            let successes = std::thread::scope(|scope| {
                let mut handles = Vec::new();
                for _ in 0..thread_count {
                    let store = store.clone();
                    let payload = &payload;
                    handles.push(scope.spawn(move || {
                        store
                            .write_file_at(
                                keyspace_id,
                                conflict_file,
                                0,
                                payload,
                                WriteDurability::Acknowledged,
                            )
                            .is_ok()
                    }));
                }
                handles
                    .into_iter()
                    .map(|handle| handle.join().unwrap())
                    .filter(|success| *success)
                    .count()
            });
            black_box(successes)
        })
    });

    group.bench_function("independent_appends_4_threads", |b| {
        b.iter(|| {
            std::thread::scope(|scope| {
                for file_id in files.iter().copied() {
                    let store = store.clone();
                    scope.spawn(move || {
                        let lease = store.acquire_append_lease(keyspace_id, file_id).unwrap();
                        store
                            .append_file(lease, &[8], WriteDurability::Acknowledged)
                            .unwrap();
                    });
                }
            })
        })
    });

    group.bench_function("conflicting_appends_4_threads", |b| {
        b.iter(|| {
            let leases: Vec<_> = (0..thread_count)
                .map(|_| {
                    store
                        .acquire_append_lease(keyspace_id, conflict_file)
                        .unwrap()
                })
                .collect();
            let successes = std::thread::scope(|scope| {
                let mut handles = Vec::new();
                for lease in leases {
                    let store = store.clone();
                    handles.push(scope.spawn(move || {
                        store
                            .append_file(lease, &[6], WriteDurability::Acknowledged)
                            .is_ok()
                    }));
                }
                handles
                    .into_iter()
                    .map(|handle| handle.join().unwrap())
                    .filter(|success| *success)
                    .count()
            });
            black_box(successes)
        })
    });

    group.finish();
}

static NEXT_DURABLE_BENCH_ROOT: AtomicU64 = AtomicU64::new(1);
const LARGE_DURABLE_HISTORY_WRITES: u64 = 256;

fn durable_bench_config() -> LocalStoreConfig {
    LocalStoreConfig {
        shard_count: 4,
        block_size: 4096,
        file_root_blocks: 1024,
        metadata_fanout: 4,
        metadata_leaf_blocks: 16,
        storage_node: toy_cow_block_storage::StorageNodeId::from_raw(1),
    }
}

fn durable_bench_root(name: &str) -> PathBuf {
    let id = NEXT_DURABLE_BENCH_ROOT.fetch_add(1, Ordering::Relaxed);
    std::env::temp_dir()
        .join("toy-cow-block-storage-benches")
        .join(format!("{name}-{}-{id}", std::process::id()))
}

fn cleanup_durable_bench_root(path: &Path) {
    let _ = fs::remove_dir_all(path);
}

fn elapsed_durable_iters(iters: u64, mut run_one: impl FnMut() -> Duration) -> Duration {
    let mut elapsed = Duration::ZERO;
    for _ in 0..iters {
        elapsed += run_one();
    }
    elapsed
}

fn create_durable_block_device(store: &DurableObjectStore, logical_blocks: u64) -> DeviceId {
    store
        .create_device(toy_cow_block_storage::CreateDeviceRequest {
            spec: DeviceSpec {
                logical_blocks,
                block_size: 4096,
            },
            name: None,
        })
        .unwrap()
}

fn create_durable_native_file(store: &DurableObjectStore) -> (KeyspaceId, FileId) {
    let keyspace_id = store
        .create_keyspace(toy_cow_block_storage::CreateKeyspaceRequest { name: None })
        .unwrap();
    let file_id = store
        .create_file(
            keyspace_id,
            toy_cow_block_storage::CreateFileRequest {
                spec: toy_cow_block_storage::FileSpec { name: None },
            },
        )
        .unwrap();
    (keyspace_id, file_id)
}

fn seed_durable_block_history(store: &DurableObjectStore, device_id: DeviceId, writes: u64) {
    seed_durable_block_history_with_durability(
        store,
        device_id,
        writes,
        WriteDurability::Acknowledged,
    );
}

fn seed_durable_block_history_with_durability(
    store: &DurableObjectStore,
    device_id: DeviceId,
    writes: u64,
    durability: WriteDurability,
) {
    for block in 0..writes {
        store
            .write_device(device_id, block * 4096, &[block as u8; 4096], durability)
            .unwrap();
    }
}

fn seed_durable_native_history(
    store: &DurableObjectStore,
    keyspace_id: KeyspaceId,
    file_id: FileId,
    writes: u64,
) {
    seed_durable_native_history_with_durability(
        store,
        keyspace_id,
        file_id,
        writes,
        WriteDurability::Acknowledged,
    );
}

fn seed_durable_native_history_with_durability(
    store: &DurableObjectStore,
    keyspace_id: KeyspaceId,
    file_id: FileId,
    writes: u64,
    durability: WriteDurability,
) {
    for block in 0..writes {
        store
            .write_file_at(
                keyspace_id,
                file_id,
                block * 4096,
                &[block as u8; 4096],
                durability,
            )
            .unwrap();
    }
}

fn maintenance_bench_policy(mode: MaintenanceMode) -> MaintenancePolicy {
    MaintenancePolicy {
        mode,
        data_log_policy: DurableDataLogPolicy {
            target_data_log_bytes: 4096,
            min_reclaimable_ratio_ppm: 1,
            min_reclaimable_bytes: 1,
            max_compaction_copy_bytes: u64::MAX,
        },
        write_backpressure_enabled: true,
        dirty_low_watermark_bytes: 1,
        dirty_high_watermark_bytes: u64::MAX,
        max_sealed_logs: 1024,
        max_reclaimable_debt_bytes: u64::MAX,
        compaction_copy_budget_per_tick: u64::MAX,
        max_sqlite_wal_bytes: u64::MAX,
        max_logs_scanned_per_tick: 1024,
        max_concurrent_compaction_jobs: 1,
    }
}

fn seed_durable_compaction_debt(store: &DurableObjectStore, device_id: DeviceId, writes: u64) {
    for block in 0..writes {
        store
            .write_device(
                device_id,
                block * 4096,
                &[block as u8; 4096],
                WriteDurability::Flushed,
            )
            .unwrap();
    }
    store
        .write_device(device_id, 0, &[255; 4096], WriteDurability::Flushed)
        .unwrap();
    store
        .run_metadata_custodian(RetentionPolicy::expire_deleted_immediately())
        .unwrap();
    store
        .run_storage_node_custodian(&std::collections::BTreeSet::new())
        .unwrap();
}

fn durable_sqlite_operational_row_count(root: &Path) -> i64 {
    let conn = Connection::open(root.join("metadata.sqlite")).unwrap();
    let mut rows = 0;
    for table in [
        "store_meta",
        "maintenance_state",
        "data_logs",
        "segment_placements",
        "storage_nodes",
        "device_specs",
        "device_heads",
        "deleted_device_heads",
        "keyspace_heads",
        "keyspace_roots",
        "keyspace_catalog_shards",
        "file_writer_epochs",
        "metadata_nodes",
        "commit_groups",
        "shard_commits",
        "keyspace_commits",
        "file_commits",
        "fork_records",
        "delete_records",
        "checkpoints",
        "metadata_gc_marks",
        "segment_gc_marks",
        "segment_records",
        "segment_catalog_entries",
    ] {
        rows += conn
            .query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |row| {
                row.get::<_, i64>(0)
            })
            .unwrap();
    }
    rows
}

fn durable_sqlite_wal_bytes(root: &Path) -> u64 {
    fs::metadata(root.join("metadata.sqlite-wal"))
        .map(|metadata| metadata.len())
        .unwrap_or(0)
}

fn bench_durable_provider(c: &mut Criterion) {
    let mut group = c.benchmark_group("durable_provider");
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(2));

    group.bench_function("block_write_4k_acknowledged_fresh", |b| {
        b.iter_custom(|iters| {
            elapsed_durable_iters(iters, || {
                let root = durable_bench_root("block-write-ack-fresh");
                let store = DurableObjectStore::open(&root, durable_bench_config()).unwrap();
                let device_id = create_durable_block_device(&store, 1024);
                let payload = vec![3; 4096];
                let started = Instant::now();
                store
                    .write_device(
                        black_box(device_id),
                        black_box(0),
                        black_box(&payload),
                        WriteDurability::Acknowledged,
                    )
                    .unwrap();
                let elapsed = started.elapsed();
                cleanup_durable_bench_root(&root);
                black_box(elapsed)
            })
        })
    });

    group.bench_function("block_write_4k_flushed_fresh", |b| {
        b.iter_custom(|iters| {
            elapsed_durable_iters(iters, || {
                let root = durable_bench_root("block-write-flushed-fresh");
                let store = DurableObjectStore::open(&root, durable_bench_config()).unwrap();
                let device_id = create_durable_block_device(&store, 1024);
                let payload = vec![3; 4096];
                let started = Instant::now();
                store
                    .write_device(
                        black_box(device_id),
                        black_box(0),
                        black_box(&payload),
                        WriteDurability::Flushed,
                    )
                    .unwrap();
                let elapsed = started.elapsed();
                cleanup_durable_bench_root(&root);
                black_box(elapsed)
            })
        })
    });

    group.bench_function("block_flush_after_32_acknowledged_writes", |b| {
        b.iter_custom(|iters| {
            elapsed_durable_iters(iters, || {
                let root = durable_bench_root("block-flush-after-32");
                let store = DurableObjectStore::open(&root, durable_bench_config()).unwrap();
                let device_id = create_durable_block_device(&store, 1024);
                seed_durable_block_history(&store, device_id, 32);
                let started = Instant::now();
                store.flush_device(black_box(device_id)).unwrap();
                let elapsed = started.elapsed();
                cleanup_durable_bench_root(&root);
                black_box(elapsed)
            })
        })
    });

    group.bench_function("block_overwrite_4k_flushed_after_32_flushed_writes", |b| {
        b.iter_custom(|iters| {
            elapsed_durable_iters(iters, || {
                let root = durable_bench_root("block-overwrite-flushed-after-32");
                let store = DurableObjectStore::open(&root, durable_bench_config()).unwrap();
                let device_id = create_durable_block_device(&store, 1024);
                seed_durable_block_history_with_durability(
                    &store,
                    device_id,
                    32,
                    WriteDurability::Flushed,
                );
                let payload = vec![9; 4096];
                let started = Instant::now();
                store
                    .write_device(
                        black_box(device_id),
                        black_box(0),
                        black_box(&payload),
                        WriteDurability::Flushed,
                    )
                    .unwrap();
                let elapsed = started.elapsed();
                cleanup_durable_bench_root(&root);
                black_box(elapsed)
            })
        })
    });

    group.bench_function("block_read_4k_after_reopen", |b| {
        b.iter_custom(|iters| {
            elapsed_durable_iters(iters, || {
                let root = durable_bench_root("block-read-reopen");
                let store = DurableObjectStore::open(&root, durable_bench_config()).unwrap();
                let device_id = create_durable_block_device(&store, 1024);
                store
                    .write_device(device_id, 0, &[7; 4096], WriteDurability::Flushed)
                    .unwrap();
                drop(store);
                let reopened = DurableObjectStore::open(&root, durable_bench_config()).unwrap();
                let mut buf = vec![0; 4096];
                let started = Instant::now();
                reopened
                    .read_device(
                        black_box(device_id),
                        black_box(ByteRange::new(0, 4096)),
                        black_box(&mut buf),
                    )
                    .unwrap();
                let elapsed = started.elapsed();
                cleanup_durable_bench_root(&root);
                black_box(buf[0]);
                elapsed
            })
        })
    });

    group.bench_function("open_after_32_block_writes", |b| {
        b.iter_custom(|iters| {
            elapsed_durable_iters(iters, || {
                let root = durable_bench_root("open-after-32-block-writes");
                let store = DurableObjectStore::open(&root, durable_bench_config()).unwrap();
                let device_id = create_durable_block_device(&store, 1024);
                seed_durable_block_history_with_durability(
                    &store,
                    device_id,
                    32,
                    WriteDurability::Flushed,
                );
                drop(store);
                let started = Instant::now();
                let reopened = DurableObjectStore::open(&root, durable_bench_config()).unwrap();
                let elapsed = started.elapsed();
                black_box(reopened.device_info(device_id).unwrap());
                drop(reopened);
                cleanup_durable_bench_root(&root);
                black_box(elapsed)
            })
        })
    });

    group.bench_function("block_flush_after_256_acknowledged_writes", |b| {
        b.iter_custom(|iters| {
            elapsed_durable_iters(iters, || {
                let root = durable_bench_root("block-flush-after-256");
                let store = DurableObjectStore::open(&root, durable_bench_config()).unwrap();
                let device_id = create_durable_block_device(&store, 1024);
                seed_durable_block_history(&store, device_id, LARGE_DURABLE_HISTORY_WRITES);
                let started = Instant::now();
                store.flush_device(black_box(device_id)).unwrap();
                let elapsed = started.elapsed();
                cleanup_durable_bench_root(&root);
                black_box(elapsed)
            })
        })
    });

    group.bench_function("open_after_256_block_writes", |b| {
        b.iter_custom(|iters| {
            elapsed_durable_iters(iters, || {
                let root = durable_bench_root("open-after-256-block-writes");
                let store = DurableObjectStore::open(&root, durable_bench_config()).unwrap();
                let device_id = create_durable_block_device(&store, 1024);
                seed_durable_block_history_with_durability(
                    &store,
                    device_id,
                    LARGE_DURABLE_HISTORY_WRITES,
                    WriteDurability::Flushed,
                );
                drop(store);
                let started = Instant::now();
                let reopened = DurableObjectStore::open(&root, durable_bench_config()).unwrap();
                let elapsed = started.elapsed();
                black_box(reopened.device_info(device_id).unwrap());
                drop(reopened);
                cleanup_durable_bench_root(&root);
                black_box(elapsed)
            })
        })
    });

    group.bench_function("checkpoint_after_256_block_writes", |b| {
        b.iter_custom(|iters| {
            elapsed_durable_iters(iters, || {
                let root = durable_bench_root("checkpoint-after-256-block-writes");
                let store = DurableObjectStore::open(&root, durable_bench_config()).unwrap();
                let device_id = create_durable_block_device(&store, 1024);
                seed_durable_block_history_with_durability(
                    &store,
                    device_id,
                    LARGE_DURABLE_HISTORY_WRITES,
                    WriteDurability::Flushed,
                );
                let started = Instant::now();
                let checkpoint = store.checkpoint(black_box(device_id)).unwrap();
                let elapsed = started.elapsed();
                cleanup_durable_bench_root(&root);
                black_box(checkpoint);
                black_box(elapsed)
            })
        })
    });

    group.bench_function("fork_after_256_block_writes", |b| {
        b.iter_custom(|iters| {
            elapsed_durable_iters(iters, || {
                let root = durable_bench_root("fork-after-256-block-writes");
                let store = DurableObjectStore::open(&root, durable_bench_config()).unwrap();
                let device_id = create_durable_block_device(&store, 1024);
                seed_durable_block_history_with_durability(
                    &store,
                    device_id,
                    LARGE_DURABLE_HISTORY_WRITES,
                    WriteDurability::Flushed,
                );
                let started = Instant::now();
                let forked = store
                    .fork_device(
                        black_box(device_id),
                        ForkRequest {
                            target: None,
                            name: None,
                        },
                    )
                    .unwrap();
                let elapsed = started.elapsed();
                cleanup_durable_bench_root(&root);
                black_box(forked);
                black_box(elapsed)
            })
        })
    });

    group.bench_function("restore_checkpoint_after_256_block_writes", |b| {
        b.iter_custom(|iters| {
            elapsed_durable_iters(iters, || {
                let root = durable_bench_root("restore-after-256-block-writes");
                let store = DurableObjectStore::open(&root, durable_bench_config()).unwrap();
                let device_id = create_durable_block_device(&store, 1024);
                seed_durable_block_history_with_durability(
                    &store,
                    device_id,
                    LARGE_DURABLE_HISTORY_WRITES,
                    WriteDurability::Flushed,
                );
                let checkpoint = store.checkpoint(device_id).unwrap();
                store
                    .write_device(device_id, 0, &[9; 4096], WriteDurability::Flushed)
                    .unwrap();
                let started = Instant::now();
                let restored = store
                    .restore_device(
                        black_box(device_id),
                        black_box(RestorePoint::Checkpoint(checkpoint)),
                    )
                    .unwrap();
                let elapsed = started.elapsed();
                cleanup_durable_bench_root(&root);
                black_box(restored);
                black_box(elapsed)
            })
        })
    });

    group.bench_function("custodian_publish_after_256_deleted_block_writes", |b| {
        b.iter_custom(|iters| {
            elapsed_durable_iters(iters, || {
                let root = durable_bench_root("custodian-after-256-deleted-block-writes");
                let store = DurableObjectStore::open(&root, durable_bench_config()).unwrap();
                let device_id = create_durable_block_device(&store, 1024);
                seed_durable_block_history_with_durability(
                    &store,
                    device_id,
                    LARGE_DURABLE_HISTORY_WRITES,
                    WriteDurability::Flushed,
                );
                store.delete_device(device_id).unwrap();
                let started = Instant::now();
                let metadata = store
                    .run_metadata_custodian(RetentionPolicy::expire_deleted_immediately())
                    .unwrap();
                let storage = store
                    .run_storage_node_custodian(&std::collections::BTreeSet::new())
                    .unwrap();
                let elapsed = started.elapsed();
                cleanup_durable_bench_root(&root);
                black_box(metadata);
                black_box(storage);
                black_box(elapsed)
            })
        })
    });

    group.bench_function("sqlite_rows_and_wal_after_256_block_writes", |b| {
        b.iter_custom(|iters| {
            elapsed_durable_iters(iters, || {
                let root = durable_bench_root("sqlite-rows-wal-after-256-block-writes");
                let store = DurableObjectStore::open(&root, durable_bench_config()).unwrap();
                let device_id = create_durable_block_device(&store, 1024);
                seed_durable_block_history_with_durability(
                    &store,
                    device_id,
                    LARGE_DURABLE_HISTORY_WRITES,
                    WriteDurability::Flushed,
                );
                drop(store);
                let started = Instant::now();
                let rows = durable_sqlite_operational_row_count(&root);
                let wal_bytes = durable_sqlite_wal_bytes(&root);
                let elapsed = started.elapsed();
                cleanup_durable_bench_root(&root);
                black_box((rows, wal_bytes));
                black_box(elapsed)
            })
        })
    });

    group.bench_function("compact_after_32_block_writes", |b| {
        b.iter_custom(|iters| {
            elapsed_durable_iters(iters, || {
                let root = durable_bench_root("compact-after-32-block-writes");
                let store = DurableObjectStore::open(&root, durable_bench_config()).unwrap();
                let device_id = create_durable_block_device(&store, 1024);
                seed_durable_block_history_with_durability(
                    &store,
                    device_id,
                    32,
                    WriteDurability::Flushed,
                );
                let started = Instant::now();
                store
                    .compact_data_logs(DurableDataLogPolicy {
                        target_data_log_bytes: 64 * 1024 * 1024,
                        min_reclaimable_ratio_ppm: 0,
                        min_reclaimable_bytes: 0,
                        max_compaction_copy_bytes: u64::MAX,
                    })
                    .unwrap();
                let elapsed = started.elapsed();
                cleanup_durable_bench_root(&root);
                black_box(elapsed)
            })
        })
    });

    group.bench_function("maintenance_observe_idle", |b| {
        b.iter_custom(|iters| {
            elapsed_durable_iters(iters, || {
                let root = durable_bench_root("maintenance-observe-idle");
                let store = DurableObjectStore::open_with_maintenance_policy(
                    &root,
                    durable_bench_config(),
                    maintenance_bench_policy(MaintenanceMode::Manual),
                )
                .unwrap();
                let started = Instant::now();
                let observation = store.observe_maintenance().unwrap();
                let elapsed = started.elapsed();
                cleanup_durable_bench_root(&root);
                black_box(observation);
                black_box(elapsed)
            })
        })
    });

    group.bench_function("maintenance_tick_idle", |b| {
        b.iter_custom(|iters| {
            elapsed_durable_iters(iters, || {
                let root = durable_bench_root("maintenance-tick-idle");
                let store = DurableObjectStore::open_with_maintenance_policy(
                    &root,
                    durable_bench_config(),
                    maintenance_bench_policy(MaintenanceMode::Manual),
                )
                .unwrap();
                let started = Instant::now();
                let report = store.run_maintenance_tick().unwrap();
                let elapsed = started.elapsed();
                cleanup_durable_bench_root(&root);
                black_box(report);
                black_box(elapsed)
            })
        })
    });

    group.bench_function("maintenance_enabled_idle_block_write_4k_flushed", |b| {
        b.iter_custom(|iters| {
            elapsed_durable_iters(iters, || {
                let root = durable_bench_root("maintenance-enabled-idle-write");
                let store = DurableObjectStore::open_with_maintenance_policy(
                    &root,
                    durable_bench_config(),
                    maintenance_bench_policy(MaintenanceMode::AlwaysOn),
                )
                .unwrap();
                let device_id = create_durable_block_device(&store, 1024);
                let payload = vec![4; 4096];
                let started = Instant::now();
                let commit = store
                    .write_device(
                        black_box(device_id),
                        black_box(0),
                        black_box(&payload),
                        WriteDurability::Flushed,
                    )
                    .unwrap();
                let elapsed = started.elapsed();
                store.shutdown_maintenance();
                cleanup_durable_bench_root(&root);
                black_box(commit);
                black_box(elapsed)
            })
        })
    });

    group.bench_function("maintenance_tick_active_compaction", |b| {
        b.iter_custom(|iters| {
            elapsed_durable_iters(iters, || {
                let root = durable_bench_root("maintenance-tick-active");
                let store = DurableObjectStore::open_with_maintenance_policy(
                    &root,
                    durable_bench_config(),
                    maintenance_bench_policy(MaintenanceMode::Manual),
                )
                .unwrap();
                let device_id = create_durable_block_device(&store, 1024);
                seed_durable_compaction_debt(&store, device_id, 32);
                let started = Instant::now();
                let report = store.run_maintenance_tick().unwrap();
                let elapsed = started.elapsed();
                cleanup_durable_bench_root(&root);
                black_box(report);
                black_box(elapsed)
            })
        })
    });

    group.bench_function("maintenance_throttled_write", |b| {
        b.iter_custom(|iters| {
            elapsed_durable_iters(iters, || {
                let root = durable_bench_root("maintenance-throttled-write");
                let mut policy = maintenance_bench_policy(MaintenanceMode::Manual);
                policy.dirty_high_watermark_bytes = 1;
                let store = DurableObjectStore::open_with_maintenance_policy(
                    &root,
                    durable_bench_config(),
                    policy,
                )
                .unwrap();
                let device_id = create_durable_block_device(&store, 1024);
                seed_durable_compaction_debt(&store, device_id, 32);
                let started = Instant::now();
                let err = store
                    .write_device(device_id, 4096, &[7; 4096], WriteDurability::Flushed)
                    .unwrap_err();
                let elapsed = started.elapsed();
                cleanup_durable_bench_root(&root);
                black_box(err);
                black_box(elapsed)
            })
        })
    });

    group.bench_function("native_write_at_4k_acknowledged_fresh", |b| {
        b.iter_custom(|iters| {
            elapsed_durable_iters(iters, || {
                let root = durable_bench_root("native-write-ack-fresh");
                let store = DurableObjectStore::open(&root, durable_bench_config()).unwrap();
                let (keyspace_id, file_id) = create_durable_native_file(&store);
                let payload = vec![4; 4096];
                let started = Instant::now();
                store
                    .write_file_at(
                        black_box(keyspace_id),
                        black_box(file_id),
                        black_box(0),
                        black_box(&payload),
                        WriteDurability::Acknowledged,
                    )
                    .unwrap();
                let elapsed = started.elapsed();
                cleanup_durable_bench_root(&root);
                black_box(elapsed)
            })
        })
    });

    group.bench_function("native_write_at_4k_flushed_fresh", |b| {
        b.iter_custom(|iters| {
            elapsed_durable_iters(iters, || {
                let root = durable_bench_root("native-write-flushed-fresh");
                let store = DurableObjectStore::open(&root, durable_bench_config()).unwrap();
                let (keyspace_id, file_id) = create_durable_native_file(&store);
                let payload = vec![4; 4096];
                let started = Instant::now();
                store
                    .write_file_at(
                        black_box(keyspace_id),
                        black_box(file_id),
                        black_box(0),
                        black_box(&payload),
                        WriteDurability::Flushed,
                    )
                    .unwrap();
                let elapsed = started.elapsed();
                cleanup_durable_bench_root(&root);
                black_box(elapsed)
            })
        })
    });

    group.bench_function("native_flush_after_32_acknowledged_writes", |b| {
        b.iter_custom(|iters| {
            elapsed_durable_iters(iters, || {
                let root = durable_bench_root("native-flush-after-32");
                let store = DurableObjectStore::open(&root, durable_bench_config()).unwrap();
                let (keyspace_id, file_id) = create_durable_native_file(&store);
                seed_durable_native_history(&store, keyspace_id, file_id, 32);
                let started = Instant::now();
                store
                    .flush_file(black_box(keyspace_id), black_box(file_id))
                    .unwrap();
                let elapsed = started.elapsed();
                cleanup_durable_bench_root(&root);
                black_box(elapsed)
            })
        })
    });

    group.bench_function("native_write_at_4k_flushed_after_32_flushed_writes", |b| {
        b.iter_custom(|iters| {
            elapsed_durable_iters(iters, || {
                let root = durable_bench_root("native-write-flushed-after-32");
                let store = DurableObjectStore::open(&root, durable_bench_config()).unwrap();
                let (keyspace_id, file_id) = create_durable_native_file(&store);
                seed_durable_native_history_with_durability(
                    &store,
                    keyspace_id,
                    file_id,
                    32,
                    WriteDurability::Flushed,
                );
                let payload = vec![8; 4096];
                let started = Instant::now();
                store
                    .write_file_at(
                        black_box(keyspace_id),
                        black_box(file_id),
                        black_box(0),
                        black_box(&payload),
                        WriteDurability::Flushed,
                    )
                    .unwrap();
                let elapsed = started.elapsed();
                cleanup_durable_bench_root(&root);
                black_box(elapsed)
            })
        })
    });

    group.bench_function("native_append_4k_acknowledged_fresh", |b| {
        b.iter_custom(|iters| {
            elapsed_durable_iters(iters, || {
                let root = durable_bench_root("native-append-ack-fresh");
                let store = DurableObjectStore::open(&root, durable_bench_config()).unwrap();
                let (keyspace_id, file_id) = create_durable_native_file(&store);
                let lease = store.acquire_append_lease(keyspace_id, file_id).unwrap();
                let payload = vec![5; 4096];
                let started = Instant::now();
                store
                    .append_file(
                        black_box(lease),
                        black_box(&payload),
                        WriteDurability::Acknowledged,
                    )
                    .unwrap();
                let elapsed = started.elapsed();
                cleanup_durable_bench_root(&root);
                black_box(elapsed)
            })
        })
    });

    group.bench_function("native_append_4k_flushed_fresh", |b| {
        b.iter_custom(|iters| {
            elapsed_durable_iters(iters, || {
                let root = durable_bench_root("native-append-flushed-fresh");
                let store = DurableObjectStore::open(&root, durable_bench_config()).unwrap();
                let (keyspace_id, file_id) = create_durable_native_file(&store);
                let lease = store.acquire_append_lease(keyspace_id, file_id).unwrap();
                let payload = vec![5; 4096];
                let started = Instant::now();
                store
                    .append_file(
                        black_box(lease),
                        black_box(&payload),
                        WriteDurability::Flushed,
                    )
                    .unwrap();
                let elapsed = started.elapsed();
                cleanup_durable_bench_root(&root);
                black_box(elapsed)
            })
        })
    });

    group.finish();
}

criterion_group! {
    name = regression;
    config = Criterion::default().noise_threshold(0.05);
    targets = bench_byte_range_validation, bench_block_request_validation, bench_native_append_validation, bench_native_write_validation, bench_block_range_helpers, bench_metadata_leaf_validation, bench_in_memory_metadata_node_lookup, bench_in_memory_segment_read, bench_local_empty_device_read, bench_local_read_by_mapping_count, bench_local_single_shard_write, bench_local_multi_node_placement, bench_local_single_shard_write_by_tree_depth, bench_local_multi_shard_atomic_write, bench_local_native_append, bench_local_native_large_append, bench_local_native_write_at, bench_local_native_stale_lease_rejection, bench_local_fork_vs_device_size, bench_local_checkpoint_restore, bench_local_native_keyspace_checkpoint_restore, bench_roots_for_gc_with_deleted_retention, bench_metadata_gc_mark_traversal, bench_seeded_rng, bench_native_keyspace_scaling, bench_native_alignment_paths, bench_native_snapshot_restore_root_copy, bench_native_concurrent_batches, bench_durable_provider
}
criterion_main!(regression);
