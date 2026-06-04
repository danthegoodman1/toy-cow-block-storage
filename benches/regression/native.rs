const NATIVE_KEYSPACE_SCALING_SIZES: [usize; 3] = [1, 1_000, 100_000];

fn native_scaling_config() -> LocalStoreConfig {
    LocalStoreConfig {
        shard_count: 1,
        block_size: 4096,
        file_root_blocks: 1_000_000,
        metadata_fanout: 2,
        metadata_leaf_blocks: 1_000_000,
        storage_node: toy_cow_block_storage::StorageNodeId::from_raw(1),
        observability_event_capacity: 1024,
    }
}

fn create_native_keyspace(store: &LocalCoordinator) -> KeyspaceId {
    store
        .metadata()
        .create_keyspace(MetadataCreateKeyspaceRequest {
            request: toy_cow_block_storage::CreateKeyspaceRequest { name: None },
        })
        .unwrap()
        .keyspace_id
}

fn create_native_file(store: &LocalCoordinator, keyspace_id: KeyspaceId) -> FileId {
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

fn create_native_file_for_bench(store: &LocalCoordinator) -> (KeyspaceId, FileId) {
    let keyspace_id = create_native_keyspace(store);
    let file_id = create_native_file(store, keyspace_id);
    (keyspace_id, file_id)
}

fn open_local_append_stream(
    store: &LocalCoordinator,
    keyspace_id: KeyspaceId,
    file_id: FileId,
) -> AppendStream {
    store.open_append_stream(keyspace_id, file_id).unwrap()
}

fn append_local_file_once(
    store: &LocalCoordinator,
    keyspace_id: KeyspaceId,
    file_id: FileId,
    bytes: &[u8],
    durability: WriteDurability,
) {
    let stream = open_local_append_stream(store, keyspace_id, file_id);
    let ticket = store.append_stream(&stream, bytes, durability).unwrap();
    store
        .publish_append_stream(&stream, ticket.range.end_exclusive().unwrap(), durability)
        .unwrap();
}

fn seed_native_keyspace(file_count: usize) -> (LocalCoordinator, KeyspaceId, Vec<FileId>) {
    let store = LocalCoordinator::with_config(native_scaling_config()).unwrap();
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
            .commit_file_batch(
                keyspace_id,
                target,
                &[FileBatchWrite::new(0, payload.clone())],
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
            BenchmarkId::new("commit_file_batch_4k", file_count),
            &file_count,
            |b, _| {
                b.iter(|| {
                    store
                        .commit_file_batch(
                            black_box(keyspace_id),
                            black_box(target),
                            black_box(&[FileBatchWrite::new(0, payload.clone())]),
                            WriteDurability::Acknowledged,
                        )
                        .unwrap()
                })
            },
        );

        group.bench_with_input(
            BenchmarkId::new("append_1b_with_fresh_stream", file_count),
            &file_count,
            |b, _| {
                b.iter(|| {
                    append_local_file_once(
                        &store,
                        black_box(keyspace_id),
                        black_box(alternate),
                        black_box(&[9]),
                        WriteDurability::Acknowledged,
                    )
                })
            },
        );

        let stale_stream = store.open_append_stream(keyspace_id, target).unwrap();
        let _fresh = store.open_append_stream(keyspace_id, target).unwrap();
        group.bench_with_input(
            BenchmarkId::new("stale_stream_rejection", file_count),
            &file_count,
            |b, _| {
                b.iter(|| {
                    black_box(
                        store
                            .append_stream(
                                black_box(&stale_stream),
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
        .commit_file_batch(
            keyspace_id,
            file_id,
            &[FileBatchWrite::new(0, aligned.clone())],
            WriteDurability::Acknowledged,
        )
        .unwrap();

    group.bench_function("commit_batch_aligned_4k", |b| {
        b.iter(|| {
            store
                .commit_file_batch(
                    black_box(keyspace_id),
                    black_box(file_id),
                    black_box(&[FileBatchWrite::new(0, aligned.clone())]),
                    WriteDurability::Acknowledged,
                )
                .unwrap()
        })
    });

    group.bench_function("commit_batch_unaligned_partial_block", |b| {
        b.iter(|| {
            store
                .commit_file_batch(
                    black_box(keyspace_id),
                    black_box(file_id),
                    black_box(&[FileBatchWrite::new(1, unaligned.clone())]),
                    WriteDurability::Acknowledged,
                )
                .unwrap()
        })
    });

    group.bench_function("append_aligned_4k", |b| {
        b.iter_batched(
            || {
                let (store, keyspace_id, files) = seed_native_keyspace(1);
                let stream = open_local_append_stream(&store, keyspace_id, files[0]);
                (store, stream, aligned.clone())
            },
            |(store, stream, payload)| {
                store
                    .append_stream(
                        black_box(&stream),
                        black_box(&payload),
                        WriteDurability::Acknowledged,
                    )
                    .unwrap()
            },
            BatchSize::SmallInput,
        )
    });

    group.bench_function("append_unaligned_17b", |b| {
        b.iter_batched(
            || {
                let (store, keyspace_id, files) = seed_native_keyspace(1);
                let stream = open_local_append_stream(&store, keyspace_id, files[0]);
                (store, stream, unaligned.clone())
            },
            |(store, stream, payload)| {
                store
                    .append_stream(
                        black_box(&stream),
                        black_box(&payload),
                        WriteDurability::Acknowledged,
                    )
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
                            .commit_file_batch(
                                keyspace_id,
                                file_id,
                                &[FileBatchWrite::new(0, payload.as_slice().to_vec())],
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
                            .commit_file_batch(
                                keyspace_id,
                                conflict_file,
                                &[FileBatchWrite::new(0, payload.as_slice().to_vec())],
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
                        append_local_file_once(
                            &store,
                            keyspace_id,
                            file_id,
                            &[8],
                            WriteDurability::Acknowledged,
                        );
                    });
                }
            })
        })
    });

    group.bench_function("conflicting_appends_4_threads", |b| {
        b.iter(|| {
            let streams: Vec<_> = (0..thread_count)
                .map(|_| open_local_append_stream(&store, keyspace_id, conflict_file))
                .collect();
            let successes = std::thread::scope(|scope| {
                let mut handles = Vec::new();
                for stream in streams {
                    let store = store.clone();
                    handles.push(scope.spawn(move || {
                        store
                            .append_stream(&stream, &[6], WriteDurability::Acknowledged)
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
