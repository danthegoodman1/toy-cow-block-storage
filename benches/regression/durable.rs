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
        observability_event_capacity: 1024,
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

fn create_durable_block_device(store: &DurableCoordinator, logical_blocks: u64) -> DeviceId {
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

fn create_durable_native_file(store: &DurableCoordinator) -> (KeyspaceId, FileId) {
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

fn seed_durable_block_history(store: &DurableCoordinator, device_id: DeviceId, writes: u64) {
    seed_durable_block_history_with_durability(
        store,
        device_id,
        writes,
        WriteDurability::Acknowledged,
    );
}

fn seed_durable_block_history_with_durability(
    store: &DurableCoordinator,
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
    store: &DurableCoordinator,
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
    store: &DurableCoordinator,
    keyspace_id: KeyspaceId,
    file_id: FileId,
    writes: u64,
    durability: WriteDurability,
) {
    for block in 0..writes {
        store
            .commit_file_batch(
                keyspace_id,
                file_id,
                &[FileBatchWrite::new(block * 4096, vec![block as u8; 4096])],
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

fn seed_durable_compaction_debt(store: &DurableCoordinator, device_id: DeviceId, writes: u64) {
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
    let mut rows = sqlite_count_rows(
        &conn,
        &[
            "store_meta",
            "maintenance_state",
            "device_specs",
            "device_manifests",
            "deleted_device_manifests",
            "device_shard_heads",
            "deleted_device_shard_heads",
            "keyspace_manifests",
            "keyspace_shard_heads",
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
        ],
    );
    for catalog_path in durable_node_catalog_paths(root) {
        let conn = Connection::open(catalog_path).unwrap();
        rows += sqlite_count_rows(
            &conn,
            &[
                "node_meta",
                "data_logs",
                "segment_placements",
                "segment_catalog_entries",
            ],
        );
    }
    rows
}

fn sqlite_count_rows(conn: &Connection, tables: &[&str]) -> i64 {
    let query = tables
        .iter()
        .map(|table| format!("(SELECT COUNT(*) FROM {table})"))
        .collect::<Vec<_>>()
        .join(" + ");
    conn.query_row(&format!("SELECT {query}"), [], |row| row.get::<_, i64>(0))
        .unwrap()
}

fn durable_sqlite_wal_bytes(root: &Path) -> u64 {
    let mut bytes = fs::metadata(root.join("metadata.sqlite-wal"))
        .map(|metadata| metadata.len())
        .unwrap_or(0);
    for catalog_path in durable_node_catalog_paths(root) {
        let Some(file_name) = catalog_path.file_name().and_then(|name| name.to_str()) else {
            continue;
        };
        bytes += fs::metadata(catalog_path.with_file_name(format!("{file_name}-wal")))
            .map(|metadata| metadata.len())
            .unwrap_or(0);
    }
    bytes
}

fn durable_node_catalog_paths(root: &Path) -> Vec<std::path::PathBuf> {
    let data_dir = root.join("data");
    let mut paths = Vec::new();
    let Ok(entries) = fs::read_dir(data_dir) else {
        return paths;
    };
    for entry in entries.flatten() {
        let Ok(file_type) = entry.file_type() else {
            continue;
        };
        if !file_type.is_dir() {
            continue;
        }
        let path = entry.path().join("catalog.sqlite");
        if path.exists() {
            paths.push(path);
        }
    }
    paths.sort();
    paths
}

fn bench_durable_provider(c: &mut Criterion) {
    let mut group = c.benchmark_group("durable_provider");
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(2));

    group.bench_function("block_write_4k_acknowledged_fresh", |b| {
        b.iter_custom(|iters| {
            elapsed_durable_iters(iters, || {
                let root = durable_bench_root("block-write-ack-fresh");
                let store = DurableCoordinator::open(&root, durable_bench_config()).unwrap();
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

    group.bench_function(
        "block_write_4k_acknowledged_observability_not_drained",
        |b| {
            b.iter_custom(|iters| {
                elapsed_durable_iters(iters, || {
                    let root = durable_bench_root("block-write-ack-observability");
                    let store = DurableCoordinator::open(&root, durable_bench_config()).unwrap();
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
        },
    );

    group.bench_function("diagnostics_snapshot_after_32_block_writes", |b| {
        let root = durable_bench_root("diagnostics-snapshot");
        let store = DurableCoordinator::open(&root, durable_bench_config()).unwrap();
        let device_id = create_durable_block_device(&store, 1024);
        seed_durable_block_history_with_durability(&store, device_id, 32, WriteDurability::Flushed);
        b.iter(|| black_box(store.diagnostics_snapshot().unwrap()));
        drop(store);
        cleanup_durable_bench_root(&root);
    });

    group.bench_function("drain_events_after_4k_write", |b| {
        b.iter_batched(
            || {
                let store = LocalCoordinator::with_config(LocalStoreConfig::default()).unwrap();
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
                store
                    .write_device(
                        head.device_id,
                        0,
                        &vec![9; 4096],
                        WriteDurability::Acknowledged,
                    )
                    .unwrap();
                store
            },
            |store| black_box(store.drain_events(usize::MAX).unwrap()),
            BatchSize::SmallInput,
        )
    });

    group.bench_function("block_write_4k_flushed_fresh", |b| {
        b.iter_custom(|iters| {
            elapsed_durable_iters(iters, || {
                let root = durable_bench_root("block-write-flushed-fresh");
                let store = DurableCoordinator::open(&root, durable_bench_config()).unwrap();
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
                let store = DurableCoordinator::open(&root, durable_bench_config()).unwrap();
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
                let store = DurableCoordinator::open(&root, durable_bench_config()).unwrap();
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
                let store = DurableCoordinator::open(&root, durable_bench_config()).unwrap();
                let device_id = create_durable_block_device(&store, 1024);
                store
                    .write_device(device_id, 0, &[7; 4096], WriteDurability::Flushed)
                    .unwrap();
                drop(store);
                let reopened = DurableCoordinator::open(&root, durable_bench_config()).unwrap();
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
                let store = DurableCoordinator::open(&root, durable_bench_config()).unwrap();
                let device_id = create_durable_block_device(&store, 1024);
                seed_durable_block_history_with_durability(
                    &store,
                    device_id,
                    32,
                    WriteDurability::Flushed,
                );
                drop(store);
                let started = Instant::now();
                let reopened = DurableCoordinator::open(&root, durable_bench_config()).unwrap();
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
                let store = DurableCoordinator::open(&root, durable_bench_config()).unwrap();
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
                let store = DurableCoordinator::open(&root, durable_bench_config()).unwrap();
                let device_id = create_durable_block_device(&store, 1024);
                seed_durable_block_history_with_durability(
                    &store,
                    device_id,
                    LARGE_DURABLE_HISTORY_WRITES,
                    WriteDurability::Flushed,
                );
                drop(store);
                let started = Instant::now();
                let reopened = DurableCoordinator::open(&root, durable_bench_config()).unwrap();
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
                let store = DurableCoordinator::open(&root, durable_bench_config()).unwrap();
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
                let store = DurableCoordinator::open(&root, durable_bench_config()).unwrap();
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
                let store = DurableCoordinator::open(&root, durable_bench_config()).unwrap();
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
                let store = DurableCoordinator::open(&root, durable_bench_config()).unwrap();
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
                let store = DurableCoordinator::open(&root, durable_bench_config()).unwrap();
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
                let store = DurableCoordinator::open(&root, durable_bench_config()).unwrap();
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
                let store = DurableCoordinator::open_with_maintenance_policy(
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
                let store = DurableCoordinator::open_with_maintenance_policy(
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
                let store = DurableCoordinator::open_with_maintenance_policy(
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
                let store = DurableCoordinator::open_with_maintenance_policy(
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
                let store = DurableCoordinator::open_with_maintenance_policy(
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

    group.bench_function("native_commit_file_batch_4k_acknowledged_fresh", |b| {
        b.iter_custom(|iters| {
            elapsed_durable_iters(iters, || {
                let root = durable_bench_root("native-write-ack-fresh");
                let store = DurableCoordinator::open(&root, durable_bench_config()).unwrap();
                let (keyspace_id, file_id) = create_durable_native_file(&store);
                let payload = vec![4; 4096];
                let started = Instant::now();
                store
                    .commit_file_batch(
                        black_box(keyspace_id),
                        black_box(file_id),
                        black_box(&[FileBatchWrite::new(0, payload)]),
                        WriteDurability::Acknowledged,
                    )
                    .unwrap();
                let elapsed = started.elapsed();
                cleanup_durable_bench_root(&root);
                black_box(elapsed)
            })
        })
    });

    group.bench_function("native_commit_file_batch_4k_flushed_fresh", |b| {
        b.iter_custom(|iters| {
            elapsed_durable_iters(iters, || {
                let root = durable_bench_root("native-write-flushed-fresh");
                let store = DurableCoordinator::open(&root, durable_bench_config()).unwrap();
                let (keyspace_id, file_id) = create_durable_native_file(&store);
                let payload = vec![4; 4096];
                let started = Instant::now();
                store
                    .commit_file_batch(
                        black_box(keyspace_id),
                        black_box(file_id),
                        black_box(&[FileBatchWrite::new(0, payload)]),
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
                let store = DurableCoordinator::open(&root, durable_bench_config()).unwrap();
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

    group.bench_function(
        "native_commit_file_batch_4k_flushed_after_32_flushed_writes",
        |b| {
            b.iter_custom(|iters| {
                elapsed_durable_iters(iters, || {
                    let root = durable_bench_root("native-write-flushed-after-32");
                    let store = DurableCoordinator::open(&root, durable_bench_config()).unwrap();
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
                        .commit_file_batch(
                            black_box(keyspace_id),
                            black_box(file_id),
                            black_box(&[FileBatchWrite::new(0, payload)]),
                            WriteDurability::Flushed,
                        )
                        .unwrap();
                    let elapsed = started.elapsed();
                    cleanup_durable_bench_root(&root);
                    black_box(elapsed)
                })
            })
        },
    );

    group.bench_function("native_append_4k_acknowledged_fresh", |b| {
        b.iter_custom(|iters| {
            elapsed_durable_iters(iters, || {
                let root = durable_bench_root("native-append-ack-fresh");
                let store = DurableCoordinator::open(&root, durable_bench_config()).unwrap();
                let (keyspace_id, file_id) = create_durable_native_file(&store);
                let stream = store.open_append_stream(keyspace_id, file_id).unwrap();
                let payload = vec![5; 4096];
                let started = Instant::now();
                store
                    .append_stream(
                        black_box(&stream),
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
                let store = DurableCoordinator::open(&root, durable_bench_config()).unwrap();
                let (keyspace_id, file_id) = create_durable_native_file(&store);
                let stream = store.open_append_stream(keyspace_id, file_id).unwrap();
                let payload = vec![5; 4096];
                let started = Instant::now();
                store
                    .append_stream(
                        black_box(&stream),
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
