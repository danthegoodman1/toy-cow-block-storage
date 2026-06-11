fn fs_error(error: std::io::Error) -> StorageError {
    StorageError::unavailable(format!("filesystem operation failed: {error}"))
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use super::*;

    #[test]
    fn partitioned_file_index_gives_unique_lanes_across_workers() {
        let concurrency = 16;
        let files_len = 128;

        for op_index in 0..32 {
            let mut seen = BTreeSet::new();
            for worker in 0..concurrency {
                let file_index =
                    partitioned_file_index(worker as u64, op_index, concurrency, files_len);
                assert!(
                    seen.insert(file_index),
                    "worker {worker} collided on file {file_index} at op {op_index}"
                );
            }
        }
    }

    #[test]
    fn partitioned_file_index_stays_inside_worker_partition() {
        let concurrency = 4;
        let files_len = 10;

        for worker in 0..concurrency {
            let base = files_len * worker / concurrency;
            let next_base = files_len * (worker + 1) / concurrency;
            for op_index in 0..32 {
                let file_index =
                    partitioned_file_index(worker as u64, op_index, concurrency, files_len);
                assert!(
                    (base..next_base).contains(&file_index),
                    "file {file_index} escaped worker {worker} partition {base}..{next_base}"
                );
            }
        }
    }

    #[test]
    fn partitioned_file_index_handles_more_workers_than_files() {
        let files_len = 3;
        let concurrency = 8;

        for worker in 0..concurrency {
            let file_index = partitioned_file_index(worker as u64, 0, concurrency, files_len);
            assert_eq!(file_index, worker % files_len);
        }
    }

    #[test]
    fn worker_state_remembers_last_partitioned_file_index() {
        let mut state = WorkerState::default();
        let file_index = state.next_partitioned_file_index(2, 4, 16);

        assert_eq!(file_index, 8);
        assert_eq!(state.last_native_file_index, Some(8));
        assert_eq!(state.native_file_op, 1);
    }

    #[test]
    fn append_stream_suite_uses_explicit_ingest_and_publish_names() {
        let suite = Workload::append_stream_suite();
        assert!(suite.contains(&Workload::NativeStreamIngest1m));
        assert!(suite.contains(&Workload::NativeStreamPublishPrefix1m));
        assert!(suite.contains(&Workload::NativeStreamPublishServerPersisted1m));
        assert!(suite.contains(&Workload::NativeStreamPublishPipelined1m));
        assert!(Workload::from_str("native-stream-append-1m").is_err());
        assert!(Workload::from_str("native-stream-publish-1m").is_err());
    }

    #[test]
    fn durable_publish_suite_names_visible_publish_boundaries() {
        let suite = parse_workloads("durable-publish").unwrap();
        assert_eq!(
            suite,
            vec![
                Workload::NativeStreamPublishInterval1m,
                Workload::NativeStreamPublishInterval4m,
                Workload::NativeStreamPublishInterval32m,
                Workload::NativeStreamPublishAtEnd1m,
                Workload::NativeStreamPublishAtEnd4m,
                Workload::NativeStreamPublishAtEnd32m,
                Workload::NativeStreamPublishBarrierAtEnd1m,
                Workload::NativeStreamPublishBarrierAtEnd4m,
                Workload::NativeStreamPublishBarrierAtEnd32m,
            ]
        );
        assert_eq!(parse_workloads("native-durable-publish").unwrap(), suite);
    }

    #[test]
    fn fixed_stream_publish_workloads_parse_explicit_names() {
        assert_eq!(
            Workload::from_str("native-stream-publish-interval-1m").unwrap(),
            Workload::NativeStreamPublishInterval1m
        );
        assert_eq!(
            Workload::from_str("native-stream-publish-interval-4m").unwrap(),
            Workload::NativeStreamPublishInterval4m
        );
        assert_eq!(
            Workload::from_str("native-stream-publish-interval-16m").unwrap(),
            Workload::NativeStreamPublishInterval16m
        );
        assert_eq!(
            Workload::from_str("native-stream-publish-interval-32m").unwrap(),
            Workload::NativeStreamPublishInterval32m
        );
        assert_eq!(
            Workload::from_str("native-stream-publish-at-end-1m").unwrap(),
            Workload::NativeStreamPublishAtEnd1m
        );
        assert_eq!(
            Workload::from_str("native-stream-publish-at-end-4m").unwrap(),
            Workload::NativeStreamPublishAtEnd4m
        );
        assert_eq!(
            Workload::from_str("native-stream-publish-at-end-32m").unwrap(),
            Workload::NativeStreamPublishAtEnd32m
        );
        assert_eq!(
            Workload::from_str("native-stream-publish-barrier-at-end-1m").unwrap(),
            Workload::NativeStreamPublishBarrierAtEnd1m
        );
        assert_eq!(
            Workload::from_str("native-stream-publish-barrier-at-end-4m").unwrap(),
            Workload::NativeStreamPublishBarrierAtEnd4m
        );
        assert_eq!(
            Workload::from_str("native-stream-publish-barrier-at-end-32m").unwrap(),
            Workload::NativeStreamPublishBarrierAtEnd32m
        );
    }

    #[test]
    fn append_log_microbench_workloads_parse_explicit_names() {
        assert_eq!(
            Workload::from_str("append-log-microbench-stream-private-4m").unwrap(),
            Workload::AppendLogMicrobenchStreamPrivate4m
        );
        assert_eq!(
            Workload::from_str("append-log-microbench-node-shared-4m").unwrap(),
            Workload::AppendLogMicrobenchNodeShared4m
        );
    }

    #[test]
    fn fixed_stream_total_must_match_workload_boundaries() {
        assert!(validate_fixed_stream_total_bytes(1024 * 1024 * 1024, 32 * 1024 * 1024).is_ok());
        assert!(validate_fixed_stream_total_bytes(1025 * 1024 * 1024, 32 * 1024 * 1024).is_err());
        assert!(validate_fixed_stream_total_bytes(0, 32 * 1024 * 1024).is_err());
        assert!(validate_fixed_stream_total_bytes(DEFAULT_FILE_CAPACITY_BYTES + 1, 1024).is_err());
    }

    #[test]
    fn fixed_stream_interval_must_match_workload_boundaries() {
        assert!(validate_fixed_stream_publish_interval(128 * 1024 * 1024, 32 * 1024 * 1024)
            .is_ok());
        assert!(validate_fixed_stream_publish_interval(96 * 1024 * 1024, 32 * 1024 * 1024)
            .is_ok());
        assert!(validate_fixed_stream_publish_interval(100 * 1024 * 1024, 32 * 1024 * 1024)
            .is_err());
        assert!(validate_fixed_stream_publish_interval(0, 32 * 1024 * 1024).is_err());
    }

    #[test]
    fn fixed_stream_interval_publishes_boundaries_and_final_tail() {
        let workload = Workload::NativeStreamPublishInterval32m;
        let mut target = Some(128 * 1024 * 1024);
        let mut published = Vec::new();
        for next in [
            32 * 1024 * 1024,
            64 * 1024 * 1024,
            96 * 1024 * 1024,
            128 * 1024 * 1024,
            160 * 1024 * 1024,
            192 * 1024 * 1024,
        ] {
            if let Some(publish_through) =
                fixed_stream_publish_target(workload, next, 192 * 1024 * 1024, target)
            {
                published.push(publish_through);
                target = Some(publish_through + 128 * 1024 * 1024);
            }
        }
        assert_eq!(
            published,
            vec![128 * 1024 * 1024, 192 * 1024 * 1024]
        );
    }

    #[test]
    fn fixed_stream_at_end_publishes_once() {
        let workload = Workload::NativeStreamPublishAtEnd32m;
        let mut published = Vec::new();
        for next in [
            32 * 1024 * 1024,
            64 * 1024 * 1024,
            96 * 1024 * 1024,
        ] {
            if let Some(publish_through) =
                fixed_stream_publish_target(workload, next, 96 * 1024 * 1024, None)
            {
                published.push(publish_through);
            }
        }
        assert_eq!(published, vec![96 * 1024 * 1024]);
    }

    #[test]
    fn fixed_stream_at_end_does_not_require_interval_config() {
        let at_end = FixedStreamPublishConfig {
            workload: Workload::NativeStreamPublishAtEnd32m,
            concurrency: 1,
            modeled_delay: Duration::ZERO,
            delay_mode: DelayMode::Spin,
            samples_per_worker: 16,
            stream_publish_bytes: None,
            payload_integrity: PayloadIntegrity::Verified,
        };
        assert_eq!(fixed_stream_worker_publish_interval(&at_end).unwrap(), None);

        let interval = FixedStreamPublishConfig {
            workload: Workload::NativeStreamPublishInterval32m,
            ..at_end
        };
        assert!(fixed_stream_worker_publish_interval(&interval).is_err());
    }

    #[test]
    fn fixed_stream_barrier_at_end_is_fixed_but_not_inline_at_end() {
        let workload = Workload::NativeStreamPublishBarrierAtEnd32m;

        assert!(workload.is_native_stream_publish_fixed());
        assert!(workload.is_native_stream_publish_barrier_at_end());
        assert!(!workload.is_native_stream_publish_at_end());
    }

    #[test]
    fn stream_ingest_does_not_publish_when_threshold_is_configured() {
        assert!(!should_publish_after_stream_append(
            Workload::NativeStreamIngest1m,
            true
        ));
        assert!(!should_publish_after_stream_append(
            Workload::NativeStreamIngest4m,
            true
        ));
        assert!(!should_publish_after_stream_append(
            Workload::NativeStreamIngest32m,
            true
        ));
        assert!(should_publish_after_stream_append(
            Workload::NativeStreamPublishPrefix1m,
            false
        ));
        assert!(should_publish_after_stream_append(
            Workload::NativeStreamPublishPipelined1m,
            false
        ));
        assert!(!should_publish_after_stream_append(
            Workload::NativeStreamPublishServerPersisted1m,
            true
        ));
    }

    #[test]
    fn native_metadata_suite_names_contention_shapes() {
        let suite = parse_workloads("native-metadata").unwrap();
        assert!(suite.contains(&Workload::NativeWrite4kSameFile));
        assert!(suite.contains(&Workload::NativeWrite4kFileLanes));
        assert!(suite.contains(&Workload::NativeAppend4kSameFile));
        assert!(suite.contains(&Workload::NativeAppend4kFileLanes));
    }

    #[test]
    fn native_hot_append_c4_reports_successes_without_conflict_errors() {
        assert_native_hot_append_reports_successes_without_errors(4);
    }

    #[test]
    fn native_hot_append_c16_reports_successes_without_conflict_errors() {
        assert_native_hot_append_reports_successes_without_errors(16);
    }

    #[test]
    fn native_file_batch_suite_names_client_commit_shapes() {
        let suite = parse_workloads("native-file-batch").unwrap();
        assert!(suite.contains(&Workload::NativeFileBatch4k16Ops));
        assert!(suite.contains(&Workload::NativeFileBatch4k256Ops));
        assert!(suite.contains(&Workload::NativeFileBatch4k4096Ops));
        assert!(suite.contains(&Workload::NativeFileBatch1m16Ops));
        assert!(suite.contains(&Workload::NativeFileBatchOverwriteCollapse));
        assert!(suite.contains(&Workload::NativeFileBatchFsyncInterval));
    }

    #[test]
    fn native_mixed_suite_names_append_and_tiny_io_starvation_shapes() {
        assert_eq!(
            Workload::from_str("native-mixed-append-batch-4k-16ops").unwrap(),
            Workload::NativeMixedAppendBatch4k16Ops
        );
        assert_eq!(
            Workload::from_str("native-mixed-append-batch-4k-256ops").unwrap(),
            Workload::NativeMixedAppendBatch4k256Ops
        );
        assert_eq!(
            parse_workloads("native-mixed").unwrap(),
            vec![
                Workload::NativeMixedAppendBatch4k16Ops,
                Workload::NativeMixedAppendBatch4k256Ops,
            ]
        );
        assert!(Workload::NativeMixedAppendBatch4k16Ops.is_native_mixed());
        assert!(!Workload::NativeMixedAppendBatch4k16Ops.is_native_file_batch());
    }

    #[test]
    fn block_batch_suite_names_commit_boundary_shapes() {
        let suite = parse_workloads("block-batch").unwrap();
        assert!(suite.contains(&Workload::BlockBatch4k16Ops));
        assert!(suite.contains(&Workload::BlockBatch4k256Ops));
        assert!(suite.contains(&Workload::BlockBatch4k4096Ops));
        assert!(suite.contains(&Workload::BlockBatch1m16Ops));
        assert!(suite.contains(&Workload::BlockBatch1m128Ops));
        assert!(suite.contains(&Workload::BlockBatchOverwriteCollapse));
        assert!(suite.contains(&Workload::BlockBatchFsyncInterval));
    }

    #[test]
    fn block_writeback_suite_names_client_writeback_shapes() {
        let suite = parse_workloads("block-writeback").unwrap();
        assert_eq!(
            suite,
            vec![
                Workload::BlockWritebackFsync1m,
                Workload::BlockWritebackFsync2m,
                Workload::BlockWritebackFsync4m,
                Workload::BlockWritebackFsync16m,
            ]
        );
        assert!(Workload::from_str("block-writeback-write-4k").is_err());
        assert!(Workload::from_str("block-writeback-read-4k").is_err());
    }

    #[test]
    fn block_durable_boundary_suite_names_flush_boundary_shapes() {
        let suite = parse_workloads("block-durable-boundary").unwrap();
        assert_eq!(
            suite,
            vec![
                Workload::BlockWrite4k,
                Workload::BlockWrite1m,
                Workload::BlockBatchFsyncInterval,
                Workload::BlockWritebackFsync1m,
                Workload::BlockWritebackFsync2m,
                Workload::BlockWritebackFsync4m,
                Workload::BlockWritebackFsync16m,
                Workload::BlockWritebackPrestagedFsync1m,
                Workload::BlockWritebackPrestagedFsync2m,
                Workload::BlockWritebackPrestagedFsync4m,
                Workload::BlockWritebackPrestagedFsync16m,
            ]
        );
    }

    #[test]
    fn prestaged_block_writeback_suite_names_fsync_only_shapes() {
        let suite = parse_workloads("block-writeback-prestaged").unwrap();
        assert_eq!(
            suite,
            vec![
                Workload::BlockWritebackPrestagedFsync1m,
                Workload::BlockWritebackPrestagedFsync2m,
                Workload::BlockWritebackPrestagedFsync4m,
                Workload::BlockWritebackPrestagedFsync16m,
            ]
        );
        assert!(Workload::BlockWritebackPrestagedFsync1m.is_block_writeback_prestaged());
        assert_eq!(
            Workload::from_str("block-writeback-prestaged-fsync-1m").unwrap(),
            Workload::BlockWritebackPrestagedFsync1m
        );
    }

    #[test]
    fn block_writeback_state_stages_fsync_writes_without_claiming_local_iops() {
        let mut state = BlockWritebackState::default();
        state
            .push_write(0, &[1; 4096], PayloadIntegrity::Verified)
            .unwrap();
        state
            .push_write(4096, &[2; 4096], PayloadIntegrity::Verified)
            .unwrap();

        assert_eq!(state.dirty_bytes(), 8192);
        assert_eq!(state.writes.len(), 2);
        assert_eq!(state.writes[0].offset, 0);
        assert_eq!(state.writes[1].offset, 4096);
        assert!(state.staged_commit.is_none());
    }

    #[test]
    fn integrity_flags_parse_explicit_modes() {
        assert_eq!(
            parse_payload_integrity("unchecked").unwrap(),
            PayloadIntegrity::Unchecked
        );
        assert_eq!(
            parse_read_verification("require-verified").unwrap(),
            ReadVerification::RequireVerified
        );
        assert!(parse_payload_integrity("maybe").is_err());
        assert!(parse_read_verification("maybe").is_err());
    }

    #[test]
    fn bench_report_aggregates_durable_and_published_bytes() {
        let mut first = WorkerReport::new(8);
        let mut second = WorkerReport::new(8);
        let mut rng = Lcg::new(1);

        first.record(
            10,
            100,
            OpProgress {
                durable_bytes: 64,
                published_bytes: 32,
                block_batch_profile: None,
                native_file_batch_profile: None,
            },
            true,
            &mut rng,
        );
        second.record(
            20,
            200,
            OpProgress {
                durable_bytes: 128,
                published_bytes: 96,
                block_batch_profile: None,
                native_file_batch_profile: None,
            },
            true,
            &mut rng,
        );

        let report = BenchReport::from_workers(Duration::from_secs(1), vec![first, second]);
        assert_eq!(report.bytes, 300);
        assert_eq!(report.durable_bytes, 192);
        assert_eq!(report.published_bytes, 128);
    }

    #[test]
    fn bench_report_records_final_drain_without_counting_operations() {
        let mut worker = WorkerReport::new(8);
        let mut rng = Lcg::new(1);

        worker.record_stream_append(10, 100, OpProgress::default(), true, &mut rng);
        worker.record_stream_final_drain(200, &mut rng);
        worker.record_stream_barrier_wait(50, &mut rng);
        worker.record_stream_phases(1_000_000_000, 200_000_000);

        let report = BenchReport::from_workers(Duration::from_secs(2), vec![worker]);
        assert_eq!(report.attempts, 1);
        assert_eq!(report.successes, 1);
        assert_eq!(report.bytes, 100);
        assert_eq!(report.stream_final_drain_p99_nanos, 200);
        assert_eq!(report.stream_barrier_wait_p99_nanos, 50);
        assert_eq!(report.stream_append_phase_nanos, 1_000_000_000);
        assert_eq!(report.stream_boundary_phase_nanos, 200_000_000);
    }

    #[test]
    fn csv_append_writes_header_for_new_file() {
        let path = env::temp_dir().join(format!(
            "toy-cow-block-storage-loadbench-csv-new-{}.csv",
            NEXT_ROOT_ID.fetch_add(1, Ordering::Relaxed)
        ));
        let _ = fs::remove_file(&path);

        {
            let mut file = open_csv_append(&path, "first,second").unwrap();
            writeln!(file, "1,2").unwrap();
        }

        let contents = fs::read_to_string(&path).unwrap();
        assert_eq!(contents, "first,second\n1,2\n");
        let _ = fs::remove_file(&path);
    }

    #[test]
    fn csv_append_rejects_mismatched_header() {
        let path = env::temp_dir().join(format!(
            "toy-cow-block-storage-loadbench-csv-mismatch-{}.csv",
            NEXT_ROOT_ID.fetch_add(1, Ordering::Relaxed)
        ));
        fs::write(&path, "old,header\n1,2\n").unwrap();

        let error = open_csv_append(&path, "new,header").unwrap_err();
        assert!(
            error.to_string().contains("CSV header mismatch"),
            "unexpected error: {error}"
        );
        let contents = fs::read_to_string(&path).unwrap();
        assert_eq!(contents, "old,header\n1,2\n");
        let _ = fs::remove_file(&path);
    }

    fn assert_native_hot_append_reports_successes_without_errors(concurrency: usize) {
        let root = env::temp_dir().join(format!(
            "toy-cow-block-storage-hot-append-test-{}-c{}",
            std::process::id(),
            concurrency
        ));
        let _ = fs::remove_dir_all(&root);
        let args = test_loadbench_args(root.clone());

        let report = run_case(&args, Workload::NativeHotAppend4k, concurrency).unwrap();

        let _ = fs::remove_dir_all(&root);
        assert_eq!(report.workload, Workload::NativeHotAppend4k);
        assert_eq!(report.concurrency, concurrency);
        assert!(
            report.successes > 0,
            "expected successful hot append operations at c{concurrency}"
        );
        assert_eq!(
            report.errors, 0,
            "native-hot-append-4k should not report expected fencing conflicts as errors"
        );
    }

    fn test_loadbench_args(root: PathBuf) -> Args {
        Args {
            provider: ProviderKind::Durable,
            durability: DurabilityMode::Acknowledged,
            workloads: vec![Workload::NativeHotAppend4k],
            concurrency: vec![1],
            duration: Duration::from_millis(200),
            warmup: Duration::ZERO,
            rtt: Duration::ZERO,
            serial_rtts: 1,
            delay_mode: DelayMode::Spin,
            root,
            append_visible_journal_dir: None,
            storage_node_data_dirs: Vec::new(),
            files: 16,
            shards: 64,
            storage_nodes: 1,
            device_blocks: DEFAULT_DEVICE_BLOCKS,
            samples_per_worker: 1024,
            matrix_csv: None,
            durable_profile_csv: None,
            append_publish_profile_csv: None,
            metadata_profile_csv: None,
            block_write_profile_csv: None,
            block_batch_profile_csv: None,
            native_file_batch_profile_csv: None,
            native_file_batch_commit_profile_csv: None,
            append_ingest_profile_csv: None,
            append_log_profile_csv: None,
            read_profile_csv: None,
            target_data_log_bytes: 64 * 1024 * 1024,
            data_log_file_sync_fanout: 4,
            append_publish_batch_policy: AppendPublishBatchPolicy::default(),
            append_ingest_policy: AppendIngestPolicy::default(),
            stream_publish_bytes: None,
            stream_total_bytes: 1024 * 1024 * 1024,
            stream_auto_persist_bytes: None,
            block_batch_ops: None,
            block_batch_bytes: None,
            block_batch_overlap: None,
            block_batch_fsync_bytes: 128 * 1024 * 1024,
            native_file_batch_ops: None,
            native_file_batch_bytes: None,
            native_file_batch_overlap: None,
            native_file_batch_fsync_bytes: 16 * 1024 * 1024,
            payload_integrity: PayloadIntegrity::Unchecked,
            read_verification: ReadVerification::Default,
        }
    }
}
