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
    fn append_stream_suite_uses_explicit_ingest_flush_and_publish_names() {
        let suite = Workload::append_stream_suite();
        assert!(suite.contains(&Workload::NativeStreamIngest1m));
        assert!(suite.contains(&Workload::NativeStreamAppendFlush1m));
        assert!(suite.contains(&Workload::NativeStreamPublishPreflushed1m));
        assert!(suite.contains(&Workload::NativeStreamFlushPublish1m));
        assert!(Workload::from_str("native-stream-append-1m").is_err());
        assert!(Workload::from_str("native-stream-publish-1m").is_err());
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
            },
            true,
            &mut rng,
        );

        let report = BenchReport::from_workers(Duration::from_secs(1), vec![first, second]);
        assert_eq!(report.bytes, 300);
        assert_eq!(report.durable_bytes, 192);
        assert_eq!(report.published_bytes, 128);
    }
}
