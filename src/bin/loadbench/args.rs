#[derive(Debug, Clone)]
struct Args {
    provider: ProviderKind,
    durability: DurabilityMode,
    workloads: Vec<Workload>,
    concurrency: Vec<usize>,
    duration: Duration,
    warmup: Duration,
    rtt: Duration,
    serial_rtts: u32,
    delay_mode: DelayMode,
    root: PathBuf,
    append_visible_journal_dir: Option<PathBuf>,
    storage_node_data_dirs: Vec<PathBuf>,
    files: usize,
    shards: usize,
    storage_nodes: usize,
    device_blocks: u64,
    samples_per_worker: usize,
    matrix_csv: Option<PathBuf>,
    durable_profile_csv: Option<PathBuf>,
    append_publish_profile_csv: Option<PathBuf>,
    metadata_profile_csv: Option<PathBuf>,
    block_write_profile_csv: Option<PathBuf>,
    block_batch_profile_csv: Option<PathBuf>,
    append_log_profile_csv: Option<PathBuf>,
    read_profile_csv: Option<PathBuf>,
    target_data_log_bytes: u64,
    data_log_file_sync_fanout: usize,
    append_publish_batch_policy: AppendPublishBatchPolicy,
    stream_publish_bytes: Option<u64>,
    stream_total_bytes: u64,
    stream_auto_persist_bytes: Option<u64>,
    block_batch_ops: Option<usize>,
    block_batch_bytes: Option<usize>,
    block_batch_overlap: Option<BlockBatchOverlap>,
    block_batch_fsync_bytes: u64,
    native_file_batch_ops: Option<usize>,
    native_file_batch_bytes: Option<usize>,
    native_file_batch_overlap: Option<NativeFileBatchOverlap>,
    native_file_batch_fsync_bytes: u64,
    payload_integrity: PayloadIntegrity,
    read_verification: ReadVerification,
}

impl Args {
    fn parse() -> Result<Self> {
        let mut args = Self {
            provider: ProviderKind::Local,
            durability: DurabilityMode::Acknowledged,
            workloads: Workload::north_star_suite(),
            concurrency: vec![1, 4, 16],
            duration: Duration::from_secs(1),
            warmup: Duration::from_millis(200),
            rtt: Duration::ZERO,
            serial_rtts: 1,
            delay_mode: DelayMode::Spin,
            root: env::temp_dir().join("toy-cow-block-storage-loadbench"),
            append_visible_journal_dir: None,
            storage_node_data_dirs: Vec::new(),
            files: 1024,
            shards: 64,
            storage_nodes: 1,
            device_blocks: DEFAULT_DEVICE_BLOCKS,
            samples_per_worker: 200_000,
            matrix_csv: None,
            durable_profile_csv: None,
            append_publish_profile_csv: None,
            metadata_profile_csv: None,
            block_write_profile_csv: None,
            block_batch_profile_csv: None,
            append_log_profile_csv: None,
            read_profile_csv: None,
            target_data_log_bytes: 64 * 1024 * 1024,
            data_log_file_sync_fanout: 4,
            append_publish_batch_policy: AppendPublishBatchPolicy::default(),
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
            payload_integrity: PayloadIntegrity::Verified,
            read_verification: ReadVerification::Default,
        };

        let mut raw = env::args().skip(1);
        while let Some(flag) = raw.next() {
            match flag.as_str() {
                "--help" | "-h" => {
                    print_help();
                    std::process::exit(0);
                }
                "--provider" => args.provider = parse_next(&mut raw, "--provider")?,
                "--durability" => args.durability = parse_next(&mut raw, "--durability")?,
                "--workloads" => {
                    let value: String = parse_next(&mut raw, "--workloads")?;
                    args.workloads = parse_workloads(&value)?;
                }
                "--concurrency" => {
                    let value: String = parse_next(&mut raw, "--concurrency")?;
                    args.concurrency = parse_usize_list(&value, "--concurrency")?;
                }
                "--duration-ms" => {
                    let value: u64 = parse_next(&mut raw, "--duration-ms")?;
                    args.duration = Duration::from_millis(value);
                }
                "--warmup-ms" => {
                    let value: u64 = parse_next(&mut raw, "--warmup-ms")?;
                    args.warmup = Duration::from_millis(value);
                }
                "--rtt-us" => {
                    let value: u64 = parse_next(&mut raw, "--rtt-us")?;
                    args.rtt = Duration::from_micros(value);
                }
                "--serial-rtts" => args.serial_rtts = parse_next(&mut raw, "--serial-rtts")?,
                "--delay-mode" => args.delay_mode = parse_next(&mut raw, "--delay-mode")?,
                "--root" => args.root = PathBuf::from(parse_next::<String>(&mut raw, "--root")?),
                "--append-visible-journal-dir" => {
                    args.append_visible_journal_dir = Some(PathBuf::from(parse_next::<String>(
                        &mut raw,
                        "--append-visible-journal-dir",
                    )?));
                }
                "--storage-node-data-dirs" => {
                    let value: String = parse_next(&mut raw, "--storage-node-data-dirs")?;
                    args.storage_node_data_dirs = parse_path_list(&value);
                }
                "--files" => args.files = parse_next(&mut raw, "--files")?,
                "--shards" => args.shards = parse_next(&mut raw, "--shards")?,
                "--storage-nodes" => args.storage_nodes = parse_next(&mut raw, "--storage-nodes")?,
                "--device-blocks" => args.device_blocks = parse_next(&mut raw, "--device-blocks")?,
                "--samples-per-worker" => {
                    args.samples_per_worker = parse_next(&mut raw, "--samples-per-worker")?;
                }
                "--matrix-csv" => {
                    args.matrix_csv = Some(PathBuf::from(parse_next::<String>(
                        &mut raw,
                        "--matrix-csv",
                    )?));
                }
                "--durable-profile-csv" => {
                    args.durable_profile_csv = Some(PathBuf::from(parse_next::<String>(
                        &mut raw,
                        "--durable-profile-csv",
                    )?));
                }
                "--append-publish-profile-csv" => {
                    args.append_publish_profile_csv = Some(PathBuf::from(parse_next::<String>(
                        &mut raw,
                        "--append-publish-profile-csv",
                    )?));
                }
                "--metadata-profile-csv" => {
                    args.metadata_profile_csv = Some(PathBuf::from(parse_next::<String>(
                        &mut raw,
                        "--metadata-profile-csv",
                    )?));
                }
                "--block-write-profile-csv" => {
                    args.block_write_profile_csv = Some(PathBuf::from(parse_next::<String>(
                        &mut raw,
                        "--block-write-profile-csv",
                    )?));
                }
                "--block-batch-profile-csv" => {
                    args.block_batch_profile_csv = Some(PathBuf::from(parse_next::<String>(
                        &mut raw,
                        "--block-batch-profile-csv",
                    )?));
                }
                "--append-log-profile-csv" => {
                    args.append_log_profile_csv = Some(PathBuf::from(parse_next::<String>(
                        &mut raw,
                        "--append-log-profile-csv",
                    )?));
                }
                "--read-profile-csv" => {
                    args.read_profile_csv = Some(PathBuf::from(parse_next::<String>(
                        &mut raw,
                        "--read-profile-csv",
                    )?));
                }
                "--target-data-log-mib" => {
                    let mib: u64 = parse_next(&mut raw, "--target-data-log-mib")?;
                    args.target_data_log_bytes = mib_to_bytes(mib, "--target-data-log-mib")?;
                }
                "--data-log-file-sync-fanout" => {
                    args.data_log_file_sync_fanout = parse_next(&mut raw, flag.as_str())?;
                }
                "--append-publish-batch-target" => {
                    args.append_publish_batch_policy.target_tickets =
                        parse_next(&mut raw, flag.as_str())?;
                }
                "--append-publish-idle-coalesce-us" => {
                    let micros: u64 = parse_next(&mut raw, flag.as_str())?;
                    args.append_publish_batch_policy.idle_coalesce_delay =
                        Duration::from_micros(micros);
                }
                "--append-publish-max-coalesce-us" => {
                    let micros: u64 = parse_next(&mut raw, flag.as_str())?;
                    args.append_publish_batch_policy.max_coalesce_delay =
                        Duration::from_micros(micros);
                }
                "--stream-publish-mib" => {
                    let mib: u64 = parse_next(&mut raw, "--stream-publish-mib")?;
                    args.stream_publish_bytes = Some(mib_to_bytes(mib, "--stream-publish-mib")?);
                }
                "--stream-total-mib" => {
                    let mib: u64 = parse_next(&mut raw, "--stream-total-mib")?;
                    args.stream_total_bytes = mib_to_bytes(mib, "--stream-total-mib")?;
                }
                "--stream-auto-persist-mib" => {
                    let mib: u64 = parse_next(&mut raw, "--stream-auto-persist-mib")?;
                    args.stream_auto_persist_bytes =
                        Some(mib_to_bytes(mib, "--stream-auto-persist-mib")?);
                }
                "--block-batch-ops" => {
                    args.block_batch_ops = Some(parse_next(&mut raw, flag.as_str())?);
                }
                "--block-batch-bytes" => {
                    args.block_batch_bytes = Some(parse_next(&mut raw, flag.as_str())?);
                }
                "--block-batch-overlap" => {
                    args.block_batch_overlap = Some(parse_next(&mut raw, flag.as_str())?);
                }
                "--block-batch-fsync-mib" => {
                    let mib: u64 = parse_next(&mut raw, flag.as_str())?;
                    args.block_batch_fsync_bytes = mib_to_bytes(mib, "--block-batch-fsync-mib")?;
                }
                "--block-batch-fsync-ms" => {
                    let _: u64 = parse_next(&mut raw, flag.as_str())?;
                }
                "--block-client-overlay" => {
                    let value: String = parse_next(&mut raw, flag.as_str())?;
                    parse_on_off(&value, "--block-client-overlay")?;
                }
                "--native-file-batch-ops" => {
                    args.native_file_batch_ops = Some(parse_next(&mut raw, flag.as_str())?);
                }
                "--native-file-batch-bytes" => {
                    args.native_file_batch_bytes = Some(parse_next(&mut raw, flag.as_str())?);
                }
                "--native-file-batch-overlap" => {
                    args.native_file_batch_overlap = Some(parse_next(&mut raw, flag.as_str())?);
                }
                "--native-file-batch-fsync-mib" => {
                    let mib: u64 = parse_next(&mut raw, flag.as_str())?;
                    args.native_file_batch_fsync_bytes =
                        mib_to_bytes(mib, "--native-file-batch-fsync-mib")?;
                }
                "--native-file-batch-fsync-ms" => {
                    let _: u64 = parse_next(&mut raw, flag.as_str())?;
                }
                "--native-client-overlay" => {
                    let value: String = parse_next(&mut raw, flag.as_str())?;
                    parse_on_off(&value, "--native-client-overlay")?;
                }
                "--payload-integrity" => {
                    let value: String = parse_next(&mut raw, "--payload-integrity")?;
                    args.payload_integrity = parse_payload_integrity(&value)?;
                }
                "--read-verification" => {
                    let value: String = parse_next(&mut raw, "--read-verification")?;
                    args.read_verification = parse_read_verification(&value)?;
                }
                other => {
                    return Err(StorageError::invalid_argument(format!(
                        "unknown loadbench flag {other}"
                    )));
                }
            }
        }

        if args.workloads.is_empty() {
            return Err(StorageError::invalid_argument(
                "at least one workload is required",
            ));
        }
        if args.concurrency.is_empty() || args.concurrency.contains(&0) {
            return Err(StorageError::invalid_argument(
                "concurrency list must contain positive values",
            ));
        }
        if args.files == 0 {
            return Err(StorageError::invalid_argument(
                "files must be greater than zero",
            ));
        }
        if args.shards == 0 {
            return Err(StorageError::invalid_argument(
                "shards must be greater than zero",
            ));
        }
        if args.storage_nodes == 0 {
            return Err(StorageError::invalid_argument(
                "storage-nodes must be greater than zero",
            ));
        }
        if !args.storage_node_data_dirs.is_empty()
            && args.storage_node_data_dirs.len() < args.storage_nodes
        {
            return Err(StorageError::invalid_argument(
                "storage-node-data-dirs must provide at least storage-nodes paths",
            ));
        }
        if args.data_log_file_sync_fanout == 0 {
            return Err(StorageError::invalid_argument(
                "data-log-file-sync-fanout must be greater than zero",
            ));
        }
        args.append_publish_batch_policy.validate()?;
        if args.device_blocks < args.shards as u64 {
            return Err(StorageError::invalid_argument(
                "device-blocks must be at least shards",
            ));
        }
        if args.samples_per_worker == 0 {
            return Err(StorageError::invalid_argument(
                "samples-per-worker must be greater than zero",
            ));
        }
        for workload in &args.workloads {
            validate_fixed_stream_workload(*workload, &args)?;
        }

        Ok(args)
    }

    fn config(&self) -> LocalStoreConfig {
        LocalStoreConfig {
            shard_count: self.shards,
            block_size: BLOCK_SIZE,
            file_root_blocks: DEFAULT_FILE_ROOT_BLOCKS,
            metadata_fanout: 4,
            metadata_leaf_blocks: 1024,
            storage_node: StorageNodeId::from_raw(1),
            observability_event_capacity: 16_384,
            stream_auto_persist_bytes: self.stream_auto_persist_bytes,
        }
    }

    fn storage_node_ids(&self) -> Vec<StorageNodeId> {
        (1..=self.storage_nodes as u128)
            .map(StorageNodeId::from_raw)
            .collect()
    }

    fn modeled_delay(&self) -> Duration {
        self.rtt
            .checked_mul(self.serial_rtts)
            .unwrap_or(Duration::MAX)
    }
}

fn print_help() {
    println!(
        "usage: cargo run --release --bin loadbench -- [options]\n\
\n\
options:\n\
  --provider local|durable|txn-serial|txn-sharded\n\
                                           default: local\n\
  --durability ack|flushed|ack-flush:N     default: ack\n\
  --workloads LIST                         default: north-star\n\
                                           aliases: north-star, durable-publish, append-batch, append-stream, block-metadata, block-batch, block-durable-boundary, block-writeback, block-writeback-prestaged, native-metadata, native-file-batch\n\
                                           names: block-write-4k,\n\
                                           block-write-4k-same-shard-contended,\n\
                                           block-write-4k-same-shard-serialized,\n\
                                           block-write-4k-shard-lanes,\n\
                                           block-write-4k-device-lanes,\n\
                                           block-read-4k, block-read-1m,\n\
                                           block-write-1m, block-write-1m-shard-lanes,\n\
                                           block-write-1m-device-lanes,\n\
                                           block-batch-4k-16ops,\n\
                                           block-batch-4k-256ops,\n\
                                           block-batch-4k-4096ops,\n\
                                           block-batch-1m-16ops,\n\
                                           block-batch-1m-128ops,\n\
                                           block-batch-overwrite-collapse,\n\
                                           block-batch-fsync-interval,\n\
                                           block-writeback-fsync-1m,\n\
                                           block-writeback-fsync-2m,\n\
                                           block-writeback-fsync-4m,\n\
                                           block-writeback-fsync-16m,\n\
                                           block-writeback-prestaged-fsync-1m,\n\
                                           block-writeback-prestaged-fsync-2m,\n\
                                           block-writeback-prestaged-fsync-4m,\n\
                                           block-writeback-prestaged-fsync-16m,\n\
                                           native-read-4k, native-read-1m,\n\
                                           native-write-4k, native-write-4k-same-file,\n\
                                           native-write-4k-file-lanes, native-write-1m,\n\
                                           native-write-4m, native-write-32m,\n\
                                           native-file-batch-4k-16ops,\n\
                                           native-file-batch-4k-256ops,\n\
                                           native-file-batch-4k-4096ops,\n\
                                           native-file-batch-1m-16ops,\n\
                                           native-file-batch-overwrite-collapse,\n\
                                           native-file-batch-fsync-interval,\n\
                                           native-append-4k, native-append-4k-same-file,\n\
                                           native-append-4k-file-lanes, native-append-1m,\n\
                                           native-append-4m, native-append-32m,\n\
                                           native-stream-ingest-1m,\n\
                                           native-stream-ingest-4m,\n\
                                           native-stream-ingest-32m,\n\
                                           native-stream-publish-prefix-1m,\n\
                                           native-stream-publish-prefix-4m,\n\
                                           native-stream-publish-prefix-32m,\n\
                                           native-stream-publish-server-persisted-1m,\n\
                                           native-stream-publish-pipelined-1m,\n\
                                           native-stream-publish-interval-1m,\n\
                                           native-stream-publish-interval-4m,\n\
                                           native-stream-publish-interval-32m,\n\
                                           native-stream-publish-at-end-1m,\n\
                                           native-stream-publish-at-end-4m,\n\
                                           native-stream-publish-at-end-32m,\n\
                                           native-stream-publish-barrier-at-end-1m,\n\
                                           native-stream-publish-barrier-at-end-4m,\n\
                                           native-stream-publish-barrier-at-end-32m,\n\
                                           append-log-microbench-stream-private-4m,\n\
                                           append-log-microbench-node-shared-4m,\n\
                                           native-hot-append-4k\n\
  --concurrency LIST                       default: 1,4,16\n\
  --duration-ms N                          default: 1000\n\
  --warmup-ms N                            default: 200\n\
  --rtt-us N                               modeled per-operation RTT, default: 0\n\
  --serial-rtts N                          modeled serial RTTs per op, default: 1\n\
  --delay-mode spin|sleep                  modeled RTT delay mode, default: spin\n\
  --files N                                native file count, default: 1024\n\
  --shards N                               block shard count, default: 64\n\
  --storage-nodes N                        local storage node count, default: 1\n\
  --device-blocks N                        logical device blocks, default: 1048576\n\
  --samples-per-worker N                   latency reservoir size, default: 200000\n\
  --matrix-csv PATH                        append main loadbench rows to CSV\n\
  --durable-profile-csv PATH               append durable persist profiles to CSV\n\
  --append-publish-profile-csv PATH        append durable append publish wait profiles to CSV\n\
  --metadata-profile-csv PATH              append txn metadata profiles to CSV\n\
  --block-write-profile-csv PATH           append txn block write pipeline profiles to CSV\n\
  --block-batch-profile-csv PATH           append block batch commit profiles to CSV\n\
  --append-log-profile-csv PATH            append append-log microbench profiles to CSV\n\
  --read-profile-csv PATH                  append block/native read profiles to CSV\n\
  --target-data-log-mib N                  durable data-log roll target, default: 64\n\
  --data-log-file-sync-fanout N            concurrent durable data-log file syncs, default: 4\n\
  --append-publish-batch-target N          durable append publish batch target, default: 4\n\
  --append-publish-idle-coalesce-us N      durable append publish idle coalesce wait, default: 250\n\
  --append-publish-max-coalesce-us N       durable append publish max coalesce wait, default: 5000\n\
  --stream-publish-mib N                   publish append streams after N MiB per stream\n\
  --stream-total-mib N                     fixed stream workload MiB per worker, default: 1024\n\
  --stream-auto-persist-mib N              durable stream payload dirty-tail sync threshold\n\
  --block-batch-ops N                      override writes per block batch workload\n\
  --block-batch-bytes N                    override bytes per write inside block batch workloads\n\
  --block-batch-overlap sequential|random|overwrite-hotset\n\
                                           block batch offset shape\n\
  --block-batch-fsync-mib N                block fsync-interval dirty threshold, default: 128\n\
  --block-batch-fsync-ms N                 accepted for adapter policy simulations; not modeled yet\n\
  --block-client-overlay on|off            accepted for future read-your-writes adapter tests\n\
  --native-file-batch-ops N                override writes per native file batch workload\n\
  --native-file-batch-bytes N              override bytes per write inside native file batch workloads\n\
  --native-file-batch-overlap sequential|random|overwrite-hotset\n\
                                           native file batch offset shape\n\
  --native-file-batch-fsync-mib N          native fsync-interval dirty threshold, default: 16\n\
  --native-file-batch-fsync-ms N           accepted for adapter policy simulations; not modeled yet\n\
  --native-client-overlay on|off           accepted for future read-your-writes adapter tests\n\
  --payload-integrity verified|unchecked   write payload integrity, default: verified\n\
  --read-verification default|require-verified|skip\n\
                                           read verification policy, default: default\n\
  --root PATH                              durable scratch root\n\
  --append-visible-journal-dir PATH        directory for per-case append-visible publish journals\n\
  --storage-node-data-dirs LIST            comma-separated per-node data directories"
    );
}

fn parse_next<T: FromStr>(raw: &mut impl Iterator<Item = String>, flag: &str) -> Result<T>
where
    T::Err: fmt::Display,
{
    let value = raw
        .next()
        .ok_or_else(|| StorageError::invalid_argument(format!("{flag} requires a value")))?;
    value
        .parse()
        .map_err(|error| StorageError::invalid_argument(format!("invalid {flag}: {error}")))
}

fn parse_usize_list(value: &str, flag: &str) -> Result<Vec<usize>> {
    value
        .split(',')
        .map(|part| {
            part.parse::<usize>().map_err(|error| {
                StorageError::invalid_argument(format!("invalid {flag} entry {part}: {error}"))
            })
        })
        .collect()
}

fn parse_path_list(value: &str) -> Vec<PathBuf> {
    value
        .split(',')
        .filter(|part| !part.is_empty())
        .map(PathBuf::from)
        .collect()
}

fn mib_to_bytes(mib: u64, flag: &str) -> Result<u64> {
    mib.checked_mul(1024 * 1024)
        .filter(|bytes| *bytes > 0)
        .ok_or_else(|| StorageError::invalid_argument(format!("{flag} must be greater than zero")))
}

fn parse_payload_integrity(value: &str) -> Result<PayloadIntegrity> {
    match value {
        "verified" | "crc32c" => Ok(PayloadIntegrity::Verified),
        "unchecked" | "none" | "no-verify" => Ok(PayloadIntegrity::Unchecked),
        _ => Err(StorageError::invalid_argument(format!(
            "unknown payload integrity {value}"
        ))),
    }
}

fn payload_integrity_name(value: PayloadIntegrity) -> &'static str {
    match value {
        PayloadIntegrity::Verified => "verified",
        PayloadIntegrity::Unchecked => "unchecked",
    }
}

fn parse_read_verification(value: &str) -> Result<ReadVerification> {
    match value {
        "default" => Ok(ReadVerification::Default),
        "require-verified" | "required" => Ok(ReadVerification::RequireVerified),
        "skip" | "none" | "no-verify" => Ok(ReadVerification::Skip),
        _ => Err(StorageError::invalid_argument(format!(
            "unknown read verification {value}"
        ))),
    }
}

fn read_verification_name(value: ReadVerification) -> &'static str {
    match value {
        ReadVerification::Default => "default",
        ReadVerification::RequireVerified => "require-verified",
        ReadVerification::Skip => "skip",
    }
}

fn parse_on_off(value: &str, flag: &str) -> Result<bool> {
    match value {
        "on" | "true" | "1" => Ok(true),
        "off" | "false" | "0" => Ok(false),
        _ => Err(StorageError::invalid_argument(format!(
            "{flag} must be on or off"
        ))),
    }
}

fn parse_workloads(value: &str) -> Result<Vec<Workload>> {
    let mut workloads = Vec::new();
    for part in value.split(',') {
        match part {
            "north-star" | "all" => workloads.extend(Workload::north_star_suite()),
            "durable-publish" | "native-durable-publish" => {
                workloads.extend(Workload::durable_publish_suite())
            }
            "append-batch" => workloads.extend(Workload::append_batch_suite()),
            "append-stream" => workloads.extend(Workload::append_stream_suite()),
            "block-metadata" => workloads.extend(Workload::block_metadata_suite()),
            "block-batch" => workloads.extend(Workload::block_batch_suite()),
            "block-durable-boundary" => {
                workloads.extend(Workload::block_durable_boundary_suite())
            }
            "block-writeback" => workloads.extend(Workload::block_writeback_suite()),
            "block-writeback-prestaged" => {
                workloads.extend(Workload::block_writeback_prestaged_suite())
            }
            "native-metadata" => workloads.extend(Workload::native_metadata_suite()),
            "native-file-batch" => workloads.extend(Workload::native_file_batch_suite()),
            _ => workloads.push(Workload::from_str(part)?),
        }
    }
    Ok(workloads)
}
