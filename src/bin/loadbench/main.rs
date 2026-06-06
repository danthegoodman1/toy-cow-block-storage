use std::env;
use std::fmt;
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Barrier, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use toy_cow_block_storage::provider::{
    MetadataCreateDeviceRequest, MetadataCreateFileRequest, MetadataCreateKeyspaceRequest,
    MetadataPlane,
};
use toy_cow_block_storage::{
    AppendPublishWaitProfile, AppendStream, AppendTicket, BlockBatchCommit, BlockBatchWrite,
    ByteRange, CreateDeviceRequest, CreateFileRequest, CreateKeyspaceRequest, DeviceId, DeviceSpec,
    DurableCoordinator, DurableDataLogPolicy, DurablePersistProfile, FileBatchWrite, FileId,
    FileSpec, FlushResult, KeyspaceId, LocalCoordinator, LocalStoreConfig, MetadataTxnMode,
    MetadataTxnProfile, PayloadIntegrity, ReadProfile, ReadVerification, Result, StorageError,
    StorageNodeId, TxnBlockCoordinator, TxnBlockWriteProfile, WriteDurability,
};

static NEXT_ROOT_ID: AtomicU64 = AtomicU64::new(1);

const BLOCK_SIZE: u32 = 4096;
const DEFAULT_DEVICE_BLOCKS: u64 = 1_048_576;
const DEFAULT_FILE_ROOT_BLOCKS: u64 = 1_048_576;
const DEFAULT_FILE_CAPACITY_BYTES: u64 = DEFAULT_FILE_ROOT_BLOCKS * BLOCK_SIZE as u64;
const STREAM_APPEND_FILE_STRIDE: usize = 64;
const DEFAULT_PROFILE_CAPACITY: usize = 1_000_000;

fn main() {
    if let Err(error) = run() {
        eprintln!("loadbench failed: {error}");
        std::process::exit(1);
    }
}

fn run() -> Result<()> {
    let args = Args::parse()?;
    println!("{}", BenchReport::csv_header());

    for workload in &args.workloads {
        for &concurrency in &args.concurrency {
            let report = run_case(&args, *workload, concurrency)?;
            report.print_csv();
            append_matrix_csv(&args, &report)?;
        }
    }

    Ok(())
}

include!("args.rs");
include!("workload.rs");
include!("store.rs");
include!("runner.rs");
include!("profiles.rs");
include!("worker.rs");
include!("report.rs");
include!("util.rs");
