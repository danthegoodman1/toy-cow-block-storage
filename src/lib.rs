//! Toy copy-on-write block storage.
//!
//! The crate is intentionally split into contracts, deterministic core
//! scaffolding, provider boundaries, and simulation utilities. The first local
//! implementation should use these boundaries directly so remote or durable
//! implementations can replace adapters later.

#![forbid(unsafe_code)]

pub mod api;
pub mod core;
pub mod error;
pub mod id;
pub mod object;
pub mod provider;
pub mod sim;

pub use api::{
    BlockClient, BlockDevice, BlockOperation, BlockRange, BlockRequest, BlockRequestEnvelope,
    BlockResponse, BlockResponseEnvelope, BlockServer, BlockTransport, ByteRange,
    CreateDeviceRequest, DeleteResult, DeviceInfo, DeviceSpec, FlushResult, FlushScope,
    ForkRequest, ReadResponse, RestorePoint, WriteCommit, WriteDurability,
};
pub use error::{Result, StorageError};
pub use id::{
    BlockCount, BlockIndex, CheckpointId, ClientEpoch, CommitSeq, DeviceGeneration, DeviceId,
    LogicalDeadline, LogicalTime, RequestId,
};
