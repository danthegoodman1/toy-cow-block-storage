use crate::api::{ByteRange, CreateDeviceRequest, DeviceSpec, RestorePoint};
use crate::error::Result;
use crate::id::{CheckpointId, DeviceGeneration, DeviceId, MetadataNodeId, SegmentId};
use crate::object::{
    Checkpoint, CommitGroup, DeviceHead, MetadataNode, SegmentDescriptor, ShardRootUpdate,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommitGroupIntent {
    pub device_id: DeviceId,
    pub expected_generation: DeviceGeneration,
    pub updates: Vec<ShardRootUpdate>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetadataCreateDeviceRequest {
    pub spec: DeviceSpec,
    pub name: Option<String>,
}

impl From<CreateDeviceRequest> for MetadataCreateDeviceRequest {
    fn from(request: CreateDeviceRequest) -> Self {
        Self {
            spec: request.spec,
            name: request.name,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetadataForkRequest {
    pub source: DeviceId,
    pub target: Option<DeviceId>,
    pub name: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RetentionPolicy {
    pub retain_deleted_devices: bool,
}

pub trait MetadataPlane: Send + Sync {
    fn create_device(&self, request: MetadataCreateDeviceRequest) -> Result<DeviceHead>;
    fn get_head(&self, device_id: DeviceId) -> Result<DeviceHead>;
    fn persist_metadata_node(&self, node: MetadataNode) -> Result<()>;
    fn get_metadata_node(&self, node_id: MetadataNodeId) -> Result<MetadataNode>;
    fn publish_commit_group(&self, intent: CommitGroupIntent) -> Result<CommitGroup>;
    fn fork_device(&self, request: MetadataForkRequest) -> Result<DeviceHead>;
    fn restore_device(&self, source: DeviceId, point: RestorePoint) -> Result<DeviceHead>;
    fn checkpoint(&self, device_id: DeviceId) -> Result<CheckpointId>;
    fn get_checkpoint(&self, checkpoint_id: CheckpointId) -> Result<Checkpoint>;
    fn roots_for_gc(&self, policy: RetentionPolicy) -> Result<Vec<MetadataNodeId>>;
}

pub trait SegmentStore: Send + Sync {
    fn write_segment(
        &self,
        reservation: &SegmentReservation,
        bytes: &[u8],
    ) -> Result<SegmentCommit>;
    fn read_segment(&self, segment_id: SegmentId, range: ByteRange, buf: &mut [u8]) -> Result<()>;
    fn sync_segment(&self, segment_id: SegmentId) -> Result<()>;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SegmentReservationIntent {
    pub bytes: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SegmentReservation {
    pub segment_id: SegmentId,
    pub bytes: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LocalSegmentPlacement {
    pub segment_id: SegmentId,
    pub storage_id: String,
    pub offset: u64,
    pub bytes: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SegmentCommit {
    pub descriptor: SegmentDescriptor,
    pub placement: LocalSegmentPlacement,
}

pub trait LocalSegmentCatalog: Send + Sync {
    fn reserve_segment(&self, intent: SegmentReservationIntent) -> Result<SegmentReservation>;
    fn commit_segment(&self, reservation: SegmentReservation, commit: SegmentCommit) -> Result<()>;
    fn locate_segment(&self, segment_id: SegmentId) -> Result<LocalSegmentPlacement>;
    fn delete_segment(&self, segment_id: SegmentId) -> Result<()>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn metadata_create_request_preserves_public_create_fields() {
        let public = CreateDeviceRequest {
            spec: DeviceSpec {
                logical_blocks: 128,
                block_size: 4096,
            },
            name: Some("root".to_string()),
        };

        let metadata = MetadataCreateDeviceRequest::from(public.clone());

        assert_eq!(metadata.spec, public.spec);
        assert_eq!(metadata.name, public.name);
    }
}
