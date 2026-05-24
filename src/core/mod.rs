use crate::id::{
    CommitGroupId, LogicalDeadline, LogicalTime, MetadataNodeId, SegmentId, StorageNodeId,
    WriteIntentId,
};
use crate::object::{Checkpoint, CommitGroup, DeviceHead, FileHead, MappingOwner, MetadataNode};
use crate::provider::{
    CommitGroupIntent, RetentionPolicy, SegmentReplicaCommit, SegmentReservation,
    SegmentReservationIntent,
};

/// Deterministic storage-core state.
///
/// The core owns pure state transitions only. It emits provider work as
/// `StorageEffect` values and does not perform I/O, read time, spawn tasks, or
/// call provider traits.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct StorageState {
    steps_applied: u64,
}

impl StorageState {
    pub const fn steps_applied(&self) -> u64 {
        self.steps_applied
    }
}

/// Stable write-intent record created before segment reservation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WriteIntentRecord {
    pub write_intent: WriteIntentId,
    pub owner: MappingOwner,
    pub bytes: u64,
    pub deadline: Option<LogicalDeadline>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StorageCommand {
    Noop,
    CreateWriteIntent(WriteIntentRecord),
    ReserveSegment(SegmentReservationIntent),
    WriteSegment {
        reservation: SegmentReservation,
        bytes: Vec<u8>,
    },
    SyncSegment {
        segment_id: SegmentId,
    },
    CommitSegmentDurablePending {
        reservation: SegmentReservation,
        commit: SegmentReplicaCommit,
    },
    MarkSegmentReferenced {
        segment_id: SegmentId,
        owner: MappingOwner,
        commit_group: CommitGroupId,
    },
    PersistMetadataNode {
        node: Box<MetadataNode>,
    },
    PublishDeviceHead {
        head: Box<DeviceHead>,
    },
    PublishFileHead {
        head: Box<FileHead>,
    },
    PublishCommitGroup {
        intent: Box<CommitGroupIntent>,
    },
    AppendTimelineCommit {
        commit: Box<CommitGroup>,
    },
    PersistCheckpoint {
        checkpoint: Box<Checkpoint>,
    },
    RunMetadataCustodian {
        policy: RetentionPolicy,
    },
    RunStorageNodeCustodian {
        storage_node: StorageNodeId,
        now: LogicalTime,
    },
    DeleteMetadataNode {
        node_id: MetadataNodeId,
    },
    DeleteSegmentReplica {
        segment_id: SegmentId,
        storage_node: StorageNodeId,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StorageEffect {
    CreateWriteIntent(WriteIntentRecord),
    ReserveSegment(SegmentReservationIntent),
    WriteSegment {
        reservation: SegmentReservation,
        bytes: Vec<u8>,
    },
    SyncSegment {
        segment_id: SegmentId,
    },
    CommitSegmentDurablePending {
        reservation: SegmentReservation,
        commit: SegmentReplicaCommit,
    },
    MarkSegmentReferenced {
        segment_id: SegmentId,
        owner: MappingOwner,
        commit_group: CommitGroupId,
    },
    PersistMetadataNode {
        node: Box<MetadataNode>,
    },
    PublishDeviceHead {
        head: Box<DeviceHead>,
    },
    PublishFileHead {
        head: Box<FileHead>,
    },
    PublishCommitGroup {
        intent: Box<CommitGroupIntent>,
    },
    AppendTimelineCommit {
        commit: Box<CommitGroup>,
    },
    PersistCheckpoint {
        checkpoint: Box<Checkpoint>,
    },
    RunMetadataCustodian {
        policy: RetentionPolicy,
    },
    RunStorageNodeCustodian {
        storage_node: StorageNodeId,
        now: LogicalTime,
    },
    DeleteMetadataNode {
        node_id: MetadataNodeId,
    },
    DeleteSegmentReplica {
        segment_id: SegmentId,
        storage_node: StorageNodeId,
    },
}

impl StorageState {
    pub fn step(&mut self, command: StorageCommand) -> Vec<StorageEffect> {
        self.steps_applied = self.steps_applied.saturating_add(1);

        match command {
            StorageCommand::Noop => Vec::new(),
            StorageCommand::CreateWriteIntent(intent) => {
                vec![StorageEffect::CreateWriteIntent(intent)]
            }
            StorageCommand::ReserveSegment(intent) => vec![StorageEffect::ReserveSegment(intent)],
            StorageCommand::WriteSegment { reservation, bytes } => {
                vec![StorageEffect::WriteSegment { reservation, bytes }]
            }
            StorageCommand::SyncSegment { segment_id } => {
                vec![StorageEffect::SyncSegment { segment_id }]
            }
            StorageCommand::CommitSegmentDurablePending {
                reservation,
                commit,
            } => vec![StorageEffect::CommitSegmentDurablePending {
                reservation,
                commit,
            }],
            StorageCommand::MarkSegmentReferenced {
                segment_id,
                owner,
                commit_group,
            } => vec![StorageEffect::MarkSegmentReferenced {
                segment_id,
                owner,
                commit_group,
            }],
            StorageCommand::PersistMetadataNode { node } => {
                vec![StorageEffect::PersistMetadataNode { node }]
            }
            StorageCommand::PublishDeviceHead { head } => {
                vec![StorageEffect::PublishDeviceHead { head }]
            }
            StorageCommand::PublishFileHead { head } => {
                vec![StorageEffect::PublishFileHead { head }]
            }
            StorageCommand::PublishCommitGroup { intent } => {
                vec![StorageEffect::PublishCommitGroup { intent }]
            }
            StorageCommand::AppendTimelineCommit { commit } => {
                vec![StorageEffect::AppendTimelineCommit { commit }]
            }
            StorageCommand::PersistCheckpoint { checkpoint } => {
                vec![StorageEffect::PersistCheckpoint { checkpoint }]
            }
            StorageCommand::RunMetadataCustodian { policy } => {
                vec![StorageEffect::RunMetadataCustodian { policy }]
            }
            StorageCommand::RunStorageNodeCustodian { storage_node, now } => {
                vec![StorageEffect::RunStorageNodeCustodian { storage_node, now }]
            }
            StorageCommand::DeleteMetadataNode { node_id } => {
                vec![StorageEffect::DeleteMetadataNode { node_id }]
            }
            StorageCommand::DeleteSegmentReplica {
                segment_id,
                storage_node,
            } => vec![StorageEffect::DeleteSegmentReplica {
                segment_id,
                storage_node,
            }],
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::BlockRange;
    use crate::id::{
        BlockCount, BlockIndex, CommitSeq, DeviceGeneration, DeviceId, FileId, FileVersion,
        SegmentId,
    };
    use crate::object::{MetadataNodeKind, RootUpdate, SegmentDescriptor, ShardRootUpdate};
    use crate::provider::{MetadataFence, SegmentReplicaPlacement};

    fn owner() -> MappingOwner {
        MappingOwner::BlockDevice(DeviceId::from_raw(7))
    }

    fn write_intent() -> WriteIntentRecord {
        WriteIntentRecord {
            write_intent: WriteIntentId::from_raw(1),
            owner: owner(),
            bytes: 4096,
            deadline: Some(LogicalDeadline::from_raw(50)),
        }
    }

    fn reservation_intent() -> SegmentReservationIntent {
        SegmentReservationIntent {
            write_intent: WriteIntentId::from_raw(1),
            owner: owner(),
            bytes: 4096,
        }
    }

    fn reservation() -> SegmentReservation {
        SegmentReservation {
            segment_id: SegmentId::from_raw(2),
            bytes: 4096,
        }
    }

    fn segment_commit() -> SegmentReplicaCommit {
        SegmentReplicaCommit {
            descriptor: SegmentDescriptor {
                segment_id: SegmentId::from_raw(2),
                blocks: BlockCount::from_raw(1),
                bytes: 4096,
                checksum: Some(99),
            },
            placement: SegmentReplicaPlacement {
                segment_id: SegmentId::from_raw(2),
                storage_node: StorageNodeId::from_raw(3),
                offset: 8192,
                bytes: 4096,
            },
        }
    }

    fn metadata_node() -> MetadataNode {
        MetadataNode {
            node_id: MetadataNodeId::from_raw(4),
            covered_range: BlockRange::new(BlockIndex::from_raw(0), BlockCount::from_raw(1)),
            kind: MetadataNodeKind::Leaf {
                entries: Vec::new(),
            },
        }
    }

    fn device_head() -> DeviceHead {
        DeviceHead {
            device_id: DeviceId::from_raw(7),
            generation: DeviceGeneration::from_raw(1),
            shard_roots: vec![MetadataNodeId::from_raw(4)],
            latest_commit: CommitSeq::from_raw(5),
        }
    }

    fn file_head() -> FileHead {
        FileHead {
            file_id: FileId::from_raw(8),
            version: FileVersion::from_raw(1),
            root: MetadataNodeId::from_raw(4),
            size: 0,
            latest_commit: CommitSeq::from_raw(5),
        }
    }

    fn commit_group_intent() -> CommitGroupIntent {
        CommitGroupIntent {
            owner: owner(),
            fence: MetadataFence::DeviceGeneration(DeviceGeneration::from_raw(1)),
            updates: vec![RootUpdate::BlockShard(ShardRootUpdate {
                shard_id: crate::id::ShardId::from_raw(0),
                old_root: MetadataNodeId::from_raw(4),
                new_root: MetadataNodeId::from_raw(5),
            })],
        }
    }

    fn commit_group() -> CommitGroup {
        CommitGroup {
            commit_group: CommitGroupId::from_raw(6),
            commit_seq: CommitSeq::from_raw(5),
            owner: owner(),
            updates: commit_group_intent().updates,
        }
    }

    fn checkpoint() -> Checkpoint {
        Checkpoint {
            checkpoint_id: crate::id::CheckpointId::from_raw(9),
            commit_seq: CommitSeq::from_raw(5),
            time: LogicalTime::from_raw(44),
            owner: owner(),
            shard_roots: vec![MetadataNodeId::from_raw(4)],
        }
    }

    #[test]
    fn noop_step_is_deterministic_and_side_effect_free() {
        let mut first = StorageState::default();
        let mut second = StorageState::default();

        assert_eq!(
            first.step(StorageCommand::Noop),
            Vec::<StorageEffect>::new()
        );
        assert_eq!(
            second.step(StorageCommand::Noop),
            Vec::<StorageEffect>::new()
        );
        assert_eq!(first, second);
        assert_eq!(first.steps_applied(), 1);
    }

    #[test]
    fn contract_commands_emit_matching_effects() {
        let cases = vec![
            (
                StorageCommand::CreateWriteIntent(write_intent()),
                vec![StorageEffect::CreateWriteIntent(write_intent())],
            ),
            (
                StorageCommand::ReserveSegment(reservation_intent()),
                vec![StorageEffect::ReserveSegment(reservation_intent())],
            ),
            (
                StorageCommand::WriteSegment {
                    reservation: reservation(),
                    bytes: vec![1, 2, 3, 4],
                },
                vec![StorageEffect::WriteSegment {
                    reservation: reservation(),
                    bytes: vec![1, 2, 3, 4],
                }],
            ),
            (
                StorageCommand::SyncSegment {
                    segment_id: SegmentId::from_raw(2),
                },
                vec![StorageEffect::SyncSegment {
                    segment_id: SegmentId::from_raw(2),
                }],
            ),
            (
                StorageCommand::CommitSegmentDurablePending {
                    reservation: reservation(),
                    commit: segment_commit(),
                },
                vec![StorageEffect::CommitSegmentDurablePending {
                    reservation: reservation(),
                    commit: segment_commit(),
                }],
            ),
            (
                StorageCommand::MarkSegmentReferenced {
                    segment_id: SegmentId::from_raw(2),
                    owner: owner(),
                    commit_group: CommitGroupId::from_raw(6),
                },
                vec![StorageEffect::MarkSegmentReferenced {
                    segment_id: SegmentId::from_raw(2),
                    owner: owner(),
                    commit_group: CommitGroupId::from_raw(6),
                }],
            ),
            (
                StorageCommand::PersistMetadataNode {
                    node: Box::new(metadata_node()),
                },
                vec![StorageEffect::PersistMetadataNode {
                    node: Box::new(metadata_node()),
                }],
            ),
            (
                StorageCommand::PublishDeviceHead {
                    head: Box::new(device_head()),
                },
                vec![StorageEffect::PublishDeviceHead {
                    head: Box::new(device_head()),
                }],
            ),
            (
                StorageCommand::PublishFileHead {
                    head: Box::new(file_head()),
                },
                vec![StorageEffect::PublishFileHead {
                    head: Box::new(file_head()),
                }],
            ),
            (
                StorageCommand::PublishCommitGroup {
                    intent: Box::new(commit_group_intent()),
                },
                vec![StorageEffect::PublishCommitGroup {
                    intent: Box::new(commit_group_intent()),
                }],
            ),
            (
                StorageCommand::AppendTimelineCommit {
                    commit: Box::new(commit_group()),
                },
                vec![StorageEffect::AppendTimelineCommit {
                    commit: Box::new(commit_group()),
                }],
            ),
            (
                StorageCommand::PersistCheckpoint {
                    checkpoint: Box::new(checkpoint()),
                },
                vec![StorageEffect::PersistCheckpoint {
                    checkpoint: Box::new(checkpoint()),
                }],
            ),
            (
                StorageCommand::RunMetadataCustodian {
                    policy: RetentionPolicy {
                        retain_deleted_devices: true,
                    },
                },
                vec![StorageEffect::RunMetadataCustodian {
                    policy: RetentionPolicy {
                        retain_deleted_devices: true,
                    },
                }],
            ),
            (
                StorageCommand::RunStorageNodeCustodian {
                    storage_node: StorageNodeId::from_raw(3),
                    now: LogicalTime::from_raw(44),
                },
                vec![StorageEffect::RunStorageNodeCustodian {
                    storage_node: StorageNodeId::from_raw(3),
                    now: LogicalTime::from_raw(44),
                }],
            ),
            (
                StorageCommand::DeleteMetadataNode {
                    node_id: MetadataNodeId::from_raw(4),
                },
                vec![StorageEffect::DeleteMetadataNode {
                    node_id: MetadataNodeId::from_raw(4),
                }],
            ),
            (
                StorageCommand::DeleteSegmentReplica {
                    segment_id: SegmentId::from_raw(2),
                    storage_node: StorageNodeId::from_raw(3),
                },
                vec![StorageEffect::DeleteSegmentReplica {
                    segment_id: SegmentId::from_raw(2),
                    storage_node: StorageNodeId::from_raw(3),
                }],
            ),
        ];

        let mut state = StorageState::default();

        for (command, expected) in cases {
            assert_eq!(state.step(command), expected);
        }
    }

    #[test]
    fn identical_command_traces_replay_to_identical_effects_and_state() {
        let commands = vec![
            StorageCommand::CreateWriteIntent(write_intent()),
            StorageCommand::ReserveSegment(reservation_intent()),
            StorageCommand::WriteSegment {
                reservation: reservation(),
                bytes: vec![1, 2, 3, 4],
            },
            StorageCommand::SyncSegment {
                segment_id: SegmentId::from_raw(2),
            },
            StorageCommand::PersistMetadataNode {
                node: Box::new(metadata_node()),
            },
            StorageCommand::PublishCommitGroup {
                intent: Box::new(commit_group_intent()),
            },
        ];

        let (first_state, first_effects) = replay(commands.clone());
        let (second_state, second_effects) = replay(commands);

        assert_eq!(first_effects, second_effects);
        assert_eq!(first_state, second_state);
        assert_eq!(first_state.steps_applied(), 6);
    }

    #[test]
    fn identical_seeded_traces_replay_to_identical_effects() {
        let (first_trace, first_commands) = seeded_trace(1234, 16);
        let (second_trace, second_commands) = seeded_trace(1234, 16);

        assert_eq!(first_trace, second_trace);

        let (first_state, first_effects) = replay(first_commands);
        let (second_state, second_effects) = replay(second_commands);

        assert_eq!(first_effects, second_effects);
        assert_eq!(first_state, second_state);
    }

    fn seeded_trace(seed: u64, steps: usize) -> (Vec<String>, Vec<StorageCommand>) {
        let mut harness = crate::sim::DeterministicHarness::new(seed);
        let mut commands = Vec::with_capacity(steps);

        for _ in 0..steps {
            let command = match harness.rng.choose_index(4).unwrap() {
                0 => {
                    harness.trace.record("noop");
                    StorageCommand::Noop
                }
                1 => {
                    harness.trace.record("reserve_segment");
                    StorageCommand::ReserveSegment(reservation_intent())
                }
                2 => {
                    harness.trace.record("sync_segment");
                    StorageCommand::SyncSegment {
                        segment_id: SegmentId::from_raw(2),
                    }
                }
                _ => {
                    harness.trace.record("delete_metadata_node");
                    StorageCommand::DeleteMetadataNode {
                        node_id: MetadataNodeId::from_raw(4),
                    }
                }
            };
            commands.push(command);
        }

        (harness.trace.into_events(), commands)
    }

    fn replay(commands: Vec<StorageCommand>) -> (StorageState, Vec<Vec<StorageEffect>>) {
        let mut state = StorageState::default();
        let effects = commands
            .into_iter()
            .map(|command| state.step(command))
            .collect();
        (state, effects)
    }
}
