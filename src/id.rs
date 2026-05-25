macro_rules! id_type {
    ($name:ident, $inner:ty) => {
        #[derive(
            Debug,
            Clone,
            Copy,
            PartialEq,
            Eq,
            PartialOrd,
            Ord,
            Hash,
            serde::Serialize,
            serde::Deserialize,
        )]
        pub struct $name($inner);

        impl $name {
            pub const fn from_raw(raw: $inner) -> Self {
                Self(raw)
            }

            pub const fn raw(self) -> $inner {
                self.0
            }
        }

        impl From<$inner> for $name {
            fn from(raw: $inner) -> Self {
                Self(raw)
            }
        }

        impl std::fmt::Display for $name {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                self.0.fmt(f)
            }
        }
    };
}

id_type!(DeviceId, u128);
id_type!(KeyspaceId, u128);
id_type!(FileId, u128);
id_type!(RequestId, u128);
id_type!(ClientEpoch, u64);
id_type!(ServerIncarnation, u64);
id_type!(DeviceGeneration, u64);
id_type!(KeyspaceGeneration, u64);
id_type!(FileVersion, u64);
id_type!(WriterEpoch, u64);
id_type!(CommitSeq, u64);
id_type!(CommitGroupId, u128);
id_type!(CheckpointId, u128);
id_type!(SegmentId, u128);
id_type!(StorageNodeId, u128);
id_type!(MetadataNodeId, u128);
id_type!(KeyspaceRootId, u128);
id_type!(KeyspaceCatalogShardId, u128);
id_type!(ShardId, u32);
id_type!(WriteIntentId, u128);
id_type!(AppendLeaseId, u128);
id_type!(ExtentId, u128);
id_type!(LogicalTime, u64);
id_type!(LogicalDeadline, u64);
id_type!(BlockIndex, u64);
id_type!(BlockCount, u64);
