use criterion::{Criterion, criterion_group, criterion_main};
use std::hint::black_box;
use toy_cow_block_storage::api::BlockRange;
use toy_cow_block_storage::id::{BlockCount, BlockIndex, MetadataNodeId, SegmentId};
use toy_cow_block_storage::local::{InMemoryMetadataPlane, InMemorySegmentStore, LocalStoreConfig};
use toy_cow_block_storage::object::{LeafEntry, MetadataNode, MetadataNodeKind, SegmentDescriptor};
use toy_cow_block_storage::provider::{MetadataPlane, SegmentReservation, SegmentStore};
use toy_cow_block_storage::sim::SeededRng;
use toy_cow_block_storage::{
    AppendLease, AppendLeaseId, BlockRequest, ByteRange, DeviceId, DeviceSpec, FileId, FileVersion,
    NativeRequest, WriteDurability, WriterEpoch,
};

fn bench_byte_range_validation(c: &mut Criterion) {
    let spec = DeviceSpec {
        logical_blocks: 1024 * 1024,
        block_size: 4096,
    };
    let range = ByteRange::new(128 * 4096, 64 * 4096);

    c.bench_function("byte_range_validation", |b| {
        b.iter(|| black_box(range).validate_for_device(black_box(&spec)))
    });
}

fn bench_block_request_validation(c: &mut Criterion) {
    let spec = DeviceSpec {
        logical_blocks: 1024 * 1024,
        block_size: 4096,
    };
    let request = BlockRequest::Write {
        device_id: DeviceId::from_raw(7),
        offset: 128 * 4096,
        bytes: vec![0; 64 * 4096],
        durability: WriteDurability::Acknowledged,
    };

    c.bench_function("block_request_validation", |b| {
        b.iter(|| black_box(&request).validate_for_existing_device(black_box(&spec)))
    });
}

fn bench_seeded_rng(c: &mut Criterion) {
    c.bench_function("seeded_rng_next_u64", |b| {
        b.iter(|| {
            let mut rng = SeededRng::new(black_box(42));
            let mut acc = 0;
            for _ in 0..1024 {
                acc ^= rng.next_u64();
            }
            black_box(acc)
        })
    });
}

fn bench_block_range_helpers(c: &mut Criterion) {
    let range = BlockRange::new(BlockIndex::from_raw(10), BlockCount::from_raw(1024));
    let other = BlockRange::new(BlockIndex::from_raw(512), BlockCount::from_raw(64));

    c.bench_function("block_range_helpers", |b| {
        b.iter(|| {
            let range = black_box(range);
            let other = black_box(other);
            black_box(range.end_exclusive()).unwrap();
            black_box(range.contains_range(other)).unwrap();
            black_box(range.overlaps(other)).unwrap();
            black_box(range.split_at(BlockIndex::from_raw(512))).unwrap()
        })
    });
}

fn bench_metadata_leaf_validation(c: &mut Criterion) {
    let segments = vec![
        SegmentDescriptor {
            segment_id: SegmentId::from_raw(1),
            blocks: BlockCount::from_raw(128),
            bytes: 128 * 4096,
            checksum: None,
        },
        SegmentDescriptor {
            segment_id: SegmentId::from_raw(2),
            blocks: BlockCount::from_raw(128),
            bytes: 128 * 4096,
            checksum: None,
        },
    ];
    let node = MetadataNode {
        node_id: MetadataNodeId::from_raw(1),
        covered_range: BlockRange::new(BlockIndex::from_raw(0), BlockCount::from_raw(256)),
        kind: MetadataNodeKind::Leaf {
            entries: vec![
                LeafEntry {
                    logical_start: BlockIndex::from_raw(0),
                    blocks: BlockCount::from_raw(64),
                    segment_id: SegmentId::from_raw(1),
                    segment_offset: BlockIndex::from_raw(0),
                },
                LeafEntry {
                    logical_start: BlockIndex::from_raw(128),
                    blocks: BlockCount::from_raw(64),
                    segment_id: SegmentId::from_raw(2),
                    segment_offset: BlockIndex::from_raw(0),
                },
            ],
        },
    };

    c.bench_function("metadata_leaf_validation", |b| {
        b.iter(|| black_box(&node).validate(black_box(&segments)))
    });
}

fn bench_in_memory_metadata_node_lookup(c: &mut Criterion) {
    let metadata = InMemoryMetadataPlane::new(LocalStoreConfig::default()).unwrap();
    let node = MetadataNode {
        node_id: MetadataNodeId::from_raw(99),
        covered_range: BlockRange::new(BlockIndex::from_raw(0), BlockCount::from_raw(128)),
        kind: MetadataNodeKind::Leaf {
            entries: Vec::new(),
        },
    };
    metadata.persist_metadata_node(node.clone()).unwrap();

    c.bench_function("in_memory_metadata_node_lookup", |b| {
        b.iter(|| metadata.get_metadata_node(black_box(node.node_id)))
    });
}

fn bench_in_memory_segment_read(c: &mut Criterion) {
    let store = InMemorySegmentStore::new(LocalStoreConfig::default()).unwrap();
    let reservation = SegmentReservation {
        segment_id: SegmentId::from_raw(42),
        bytes: 4096,
    };
    store.write_segment(&reservation, &[7; 4096]).unwrap();
    store.sync_segment(reservation.segment_id).unwrap();
    let mut buf = vec![0; 4096];

    c.bench_function("in_memory_segment_read", |b| {
        b.iter(|| {
            store
                .read_segment(
                    black_box(reservation.segment_id),
                    black_box(ByteRange::new(0, 4096)),
                    black_box(&mut buf),
                )
                .unwrap();
            black_box(buf[0])
        })
    });
}

fn bench_native_append_validation(c: &mut Criterion) {
    let file_id = FileId::from_raw(9);
    let request = NativeRequest::Append {
        file_id,
        lease: AppendLease {
            file_id,
            lease_id: AppendLeaseId::from_raw(7),
            writer_epoch: WriterEpoch::from_raw(3),
            base_version: FileVersion::from_raw(2),
        },
        bytes: vec![0; 64 * 4096],
        durability: WriteDurability::Acknowledged,
    };

    c.bench_function("native_append_validation", |b| {
        b.iter(|| black_box(&request).validate_for_existing_file())
    });
}

criterion_group! {
    name = regression;
    config = Criterion::default().noise_threshold(0.05);
    targets = bench_byte_range_validation, bench_block_request_validation, bench_native_append_validation, bench_block_range_helpers, bench_metadata_leaf_validation, bench_in_memory_metadata_node_lookup, bench_in_memory_segment_read, bench_seeded_rng
}
criterion_main!(regression);
