use criterion::{Criterion, criterion_group, criterion_main};
use std::hint::black_box;
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
    targets = bench_byte_range_validation, bench_block_request_validation, bench_native_append_validation, bench_seeded_rng
}
criterion_main!(regression);
