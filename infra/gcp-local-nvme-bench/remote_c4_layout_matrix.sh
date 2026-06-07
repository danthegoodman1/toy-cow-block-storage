#!/usr/bin/env bash
set -euo pipefail

RUN_ID="${1:?run id required}"
SRC_TGZ="${2:?source tarball required}"

RESULT_ROOT="/opt/results/${RUN_ID}"
SRC_DIR="/opt/src"
LOADBENCH="${SRC_DIR}/target/release/loadbench"
STORAGE_NODES="${STORAGE_NODES:-4}"
MIN_LOCAL_SSDS="${MIN_LOCAL_SSDS:-32}"
RTTS=(0 200 700 3600)
CONCURRENCY="${CONCURRENCY:-16,32}"
WORKLOADS="native-stream-publish-at-end-32m,native-stream-publish-interval-32m"

mkdir -p "${RESULT_ROOT}/loadbench" "${RESULT_ROOT}/monitor"

archive_results() {
  local status=$?
  tar -C "/opt/results" -czf "/tmp/${RUN_ID}-results.tgz" "${RUN_ID}" || true
  exit "${status}"
}
trap archive_results EXIT

log() {
  printf '[%s] %s\n' "$(date -Is)" "$*"
}

log "installing packages"
export DEBIAN_FRONTEND=noninteractive
apt-get update -y
apt-get install -y ca-certificates curl build-essential pkg-config libssl-dev \
  libsqlite3-dev mdadm xfsprogs sysstat python3

if ! command -v cargo >/dev/null 2>&1; then
  log "installing rust toolchain"
  curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs \
    | sh -s -- -y --profile minimal
  # shellcheck disable=SC1091
  source "${HOME}/.cargo/env"
fi

log "extracting source"
rm -rf "${SRC_DIR}"
mkdir -p "${SRC_DIR}"
tar -C "${SRC_DIR}" -xzf "${SRC_TGZ}"

log "building loadbench"
cd "${SRC_DIR}"
cargo build --release --bin loadbench

mapfile -t DISKS < <(
  find -L /dev/disk/by-id -maxdepth 1 -type b -name 'google-local-nvme-ssd-*' \
    -exec readlink -f {} \; 2>/dev/null | sort -u
)
if (( ${#DISKS[@]} == 0 )); then
  mapfile -t DISKS < <(lsblk -dn -o NAME,TYPE,TRAN | awk '$2 == "disk" && $3 == "nvme" { print "/dev/" $1 }' | sort)
fi
if (( ${#DISKS[@]} < MIN_LOCAL_SSDS )); then
  printf 'expected at least %s local NVMe disks, found %s\n' "${MIN_LOCAL_SSDS}" "${#DISKS[@]}" >&2
  lsblk -o NAME,TYPE,TRAN,SIZE,MODEL,MOUNTPOINT | tee "${RESULT_ROOT}/environment-lsblk-failure.txt"
  exit 1
fi
if (( ${#DISKS[@]} <= STORAGE_NODES )); then
  printf 'node-private layout needs one journal disk plus %s storage-node disks, found %s\n' \
    "${STORAGE_NODES}" "${#DISKS[@]}" >&2
  exit 1
fi

{
  echo "run_id=${RUN_ID}"
  echo "machine_type=$(curl -sf -H 'Metadata-Flavor: Google' http://metadata.google.internal/computeMetadata/v1/instance/machine-type | awk -F/ '{print $NF}')"
  echo "zone=$(curl -sf -H 'Metadata-Flavor: Google' http://metadata.google.internal/computeMetadata/v1/instance/zone | awk -F/ '{print $NF}')"
  echo "storage_nodes=${STORAGE_NODES}"
  echo "min_local_ssds=${MIN_LOCAL_SSDS}"
  echo "rtts=${RTTS[*]}"
  echo "concurrency=${CONCURRENCY}"
  echo "workloads=${WORKLOADS}"
  echo "disk_count=${#DISKS[@]}"
  printf 'disks=%s\n' "${DISKS[*]}"
  echo
  uname -a
  echo
  lsblk -o NAME,TYPE,TRAN,SIZE,MODEL,MOUNTPOINT
  echo
  df -h
} | tee "${RESULT_ROOT}/environment.txt"

monitor_pids=()

start_monitors() {
  local mode="$1"
  mkdir -p "${RESULT_ROOT}/monitor/${mode}"
  iostat -dxm 1 > "${RESULT_ROOT}/monitor/${mode}/iostat.log" &
  monitor_pids+=("$!")
  vmstat 1 > "${RESULT_ROOT}/monitor/${mode}/vmstat.log" &
  monitor_pids+=("$!")
  mpstat -P ALL 1 > "${RESULT_ROOT}/monitor/${mode}/mpstat.log" &
  monitor_pids+=("$!")
}

stop_monitors() {
  for pid in "${monitor_pids[@]:-}"; do
    kill "${pid}" 2>/dev/null || true
  done
  wait "${monitor_pids[@]:-}" 2>/dev/null || true
  monitor_pids=()
}

teardown_storage() {
  set +e
  stop_monitors
  for mountpoint in /mnt/raid /mnt/data /mnt/journal /mnt/node-*; do
    if mountpoint -q "${mountpoint}"; then
      umount "${mountpoint}"
    fi
  done
  if [[ -e /dev/md0 ]]; then
    mdadm --stop /dev/md0 >/dev/null 2>&1
    mdadm --remove /dev/md0 >/dev/null 2>&1
  fi
  set -e
}

wipe_disks() {
  teardown_storage
  for disk in "${DISKS[@]}"; do
    wipefs -a "${disk}" >/dev/null 2>&1 || true
  done
}

mount_xfs() {
  local device="$1"
  local mountpoint="$2"
  mkdir -p "${mountpoint}"
  mkfs.xfs -f "${device}" >/dev/null
  mount -o noatime,nodiratime "${device}" "${mountpoint}"
}

setup_raid_shared() {
  wipe_disks
  mdadm --create /dev/md0 --level=0 --raid-devices="${#DISKS[@]}" \
    --chunk=1024K "${DISKS[@]}" --run --force
  mount_xfs /dev/md0 /mnt/raid
}

setup_raid_split_journal() {
  wipe_disks
  mount_xfs "${DISKS[0]}" /mnt/journal
  local data_disks=("${DISKS[@]:1}")
  mdadm --create /dev/md0 --level=0 --raid-devices="${#data_disks[@]}" \
    --chunk=1024K "${data_disks[@]}" --run --force
  mount_xfs /dev/md0 /mnt/data
}

setup_node_private_journal() {
  wipe_disks
  mount_xfs "${DISKS[0]}" /mnt/journal
  for index in $(seq 1 "${STORAGE_NODES}"); do
    mount_xfs "${DISKS[$index]}" "/mnt/node-${index}"
  done
}

csv_join_node_dirs() {
  local out=""
  for index in $(seq 1 "${STORAGE_NODES}"); do
    if [[ -n "${out}" ]]; then
      out+=","
    fi
    out+="/mnt/node-${index}/data"
  done
  printf '%s' "${out}"
}

run_layout() {
  local mode="$1"
  local root="$2"
  local journal_dir="${3:-}"
  local node_dirs="${4:-}"
  local out="${RESULT_ROOT}/loadbench/${mode}"
  mkdir -p "${out}"
  {
    echo "mode=${mode}"
    echo "root=${root}"
    echo "journal_dir=${journal_dir}"
    echo "node_dirs=${node_dirs}"
    findmnt || true
    echo
    lsblk -o NAME,TYPE,TRAN,SIZE,MODEL,MOUNTPOINT
    echo
    df -h
  } > "${out}/layout.txt"

  start_monitors "${mode}"
  for rtt in "${RTTS[@]}"; do
    local rtt_out="${out}/rtt-${rtt}"
    mkdir -p "${rtt_out}"
    local cmd=(
      "${LOADBENCH}"
      --provider durable
      --durability ack
      --workloads "${WORKLOADS}"
      --concurrency "${CONCURRENCY}"
      --warmup-ms 0
      --rtt-us "${rtt}"
      --delay-mode spin
      --storage-nodes "${STORAGE_NODES}"
      --files 128
      --stream-total-mib 512
      --stream-publish-mib 128
      --stream-auto-persist-mib 32
      --target-data-log-mib 64
      --data-log-file-sync-fanout 16
      --root "${root}/rtt-${rtt}/root"
      --matrix-csv "${rtt_out}/matrix.csv"
      --durable-profile-csv "${rtt_out}/durable-profile.csv"
      --append-publish-profile-csv "${rtt_out}/append-publish-profile.csv"
    )
    if [[ -n "${journal_dir}" ]]; then
      cmd+=(--append-visible-journal-dir "${journal_dir}/rtt-${rtt}")
    fi
    if [[ -n "${node_dirs}" ]]; then
      cmd+=(--storage-node-data-dirs "${node_dirs}")
    fi
    log "running ${mode} rtt=${rtt}"
    "${cmd[@]}" | tee "${rtt_out}/stdout.csv"
  done
  stop_monitors
}

summarize() {
  python3 - "${RESULT_ROOT}/loadbench" <<'PY'
import csv
import math
import sys
from pathlib import Path

root = Path(sys.argv[1])

def rows_for(name):
    for path in sorted(root.glob(f"*/rtt-*/*{name}")):
        mode = path.parts[-3]
        rtt_effective = path.parts[-2].split("-", 1)[1]
        with path.open(newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                row["mode"] = mode
                row["rtt_us_effective"] = rtt_effective
                yield row

def write_combined(name, output):
    rows = list(rows_for(name))
    if not rows:
        return
    fields = ["mode", "rtt_us_effective"] + list(rows[0].keys())
    fields = list(dict.fromkeys(fields))
    with (root / output).open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)

def percentile(values, q):
    values = sorted(values)
    if not values:
        return 0.0
    index = math.ceil(q * len(values)) - 1
    return values[max(0, min(index, len(values) - 1))]

def to_float(row, key):
    try:
        return float(row.get(key, 0) or 0)
    except ValueError:
        return 0.0

def to_ms(row, key):
    return to_float(row, key) / 1_000_000.0

write_combined("matrix.csv", "combined-matrix.csv")
write_combined("durable-profile.csv", "combined-durable-profile.csv")
write_combined("append-publish-profile.csv", "combined-append-publish-profile.csv")

matrix = list(rows_for("matrix.csv"))
with (root / "headline-summary.csv").open("w", newline="") as f:
    fields = [
        "mode","rtt_us","workload","concurrency","published_mbps",
        "stream_append_p99_ms","stream_publish_p99_ms","stream_final_drain_p99_ms",
        "stream_append_phase_seconds","stream_boundary_phase_seconds",
    ]
    writer = csv.DictWriter(f, fieldnames=fields)
    writer.writeheader()
    for row in matrix:
        writer.writerow({
            "mode": row["mode"],
            "rtt_us": row["rtt_us"],
            "workload": row["workload"],
            "concurrency": row["concurrency"],
            "published_mbps": row["published_mbps"],
            "stream_append_p99_ms": to_float(row, "stream_append_p99_us") / 1000.0,
            "stream_publish_p99_ms": to_float(row, "stream_publish_p99_us") / 1000.0,
            "stream_final_drain_p99_ms": to_float(row, "stream_final_drain_p99_us") / 1000.0,
            "stream_append_phase_seconds": row["stream_append_phase_seconds"],
            "stream_boundary_phase_seconds": row["stream_boundary_phase_seconds"],
        })

append_rows = list(rows_for("append-publish-profile.csv"))
groups = {}
for row in append_rows:
    key = (row["mode"], row["rtt_us"], row["workload"], row["concurrency"])
    groups.setdefault(key, []).append(row)

append_fields = [
    "total_nanos","coordinator_wait_nanos","in_flight_wait_nanos",
    "coalesce_wait_nanos","persist_batch_nanos","payload_sync_nanos",
    "visible_metadata_commit_nanos","catalog_manifest_publish_nanos",
    "append_visible_journal_lock_wait_nanos",
    "append_visible_journal_encode_nanos",
    "append_visible_journal_open_nanos",
    "append_visible_journal_write_nanos",
    "append_visible_journal_sync_nanos",
    "append_visible_journal_dir_sync_nanos",
]
with (root / "append-publish-profile-summary.csv").open("w", newline="") as f:
    fields = ["mode","rtt_us","workload","concurrency","samples"]
    for name in append_fields:
        fields.extend([f"{name}_p50_ms", f"{name}_p99_ms", f"{name}_max_ms"])
    fields.extend([
        "persist_batches_started_sum","in_flight_waits_sum","coalesce_waits_sum",
        "max_batch_ticket_count_max","append_visible_journal_record_count_sum",
        "append_visible_journal_frame_bytes_sum",
    ])
    writer = csv.DictWriter(f, fieldnames=fields)
    writer.writeheader()
    for key, rows in sorted(groups.items()):
        out = {
            "mode": key[0],
            "rtt_us": key[1],
            "workload": key[2],
            "concurrency": key[3],
            "samples": len(rows),
            "persist_batches_started_sum": sum(int(to_float(r, "persist_batches_started")) for r in rows),
            "in_flight_waits_sum": sum(int(to_float(r, "in_flight_waits")) for r in rows),
            "coalesce_waits_sum": sum(int(to_float(r, "coalesce_waits")) for r in rows),
            "max_batch_ticket_count_max": max((int(to_float(r, "max_batch_ticket_count")) for r in rows), default=0),
            "append_visible_journal_record_count_sum": sum(int(to_float(r, "append_visible_journal_record_count")) for r in rows),
            "append_visible_journal_frame_bytes_sum": sum(int(to_float(r, "append_visible_journal_frame_bytes")) for r in rows),
        }
        for name in append_fields:
            values = [to_ms(r, name) for r in rows]
            out[f"{name}_p50_ms"] = percentile(values, 0.50)
            out[f"{name}_p99_ms"] = percentile(values, 0.99)
            out[f"{name}_max_ms"] = max(values, default=0.0)
        writer.writerow(out)

durable_rows = list(rows_for("durable-profile.csv"))
groups = {}
for row in durable_rows:
    key = (row["mode"], row["rtt_us"], row["workload"], row["concurrency"])
    groups.setdefault(key, []).append(row)

durable_fields = [
    "total_nanos","data_log_append_sync_nanos","data_log_file_sync_sum_nanos",
    "data_log_file_sync_max_nanos","node_catalog_publish_nanos",
    "root_sqlite_row_sync_nanos","root_sqlite_commit_nanos",
    "append_visible_journal_open_nanos","append_visible_journal_write_nanos",
    "append_visible_journal_sync_nanos","append_visible_journal_dir_sync_nanos",
]
with (root / "durable-profile-summary.csv").open("w", newline="") as f:
    fields = ["mode","rtt_us","workload","concurrency","samples"]
    for name in durable_fields:
        fields.extend([f"{name}_p50_ms", f"{name}_p99_ms", f"{name}_max_ms"])
    fields.extend([
        "data_log_sync_bytes_sum","data_log_write_bytes_sum",
        "data_log_files_synced_sum","append_visible_journal_frame_bytes_sum",
    ])
    writer = csv.DictWriter(f, fieldnames=fields)
    writer.writeheader()
    for key, rows in sorted(groups.items()):
        out = {
            "mode": key[0],
            "rtt_us": key[1],
            "workload": key[2],
            "concurrency": key[3],
            "samples": len(rows),
            "data_log_sync_bytes_sum": sum(int(to_float(r, "data_log_sync_bytes")) for r in rows),
            "data_log_write_bytes_sum": sum(int(to_float(r, "data_log_write_bytes")) for r in rows),
            "data_log_files_synced_sum": sum(int(to_float(r, "data_log_files_synced")) for r in rows),
            "append_visible_journal_frame_bytes_sum": sum(int(to_float(r, "append_visible_journal_frame_bytes")) for r in rows),
        }
        for name in durable_fields:
            values = [to_ms(r, name) for r in rows]
            out[f"{name}_p50_ms"] = percentile(values, 0.50)
            out[f"{name}_p99_ms"] = percentile(values, 0.99)
            out[f"{name}_max_ms"] = max(values, default=0.0)
        writer.writerow(out)
PY
}

setup_raid_shared
run_layout "raid-shared" "/mnt/raid/loadbench" "" ""

setup_raid_split_journal
run_layout "raid-split-journal" "/mnt/data/loadbench" "/mnt/journal/journals" ""

setup_node_private_journal
NODE_DIRS="$(csv_join_node_dirs)"
run_layout "node-private-journal" "/mnt/journal/loadbench" "/mnt/journal/journals" "${NODE_DIRS}"

teardown_storage
summarize
log "complete"
