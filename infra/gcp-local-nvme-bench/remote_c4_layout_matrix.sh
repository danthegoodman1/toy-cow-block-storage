#!/usr/bin/env bash
set -euo pipefail

RUN_ID="${1:?run id required}"
SRC_TGZ="${2:?source tarball required}"

RESULT_ROOT="/opt/results/${RUN_ID}"
SRC_DIR="/opt/src"
LOADBENCH="${SRC_DIR}/target/release/loadbench"
STORAGE_NODES="${STORAGE_NODES:-4}"
STORAGE_NODE_COUNTS="${STORAGE_NODE_COUNTS:-${STORAGE_NODES}}"
MIN_LOCAL_SSDS="${MIN_LOCAL_SSDS:-32}"
LAYOUTS="${LAYOUTS:-raid-shared,raid-split-journal,node-private-journal}"
NODE_RAID_GROUPS="${NODE_RAID_GROUPS:-}"
RTTS="${RTTS:-0,200,700,3600}"
REPEATS="${REPEATS:-1}"
RANDOMIZE_RTT_ORDER="${RANDOMIZE_RTT_ORDER:-0}"
DELAY_MODE="${DELAY_MODE:-spin}"
WARMUP_BEFORE_MEASURED="${WARMUP_BEFORE_MEASURED:-0}"
WARMUP_RTT_US="${WARMUP_RTT_US:-0}"
WARMUP_CONCURRENCY="${WARMUP_CONCURRENCY:-32}"
CONCURRENCY="${CONCURRENCY:-16,32}"
WORKLOADS="${WORKLOADS:-native-stream-publish-at-end-32m,native-stream-publish-interval-32m}"
APPEND_PUBLISH_BATCH_TARGET="${APPEND_PUBLISH_BATCH_TARGET:-4}"
APPEND_PUBLISH_IDLE_COALESCE_US="${APPEND_PUBLISH_IDLE_COALESCE_US:-250}"
APPEND_PUBLISH_MAX_COALESCE_US="${APPEND_PUBLISH_MAX_COALESCE_US:-5000}"
APPEND_INGEST_MAX_IN_FLIGHT_MIBS="${APPEND_INGEST_MAX_IN_FLIGHT_MIBS:-none}"
APPEND_INGEST_MAX_IN_FLIGHT_PER_STORAGE_NODE_MIBS="${APPEND_INGEST_MAX_IN_FLIGHT_PER_STORAGE_NODE_MIBS:-none}"
APPEND_INGEST_ACTIVE_LOG_LANES="${APPEND_INGEST_ACTIVE_LOG_LANES:-1}"
APPEND_INGEST_BACKGROUND_SYNC_WORKERS="${APPEND_INGEST_BACKGROUND_SYNC_WORKERS:-1}"
APPEND_INGEST_BACKGROUND_SYNC_WORKER_COUNTS="${APPEND_INGEST_BACKGROUND_SYNC_WORKER_COUNTS:-${APPEND_INGEST_BACKGROUND_SYNC_WORKERS}}"
APPEND_INGEST_BACKGROUND_SYNC_STEP_MIB="${APPEND_INGEST_BACKGROUND_SYNC_STEP_MIB:-}"
APPEND_INGEST_BACKGROUND_SYNC_STEP_MIBS="${APPEND_INGEST_BACKGROUND_SYNC_STEP_MIBS:-${APPEND_INGEST_BACKGROUND_SYNC_STEP_MIB}}"
STREAM_AUTO_PERSIST_MIBS="${STREAM_AUTO_PERSIST_MIBS:-32}"
STREAM_AUTO_PERSIST_MODES="${STREAM_AUTO_PERSIST_MODES:-inline-sync}"

IFS=',' read -r -a RTT_VALUES <<<"${RTTS}"
if (( ${#RTT_VALUES[@]} == 0 )); then
  printf 'RTTS must include at least one modeled RTT value\n' >&2
  exit 1
fi
IFS=',' read -r -a STORAGE_NODE_COUNT_VALUES <<<"${STORAGE_NODE_COUNTS}"
if (( ${#STORAGE_NODE_COUNT_VALUES[@]} == 0 )); then
  printf 'STORAGE_NODE_COUNTS must include at least one value\n' >&2
  exit 1
fi
MAX_STORAGE_NODES=0
for storage_node_count in "${STORAGE_NODE_COUNT_VALUES[@]}"; do
  if ! [[ "${storage_node_count}" =~ ^[0-9]+$ ]] || (( storage_node_count < 1 )); then
    printf 'STORAGE_NODE_COUNTS entries must be positive integers, got %s\n' \
      "${storage_node_count}" >&2
    exit 1
  fi
  if (( storage_node_count > MAX_STORAGE_NODES )); then
    MAX_STORAGE_NODES="${storage_node_count}"
  fi
done
if ! [[ "${REPEATS}" =~ ^[0-9]+$ ]] || (( REPEATS < 1 )); then
  printf 'REPEATS must be a positive integer, got %s\n' "${REPEATS}" >&2
  exit 1
fi

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
if (( ${#DISKS[@]} <= MAX_STORAGE_NODES )); then
  printf 'node-private layout needs one journal disk plus %s storage-node disks, found %s\n' \
    "${MAX_STORAGE_NODES}" "${#DISKS[@]}" >&2
  exit 1
fi

{
  echo "run_id=${RUN_ID}"
  echo "machine_type=$(curl -sf -H 'Metadata-Flavor: Google' http://metadata.google.internal/computeMetadata/v1/instance/machine-type | awk -F/ '{print $NF}')"
  echo "zone=$(curl -sf -H 'Metadata-Flavor: Google' http://metadata.google.internal/computeMetadata/v1/instance/zone | awk -F/ '{print $NF}')"
  echo "storage_nodes=${STORAGE_NODES}"
  echo "storage_node_counts=${STORAGE_NODE_COUNT_VALUES[*]}"
  echo "min_local_ssds=${MIN_LOCAL_SSDS}"
  echo "layouts=${LAYOUTS}"
  echo "node_raid_groups=${NODE_RAID_GROUPS}"
  echo "rtts=${RTT_VALUES[*]}"
  echo "repeats=${REPEATS}"
  echo "randomize_rtt_order=${RANDOMIZE_RTT_ORDER}"
  echo "delay_mode=${DELAY_MODE}"
  echo "warmup_before_measured=${WARMUP_BEFORE_MEASURED}"
  echo "warmup_rtt_us=${WARMUP_RTT_US}"
  echo "warmup_concurrency=${WARMUP_CONCURRENCY}"
  echo "concurrency=${CONCURRENCY}"
  echo "workloads=${WORKLOADS}"
  echo "append_publish_batch_target=${APPEND_PUBLISH_BATCH_TARGET}"
  echo "append_publish_idle_coalesce_us=${APPEND_PUBLISH_IDLE_COALESCE_US}"
  echo "append_publish_max_coalesce_us=${APPEND_PUBLISH_MAX_COALESCE_US}"
  echo "append_ingest_max_in_flight_mibs=${APPEND_INGEST_MAX_IN_FLIGHT_MIBS}"
  echo "append_ingest_max_in_flight_per_storage_node_mibs=${APPEND_INGEST_MAX_IN_FLIGHT_PER_STORAGE_NODE_MIBS}"
  echo "append_ingest_active_log_lanes=${APPEND_INGEST_ACTIVE_LOG_LANES}"
  echo "append_ingest_background_sync_worker_counts=${APPEND_INGEST_BACKGROUND_SYNC_WORKER_COUNTS}"
  echo "append_ingest_background_sync_step_mibs=${APPEND_INGEST_BACKGROUND_SYNC_STEP_MIBS}"
  echo "stream_auto_persist_mibs=${STREAM_AUTO_PERSIST_MIBS}"
  echo "stream_auto_persist_modes=${STREAM_AUTO_PERSIST_MODES}"
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
  for md_device in /dev/md*; do
    [[ -e "${md_device}" ]] || continue
    mdadm --stop "${md_device}" >/dev/null 2>&1 || true
    mdadm --remove "${md_device}" >/dev/null 2>&1 || true
  done
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

setup_node_private_raid_journal() {
  if [[ -z "${NODE_RAID_GROUPS}" ]]; then
    local data_disk_count=$(( ${#DISKS[@]} - 1 ))
    local base_group_size=$(( data_disk_count / STORAGE_NODES ))
    local remainder=$(( data_disk_count % STORAGE_NODES ))
    local generated_groups=()
    for index in $(seq 1 "${STORAGE_NODES}"); do
      local group_size="${base_group_size}"
      if (( index <= remainder )); then
        group_size=$(( group_size + 1 ))
      fi
      generated_groups+=("${group_size}")
    done
    NODE_RAID_GROUPS="$(IFS=,; printf '%s' "${generated_groups[*]}")"
    log "auto NODE_RAID_GROUPS=${NODE_RAID_GROUPS}"
  fi

  IFS=',' read -r -a groups <<<"${NODE_RAID_GROUPS}"
  if (( ${#groups[@]} != STORAGE_NODES )); then
    printf 'NODE_RAID_GROUPS must have one group per storage node: storage_nodes=%s groups=%s\n' \
      "${STORAGE_NODES}" "${NODE_RAID_GROUPS}" >&2
    exit 1
  fi

  local required_data_disks=0
  for group_size in "${groups[@]}"; do
    if ! [[ "${group_size}" =~ ^[0-9]+$ ]] || (( group_size < 1 )); then
      printf 'invalid NODE_RAID_GROUPS entry: %s\n' "${group_size}" >&2
      exit 1
    fi
    required_data_disks=$((required_data_disks + group_size))
  done
  if (( required_data_disks > ${#DISKS[@]} - 1 )); then
    printf 'node-private RAID groups need one journal disk plus %s data disks, found %s disks\n' \
      "${required_data_disks}" "${#DISKS[@]}" >&2
    exit 1
  fi

  wipe_disks
  mount_xfs "${DISKS[0]}" /mnt/journal

  local disk_offset=1
  local node_index=1
  for group_size in "${groups[@]}"; do
    local group_disks=("${DISKS[@]:${disk_offset}:${group_size}}")
    mdadm --create "/dev/md${node_index}" --level=0 --raid-devices="${group_size}" \
      --chunk=1024K "${group_disks[@]}" --run --force
    mount_xfs "/dev/md${node_index}" "/mnt/node-${node_index}"
    disk_offset=$((disk_offset + group_size))
    node_index=$((node_index + 1))
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
  local append_ingest_cap_mib="${5:-none}"
  local append_ingest_node_cap_mib="${6:-none}"
  local append_ingest_active_log_lanes="${7:-1}"
  local append_ingest_background_sync_workers="${8:-1}"
  local append_ingest_background_sync_step_mib="${9:-}"
  local stream_auto_persist_mib="${10:-32}"
  local stream_auto_persist_mode="${11:-inline-sync}"
  local append_ingest_label
  local append_ingest_args=()
  case "${append_ingest_cap_mib}" in
    ""|none|disabled|off|0)
      append_ingest_label="ingest-unlimited"
      ;;
    *)
      append_ingest_label="ingest-${append_ingest_cap_mib}m"
      append_ingest_args+=(--append-ingest-max-in-flight-mib "${append_ingest_cap_mib}")
      ;;
  esac
  case "${append_ingest_node_cap_mib}" in
    ""|none|disabled|off|0)
      ;;
    *)
      append_ingest_label="${append_ingest_label}-nodecap${append_ingest_node_cap_mib}m"
      append_ingest_args+=(--append-ingest-max-in-flight-per-storage-node-mib "${append_ingest_node_cap_mib}")
      ;;
  esac
  append_ingest_label="${append_ingest_label}-lanes${append_ingest_active_log_lanes}"
  append_ingest_args+=(--append-ingest-active-log-lanes "${append_ingest_active_log_lanes}")
  append_ingest_label="${append_ingest_label}-bg${append_ingest_background_sync_workers}"
  append_ingest_args+=(--append-ingest-background-sync-workers "${append_ingest_background_sync_workers}")
  case "${append_ingest_background_sync_step_mib}" in
    ""|none|default|disabled|off|0)
      ;;
    *)
      append_ingest_label="${append_ingest_label}-step${append_ingest_background_sync_step_mib}m"
      append_ingest_args+=(--append-ingest-background-sync-step-mib "${append_ingest_background_sync_step_mib}")
      ;;
  esac
  mode="${mode}-nodes${STORAGE_NODES}-${append_ingest_label}-autopersist-${stream_auto_persist_mib}m-${stream_auto_persist_mode}"
  local out="${RESULT_ROOT}/loadbench/${mode}"
  mkdir -p "${out}"
  {
    echo "mode=${mode}"
    echo "storage_nodes=${STORAGE_NODES}"
    echo "append_ingest_max_in_flight_mib=${append_ingest_cap_mib}"
    echo "append_ingest_max_in_flight_per_storage_node_mib=${append_ingest_node_cap_mib}"
    echo "append_ingest_active_log_lanes=${append_ingest_active_log_lanes}"
    echo "append_ingest_background_sync_workers=${append_ingest_background_sync_workers}"
    echo "append_ingest_background_sync_step_mib=${append_ingest_background_sync_step_mib}"
    echo "stream_auto_persist_mib=${stream_auto_persist_mib}"
    echo "stream_auto_persist_mode=${stream_auto_persist_mode}"
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
  if [[ "${WARMUP_BEFORE_MEASURED}" == "1" ]]; then
    local warmup_out="${out}/warmup"
    mkdir -p "${warmup_out}"
    local warmup_cmd=(
      "${LOADBENCH}"
      --provider durable
      --durability ack
      --workloads "${WORKLOADS}"
      --concurrency "${WARMUP_CONCURRENCY}"
      --warmup-ms 0
      --rtt-us "${WARMUP_RTT_US}"
      --delay-mode "${DELAY_MODE}"
      --storage-nodes "${STORAGE_NODES}"
      --files 128
      --stream-total-mib 512
      --stream-publish-mib 128
      --stream-auto-persist-mib "${stream_auto_persist_mib}"
      --stream-auto-persist-mode "${stream_auto_persist_mode}"
      --target-data-log-mib 64
      --data-log-file-sync-fanout 16
      --append-publish-batch-target "${APPEND_PUBLISH_BATCH_TARGET}"
      --append-publish-idle-coalesce-us "${APPEND_PUBLISH_IDLE_COALESCE_US}"
      --append-publish-max-coalesce-us "${APPEND_PUBLISH_MAX_COALESCE_US}"
      --root "${root}/warmup/root"
      --matrix-csv "${warmup_out}/matrix.csv"
      --durable-profile-csv "${warmup_out}/durable-profile.csv"
      --append-publish-profile-csv "${warmup_out}/append-publish-profile.csv"
      --append-ingest-profile-csv "${warmup_out}/append-ingest-profile.csv"
      "${append_ingest_args[@]}"
    )
    if [[ -n "${journal_dir}" ]]; then
      warmup_cmd+=(--append-visible-journal-dir "${journal_dir}/warmup")
    fi
    if [[ -n "${node_dirs}" ]]; then
      warmup_cmd+=(--storage-node-data-dirs "${node_dirs}")
    fi
    log "warming ${mode} rtt=${WARMUP_RTT_US} concurrency=${WARMUP_CONCURRENCY}"
    "${warmup_cmd[@]}" | tee "${warmup_out}/stdout.csv"
  fi
  for repeat in $(seq 1 "${REPEATS}"); do
    local repeat_rtts=("${RTT_VALUES[@]}")
    if [[ "${RANDOMIZE_RTT_ORDER}" == "1" ]]; then
      mapfile -t repeat_rtts < <(printf '%s\n' "${RTT_VALUES[@]}" | shuf)
    fi
    for rtt in "${repeat_rtts[@]}"; do
      local rtt_out="${out}/rtt-${rtt}-rep-${repeat}"
      mkdir -p "${rtt_out}"
      local cmd=(
        "${LOADBENCH}"
        --provider durable
        --durability ack
        --workloads "${WORKLOADS}"
        --concurrency "${CONCURRENCY}"
        --warmup-ms 0
        --rtt-us "${rtt}"
        --delay-mode "${DELAY_MODE}"
        --storage-nodes "${STORAGE_NODES}"
        --files 128
        --stream-total-mib 512
        --stream-publish-mib 128
        --stream-auto-persist-mib "${stream_auto_persist_mib}"
        --stream-auto-persist-mode "${stream_auto_persist_mode}"
        --target-data-log-mib 64
        --data-log-file-sync-fanout 16
        --append-publish-batch-target "${APPEND_PUBLISH_BATCH_TARGET}"
        --append-publish-idle-coalesce-us "${APPEND_PUBLISH_IDLE_COALESCE_US}"
        --append-publish-max-coalesce-us "${APPEND_PUBLISH_MAX_COALESCE_US}"
        --root "${root}/rtt-${rtt}-rep-${repeat}/root"
        --matrix-csv "${rtt_out}/matrix.csv"
        --durable-profile-csv "${rtt_out}/durable-profile.csv"
        --append-publish-profile-csv "${rtt_out}/append-publish-profile.csv"
        --append-ingest-profile-csv "${rtt_out}/append-ingest-profile.csv"
        "${append_ingest_args[@]}"
      )
      if [[ -n "${journal_dir}" ]]; then
        cmd+=(--append-visible-journal-dir "${journal_dir}/rtt-${rtt}-rep-${repeat}")
      fi
      if [[ -n "${node_dirs}" ]]; then
        cmd+=(--storage-node-data-dirs "${node_dirs}")
      fi
      log "running ${mode} repeat=${repeat} rtt=${rtt}"
      "${cmd[@]}" | tee "${rtt_out}/stdout.csv"
    done
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
        parts = path.parts[-2].split("-")
        rtt_effective = parts[1] if len(parts) > 1 else ""
        repeat = parts[3] if len(parts) > 3 and parts[2] == "rep" else "1"
        with path.open(newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                row["mode"] = mode
                row["rtt_us_effective"] = rtt_effective
                row["repeat"] = repeat
                yield row

def write_combined(name, output):
    rows = list(rows_for(name))
    if not rows:
        return
    fields = ["mode", "rtt_us_effective", "repeat"] + list(rows[0].keys())
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

def to_bool(row, key):
    return str(row.get(key, "")).lower() == "true"

def pvalue(rows, key, q):
    return percentile([to_float(row, key) for row in rows], q)

def max_int(rows, key):
    return max((int(to_float(row, key)) for row in rows), default=0)

write_combined("matrix.csv", "combined-matrix.csv")
write_combined("durable-profile.csv", "combined-durable-profile.csv")
write_combined("append-publish-profile.csv", "combined-append-publish-profile.csv")
write_combined("append-ingest-profile.csv", "combined-append-ingest-profile.csv")

matrix = list(rows_for("matrix.csv"))
with (root / "headline-summary.csv").open("w", newline="") as f:
    fields = [
        "mode","rtt_us","repeat","workload","concurrency","published_mbps",
        "stream_append_p99_ms","stream_publish_p99_ms","stream_final_drain_p99_ms",
        "stream_append_phase_seconds","stream_boundary_phase_seconds",
    ]
    writer = csv.DictWriter(f, fieldnames=fields)
    writer.writeheader()
    for row in matrix:
        writer.writerow({
            "mode": row["mode"],
            "rtt_us": row["rtt_us"],
            "repeat": row["repeat"],
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
    key = (row["mode"], row["rtt_us"], row["repeat"], row["workload"], row["concurrency"])
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
append_count_fields = [
    "in_flight_batches_waited",
    "wait_loops",
    "max_batch_ticket_count",
    "batch_planned_ticket_count",
    "batch_completed_ticket_count",
    "batch_same_file_skip_count",
    "post_batch_pending_ticket_count",
]
with (root / "append-publish-profile-summary.csv").open("w", newline="") as f:
    fields = ["mode","rtt_us","repeat","workload","concurrency","samples"]
    for name in append_fields:
        fields.extend([f"{name}_p50_ms", f"{name}_p99_ms", f"{name}_max_ms"])
    for name in append_count_fields:
        fields.extend([f"{name}_p50", f"{name}_p99", f"{name}_max"])
    fields.extend([
        "persist_batches_started_sum","in_flight_waits_sum",
        "in_flight_batches_waited_sum","coalesce_waits_sum",
        "batch_coalesce_hit_target_count","max_batch_ticket_count_max",
        "batch_waiter_request_count_max","batch_metadata_pending_ticket_count_max",
        "batch_coalesce_start_demand_max","batch_coalesce_end_demand_max",
        "batch_planned_ticket_count_sum","batch_completed_ticket_count_sum",
        "batch_same_file_skip_count_sum","post_batch_request_count_max",
        "post_batch_pending_ticket_count_max","append_visible_journal_record_count_sum",
        "append_visible_journal_frame_bytes_sum",
    ])
    writer = csv.DictWriter(f, fieldnames=fields)
    writer.writeheader()
    for key, rows in sorted(groups.items()):
        out = {
            "mode": key[0],
            "rtt_us": key[1],
            "repeat": key[2],
            "workload": key[3],
            "concurrency": key[4],
            "samples": len(rows),
            "persist_batches_started_sum": sum(int(to_float(r, "persist_batches_started")) for r in rows),
            "in_flight_waits_sum": sum(int(to_float(r, "in_flight_waits")) for r in rows),
            "in_flight_batches_waited_sum": sum(int(to_float(r, "in_flight_batches_waited")) for r in rows),
            "coalesce_waits_sum": sum(int(to_float(r, "coalesce_waits")) for r in rows),
            "batch_coalesce_hit_target_count": sum(1 for r in rows if to_bool(r, "batch_coalesce_hit_target")),
            "max_batch_ticket_count_max": max((int(to_float(r, "max_batch_ticket_count")) for r in rows), default=0),
            "batch_waiter_request_count_max": max((int(to_float(r, "batch_waiter_request_count")) for r in rows), default=0),
            "batch_metadata_pending_ticket_count_max": max((int(to_float(r, "batch_metadata_pending_ticket_count")) for r in rows), default=0),
            "batch_coalesce_start_demand_max": max((int(to_float(r, "batch_coalesce_start_demand")) for r in rows), default=0),
            "batch_coalesce_end_demand_max": max((int(to_float(r, "batch_coalesce_end_demand")) for r in rows), default=0),
            "batch_planned_ticket_count_sum": sum(int(to_float(r, "batch_planned_ticket_count")) for r in rows),
            "batch_completed_ticket_count_sum": sum(int(to_float(r, "batch_completed_ticket_count")) for r in rows),
            "batch_same_file_skip_count_sum": sum(int(to_float(r, "batch_same_file_skip_count")) for r in rows),
            "post_batch_request_count_max": max((int(to_float(r, "post_batch_request_count")) for r in rows), default=0),
            "post_batch_pending_ticket_count_max": max((int(to_float(r, "post_batch_pending_ticket_count")) for r in rows), default=0),
            "append_visible_journal_record_count_sum": sum(int(to_float(r, "append_visible_journal_record_count")) for r in rows),
            "append_visible_journal_frame_bytes_sum": sum(int(to_float(r, "append_visible_journal_frame_bytes")) for r in rows),
        }
        for name in append_fields:
            values = [to_ms(r, name) for r in rows]
            out[f"{name}_p50_ms"] = percentile(values, 0.50)
            out[f"{name}_p99_ms"] = percentile(values, 0.99)
            out[f"{name}_max_ms"] = max(values, default=0.0)
        for name in append_count_fields:
            values = [to_float(r, name) for r in rows]
            out[f"{name}_p50"] = percentile(values, 0.50)
            out[f"{name}_p99"] = percentile(values, 0.99)
            out[f"{name}_max"] = max(values, default=0.0)
        writer.writerow(out)

append_ingest_rows = list(rows_for("append-ingest-profile.csv"))
groups = {}
for row in append_ingest_rows:
    key = (row["mode"], row["rtt_us"], row["repeat"], row["workload"], row["concurrency"])
    groups.setdefault(key, []).append(row)

append_ingest_fields = [
    "total_nanos",
    "admission_wait_nanos",
    "stream_lock_wait_nanos",
    "pending_lock_wait_nanos",
    "active_log_lock_wait_nanos",
    "metadata_prepare_nanos",
    "metadata_record_nanos",
    "payload_encode_nanos",
    "payload_write_nanos",
    "auto_persist_nanos",
    "auto_persist_target_nanos",
    "auto_persist_pending_nanos",
    "auto_persist_sync_nanos",
    "auto_persist_sync_file_nanos",
    "auto_persist_sync_file_max_nanos",
    "auto_persist_sync_dir_nanos",
    "auto_persist_mark_nanos",
    "auto_persist_request_nanos",
    "auto_persist_wait_nanos",
]
with (root / "append-ingest-profile-summary.csv").open("w", newline="") as f:
    fields = ["mode","rtt_us","repeat","workload","concurrency","samples"]
    for name in append_ingest_fields:
        fields.extend([f"{name}_p50_ms", f"{name}_p99_ms", f"{name}_max_ms"])
    fields.extend([
        "payload_bytes_sum",
        "background_sync_requested_bytes_sum",
        "background_sync_request_count_sum",
        "background_sync_step_bytes_max",
        "max_in_flight_bytes_max",
        "max_in_flight_bytes_per_storage_node_max",
        "active_log_lanes_max",
        "auto_persist_target_bytes_max",
        "auto_persist_wait_target_bytes_max",
        "auto_persist_pending_log_refs_max",
        "auto_persist_pending_storage_nodes_max",
        "auto_persist_sync_bytes_sum",
        "auto_persist_files_synced_sum",
        "auto_persist_sync_success_count",
        "auto_persist_request_submitted_count",
        "auto_persist_observed_synced_bytes_max",
        "auto_persist_marked_bytes_max",
        "success_count",
    ])
    writer = csv.DictWriter(f, fieldnames=fields)
    writer.writeheader()
    for key, rows in sorted(groups.items()):
        out = {
            "mode": key[0],
            "rtt_us": key[1],
            "repeat": key[2],
            "workload": key[3],
            "concurrency": key[4],
            "samples": len(rows),
            "payload_bytes_sum": sum(int(to_float(r, "payload_bytes")) for r in rows),
            "background_sync_requested_bytes_sum": sum(int(to_float(r, "background_sync_requested_bytes")) for r in rows),
            "background_sync_request_count_sum": sum(int(to_float(r, "background_sync_request_count")) for r in rows),
            "background_sync_step_bytes_max": max((int(to_float(r, "background_sync_step_bytes")) for r in rows), default=0),
            "max_in_flight_bytes_max": max((int(to_float(r, "max_in_flight_bytes")) for r in rows), default=0),
            "max_in_flight_bytes_per_storage_node_max": max((int(to_float(r, "max_in_flight_bytes_per_storage_node")) for r in rows), default=0),
            "active_log_lanes_max": max((int(to_float(r, "active_log_lanes")) for r in rows), default=0),
            "auto_persist_target_bytes_max": max((int(to_float(r, "auto_persist_target_bytes")) for r in rows), default=0),
            "auto_persist_wait_target_bytes_max": max((int(to_float(r, "auto_persist_wait_target_bytes")) for r in rows), default=0),
            "auto_persist_pending_log_refs_max": max((int(to_float(r, "auto_persist_pending_log_refs")) for r in rows), default=0),
            "auto_persist_pending_storage_nodes_max": max((int(to_float(r, "auto_persist_pending_storage_nodes")) for r in rows), default=0),
            "auto_persist_sync_bytes_sum": sum(int(to_float(r, "auto_persist_sync_bytes")) for r in rows),
            "auto_persist_files_synced_sum": sum(int(to_float(r, "auto_persist_files_synced")) for r in rows),
            "auto_persist_sync_success_count": sum(1 for r in rows if to_bool(r, "auto_persist_sync_success")),
            "auto_persist_request_submitted_count": sum(1 for r in rows if to_bool(r, "auto_persist_request_submitted")),
            "auto_persist_observed_synced_bytes_max": max((int(to_float(r, "auto_persist_observed_synced_bytes")) for r in rows), default=0),
            "auto_persist_marked_bytes_max": max((int(to_float(r, "auto_persist_marked_bytes")) for r in rows), default=0),
            "success_count": sum(1 for r in rows if to_bool(r, "success")),
        }
        for name in append_ingest_fields:
            values = [to_ms(r, name) for r in rows]
            out[f"{name}_p50_ms"] = percentile(values, 0.50)
            out[f"{name}_p99_ms"] = percentile(values, 0.99)
            out[f"{name}_max_ms"] = max(values, default=0.0)
        writer.writerow(out)

driver_batch_rows = [
    row for row in append_rows if int(to_float(row, "persist_batches_started")) > 0
]
with (root / "append-publish-driver-batches.csv").open("w", newline="") as f:
    fields = [
        "mode","rtt_us","repeat","workload","concurrency","sequence","append_publish_batch_id",
        "total_ms","coordinator_wait_ms","in_flight_wait_ms","coalesce_wait_ms",
        "persist_batch_ms","visible_metadata_commit_ms","append_visible_journal_sync_ms",
        "max_batch_ticket_count","batch_waiter_request_count",
        "batch_metadata_pending_ticket_count","batch_coalesce_start_demand",
        "batch_coalesce_end_demand","batch_coalesce_hit_target",
        "batch_planned_ticket_count","batch_completed_ticket_count",
        "batch_same_file_skip_count","post_batch_request_count",
        "post_batch_pending_ticket_count",
    ]
    writer = csv.DictWriter(f, fieldnames=fields)
    writer.writeheader()
    for row in driver_batch_rows:
        writer.writerow({
            "mode": row["mode"],
            "rtt_us": row["rtt_us"],
            "repeat": row["repeat"],
            "workload": row["workload"],
            "concurrency": row["concurrency"],
            "sequence": row["sequence"],
            "append_publish_batch_id": row["append_publish_batch_id"],
            "total_ms": to_ms(row, "total_nanos"),
            "coordinator_wait_ms": to_ms(row, "coordinator_wait_nanos"),
            "in_flight_wait_ms": to_ms(row, "in_flight_wait_nanos"),
            "coalesce_wait_ms": to_ms(row, "coalesce_wait_nanos"),
            "persist_batch_ms": to_ms(row, "persist_batch_nanos"),
            "visible_metadata_commit_ms": to_ms(row, "visible_metadata_commit_nanos"),
            "append_visible_journal_sync_ms": to_ms(row, "append_visible_journal_sync_nanos"),
            "max_batch_ticket_count": int(to_float(row, "max_batch_ticket_count")),
            "batch_waiter_request_count": int(to_float(row, "batch_waiter_request_count")),
            "batch_metadata_pending_ticket_count": int(to_float(row, "batch_metadata_pending_ticket_count")),
            "batch_coalesce_start_demand": int(to_float(row, "batch_coalesce_start_demand")),
            "batch_coalesce_end_demand": int(to_float(row, "batch_coalesce_end_demand")),
            "batch_coalesce_hit_target": row.get("batch_coalesce_hit_target", "false"),
            "batch_planned_ticket_count": int(to_float(row, "batch_planned_ticket_count")),
            "batch_completed_ticket_count": int(to_float(row, "batch_completed_ticket_count")),
            "batch_same_file_skip_count": int(to_float(row, "batch_same_file_skip_count")),
            "post_batch_request_count": int(to_float(row, "post_batch_request_count")),
            "post_batch_pending_ticket_count": int(to_float(row, "post_batch_pending_ticket_count")),
        })

durable_rows = list(rows_for("durable-profile.csv"))
groups = {}
for row in durable_rows:
    key = (row["mode"], row["rtt_us"], row["repeat"], row["workload"], row["concurrency"])
    groups.setdefault(key, []).append(row)

durable_fields = [
    "total_nanos","data_log_append_sync_nanos","data_log_file_sync_sum_nanos",
    "data_log_file_sync_max_nanos","node_catalog_publish_nanos",
    "root_sqlite_row_sync_nanos","root_sqlite_commit_nanos",
    "append_visible_journal_open_nanos","append_visible_journal_write_nanos",
    "append_visible_journal_sync_nanos","append_visible_journal_dir_sync_nanos",
]
with (root / "durable-profile-summary.csv").open("w", newline="") as f:
    fields = ["mode","rtt_us","repeat","workload","concurrency","samples"]
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
            "repeat": key[2],
            "workload": key[3],
            "concurrency": key[4],
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

def parse_mount_sources(mode):
    layout = root / mode / "layout.txt"
    journal = None
    data = []
    if not layout.exists():
        return journal, data
    for line in layout.read_text(errors="ignore").splitlines():
        parts = line.split()
        if len(parts) < 2:
            continue
        target = parts[0].lstrip("├─└─│")
        source = parts[1]
        if target == "/mnt/journal":
            journal = source
        elif target in ("/mnt/data", "/mnt/raid") or target.startswith("/mnt/node-"):
            data.append(source)
    return journal, data

def device_name(source):
    if not source:
        return ""
    return source.rsplit("/", 1)[-1]

iostat_fields = [
    "device","r/s","rMB/s","rrqm/s","%rrqm","r_await","rareq-sz",
    "w/s","wMB/s","wrqm/s","%wrqm","w_await","wareq-sz",
    "d/s","dMB/s","drqm/s","%drqm","d_await","dareq-sz",
    "f/s","f_await","aqu-sz","%util",
]

def iostat_rows(mode):
    path = root.parent / "monitor" / mode / "iostat.log"
    out = []
    if not path.exists():
        return out
    for line in path.read_text(errors="ignore").splitlines():
        if not line or line.startswith(("Linux", "Device")):
            continue
        parts = line.split()
        if len(parts) != len(iostat_fields):
            continue
        if not (parts[0].startswith("md") or parts[0].startswith("nvme")):
            continue
        try:
            out.append({
                field: (parts[index] if field == "device" else float(parts[index]))
                for index, field in enumerate(iostat_fields)
            })
        except ValueError:
            continue
    return out

def summarize_devices(rows, devices):
    summaries = []
    for device in devices:
        selected = [row for row in rows if row["device"] == device]
        active = [row for row in selected if row["wMB/s"] > 1.0]
        if not active:
            active = selected
        if not active:
            continue
        summaries.append({
            "device": device,
            "samples": len(active),
            "wMBps_p50": pvalue(active, "wMB/s", 0.50),
            "wMBps_p90": pvalue(active, "wMB/s", 0.90),
            "wMBps_max": max((row["wMB/s"] for row in active), default=0.0),
            "w_await_p50_ms": pvalue(active, "w_await", 0.50),
            "w_await_p90_ms": pvalue(active, "w_await", 0.90),
            "w_await_max_ms": max((row["w_await"] for row in active), default=0.0),
            "util_p50": pvalue(active, "%util", 0.50),
            "util_p90": pvalue(active, "%util", 0.90),
            "util_max": max((row["%util"] for row in active), default=0.0),
        })
    return summaries

modes = sorted({row["mode"] for row in matrix} | {row["mode"] for row in append_rows})
device_summaries = {}
for mode in modes:
    journal_source, data_sources = parse_mount_sources(mode)
    rows_iostat = iostat_rows(mode)
    data_devices = [device_name(source) for source in data_sources]
    journal_device = device_name(journal_source)
    if not data_devices and mode.startswith("raid-"):
        data_devices = ["md0"]
    data_summary = summarize_devices(rows_iostat, list(dict.fromkeys(data_devices)))
    journal_summary = summarize_devices(rows_iostat, [journal_device] if journal_device else [])
    device_summaries[mode] = {
        "journal_device": journal_device,
        "data_devices": ",".join(data_devices),
        "data_device_count": len(data_devices),
        "data_wMBps_p90_sum": sum(item["wMBps_p90"] for item in data_summary),
        "data_w_await_p90_max_ms": max((item["w_await_p90_ms"] for item in data_summary), default=0.0),
        "data_util_p90_max": max((item["util_p90"] for item in data_summary), default=0.0),
        "journal_wMBps_p90": max((item["wMBps_p90"] for item in journal_summary), default=0.0),
        "journal_w_await_p90_ms": max((item["w_await_p90_ms"] for item in journal_summary), default=0.0),
        "journal_util_p90": max((item["util_p90"] for item in journal_summary), default=0.0),
    }

headline_by_key = {
    (row["mode"], row["rtt_us"], row["repeat"], row["workload"], row["concurrency"]): row
    for row in csv.DictReader((root / "headline-summary.csv").open())
}
append_summary_by_key = {
    (row["mode"], row["rtt_us"], row["repeat"], row["workload"], row["concurrency"]): row
    for row in csv.DictReader((root / "append-publish-profile-summary.csv").open())
}
append_ingest_summary_path = root / "append-ingest-profile-summary.csv"
append_ingest_summary_by_key = {
    (row["mode"], row["rtt_us"], row["repeat"], row["workload"], row["concurrency"]): row
    for row in csv.DictReader(append_ingest_summary_path.open())
} if append_ingest_summary_path.exists() else {}
durable_summary_by_key = {
    (row["mode"], row["rtt_us"], row["repeat"], row["workload"], row["concurrency"]): row
    for row in csv.DictReader((root / "durable-profile-summary.csv").open())
}

def ratio(numerator, denominator):
    return numerator / denominator if denominator > 0 else 0.0

def classify_publish_tail(row, append):
    publish_p99 = to_float(row, "stream_publish_p99_ms")
    total = to_float(append, "total_nanos_p99_ms")
    sync = to_float(append, "append_visible_journal_sync_nanos_p99_ms")
    inflight = to_float(append, "in_flight_wait_nanos_p99_ms")
    coalesce = to_float(append, "coalesce_wait_nanos_p99_ms")
    persist = to_float(append, "persist_batch_nanos_p99_ms")
    same_file_skips = int(to_float(append, "batch_same_file_skip_count_sum"))
    batches_waited_p99 = to_float(append, "in_flight_batches_waited_p99")
    sync_ratio = ratio(sync, total)
    inflight_ratio = ratio(inflight, total)
    coalesce_ratio = ratio(coalesce, total)
    persist_ratio = ratio(persist, total)

    if publish_p99 < 10.0 and total < 10.0:
        return (
            "publish_tail_within_target",
            "publish tail is already single digit; optimize throughput or device queueing first",
        )
    if same_file_skips > max(16, int(to_float(append, "samples")) // 8):
        return (
            "same_file_serialization",
            "same-file publish ordering is dominating; parallelism cannot safely improve this workload without changing semantics",
        )
    if sync_ratio >= 0.65 and sync >= 8.0:
        return (
            "journal_sync_bound",
            "reduce or shard append-visible journal syncs; batching policy alone will not remove the tail",
        )
    if inflight_ratio >= 0.50 and batches_waited_p99 >= 2.0:
        return (
            "publish_lane_serialization",
            "remove the single append-visible publish lane with sharded publish lanes or use a layout that avoids cross-node publish contention",
        )
    if coalesce_ratio >= 0.25:
        return (
            "coalesce_policy_bound",
            "tune adaptive coalescing or the target batch threshold",
        )
    if persist_ratio >= 0.60:
        return (
            "durable_persist_bound",
            "split persist work into independent durable lanes or reduce per-batch durable work",
        )
    return (
        "sync_floor_or_mixed",
        "no single dominant software wait remains; compare against device baseline before changing coordinator policy",
    )

def classify_throughput(row, devices):
    data_util = devices["data_util_p90_max"]
    data_await = devices["data_w_await_p90_max_ms"]
    journal_util = devices["journal_util_p90"]
    journal_await = devices["journal_w_await_p90_ms"]
    published_mbps = to_float(row, "published_mbps")
    append_p99 = to_float(row, "stream_append_p99_ms")
    publish_p99 = to_float(row, "stream_publish_p99_ms")

    if data_util >= 90.0 and data_await >= 20.0:
        return (
            "data_device_bound",
            "increase data-device parallelism; for node-private layouts use per-node RAID groups instead of one SSD per node",
        )
    if journal_util >= 80.0 and journal_await >= 8.0:
        return (
            "journal_device_bound",
            "move the visible journal off the contended device or shard the visible journal",
        )
    if append_p99 > publish_p99 * 10 and published_mbps < 3000:
        return (
            "data_device_bound",
            "append throughput is limiting published throughput; increase per-node data device bandwidth",
        )
    return (
        "not_device_limited",
        "throughput is not clearly device-limited by the collected iostat counters",
    )

def combined_classification(row, append, publish_tail, throughput):
    publish_p99 = to_float(row, "stream_publish_p99_ms")
    append_p99 = to_float(row, "stream_append_p99_ms")
    if publish_tail[0] in ("journal_sync_bound", "publish_lane_serialization", "coalesce_policy_bound") and publish_p99 >= 10.0:
        return publish_tail
    if throughput[0] == "data_device_bound" and append_p99 >= publish_p99 * 4:
        return throughput
    if publish_tail[0] not in ("publish_tail_within_target", "sync_floor_or_mixed"):
        return publish_tail
    return throughput

with (root / "bottleneck-summary.csv").open("w", newline="") as f:
    fields = [
        "mode","rtt_us","repeat","workload","concurrency","published_mbps",
        "stream_append_p99_ms","stream_publish_p99_ms",
        "bottleneck","publish_tail_bottleneck","throughput_bottleneck",
        "recommended_next_action",
        "total_p99_ms","journal_sync_p99_ms","journal_sync_ratio",
        "in_flight_wait_p99_ms","in_flight_wait_ratio",
        "in_flight_batches_waited_p99","coalesce_wait_p99_ms",
        "coalesce_wait_ratio","persist_batch_p99_ms","persist_batch_ratio",
        "max_batch_ticket_count_max","post_batch_pending_ticket_count_max",
        "same_file_skip_count_sum","data_devices","data_device_count",
        "data_wMBps_p90_sum","data_w_await_p90_max_ms","data_util_p90_max",
        "journal_device","journal_wMBps_p90","journal_w_await_p90_ms",
        "journal_util_p90",
        "append_ingest_total_p99_ms",
        "append_ingest_admission_wait_p99_ms",
        "append_ingest_active_log_lock_wait_p99_ms",
        "append_ingest_payload_write_p99_ms",
        "append_ingest_auto_persist_p99_ms",
        "append_ingest_auto_persist_sync_p99_ms",
        "append_ingest_auto_persist_sync_file_p99_ms",
        "append_ingest_auto_persist_sync_file_max_p99_ms",
        "append_ingest_auto_persist_sync_dir_p99_ms",
        "append_ingest_auto_persist_mark_p99_ms",
        "append_ingest_auto_persist_request_p99_ms",
        "append_ingest_auto_persist_wait_p99_ms",
        "append_ingest_auto_persist_sync_bytes_sum",
        "append_ingest_auto_persist_files_synced_sum",
        "append_ingest_auto_persist_target_bytes_max",
        "append_ingest_auto_persist_wait_target_bytes_max",
        "append_ingest_auto_persist_pending_log_refs_max",
        "append_ingest_auto_persist_pending_storage_nodes_max",
        "append_ingest_auto_persist_sync_success_count",
        "append_ingest_auto_persist_request_submitted_count",
        "append_ingest_auto_persist_observed_synced_bytes",
        "append_ingest_auto_persist_marked_bytes",
        "append_ingest_background_sync_requested_bytes_sum",
        "append_ingest_background_sync_request_count_sum",
        "append_ingest_background_sync_step_bytes",
        "append_ingest_max_in_flight_bytes",
        "append_ingest_max_in_flight_per_storage_node_bytes",
        "append_ingest_active_log_lanes",
    ]
    writer = csv.DictWriter(f, fieldnames=fields)
    writer.writeheader()
    for key, row in sorted(headline_by_key.items()):
        append = append_summary_by_key.get(key, {})
        ingest = append_ingest_summary_by_key.get(key, {})
        durable = durable_summary_by_key.get(key, {})
        devices = device_summaries.get(key[0], {
            "journal_device": "",
            "data_devices": "",
            "data_device_count": 0,
            "data_wMBps_p90_sum": 0.0,
            "data_w_await_p90_max_ms": 0.0,
            "data_util_p90_max": 0.0,
            "journal_wMBps_p90": 0.0,
            "journal_w_await_p90_ms": 0.0,
            "journal_util_p90": 0.0,
        })
        publish_tail = classify_publish_tail(row, append)
        throughput = classify_throughput(row, devices)
        bottleneck, action = combined_classification(row, append, publish_tail, throughput)
        total = to_float(append, "total_nanos_p99_ms")
        sync = to_float(append, "append_visible_journal_sync_nanos_p99_ms")
        inflight = to_float(append, "in_flight_wait_nanos_p99_ms")
        coalesce = to_float(append, "coalesce_wait_nanos_p99_ms")
        persist = to_float(append, "persist_batch_nanos_p99_ms")
        writer.writerow({
            "mode": key[0],
            "rtt_us": key[1],
            "repeat": key[2],
            "workload": key[3],
            "concurrency": key[4],
            "published_mbps": row["published_mbps"],
            "stream_append_p99_ms": row["stream_append_p99_ms"],
            "stream_publish_p99_ms": row["stream_publish_p99_ms"],
            "bottleneck": bottleneck,
            "publish_tail_bottleneck": publish_tail[0],
            "throughput_bottleneck": throughput[0],
            "recommended_next_action": action,
            "total_p99_ms": total,
            "journal_sync_p99_ms": sync,
            "journal_sync_ratio": ratio(sync, total),
            "in_flight_wait_p99_ms": inflight,
            "in_flight_wait_ratio": ratio(inflight, total),
            "in_flight_batches_waited_p99": to_float(append, "in_flight_batches_waited_p99"),
            "coalesce_wait_p99_ms": coalesce,
            "coalesce_wait_ratio": ratio(coalesce, total),
            "persist_batch_p99_ms": persist,
            "persist_batch_ratio": ratio(persist, total),
            "max_batch_ticket_count_max": append.get("max_batch_ticket_count_max", 0),
            "post_batch_pending_ticket_count_max": append.get("post_batch_pending_ticket_count_max", 0),
            "same_file_skip_count_sum": append.get("batch_same_file_skip_count_sum", 0),
            "data_devices": devices["data_devices"],
            "data_device_count": devices["data_device_count"],
            "data_wMBps_p90_sum": devices["data_wMBps_p90_sum"],
            "data_w_await_p90_max_ms": devices["data_w_await_p90_max_ms"],
            "data_util_p90_max": devices["data_util_p90_max"],
            "journal_device": devices["journal_device"],
            "journal_wMBps_p90": devices["journal_wMBps_p90"],
            "journal_w_await_p90_ms": devices["journal_w_await_p90_ms"],
            "journal_util_p90": devices["journal_util_p90"],
            "append_ingest_total_p99_ms": ingest.get("total_nanos_p99_ms", 0),
            "append_ingest_admission_wait_p99_ms": ingest.get("admission_wait_nanos_p99_ms", 0),
            "append_ingest_active_log_lock_wait_p99_ms": ingest.get("active_log_lock_wait_nanos_p99_ms", 0),
            "append_ingest_payload_write_p99_ms": ingest.get("payload_write_nanos_p99_ms", 0),
            "append_ingest_auto_persist_p99_ms": ingest.get("auto_persist_nanos_p99_ms", 0),
            "append_ingest_auto_persist_sync_p99_ms": ingest.get("auto_persist_sync_nanos_p99_ms", 0),
            "append_ingest_auto_persist_sync_file_p99_ms": ingest.get("auto_persist_sync_file_nanos_p99_ms", 0),
            "append_ingest_auto_persist_sync_file_max_p99_ms": ingest.get("auto_persist_sync_file_max_nanos_p99_ms", 0),
            "append_ingest_auto_persist_sync_dir_p99_ms": ingest.get("auto_persist_sync_dir_nanos_p99_ms", 0),
            "append_ingest_auto_persist_mark_p99_ms": ingest.get("auto_persist_mark_nanos_p99_ms", 0),
            "append_ingest_auto_persist_request_p99_ms": ingest.get("auto_persist_request_nanos_p99_ms", 0),
            "append_ingest_auto_persist_wait_p99_ms": ingest.get("auto_persist_wait_nanos_p99_ms", 0),
            "append_ingest_auto_persist_sync_bytes_sum": ingest.get("auto_persist_sync_bytes_sum", 0),
            "append_ingest_auto_persist_files_synced_sum": ingest.get("auto_persist_files_synced_sum", 0),
            "append_ingest_auto_persist_target_bytes_max": ingest.get("auto_persist_target_bytes_max", 0),
            "append_ingest_auto_persist_wait_target_bytes_max": ingest.get("auto_persist_wait_target_bytes_max", 0),
            "append_ingest_auto_persist_pending_log_refs_max": ingest.get("auto_persist_pending_log_refs_max", 0),
            "append_ingest_auto_persist_pending_storage_nodes_max": ingest.get("auto_persist_pending_storage_nodes_max", 0),
            "append_ingest_auto_persist_sync_success_count": ingest.get("auto_persist_sync_success_count", 0),
            "append_ingest_auto_persist_request_submitted_count": ingest.get("auto_persist_request_submitted_count", 0),
            "append_ingest_auto_persist_observed_synced_bytes": ingest.get("auto_persist_observed_synced_bytes_max", 0),
            "append_ingest_auto_persist_marked_bytes": ingest.get("auto_persist_marked_bytes_max", 0),
            "append_ingest_background_sync_requested_bytes_sum": ingest.get("background_sync_requested_bytes_sum", 0),
            "append_ingest_background_sync_request_count_sum": ingest.get("background_sync_request_count_sum", 0),
            "append_ingest_background_sync_step_bytes": ingest.get("background_sync_step_bytes_max", 0),
            "append_ingest_max_in_flight_bytes": ingest.get("max_in_flight_bytes_max", 0),
            "append_ingest_max_in_flight_per_storage_node_bytes": ingest.get("max_in_flight_bytes_per_storage_node_max", 0),
            "append_ingest_active_log_lanes": ingest.get("active_log_lanes_max", 0),
        })
PY
}

IFS=',' read -r -a requested_layouts <<<"${LAYOUTS}"
IFS=',' read -r -a requested_append_ingest_caps <<<"${APPEND_INGEST_MAX_IN_FLIGHT_MIBS}"
if (( ${#requested_append_ingest_caps[@]} == 0 )); then
  requested_append_ingest_caps=("none")
fi
IFS=',' read -r -a requested_append_ingest_node_caps <<<"${APPEND_INGEST_MAX_IN_FLIGHT_PER_STORAGE_NODE_MIBS}"
if (( ${#requested_append_ingest_node_caps[@]} == 0 )); then
  requested_append_ingest_node_caps=("none")
fi
IFS=',' read -r -a requested_append_ingest_active_log_lanes <<<"${APPEND_INGEST_ACTIVE_LOG_LANES}"
if (( ${#requested_append_ingest_active_log_lanes[@]} == 0 )); then
  requested_append_ingest_active_log_lanes=("1")
fi
IFS=',' read -r -a requested_append_ingest_background_sync_workers <<<"${APPEND_INGEST_BACKGROUND_SYNC_WORKER_COUNTS}"
if (( ${#requested_append_ingest_background_sync_workers[@]} == 0 )); then
  requested_append_ingest_background_sync_workers=("1")
fi
IFS=',' read -r -a requested_append_ingest_background_sync_steps <<<"${APPEND_INGEST_BACKGROUND_SYNC_STEP_MIBS}"
if (( ${#requested_append_ingest_background_sync_steps[@]} == 0 )); then
  requested_append_ingest_background_sync_steps=("")
fi
IFS=',' read -r -a requested_stream_auto_persist_mibs <<<"${STREAM_AUTO_PERSIST_MIBS}"
if (( ${#requested_stream_auto_persist_mibs[@]} == 0 )); then
  requested_stream_auto_persist_mibs=("32")
fi
IFS=',' read -r -a requested_stream_auto_persist_modes <<<"${STREAM_AUTO_PERSIST_MODES}"
if (( ${#requested_stream_auto_persist_modes[@]} == 0 )); then
  requested_stream_auto_persist_modes=("inline-sync")
fi

run_layout_policy_matrix() {
  local mode="$1"
  local root="$2"
  local journal_dir="${3:-}"
  local node_dirs="${4:-}"
  for append_ingest_cap in "${requested_append_ingest_caps[@]}"; do
    for append_ingest_node_cap in "${requested_append_ingest_node_caps[@]}"; do
      for append_ingest_active_log_lanes in "${requested_append_ingest_active_log_lanes[@]}"; do
        for append_ingest_background_sync_workers in "${requested_append_ingest_background_sync_workers[@]}"; do
          for append_ingest_background_sync_step in "${requested_append_ingest_background_sync_steps[@]}"; do
            for stream_auto_persist_mib in "${requested_stream_auto_persist_mibs[@]}"; do
              for stream_auto_persist_mode in "${requested_stream_auto_persist_modes[@]}"; do
                run_layout \
                  "${mode}" \
                  "${root}" \
                  "${journal_dir}" \
                  "${node_dirs}" \
                  "${append_ingest_cap}" \
                  "${append_ingest_node_cap}" \
                  "${append_ingest_active_log_lanes}" \
                  "${append_ingest_background_sync_workers}" \
                  "${append_ingest_background_sync_step}" \
                  "${stream_auto_persist_mib}" \
                  "${stream_auto_persist_mode}"
              done
            done
          done
        done
      done
    done
  done
}

base_node_raid_groups="${NODE_RAID_GROUPS}"
for layout in "${requested_layouts[@]}"; do
  for storage_node_count in "${STORAGE_NODE_COUNT_VALUES[@]}"; do
    STORAGE_NODES="${storage_node_count}"
    NODE_RAID_GROUPS="${base_node_raid_groups}"
    case "${layout}" in
      raid-shared)
        setup_raid_shared
        run_layout_policy_matrix "raid-shared" "/mnt/raid/loadbench" "" ""
        ;;
      raid-split-journal)
        setup_raid_split_journal
        run_layout_policy_matrix "raid-split-journal" "/mnt/data/loadbench" "/mnt/journal/journals" ""
        ;;
      node-private-journal)
        setup_node_private_journal
        NODE_DIRS="$(csv_join_node_dirs)"
        run_layout_policy_matrix "node-private-journal" "/mnt/journal/loadbench" "/mnt/journal/journals" "${NODE_DIRS}"
        ;;
      node-private-raid-journal)
        setup_node_private_raid_journal
        NODE_DIRS="$(csv_join_node_dirs)"
        run_layout_policy_matrix "node-private-raid-journal" "/mnt/journal/loadbench" "/mnt/journal/journals" "${NODE_DIRS}"
        ;;
      *)
        printf 'unknown layout: %s\n' "${layout}" >&2
        exit 1
        ;;
    esac
  done
done

teardown_storage
summarize
log "complete"
