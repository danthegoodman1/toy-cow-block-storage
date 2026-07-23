#!/usr/bin/env bash
set -euo pipefail

# Phase 2 confirm-trip A/B: two source builds (base vs fix) on one instance,
# interleaved per rep so instance drift cannot masquerade as a delta. Runs
# only the confirm menu: 64k write gate rows, block-read-4k/1m rows, and a
# 4k collateral guard. No Ceph, no chunk sweep, no other sizes.

RUN_ID="${1:?run id required}"
BASE_TGZ="${2:?base source tarball required}"
FIX_TGZ="${3:?fix source tarball required}"

RESULT_ROOT="/opt/results/${RUN_ID}"
SRC_BASE_DIR="/opt/src-base"
SRC_FIX_DIR="/opt/src-fix"
LOADBENCH_BASE="${LOADBENCH_BASE:-${SRC_BASE_DIR}/target/release/loadbench}"
LOADBENCH_FIX="${LOADBENCH_FIX:-${SRC_FIX_DIR}/target/release/loadbench}"

MIN_LOCAL_SSDS="${MIN_LOCAL_SSDS:-5}"
STORAGE_NODES="${STORAGE_NODES:-5}"
AB_REPEATS="${AB_REPEATS:-2}"
WRITE_CONCURRENCY="${WRITE_CONCURRENCY:-16,32}"
READ_CONCURRENCY="${READ_CONCURRENCY:-16,32}"
GUARD_CONCURRENCY="${GUARD_CONCURRENCY:-16,32}"
TOY_DURABLE_IO_BACKEND="${TOY_DURABLE_IO_BACKEND:-filesystem}"
TOY_CHUNK_MIB="${TOY_CHUNK_MIB:-2}"
DELAY_MODE="${DELAY_MODE:-spin}"
DURATION_MS="${DURATION_MS:-5000}"
WARMUP_MS="${WARMUP_MS:-1000}"
DEVICE_BLOCKS="${DEVICE_BLOCKS:-1048576}"
SHARDS="${SHARDS:-64}"
BASE_REF="${BASE_REF:-unknown}"
FIX_REF="${FIX_REF:-unknown}"
DRY_RUN="${DRY_RUN:-0}"

mkdir -p "${RESULT_ROOT}/base" "${RESULT_ROOT}/fix" "${RESULT_ROOT}/monitor"

archive_results() {
  local status=$?
  tar -C "/opt/results" -czf "/tmp/${RUN_ID}-results.tgz" "${RUN_ID}" || true
  exit "${status}"
}
trap archive_results EXIT

log() {
  printf '[%s] %s\n' "$(date -Is)" "$*"
}

size_bytes() {
  case "$1" in
    4k) printf '%s\n' 4096 ;;
    64k) printf '%s\n' 65536 ;;
    *)
      printf 'unknown IO size %s\n' "$1" >&2
      return 1
      ;;
  esac
}

loadbench_for_side() {
  case "$1" in
    base) printf '%s' "${LOADBENCH_BASE}" ;;
    fix) printf '%s' "${LOADBENCH_FIX}" ;;
    *)
      printf 'unknown side %s\n' "$1" >&2
      return 1
      ;;
  esac
}

if [[ "${DRY_RUN}" != "1" ]]; then
  log "installing packages"
  export DEBIAN_FRONTEND=noninteractive
  apt-get update -y
  apt-get install -y ca-certificates curl build-essential pkg-config libssl-dev \
    libsqlite3-dev xfsprogs sysstat python3 jq

  if ! command -v cargo >/dev/null 2>&1; then
    log "installing rust toolchain"
    curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs \
      | sh -s -- -y --profile minimal
    # shellcheck disable=SC1091
    source "${HOME}/.cargo/env"
  fi

  log "extracting and building base tree"
  rm -rf "${SRC_BASE_DIR}"
  mkdir -p "${SRC_BASE_DIR}"
  tar -C "${SRC_BASE_DIR}" -xzf "${BASE_TGZ}"
  (cd "${SRC_BASE_DIR}" && cargo build --release --bin loadbench)

  log "extracting and building fix tree"
  rm -rf "${SRC_FIX_DIR}"
  mkdir -p "${SRC_FIX_DIR}"
  tar -C "${SRC_FIX_DIR}" -xzf "${FIX_TGZ}"
  (cd "${SRC_FIX_DIR}" && cargo build --release --bin loadbench)

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
else
  DISKS=(/dev/dry-run-only)
fi

# Disk 0 is the journal disk. Nodes get dedicated disks when there are
# enough; when there is exactly one disk fewer than nodes, the last node
# colocates with the journal so every spindle carries data.
COLOCATED_NODE=0
if (( ${#DISKS[@]} == STORAGE_NODES )); then
  COLOCATED_NODE="${STORAGE_NODES}"
elif (( ${#DISKS[@]} < STORAGE_NODES )) && [[ "${DRY_RUN}" != "1" ]]; then
  printf 'toy layout needs at least %s disks for %s storage nodes, found %s\n' \
    "${STORAGE_NODES}" "${STORAGE_NODES}" "${#DISKS[@]}" >&2
  exit 1
fi

{
  echo "run_id=${RUN_ID}"
  echo "base_ref=${BASE_REF}"
  echo "fix_ref=${FIX_REF}"
  if [[ "${DRY_RUN}" != "1" ]]; then
    echo "machine_type=$(curl -sf -H 'Metadata-Flavor: Google' http://metadata.google.internal/computeMetadata/v1/instance/machine-type | awk -F/ '{print $NF}')"
    echo "zone=$(curl -sf -H 'Metadata-Flavor: Google' http://metadata.google.internal/computeMetadata/v1/instance/zone | awk -F/ '{print $NF}')"
  fi
  echo "min_local_ssds=${MIN_LOCAL_SSDS}"
  echo "storage_nodes=${STORAGE_NODES}"
  echo "ab_repeats=${AB_REPEATS}"
  echo "write_concurrency=${WRITE_CONCURRENCY}"
  echo "read_concurrency=${READ_CONCURRENCY}"
  echo "guard_concurrency=${GUARD_CONCURRENCY}"
  echo "toy_durable_io_backend=${TOY_DURABLE_IO_BACKEND}"
  echo "toy_chunk_mib=${TOY_CHUNK_MIB}"
  echo "colocated_node=${COLOCATED_NODE}"
  echo "delay_mode=${DELAY_MODE}"
  echo "duration_ms=${DURATION_MS}"
  echo "warmup_ms=${WARMUP_MS}"
  echo "device_blocks=${DEVICE_BLOCKS}"
  echo "shards=${SHARDS}"
  echo "disk_count=${#DISKS[@]}"
  printf 'disks=%s\n' "${DISKS[*]}"
  echo
  uname -a
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
  for mountpoint in /mnt/toy-journal /mnt/toy-node-*; do
    if mountpoint -q "${mountpoint}"; then
      umount "${mountpoint}"
    fi
  done
  set -e
}

wipe_disks() {
  teardown_storage
  for disk in "${DISKS[@]}"; do
    wipefs -a "${disk}" >/dev/null 2>&1 || true
    blkdiscard -f "${disk}" >/dev/null 2>&1 || true
  done
}

mount_xfs() {
  local device="$1"
  local mountpoint="$2"
  mkdir -p "${mountpoint}"
  mkfs.xfs -f "${device}" >/dev/null
  mount -o noatime,nodiratime "${device}" "${mountpoint}"
}

toy_node_mount() {
  local index="$1"
  if (( index == COLOCATED_NODE )); then
    printf '%s' /mnt/toy-journal
  else
    printf '%s' "/mnt/toy-node-${index}"
  fi
}

toy_node_dirs_for_run() {
  local run_key="$1"
  local out=""
  for index in $(seq 1 "${STORAGE_NODES}"); do
    local path
    path="$(toy_node_mount "${index}")/loadbench/${run_key}/node-${index}"
    mkdir -p "${path}"
    if [[ -n "${out}" ]]; then
      out+=","
    fi
    out+="${path}"
  done
  printf '%s' "${out}"
}

setup_toy_storage() {
  wipe_disks
  mount_xfs "${DISKS[0]}" /mnt/toy-journal
  for index in $(seq 1 "${STORAGE_NODES}"); do
    if (( index == COLOCATED_NODE )); then
      continue
    fi
    mount_xfs "${DISKS[$index]}" "/mnt/toy-node-${index}"
  done
}

cleanup_toy_run() {
  local side="$1"
  local run_key="$2"
  rm -rf "/mnt/toy-journal/loadbench/${side}-${run_key}"
  for index in $(seq 1 "${STORAGE_NODES}"); do
    rm -rf "$(toy_node_mount "${index}")/loadbench/${side}-${run_key}"
  done
}

# One loadbench case for one side. Extra arguments after concurrency are
# forwarded verbatim (per-scenario flags such as --block-batch-bytes).
run_ab_case() {
  local side="$1"
  local run_key="$2"
  local workloads="$3"
  local concurrency="$4"
  shift 4
  local loadbench
  loadbench="$(loadbench_for_side "${side}")"
  local out="${RESULT_ROOT}/${side}/${run_key}"
  local scoped_key="${side}-${run_key}"
  local root="/mnt/toy-journal/loadbench/${scoped_key}/root"
  local node_dirs
  node_dirs="$(toy_node_dirs_for_run "${scoped_key}")"
  mkdir -p "${out}" "$(dirname "${root}")"
  # Catalog-mutex holder attribution exists only on sides built after the
  # flag landed; pass it only when this side's binary advertises it so a
  # BASE_REF binary without the flag runs exactly as before.
  local hold_csv_args=()
  if "${loadbench}" --help 2>/dev/null | grep -q -- '--catalog-hold-csv'; then
    hold_csv_args=(--catalog-hold-csv "${out}/catalog-hold.csv")
  fi
  log "running ${side} ${run_key} workloads=${workloads} concurrency=${concurrency}"
  "${loadbench}" \
    --provider durable \
    --durability flushed \
    --durable-io-backend "${TOY_DURABLE_IO_BACKEND}" \
    --workloads "${workloads}" \
    --duration-ms "${DURATION_MS}" \
    --warmup-ms "${WARMUP_MS}" \
    --concurrency "${concurrency}" \
    --device-blocks "${DEVICE_BLOCKS}" \
    --shards "${SHARDS}" \
    --storage-nodes "${STORAGE_NODES}" \
    --rtt-us 0 \
    --delay-mode "${DELAY_MODE}" \
    --payload-integrity verified \
    --target-data-log-mib 64 \
    --data-log-file-sync-fanout 16 \
    --block-journal-chunk-mib "${TOY_CHUNK_MIB}" \
    --root "${root}" \
    --storage-node-data-dirs "${node_dirs}" \
    --matrix-csv "${out}/matrix.csv" \
    --durable-profile-csv "${out}/durable-profile.csv" \
    ${hold_csv_args[@]+"${hold_csv_args[@]}"} \
    "$@" \
    | tee "${out}/stdout.csv"
  cleanup_toy_run "${side}" "${run_key}"
}

run_confirm_matrix() {
  if [[ "${DRY_RUN}" != "1" ]]; then
    setup_toy_storage
    start_monitors ab
  fi
  local bytes_64k
  bytes_64k="$(size_bytes 64k)"
  local bytes_4k
  bytes_4k="$(size_bytes 4k)"
  # Interleave sides per rep so instance drift cannot masquerade as a
  # base-vs-fix delta.
  for rep in $(seq 1 "${AB_REPEATS}"); do
    for side in base fix; do
      run_ab_case "${side}" "size-64k-rtt-0-dur-flushed-rep-${rep}" \
        block-batch-4k-16ops "${WRITE_CONCURRENCY}" \
        --block-batch-ops 1 \
        --block-batch-bytes "${bytes_64k}" \
        --block-batch-overlap random \
        --block-batch-profile-csv "${RESULT_ROOT}/${side}/size-64k-rtt-0-dur-flushed-rep-${rep}/block-batch-profile.csv"
    done
    for side in base fix; do
      run_ab_case "${side}" "read-rtt-0-dur-flushed-rep-${rep}" \
        block-read-4k,block-read-1m "${READ_CONCURRENCY}"
    done
  done
  for side in base fix; do
    run_ab_case "${side}" "size-4k-rtt-0-dur-flushed-rep-1" \
      block-batch-4k-16ops "${GUARD_CONCURRENCY}" \
      --block-batch-ops 1 \
      --block-batch-bytes "${bytes_4k}" \
      --block-batch-overlap random
  done
  if [[ "${DRY_RUN}" != "1" ]]; then
    teardown_storage
  fi
}

run_confirm_matrix
log "confirm A/B complete"
