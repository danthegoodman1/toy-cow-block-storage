#!/usr/bin/env bash
set -euo pipefail

RUN_ID="${1:?run id required}"
SRC_TGZ="${2:?source tarball required}"

RESULT_ROOT="/opt/results/${RUN_ID}"
SRC_DIR="/opt/src"
LOADBENCH="${SRC_DIR}/target/release/loadbench"

MIN_LOCAL_SSDS="${MIN_LOCAL_SSDS:-5}"
STORAGE_NODES="${STORAGE_NODES:-4}"
CONCURRENCY="${CONCURRENCY:-1,4,16,32}"
IO_SIZES="${IO_SIZES:-4k,64k,256k,1m,32m}"
TOY_RTTS="${TOY_RTTS:-0,200}"
TOY_DURABILITIES="${TOY_DURABILITIES:-ack-flush:1,flushed}"
TOY_REPEATS="${TOY_REPEATS:-2}"
SKIP_TOY="${SKIP_TOY:-0}"
DELAY_MODE="${DELAY_MODE:-spin}"
DURATION_MS="${DURATION_MS:-5000}"
WARMUP_MS="${WARMUP_MS:-1000}"
FIO_RUNTIME_SEC="${FIO_RUNTIME_SEC:-5}"
FIO_RAMP_SEC="${FIO_RAMP_SEC:-1}"
CEPH_POOL_SIZE="${CEPH_POOL_SIZE:-1}"
RBD_SIZE_MIB="${RBD_SIZE_MIB:-65536}"
MICROCEPH_CHANNEL="${MICROCEPH_CHANNEL:-squid/stable}"
DEVICE_BLOCKS="${DEVICE_BLOCKS:-1048576}"
SHARDS="${SHARDS:-64}"

mkdir -p "${RESULT_ROOT}/toy" "${RESULT_ROOT}/ceph" "${RESULT_ROOT}/monitor"

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
    256k|265k) printf '%s\n' 262144 ;;
    1m) printf '%s\n' 1048576 ;;
    32m) printf '%s\n' 33554432 ;;
    *)
      printf 'unknown IO size %s\n' "$1" >&2
      return 1
      ;;
  esac
}

normalize_size_label() {
  case "$1" in
    265k) printf '%s\n' 256k ;;
    *) printf '%s\n' "$1" ;;
  esac
}

log "installing packages"
export DEBIAN_FRONTEND=noninteractive
apt-get update -y
apt-get install -y ca-certificates curl build-essential pkg-config libssl-dev \
  libsqlite3-dev mdadm xfsprogs sysstat python3 fio jq snapd
systemctl enable --now snapd >/dev/null 2>&1 || true

if [[ "${SKIP_TOY}" != "1" ]] && ! command -v cargo >/dev/null 2>&1; then
  log "installing rust toolchain"
  curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs \
    | sh -s -- -y --profile minimal
  # shellcheck disable=SC1091
  source "${HOME}/.cargo/env"
fi

if [[ "${SKIP_TOY}" == "1" ]]; then
  log "skipping loadbench build"
else
  log "extracting source"
  rm -rf "${SRC_DIR}"
  mkdir -p "${SRC_DIR}"
  tar -C "${SRC_DIR}" -xzf "${SRC_TGZ}"

  log "building loadbench"
  cd "${SRC_DIR}"
  cargo build --release --bin loadbench
fi

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
  printf 'toy layout needs one journal disk plus %s storage-node disks, found %s\n' \
    "${STORAGE_NODES}" "${#DISKS[@]}" >&2
  exit 1
fi

{
  echo "run_id=${RUN_ID}"
  echo "source_ref=${SOURCE_REF:-unknown}"
  echo "machine_type=$(curl -sf -H 'Metadata-Flavor: Google' http://metadata.google.internal/computeMetadata/v1/instance/machine-type | awk -F/ '{print $NF}')"
  echo "zone=$(curl -sf -H 'Metadata-Flavor: Google' http://metadata.google.internal/computeMetadata/v1/instance/zone | awk -F/ '{print $NF}')"
  echo "min_local_ssds=${MIN_LOCAL_SSDS}"
  echo "storage_nodes=${STORAGE_NODES}"
  echo "concurrency=${CONCURRENCY}"
  echo "io_sizes=${IO_SIZES}"
  echo "toy_rtts=${TOY_RTTS}"
  echo "toy_durabilities=${TOY_DURABILITIES}"
  echo "toy_repeats=${TOY_REPEATS}"
  echo "skip_toy=${SKIP_TOY}"
  echo "delay_mode=${DELAY_MODE}"
  echo "duration_ms=${DURATION_MS}"
  echo "warmup_ms=${WARMUP_MS}"
  echo "fio_runtime_sec=${FIO_RUNTIME_SEC}"
  echo "fio_ramp_sec=${FIO_RAMP_SEC}"
  echo "ceph_pool_size=${CEPH_POOL_SIZE}"
  echo "rbd_size_mib=${RBD_SIZE_MIB}"
  echo "device_blocks=${DEVICE_BLOCKS}"
  echo "shards=${SHARDS}"
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
  rbd showmapped --format json >/tmp/rbd-showmapped.json 2>/dev/null
  python3 - <<'PY' >/tmp/rbd-devices.txt 2>/dev/null || true
import json
try:
    data = json.load(open('/tmp/rbd-showmapped.json'))
except Exception:
    data = {}
for value in data.values():
    name = value.get('name') or value.get('image')
    pool = value.get('pool')
    if pool and name:
        print(f'{pool}/{name}')
PY
  while read -r image; do
    [[ -n "${image}" ]] || continue
    rbd unmap "${image}" >/dev/null 2>&1 || true
    microceph.rbd unmap "${image}" >/dev/null 2>&1 || true
  done < /tmp/rbd-devices.txt
  for mountpoint in /mnt/toy-journal /mnt/toy-node-*; do
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

toy_node_dirs_for_run() {
  local run_key="$1"
  local out=""
  for index in $(seq 1 "${STORAGE_NODES}"); do
    local path="/mnt/toy-node-${index}/loadbench/${run_key}/node-${index}"
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
    mount_xfs "${DISKS[$index]}" "/mnt/toy-node-${index}"
  done
}

run_toy_case() {
  local run_key="$1"
  local bytes="$2"
  local rtt="$3"
  local durability="$4"
  local out="${RESULT_ROOT}/toy/${run_key}"
  local root="/mnt/toy-journal/loadbench/${run_key}/root"
  local node_dirs
  node_dirs="$(toy_node_dirs_for_run "${run_key}")"
  mkdir -p "${out}" "$(dirname "${root}")"
  log "running toy block ${run_key} bytes=${bytes}"
  "${LOADBENCH}" \
    --provider durable \
    --durability "${durability}" \
    --workloads block-batch-4k-16ops \
    --block-batch-ops 1 \
    --block-batch-bytes "${bytes}" \
    --block-batch-overlap random \
    --duration-ms "${DURATION_MS}" \
    --warmup-ms "${WARMUP_MS}" \
    --concurrency "${CONCURRENCY}" \
    --device-blocks "${DEVICE_BLOCKS}" \
    --shards "${SHARDS}" \
    --storage-nodes "${STORAGE_NODES}" \
    --rtt-us "${rtt}" \
    --delay-mode "${DELAY_MODE}" \
    --payload-integrity verified \
    --target-data-log-mib 64 \
    --data-log-file-sync-fanout 16 \
    --root "${root}" \
    --storage-node-data-dirs "${node_dirs}" \
    --matrix-csv "${out}/matrix.csv" \
    --durable-profile-csv "${out}/durable-profile.csv" \
    --block-batch-profile-csv "${out}/block-batch-profile.csv" \
    | tee "${out}/stdout.csv"
  rm -rf "/mnt/toy-journal/loadbench/${run_key}"
  for index in $(seq 1 "${STORAGE_NODES}"); do
    rm -rf "/mnt/toy-node-${index}/loadbench/${run_key}"
  done
}

run_toy_matrix() {
  setup_toy_storage
  start_monitors toy
  IFS=',' read -r -a sizes <<<"${IO_SIZES}"
  IFS=',' read -r -a rtts <<<"${TOY_RTTS}"
  IFS=',' read -r -a durabilities <<<"${TOY_DURABILITIES}"
  local cases=()
  for raw_size in "${sizes[@]}"; do
    local label
    label="$(normalize_size_label "${raw_size}")"
    local bytes
    bytes="$(size_bytes "${raw_size}")"
    for rtt in "${rtts[@]}"; do
      for durability in "${durabilities[@]}"; do
        local durlabel="${durability//:/-}"
        cases+=("size-${label}-rtt-${rtt}-dur-${durlabel}|${bytes}|${rtt}|${durability}")
      done
    done
  done
  # Repeats run in shuffled order so first-pass layout effects show up as
  # spread between repeats instead of biasing one configuration.
  for repeat in $(seq 1 "${TOY_REPEATS}"); do
    while IFS= read -r case_line; do
      [[ -n "${case_line}" ]] || continue
      IFS='|' read -r case_key bytes rtt durability <<<"${case_line}"
      run_toy_case "${case_key}-rep-${repeat}" "${bytes}" "${rtt}" "${durability}"
    done < <(printf '%s\n' "${cases[@]}" | shuf)
  done
  teardown_storage
}

install_microceph() {
  if ! command -v microceph >/dev/null 2>&1; then
    log "installing microceph"
    snap install microceph --channel "${MICROCEPH_CHANNEL}" || snap install microceph
  fi
  snap connect microceph:block-devices >/dev/null 2>&1 || true
  snap connect microceph:hardware-observe >/dev/null 2>&1 || true
}

ceph_cmd() {
  if command -v microceph.ceph >/dev/null 2>&1; then
    microceph.ceph "$@"
  else
    ceph "$@"
  fi
}

rbd_cmd() {
  if command -v microceph.rbd >/dev/null 2>&1; then
    microceph.rbd "$@"
  else
    rbd "$@"
  fi
}

wait_for_osds() {
  local expected="$1"
  for _ in $(seq 1 180); do
    if ceph_cmd osd stat 2>/dev/null | grep -q "${expected} osds: ${expected} up"; then
      return 0
    fi
    sleep 2
  done
  ceph_cmd status || true
  return 1
}

wait_for_pool_clean() {
  for _ in $(seq 1 180); do
    local status
    status="$(ceph_cmd status 2>/dev/null || true)"
    if grep -q "active+clean" <<<"${status}" \
      && ! grep -Eq "not active|peering|creating|recover|remapped|misplaced|premerge|backfill|degraded" <<<"${status}"; then
      return 0
    fi
    sleep 2
  done
  ceph_cmd status || true
  return 1
}

setup_ceph_rbd() {
  wipe_disks
  install_microceph
  log "bootstrapping microceph"
  microceph cluster bootstrap || true
  mkdir -p /etc/ceph
  for ceph_file in ceph.conf ceph.keyring ceph.client.admin.keyring; do
    if [[ -e "/var/snap/microceph/current/conf/${ceph_file}" ]]; then
      ln -sfn "/var/snap/microceph/current/conf/${ceph_file}" "/etc/ceph/${ceph_file}"
    fi
  done
  ls -l /etc/ceph | tee "${RESULT_ROOT}/ceph/etc-ceph-links.txt" || true
  microceph status | tee "${RESULT_ROOT}/ceph/microceph-status-after-bootstrap.txt" || true
  log "adding local SSDs as Ceph OSDs"
  microceph disk add "${DISKS[@]}" --wipe
  wait_for_osds "${#DISKS[@]}"
  ceph_cmd status | tee "${RESULT_ROOT}/ceph/ceph-status-after-osds.txt" || true
  ceph_cmd config set mon mon_allow_pool_size_one true \
    || ceph_cmd config set global mon_allow_pool_size_one true \
    || true
  ceph_cmd config set global osd_pool_default_size "${CEPH_POOL_SIZE}" || true
  ceph_cmd config set global osd_pool_default_min_size 1 || true
  ceph_cmd config set global osd_pool_default_pg_autoscale_mode off || true
  ceph_cmd osd pool create bench 128
  ceph_cmd osd pool set bench pg_autoscale_mode off || true
  ceph_cmd osd pool set bench size "${CEPH_POOL_SIZE}" --yes-i-really-mean-it \
    || ceph_cmd osd pool set bench size "${CEPH_POOL_SIZE}"
  ceph_cmd osd pool set bench min_size 1
  ceph_cmd osd pool application enable bench rbd
  wait_for_pool_clean
  rbd_cmd pool init -p bench
  wait_for_pool_clean
  rbd_cmd create bench/image --size "${RBD_SIZE_MIB}" --image-feature layering
  wait_for_pool_clean
  printf '%s\n' "bench/image" > "${RESULT_ROOT}/ceph/rbd-device.txt"
  ceph_cmd status | tee "${RESULT_ROOT}/ceph/ceph-status-before-fio.txt" || true
}

run_fio_case() {
  local label="$1"
  local bytes="$2"
  local concurrency="$3"
  local out="${RESULT_ROOT}/ceph/size-${label}-c${concurrency}"
  mkdir -p "${out}"
  log "running fio rbd size=${label} bytes=${bytes} concurrency=${concurrency}"
  fio \
    --name="rbd-${label}-c${concurrency}" \
    --ioengine=rbd \
    --clientname=admin \
    --pool=bench \
    --rbdname=image \
    --rw=randwrite \
    --bs="${bytes}" \
    --direct=1 \
    --numjobs="${concurrency}" \
    --iodepth=1 \
    --thread=1 \
    --time_based=1 \
    --runtime="${FIO_RUNTIME_SEC}" \
    --ramp_time="${FIO_RAMP_SEC}" \
    --size="${RBD_SIZE_MIB}M" \
    --randrepeat=0 \
    --norandommap=1 \
    --group_reporting=1 \
    --output-format=json \
    --output="${out}/fio.json"
}

run_ceph_matrix() {
  setup_ceph_rbd
  start_monitors ceph
  IFS=',' read -r -a sizes <<<"${IO_SIZES}"
  IFS=',' read -r -a concurrencies <<<"${CONCURRENCY}"
  for raw_size in "${sizes[@]}"; do
    local label
    label="$(normalize_size_label "${raw_size}")"
    local bytes
    bytes="$(size_bytes "${raw_size}")"
    for concurrency in "${concurrencies[@]}"; do
      run_fio_case "${label}" "${bytes}" "${concurrency}"
    done
  done
  stop_monitors
  ceph_cmd status | tee "${RESULT_ROOT}/ceph/ceph-status-after-fio.txt" || true
}

summarize_results() {
  python3 - "${RESULT_ROOT}" <<'PY'
import csv
import json
import math
import re
import sys
from pathlib import Path

root = Path(sys.argv[1])

def parse_size_label(label):
    table = {
        "4k": 4096,
        "64k": 65536,
        "256k": 262144,
        "1m": 1048576,
        "32m": 33554432,
    }
    return table[label]

def as_float(value, default=0.0):
    try:
        return float(value)
    except (TypeError, ValueError):
        return default

def fio_percentile(write, key):
    for source in ("clat_ns", "lat_ns"):
        percentiles = write.get(source, {}).get("percentile", {})
        if key in percentiles:
            return as_float(percentiles[key]) / 1000.0
    return 0.0

raw_rows = []
for path in sorted((root / "toy").glob("size-*/matrix.csv")):
    match = re.search(
        r"size-([^-]+)-rtt-([0-9]+)-dur-([^/]+?)-rep-([0-9]+)", str(path)
    )
    if not match:
        continue
    io_size = match.group(1)
    rtt_us = match.group(2)
    durability = match.group(3)
    repeat = int(match.group(4))
    with path.open(newline="") as f:
        for row in csv.DictReader(f):
            raw_rows.append({
                "system": f"toy-block-{durability}",
                "io_size": io_size,
                "size_bytes": parse_size_label(io_size),
                "rtt_us": rtt_us,
                "repeat": repeat,
                "concurrency": row["concurrency"],
                "throughput_MBps": as_float(row["published_mbps"]),
                "iops": as_float(row["success_iops"]),
                "p50_us": as_float(row["p50_us"]),
                "p90_us": as_float(row["p90_us"]),
                "p99_us": as_float(row["p99_us"]),
                "p999_us": as_float(row["p999_us"]),
                "max_us": as_float(row["max_us"]),
                "errors": row["errors"],
                "source": str(path.relative_to(root)),
                "semantics": (
                    f"loadbench durable {durability}, "
                    "one random block-batch write per op"
                ),
            })

def median(values):
    values = sorted(values)
    if not values:
        return 0.0
    middle = len(values) // 2
    if len(values) % 2:
        return values[middle]
    return (values[middle - 1] + values[middle]) / 2.0

# Collapse repeats into per-cell medians so one anomalous pass cannot bias
# the comparison.
rows = []
by_cell = {}
for row in raw_rows:
    key = (row["system"], row["io_size"], row["rtt_us"], row["concurrency"])
    by_cell.setdefault(key, []).append(row)
for cell_rows in by_cell.values():
    base = dict(cell_rows[0])
    for field in ("throughput_MBps", "iops", "p50_us", "p90_us", "p99_us", "p999_us", "max_us"):
        base[field] = median([row[field] for row in cell_rows])
    base["source"] = ";".join(sorted(row["source"] for row in cell_rows))
    del base["repeat"]
    rows.append(base)

for path in sorted((root / "ceph").glob("size-*/fio.json")):
    match = re.search(r"size-([^-]+)-c([0-9]+)", str(path))
    if not match:
        continue
    io_size = match.group(1)
    concurrency = match.group(2)
    data = json.loads(path.read_text())
    job = data.get("jobs", [{}])[0]
    write = job.get("write", {})
    bw_bytes = as_float(write.get("bw_bytes"))
    rows.append({
        "system": "ceph-rbd",
        "io_size": io_size,
        "size_bytes": parse_size_label(io_size),
        "rtt_us": "0",
        "concurrency": concurrency,
        "throughput_MBps": bw_bytes / 1_000_000.0,
        "iops": as_float(write.get("iops")),
        "p50_us": fio_percentile(write, "50.000000"),
        "p90_us": fio_percentile(write, "90.000000"),
        "p99_us": fio_percentile(write, "99.000000"),
        "p999_us": fio_percentile(write, "99.900000"),
        "max_us": as_float(write.get("clat_ns", {}).get("max", 0)) / 1000.0,
        "errors": job.get("error", 0),
        "source": str(path.relative_to(root)),
        "semantics": "fio randwrite direct=1 through librbd, pool size 1",
    })

fields = [
    "system",
    "io_size",
    "size_bytes",
    "rtt_us",
    "concurrency",
    "throughput_MBps",
    "iops",
    "p50_us",
    "p90_us",
    "p99_us",
    "p999_us",
    "max_us",
    "errors",
    "source",
    "semantics",
]
with (root / "comparison-summary.csv").open("w", newline="") as f:
    writer = csv.DictWriter(f, fieldnames=fields)
    writer.writeheader()
    writer.writerows(sorted(rows, key=lambda row: (
        row["io_size"],
        int(row["concurrency"]),
        int(row["rtt_us"]),
        row["system"],
    )))

by_key = {}
for row in rows:
    by_key[(row["system"], row["io_size"], row["rtt_us"], row["concurrency"])] = row

ratio_rows = []
for toy_key, toy in by_key.items():
    system, io_size, rtt_us, concurrency = toy_key
    if not system.startswith("toy-block"):
        continue
    ceph = by_key.get(("ceph-rbd", io_size, "0", concurrency))
    if not ceph:
        continue
    toy_tp = as_float(toy["throughput_MBps"])
    ceph_tp = as_float(ceph["throughput_MBps"])
    toy_p99 = as_float(toy["p99_us"])
    ceph_p99 = as_float(ceph["p99_us"])
    ratio_rows.append({
        "system": system,
        "io_size": io_size,
        "rtt_us": rtt_us,
        "concurrency": concurrency,
        "toy_MBps": toy_tp,
        "ceph_MBps": ceph_tp,
        "toy_vs_ceph_throughput": toy_tp / ceph_tp if ceph_tp else 0.0,
        "toy_p99_us": toy_p99,
        "ceph_p99_us": ceph_p99,
        "toy_vs_ceph_p99": toy_p99 / ceph_p99 if ceph_p99 else 0.0,
    })

with (root / "relative-summary.csv").open("w", newline="") as f:
    fields = [
        "system",
        "io_size",
        "rtt_us",
        "concurrency",
        "toy_MBps",
        "ceph_MBps",
        "toy_vs_ceph_throughput",
        "toy_p99_us",
        "ceph_p99_us",
        "toy_vs_ceph_p99",
    ]
    writer = csv.DictWriter(f, fieldnames=fields)
    writer.writeheader()
    writer.writerows(sorted(ratio_rows, key=lambda row: (
        row["system"],
        row["io_size"],
        int(row["concurrency"]),
        int(row["rtt_us"]),
    )))
PY
}

if [[ "${SKIP_TOY}" == "1" ]]; then
  log "skipping toy matrix"
else
  run_toy_matrix
fi
run_ceph_matrix
summarize_results
teardown_storage
log "complete"
