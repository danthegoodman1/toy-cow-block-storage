#!/usr/bin/env bash
set -euo pipefail

# Phase 2 confirm-trip driver: provisions one instance and runs the base-vs-fix
# A/B (remote_phase2_confirm_ab.sh) with two source tarballs — BASE_REF via
# `git archive` and FIX_REF (default: the working tree, packaged like the main
# driver's SOURCE_REF=worktree). Provisioning, zone walking, cleanup, and
# result pull-back mirror run_block_vs_rbd.sh.

PROJECT="${PROJECT:-projectvoice-442316}"
ZONE="${ZONE:-auto}"
REGION="${REGION:-all}"
ZONE_SCOPE="${ZONE_SCOPE:-us}"
PREFERRED_REGIONS="${PREFERRED_REGIONS:-us-east1,us-central1,us-east4,us-west1,us-west2,us-west3,us-west4,us-south1}"
MACHINE_TYPE="${MACHINE_TYPE:-c4-standard-32-lssd}"
MIN_LOCAL_SSDS="${MIN_LOCAL_SSDS:-5}"
RUN_ID="${RUN_ID:-phase2-confirm-ab-$(date +%Y%m%d-%H%M%S)}"
VM_NAME="${VM_NAME:-toy-cow-p2ab-${RUN_ID}}"
NETWORK="${NETWORK:-toy-cow-p2ab-${RUN_ID}}"
SUBNET="${SUBNET:-toy-cow-p2ab-${RUN_ID}}"
SUBNET_BASE="${SUBNET}"
FIREWALL="${FIREWALL:-toy-cow-p2ab-ssh-${RUN_ID}}"
RESULT_DIR="${RESULT_DIR:-infra/gcp-local-nvme-bench/results/${RUN_ID}}"
BASE_REF="${BASE_REF:-55dc00c}"
FIX_REF="${FIX_REF:-worktree}"

STORAGE_NODES="${STORAGE_NODES:-5}"
AB_REPEATS="${AB_REPEATS:-2}"
WRITE_CONCURRENCY="${WRITE_CONCURRENCY:-16,32}"
READ_CONCURRENCY="${READ_CONCURRENCY:-16,32}"
FOURK_CONCURRENCY="${FOURK_CONCURRENCY:-1,4,16,32}"
TOY_DURABLE_IO_BACKEND="${TOY_DURABLE_IO_BACKEND:-filesystem}"
TOY_CHUNK_MIB="${TOY_CHUNK_MIB:-2}"
DELAY_MODE="${DELAY_MODE:-spin}"
DURATION_MS="${DURATION_MS:-5000}"
WARMUP_MS="${WARMUP_MS:-1000}"
DEVICE_BLOCKS="${DEVICE_BLOCKS:-1048576}"
SHARDS="${SHARDS:-64}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
REMOTE_SCRIPT="${SCRIPT_DIR}/remote_phase2_confirm_ab.sh"

created_vm=0
created_firewall=0
created_network=0
created_subnet_names=()
created_subnet_regions=()
last_attempt_zone=""

zone_region() {
  sed -E 's/-[a-z]$//' <<<"$1"
}

filter_zone_scope() {
  case "${ZONE_SCOPE}" in
    us)
      awk '/^us-/'
      ;;
    all)
      cat
      ;;
    *)
      awk -v prefix="${ZONE_SCOPE}" 'index($0, prefix) == 1'
      ;;
  esac
}

sort_candidate_zones() {
  awk -v preferred_regions="${PREFERRED_REGIONS}" '
    BEGIN {
      preferred_count = split(preferred_regions, preferred, /[, ]+/)
      for (preferred_index = 1; preferred_index <= preferred_count; preferred_index++) {
        if (preferred[preferred_index] != "") {
          region_rank[preferred[preferred_index]] = preferred_index
        }
      }
      fallback_rank = 10000
    }
    NF {
      zone = $0
      region = zone
      sub(/-[a-z]$/, "", region)
      rank = fallback_rank
      if (region in region_rank) {
        rank = region_rank[region]
      }
      printf "%05d %s\n", rank, zone
    }
  ' | sort -k1,1n -k2,2 | awk '{ print $2 }'
}

candidate_zones() {
  if [[ -n "${ZONES:-}" ]]; then
    # shellcheck disable=SC2020 # both separators map to newline on purpose
    tr ', ' '\n\n' <<<"${ZONES}" | awk 'NF'
    return
  fi
  if [[ "${ZONE}" != "auto" ]]; then
    printf '%s\n' "${ZONE}"
    return
  fi
  if [[ "${REGION}" == "all" ]]; then
    gcloud compute machine-types list \
      --project "${PROJECT}" \
      --filter="name=${MACHINE_TYPE}" \
      --format="value(zone)" | filter_zone_scope | sort -u | sort_candidate_zones
  else
    gcloud compute machine-types list \
      --project "${PROJECT}" \
      --filter="name=${MACHINE_TYPE} AND zone~^${REGION}-" \
      --format="value(zone)" | filter_zone_scope | sort -u
  fi
}

ensure_subnet_for_region() {
  local region="$1"
  local zone_index="$2"
  for existing_index in "${!created_subnet_regions[@]}"; do
    if [[ "${created_subnet_regions[$existing_index]}" == "${region}" ]]; then
      SUBNET="${created_subnet_names[$existing_index]}"
      return
    fi
  done
  local subnet_name="${SUBNET_BASE}-${region}"
  local cidr="10.44.${zone_index}.0/24"
  gcloud compute networks subnets create "${subnet_name}" \
    --project "${PROJECT}" --network "${NETWORK}" --region "${region}" \
    --range "${cidr}"
  created_subnet_names+=("${subnet_name}")
  created_subnet_regions+=("${region}")
  SUBNET="${subnet_name}"
}

# shellcheck disable=SC2317 # invoked via the EXIT trap
cleanup() {
  local status=$?
  set +e
  if [[ "${created_vm}" == "1" ]]; then
    gcloud compute instances delete "${VM_NAME}" \
      --project "${PROJECT}" --zone "${ZONE}" --quiet
  elif [[ -n "${last_attempt_zone}" ]]; then
    gcloud compute instances delete "${VM_NAME}" \
      --project "${PROJECT}" --zone "${last_attempt_zone}" --quiet >/dev/null 2>&1 || true
  fi
  if [[ "${created_firewall}" == "1" ]]; then
    gcloud compute firewall-rules delete "${FIREWALL}" \
      --project "${PROJECT}" --quiet
  fi
  for index in "${!created_subnet_names[@]}"; do
    gcloud compute networks subnets delete "${created_subnet_names[$index]}" \
      --project "${PROJECT}" --region "${created_subnet_regions[$index]}" --quiet
  done
  if [[ "${created_network}" == "1" ]]; then
    gcloud compute networks delete "${NETWORK}" \
      --project "${PROJECT}" --quiet
  fi
  exit "${status}"
}
trap cleanup EXIT

mkdir -p "${RESULT_DIR}"
base_tgz="$(mktemp -t toy-cow-base.XXXXXX.tgz)"
fix_tgz="$(mktemp -t toy-cow-fix.XXXXXX.tgz)"

(
  cd "${REPO_ROOT}"
  git archive --format=tar.gz -o "${base_tgz}" "${BASE_REF}"
  if [[ "${FIX_REF}" == "worktree" ]]; then
    git ls-files -z | tar --null -T - -czf "${fix_tgz}"
  else
    git archive --format=tar.gz -o "${fix_tgz}" "${FIX_REF}"
  fi
)

gcloud compute networks create "${NETWORK}" \
  --project "${PROJECT}" --subnet-mode custom
created_network=1

gcloud compute firewall-rules create "${FIREWALL}" \
  --project "${PROJECT}" --network "${NETWORK}" \
  --allow tcp:22 --source-ranges 0.0.0.0/0
created_firewall=1

CANDIDATE_ZONES=()
while IFS= read -r candidate_zone; do
  if [[ -n "${candidate_zone}" ]]; then
    CANDIDATE_ZONES+=("${candidate_zone}")
  fi
done < <(candidate_zones)
if [[ "${#CANDIDATE_ZONES[@]}" == "0" ]]; then
  echo "No candidate zones found for ${MACHINE_TYPE}" >&2
  exit 1
fi

attempt=0
for candidate_zone in "${CANDIDATE_ZONES[@]}"; do
  attempt=$((attempt + 1))
  last_attempt_zone="${candidate_zone}"
  candidate_region="$(zone_region "${candidate_zone}")"
  ensure_subnet_for_region "${candidate_region}" "${attempt}"
  echo "Trying ${MACHINE_TYPE} in ${candidate_zone}..."
  set +e
  gcloud compute instances create "${VM_NAME}" \
    --project "${PROJECT}" --zone "${candidate_zone}" \
    --machine-type "${MACHINE_TYPE}" \
    --image-family ubuntu-2404-lts-amd64 \
    --image-project ubuntu-os-cloud \
    --boot-disk-size 200GB \
    --boot-disk-type hyperdisk-balanced \
    --maintenance-policy TERMINATE \
    --network-interface "network=${NETWORK},subnet=${SUBNET},nic-type=GVNIC" \
    --scopes https://www.googleapis.com/auth/cloud-platform
  create_status=$?
  set -e
  if [[ "${create_status}" == "0" ]]; then
    ZONE="${candidate_zone}"
    created_vm=1
    break
  fi
  echo "Create failed in ${candidate_zone}; trying next candidate zone."
  gcloud compute instances delete "${VM_NAME}" \
    --project "${PROJECT}" --zone "${candidate_zone}" --quiet >/dev/null 2>&1 || true
done

if [[ "${created_vm}" != "1" ]]; then
  echo "Failed to create ${MACHINE_TYPE} in any candidate zone." >&2
  exit 1
fi

echo "Waiting for SSH..."
for _ in $(seq 1 90); do
  if gcloud compute ssh "${VM_NAME}" \
    --project "${PROJECT}" --zone "${ZONE}" \
    --command "true" >/dev/null 2>&1; then
    break
  fi
  sleep 5
done

gcloud compute scp "${base_tgz}" "${VM_NAME}:/tmp/base-source.tgz" \
  --project "${PROJECT}" --zone "${ZONE}"
gcloud compute scp "${fix_tgz}" "${VM_NAME}:/tmp/fix-source.tgz" \
  --project "${PROJECT}" --zone "${ZONE}"
gcloud compute scp "${REMOTE_SCRIPT}" "${VM_NAME}:/tmp/remote_phase2_confirm_ab.sh" \
  --project "${PROJECT}" --zone "${ZONE}"

set +e
gcloud compute ssh "${VM_NAME}" \
  --project "${PROJECT}" --zone "${ZONE}" \
  --command "sudo env MIN_LOCAL_SSDS='${MIN_LOCAL_SSDS}' STORAGE_NODES='${STORAGE_NODES}' AB_REPEATS='${AB_REPEATS}' WRITE_CONCURRENCY='${WRITE_CONCURRENCY}' READ_CONCURRENCY='${READ_CONCURRENCY}' FOURK_CONCURRENCY='${FOURK_CONCURRENCY}' TOY_DURABLE_IO_BACKEND='${TOY_DURABLE_IO_BACKEND}' TOY_CHUNK_MIB='${TOY_CHUNK_MIB}' DELAY_MODE='${DELAY_MODE}' DURATION_MS='${DURATION_MS}' WARMUP_MS='${WARMUP_MS}' DEVICE_BLOCKS='${DEVICE_BLOCKS}' SHARDS='${SHARDS}' BASE_REF='${BASE_REF}' FIX_REF='${FIX_REF}' bash /tmp/remote_phase2_confirm_ab.sh '${RUN_ID}' /tmp/base-source.tgz /tmp/fix-source.tgz"
run_status=$?
set -e

set +e
gcloud compute scp "${VM_NAME}:/tmp/${RUN_ID}-results.tgz" \
  "${RESULT_DIR}-results.tgz" \
  --project "${PROJECT}" --zone "${ZONE}"
scp_status=$?
set -e

if [[ "${scp_status}" == "0" ]]; then
  tar -C "${RESULT_DIR}" --strip-components=1 -xzf "${RESULT_DIR}-results.tgz"
fi

echo "Results: ${RESULT_DIR}"
echo "Archive: ${RESULT_DIR}-results.tgz"
exit "${run_status}"
