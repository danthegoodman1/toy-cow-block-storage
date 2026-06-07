#!/usr/bin/env bash
set -euo pipefail

PROJECT="${PROJECT:-projectvoice-442316}"
ZONE="${ZONE:-auto}"
REGION="${REGION:-us-central1}"
ZONE_SCOPE="${ZONE_SCOPE:-us}"
MACHINE_TYPE="${MACHINE_TYPE:-c4-standard-32-lssd}"
MIN_LOCAL_SSDS="${MIN_LOCAL_SSDS:-5}"
STORAGE_NODES="${STORAGE_NODES:-4}"
CONCURRENCY="${CONCURRENCY:-16,32}"
RUN_ID="${RUN_ID:-gcp-c4-layout-$(date +%Y%m%d-%H%M%S)}"
VM_NAME="${VM_NAME:-toy-cow-nvme-bench-${RUN_ID}}"
NETWORK="${NETWORK:-toy-cow-nvme-${RUN_ID}}"
SUBNET="${SUBNET:-toy-cow-nvme-${RUN_ID}}"
SUBNET_BASE="${SUBNET}"
FIREWALL="${FIREWALL:-toy-cow-nvme-ssh-${RUN_ID}}"
RESULT_DIR="${RESULT_DIR:-infra/gcp-local-nvme-bench/results/${RUN_ID}}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
REMOTE_SCRIPT="${SCRIPT_DIR}/remote_c4_layout_matrix.sh"

created_vm=0
created_firewall=0
created_network=0
created_subnet_names=()
created_subnet_regions=()
last_attempt_zone=""

zone_region() {
  sed -E 's/-[a-z]$//' <<<"$1"
}

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

candidate_zones() {
  if [[ -n "${ZONES:-}" ]]; then
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
      --format="value(zone)" | filter_zone_scope | sort -u
  else
    gcloud compute machine-types list \
      --project "${PROJECT}" \
      --filter="name=${MACHINE_TYPE} AND zone~^${REGION}-" \
      --format="value(zone)" | filter_zone_scope | sort -u
  fi
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
  local cidr="10.42.${zone_index}.0/24"
  gcloud compute networks subnets create "${subnet_name}" \
    --project "${PROJECT}" --network "${NETWORK}" --region "${region}" \
    --range "${cidr}"
  created_subnet_names+=("${subnet_name}")
  created_subnet_regions+=("${region}")
  SUBNET="${subnet_name}"
}

mkdir -p "${RESULT_DIR}"
src_tgz="$(mktemp -t toy-cow-source.XXXXXX.tgz)"

(
  cd "${REPO_ROOT}"
  git ls-files -z | tar --null -T - -czf "${src_tgz}"
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
    REGION="${candidate_region}"
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
for _ in $(seq 1 60); do
  if gcloud compute ssh "${VM_NAME}" \
    --project "${PROJECT}" --zone "${ZONE}" \
    --command "true" >/dev/null 2>&1; then
    break
  fi
  sleep 5
done

gcloud compute scp "${src_tgz}" "${VM_NAME}:/tmp/source.tgz" \
  --project "${PROJECT}" --zone "${ZONE}"
gcloud compute scp "${REMOTE_SCRIPT}" "${VM_NAME}:/tmp/remote_c4_layout_matrix.sh" \
  --project "${PROJECT}" --zone "${ZONE}"

set +e
gcloud compute ssh "${VM_NAME}" \
  --project "${PROJECT}" --zone "${ZONE}" \
  --command "sudo MIN_LOCAL_SSDS='${MIN_LOCAL_SSDS}' STORAGE_NODES='${STORAGE_NODES}' CONCURRENCY='${CONCURRENCY}' bash /tmp/remote_c4_layout_matrix.sh '${RUN_ID}' /tmp/source.tgz"
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
