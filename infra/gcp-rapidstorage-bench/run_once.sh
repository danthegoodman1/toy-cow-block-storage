#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
applied=0
destroyed=0

cleanup() {
  local status=$?
  if [[ "${applied}" == "1" && "${destroyed}" == "0" ]]; then
    echo "Destroying Rapid Storage benchmark infra..."
    terraform -chdir="${script_dir}" destroy -auto-approve -input=false || true
  fi
  exit "${status}"
}

trap cleanup EXIT

if [[ "$#" == "0" ]]; then
  set -- \
    --workers=16,32,64 \
    --op-mib=4,32 \
    --total-mib=512 \
    --publish-mib=128 \
    --mode=at-end,interval,close-at-end \
    --csv=rapid-results-c3-88-tier1.csv
fi

terraform -chdir="${script_dir}" init -backend=false
applied=1
terraform -chdir="${script_dir}" apply -auto-approve -input=false

"${script_dir}/run_remote.sh" "$@"

terraform -chdir="${script_dir}" destroy -auto-approve -input=false
destroyed=1
