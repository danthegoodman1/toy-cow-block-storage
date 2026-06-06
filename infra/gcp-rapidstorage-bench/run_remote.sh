#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
tool_dir="$(cd "${script_dir}/../../tools/gcp-rapidstorage-bench" && pwd)"

project_id="$(terraform -chdir="${script_dir}" output -raw project_id)"
zone="$(terraform -chdir="${script_dir}" output -raw zone)"
vm_name="$(terraform -chdir="${script_dir}" output -raw vm_name)"
bucket_name="$(terraform -chdir="${script_dir}" output -raw bucket_name)"

remote_dir="gcp-rapidstorage-bench"
csv_path=""
for arg in "$@"; do
  case "${arg}" in
    --csv=*)
      csv_path="${arg#--csv=}"
      ;;
  esac
done

echo "Waiting for benchmark VM startup script to install Go..."
ready=0
for attempt in {1..60}; do
  if gcloud compute ssh "${vm_name}" \
    --project="${project_id}" \
    --zone="${zone}" \
    --command="test -x /usr/local/go/bin/go && /usr/local/go/bin/go version" >/dev/null 2>&1; then
    ready=1
    break
  fi
  sleep 10
done

if [[ "${ready}" != "1" ]]; then
  echo "Timed out waiting for /usr/local/go/bin/go on ${vm_name}" >&2
  exit 1
fi

gcloud compute ssh "${vm_name}" \
  --project="${project_id}" \
  --zone="${zone}" \
  --command="rm -rf ~/${remote_dir} && mkdir -p ~/${remote_dir}"

gcloud compute scp --recurse "${tool_dir}/." "${vm_name}:~/${remote_dir}/" \
  --project="${project_id}" \
  --zone="${zone}"

gcloud compute ssh "${vm_name}" \
  --project="${project_id}" \
  --zone="${zone}" \
  --command="cd ~/${remote_dir} && PATH=/usr/local/go/bin:\$PATH go run . --bucket='${bucket_name}' $*"

if [[ -n "${csv_path}" ]]; then
  mkdir -p "${script_dir}/results"
  gcloud compute scp "${vm_name}:~/${remote_dir}/${csv_path}" "${script_dir}/results/$(basename "${csv_path}")" \
    --project="${project_id}" \
    --zone="${zone}"
fi
