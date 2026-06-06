output "project_id" {
  value = var.project_id
}

output "zone" {
  value = var.zone
}

output "bucket_name" {
  value = local.bucket_name
}

output "vm_name" {
  value = google_compute_instance.bench.name
}

output "ssh_command" {
  value = "gcloud compute ssh ${google_compute_instance.bench.name} --project=${var.project_id} --zone=${var.zone}"
}

output "copy_and_run_command" {
  value = "./run_remote.sh --workers=16,32,64 --op-mib=4,32 --total-mib=512 --publish-mib=128 --mode=at-end,interval,close-at-end --csv=rapid-results-c3-88-tier1.csv"
}
