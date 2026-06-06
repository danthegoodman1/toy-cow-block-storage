resource "google_project_service" "compute" {
  project            = var.project_id
  service            = "compute.googleapis.com"
  disable_on_destroy = false
}

resource "google_project_service" "storage" {
  project            = var.project_id
  service            = "storage.googleapis.com"
  disable_on_destroy = false
}

resource "random_id" "bucket_suffix" {
  byte_length = 4
}

locals {
  bucket_name = var.bucket_name != "" ? var.bucket_name : "rapidbench-${var.project_id}-${random_id.bucket_suffix.hex}"
}

resource "google_compute_network" "bench" {
  name                    = "${var.vm_name}-net"
  project                 = var.project_id
  auto_create_subnetworks = false

  depends_on = [google_project_service.compute]
}

resource "google_compute_subnetwork" "bench" {
  name                     = "${var.vm_name}-subnet"
  project                  = var.project_id
  region                   = var.region
  network                  = google_compute_network.bench.id
  ip_cidr_range            = var.subnet_cidr
  private_ip_google_access = true
}

resource "google_compute_firewall" "ssh" {
  name          = "${var.vm_name}-allow-ssh"
  project       = var.project_id
  network       = google_compute_network.bench.name
  source_ranges = var.ssh_source_ranges

  allow {
    protocol = "tcp"
    ports    = ["22"]
  }
}

resource "terraform_data" "rapid_bucket" {
  input = {
    bucket_name          = local.bucket_name
    force_destroy_bucket = var.force_destroy_bucket
    project_id           = var.project_id
    region               = var.region
    zone                 = var.zone
  }

  provisioner "local-exec" {
    interpreter = ["/bin/bash", "-c"]
    command     = <<-EOT
      set -euo pipefail
      if gcloud storage buckets describe "gs://${self.input.bucket_name}" --project="${self.input.project_id}" >/dev/null 2>&1; then
        echo "Rapid bucket gs://${self.input.bucket_name} already exists"
      else
        gcloud storage buckets create "gs://${self.input.bucket_name}" \
          --project="${self.input.project_id}" \
          --location="${self.input.region}" \
          --placement="${self.input.zone}" \
          --enable-hierarchical-namespace \
          --uniform-bucket-level-access \
          --default-storage-class=RAPID
      fi
    EOT
  }

  provisioner "local-exec" {
    when        = destroy
    interpreter = ["/bin/bash", "-c"]
    command     = <<-EOT
      set -euo pipefail
      if [ "${self.input.force_destroy_bucket}" = "true" ]; then
        gcloud storage rm --recursive "gs://${self.input.bucket_name}" --project="${self.input.project_id}" --quiet || true
      else
        echo "Leaving gs://${self.input.bucket_name} because force_destroy_bucket=false"
      fi
    EOT
  }

  depends_on = [google_project_service.storage]
}

resource "google_service_account" "bench" {
  account_id   = "rapidbench-${random_id.bucket_suffix.hex}"
  display_name = "Rapid Storage benchmark VM"
  project      = var.project_id

  depends_on = [google_project_service.compute]
}

resource "google_storage_bucket_iam_member" "bench_object_admin" {
  bucket = local.bucket_name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${google_service_account.bench.email}"

  depends_on = [terraform_data.rapid_bucket]
}

resource "google_compute_instance" "bench" {
  name         = var.vm_name
  project      = var.project_id
  zone         = var.zone
  machine_type = var.machine_type

  network_performance_config {
    total_egress_bandwidth_tier = var.total_egress_bandwidth_tier
  }

  boot_disk {
    initialize_params {
      image = "debian-cloud/debian-12"
      size  = 100
      type  = "pd-balanced"
    }
  }

  network_interface {
    network    = google_compute_network.bench.id
    subnetwork = google_compute_subnetwork.bench.id
    nic_type   = "GVNIC"

    access_config {
    }
  }

  service_account {
    email  = google_service_account.bench.email
    scopes = ["https://www.googleapis.com/auth/cloud-platform"]
  }

  metadata_startup_script = templatefile("${path.module}/startup.sh.tftpl", {
    go_version = var.go_version
  })

  depends_on = [
    google_project_service.compute,
    google_compute_firewall.ssh,
    google_storage_bucket_iam_member.bench_object_admin,
  ]
}
