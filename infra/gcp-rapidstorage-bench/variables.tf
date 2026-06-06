variable "project_id" {
  description = "GCP project to use for the Rapid Storage benchmark."
  type        = string
  default     = "projectvoice-442316"
}

variable "region" {
  description = "Region containing the benchmark zone."
  type        = string
  default     = "us-central1"
}

variable "zone" {
  description = "Zone for both the VM and Rapid Storage bucket placement."
  type        = string
  default     = "us-central1-a"
}

variable "bucket_name" {
  description = "Optional globally unique bucket name. If empty, Terraform generates one."
  type        = string
  default     = ""
}

variable "vm_name" {
  description = "Benchmark VM name."
  type        = string
  default     = "rapidstorage-bench"
}

variable "machine_type" {
  description = "Benchmark VM machine type. Use a larger C3/C4 shape for high-concurrency runs."
  type        = string
  default     = "c3-standard-88"
}

variable "total_egress_bandwidth_tier" {
  description = "Per-VM egress bandwidth tier. Use TIER_1 with gVNIC for high-bandwidth benchmark runs."
  type        = string
  default     = "TIER_1"

  validation {
    condition     = contains(["DEFAULT", "TIER_1"], var.total_egress_bandwidth_tier)
    error_message = "total_egress_bandwidth_tier must be DEFAULT or TIER_1."
  }
}

variable "go_version" {
  description = "Go toolchain version installed on the benchmark VM."
  type        = string
  default     = "1.25.0"
}

variable "force_destroy_bucket" {
  description = "Whether terraform destroy should remove benchmark objects and delete the Rapid bucket."
  type        = bool
  default     = true
}

variable "subnet_cidr" {
  description = "CIDR range for the temporary benchmark subnet."
  type        = string
  default     = "10.42.0.0/24"
}

variable "ssh_source_ranges" {
  description = "CIDR ranges allowed to SSH to the temporary benchmark VM."
  type        = list(string)
  default     = ["0.0.0.0/0"]
}
