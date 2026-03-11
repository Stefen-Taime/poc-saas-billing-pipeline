###############################################################################
# variables.tf — POC Stripe Lightspeed GKE
###############################################################################

variable "project_id" {
  description = "GCP project ID"
  type        = string
}

variable "region" {
  description = "GCP region"
  type        = string
  default     = "us-east1"
}

variable "zone" {
  description = "GCP zone"
  type        = string
  default     = "us-east1-b"
}

variable "environment" {
  description = "Environment label"
  type        = string
  default     = "poc"
}

# ---------- GKE ----------
variable "gke_cluster_name" {
  description = "GKE cluster name"
  type        = string
  default     = "poc-stripe-lightspeed"
}

variable "gke_node_machine_type" {
  description = "Machine type for GKE nodes"
  type        = string
  default     = "e2-standard-4"
}

variable "gke_node_count" {
  description = "Number of GKE nodes"
  type        = number
  default     = 2
}

variable "gke_namespace" {
  description = "Kubernetes namespace for all workloads"
  type        = string
  default     = "lightspeed"
}

# ---------- MySQL (on GKE) ----------
variable "mysql_root_password" {
  description = "MySQL root password — set via TF_VAR_mysql_root_password env var"
  type        = string
  sensitive   = true
  # No default — must be provided via env var or -var flag
}

variable "mysql_database" {
  description = "MySQL database name"
  type        = string
  default     = "lightspeed_db"
}

# ---------- BigQuery ----------
variable "bq_dataset_analytics" {
  description = "BigQuery dataset for streaming analytics"
  type        = string
  default     = "lightspeed_analytics"
}

variable "bq_dataset_batch" {
  description = "BigQuery dataset for batch bootstrap"
  type        = string
  default     = "lightspeed_batch"
}

# ---------- Pub/Sub ----------
variable "pubsub_topics" {
  description = "CDC Pub/Sub topic names"
  type        = list(string)
  default = [
    "lightspeed.merchants.cdc",
    "lightspeed.subscriptions.cdc",
    "lightspeed.invoices.cdc",
    "lightspeed.payment_attempts.cdc",
    "lightspeed.plan_changes.cdc",
  ]
}

# ---------- GCS ----------
variable "bootstrap_bucket_name" {
  description = "GCS bucket for MySQL bootstrap dump"
  type        = string
  default     = "poc-stripe-bootstrap"
}
