###############################################################################
# Module: gcs — Bootstrap bucket for MySQL dump → Dataproc
###############################################################################

resource "google_storage_bucket" "bootstrap" {
  name          = var.bucket_name
  project       = var.project_id
  location      = var.region
  force_destroy = true # POC — destroy bucket even if non-empty

  uniform_bucket_level_access = true

  lifecycle_rule {
    condition {
      age = 7 # Auto-delete after 7 days
    }
    action {
      type = "Delete"
    }
  }

  labels = {
    pipeline = "poc-stripe-lightspeed"
    purpose  = "bootstrap"
  }
}
