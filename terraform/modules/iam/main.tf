###############################################################################
# Module: iam — Service accounts + roles for pipeline components
###############################################################################

# ---------- Dataflow service account ----------
resource "google_service_account" "dataflow" {
  account_id   = "dataflow-sa"
  display_name = "Dataflow Pipeline SA"
  project      = var.project_id
}

resource "google_project_iam_member" "dataflow_roles" {
  for_each = toset([
    "roles/dataflow.worker",
    "roles/datastore.user",        # Firestore write
    "roles/bigquery.dataEditor",   # BigQuery write
    "roles/pubsub.subscriber",     # Pub/Sub read
    "roles/storage.objectViewer",  # GCS read (templates)
  ])

  project = var.project_id
  role    = each.value
  member  = "serviceAccount:${google_service_account.dataflow.email}"
}

# ---------- Dataproc service account ----------
resource "google_service_account" "dataproc" {
  account_id   = "dataproc-sa"
  display_name = "Dataproc Bootstrap SA"
  project      = var.project_id
}

resource "google_project_iam_member" "dataproc_roles" {
  for_each = toset([
    "roles/dataproc.worker",
    "roles/bigquery.dataEditor",
    "roles/storage.objectAdmin",  # GCS read/write (dump files)
  ])

  project = var.project_id
  role    = each.value
  member  = "serviceAccount:${google_service_account.dataproc.email}"
}

# ---------- Serving API (Workload Identity) ----------
resource "google_service_account" "serving_api" {
  account_id   = "serving-api-sa"
  display_name = "Flask Serving API SA"
  project      = var.project_id
}

resource "google_project_iam_member" "serving_api_roles" {
  for_each = toset([
    "roles/datastore.viewer", # Firestore read-only
  ])

  project = var.project_id
  role    = each.value
  member  = "serviceAccount:${google_service_account.serving_api.email}"
}

# Workload Identity binding — K8s SA → GCP SA
resource "google_service_account_iam_member" "serving_api_workload_identity" {
  service_account_id = google_service_account.serving_api.name
  role               = "roles/iam.workloadIdentityUser"
  member             = "serviceAccount:${var.project_id}.svc.id.goog[lightspeed/serving-api]"
}

# ---------- Debezium (Workload Identity for Pub/Sub) ----------
resource "google_service_account" "debezium" {
  account_id   = "debezium-sa"
  display_name = "Debezium CDC SA"
  project      = var.project_id
}

resource "google_project_iam_member" "debezium_roles" {
  for_each = toset([
    "roles/pubsub.publisher", # Pub/Sub write
  ])

  project = var.project_id
  role    = each.value
  member  = "serviceAccount:${google_service_account.debezium.email}"
}

resource "google_service_account_iam_member" "debezium_workload_identity" {
  service_account_id = google_service_account.debezium.name
  role               = "roles/iam.workloadIdentityUser"
  member             = "serviceAccount:${var.project_id}.svc.id.goog[lightspeed/debezium]"
}
