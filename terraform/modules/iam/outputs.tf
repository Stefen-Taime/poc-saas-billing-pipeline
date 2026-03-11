output "dataflow_sa_email" {
  value = google_service_account.dataflow.email
}

output "dataproc_sa_email" {
  value = google_service_account.dataproc.email
}

output "serving_api_sa_email" {
  value = google_service_account.serving_api.email
}

output "debezium_sa_email" {
  value = google_service_account.debezium.email
}
