###############################################################################
# outputs.tf — POC Stripe Lightspeed
###############################################################################

output "gke_cluster_name" {
  description = "GKE cluster name"
  value       = module.gke.cluster_name
}

output "gke_endpoint" {
  description = "GKE cluster endpoint"
  value       = module.gke.endpoint
  sensitive   = true
}

output "mysql_service_host" {
  description = "MySQL K8s service DNS (internal)"
  value       = module.k8s_mysql.service_host
}

output "mysql_external_ip" {
  description = "MySQL LoadBalancer IP (for local Go simulator)"
  value       = module.k8s_mysql.external_ip
}

output "serving_api_url" {
  description = "Flask serving API external URL"
  value       = module.k8s_serving_api.external_url
}

output "pubsub_topics" {
  description = "Pub/Sub CDC topic names"
  value       = module.pubsub.topic_names
}

output "bigquery_datasets" {
  description = "BigQuery dataset IDs"
  value = {
    analytics = module.bigquery.dataset_analytics_id
    batch     = module.bigquery.dataset_batch_id
  }
}

output "gcs_bootstrap_bucket" {
  description = "GCS bucket for bootstrap data"
  value       = module.gcs.bucket_name
}

# ---------- Connect instructions ----------
output "connect_instructions" {
  description = "Commands to connect after deploy"
  value       = <<-EOT
    # 1. Get GKE credentials
    gcloud container clusters get-credentials ${var.gke_cluster_name} --zone ${var.zone} --project ${var.project_id}

    # 2. Run Go simulator locally (connects to MySQL LoadBalancer IP)
    cd go-simulator && go run main.go --mysql-host=$(terraform output -raw mysql_external_ip)

    # 3. Access services
    # Serving API:  $(terraform output -raw serving_api_url)
  EOT
}
