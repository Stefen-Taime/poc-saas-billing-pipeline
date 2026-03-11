output "external_url" {
  description = "DataHub UI URL"
  value       = "http://${kubernetes_service.datahub.status[0].load_balancer[0].ingress[0].ip}:9002"
}

output "gms_url" {
  description = "DataHub GMS API URL (for emitters)"
  value       = "http://${kubernetes_service.datahub.status[0].load_balancer[0].ingress[0].ip}:8080"
}
