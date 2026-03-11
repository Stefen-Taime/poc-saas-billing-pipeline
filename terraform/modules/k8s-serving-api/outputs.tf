output "external_url" {
  description = "Flask Serving API external URL"
  value       = "http://${kubernetes_service.serving_api.status[0].load_balancer[0].ingress[0].ip}"
}
