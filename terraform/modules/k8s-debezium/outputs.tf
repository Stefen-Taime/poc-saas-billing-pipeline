output "service_host" {
  value = "${kubernetes_service.debezium.metadata[0].name}.${var.namespace}.svc.cluster.local"
}

output "health_port" {
  value = 8080
}
