output "service_host" {
  description = "MySQL internal ClusterIP service DNS"
  value       = "${kubernetes_service.mysql_internal.metadata[0].name}.${var.namespace}.svc.cluster.local"
}

output "service_port" {
  value = 3306
}

output "external_ip" {
  description = "MySQL LoadBalancer IP for local Go simulator"
  value       = kubernetes_service.mysql_external.status[0].load_balancer[0].ingress[0].ip
}
