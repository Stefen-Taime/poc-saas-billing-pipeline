###############################################################################
# Module: k8s-mysql — MySQL 8.0 on GKE with binlog ROW for Debezium
###############################################################################

resource "kubernetes_namespace" "lightspeed" {
  metadata {
    name = var.namespace
  }
}

# ---------- MySQL config (binlog ROW) ----------
resource "kubernetes_config_map" "mysql_config" {
  metadata {
    name      = "mysql-config"
    namespace = kubernetes_namespace.lightspeed.metadata[0].name
  }

  data = {
    "my.cnf" = <<-EOT
      [mysqld]
      server-id          = 1
      log_bin             = mysql-bin
      binlog_format       = ROW
      binlog_row_image    = FULL
      expire_logs_days    = 3
      gtid_mode           = ON
      enforce_gtid_consistency = ON
      default_authentication_plugin = mysql_native_password
    EOT
  }
}

# ---------- MySQL init script (schema) ----------
resource "kubernetes_config_map" "mysql_initdb" {
  metadata {
    name      = "mysql-initdb"
    namespace = kubernetes_namespace.lightspeed.metadata[0].name
  }

  data = {
    "lightspeed_db.sql" = file("${path.module}/../../../mysql/schema/lightspeed_db.sql")
  }
}

# ---------- Secret for root password ----------
resource "kubernetes_secret" "mysql_secret" {
  metadata {
    name      = "mysql-secret"
    namespace = kubernetes_namespace.lightspeed.metadata[0].name
  }

  data = {
    MYSQL_ROOT_PASSWORD = var.root_password
  }

  type = "Opaque"
}

# ---------- PVC ----------
resource "kubernetes_persistent_volume_claim" "mysql_data" {
  metadata {
    name      = "mysql-data"
    namespace = kubernetes_namespace.lightspeed.metadata[0].name
  }

  spec {
    access_modes       = ["ReadWriteOnce"]
    storage_class_name = "standard"

    resources {
      requests = {
        storage = "10Gi"
      }
    }
  }
}

# ---------- Deployment ----------
resource "kubernetes_deployment" "mysql" {
  metadata {
    name      = "mysql"
    namespace = kubernetes_namespace.lightspeed.metadata[0].name
    labels = {
      app       = "mysql"
      component = "source-db"
    }
  }

  spec {
    replicas = 1

    selector {
      match_labels = {
        app = "mysql"
      }
    }

    template {
      metadata {
        labels = {
          app = "mysql"
        }
      }

      spec {
        container {
          name  = "mysql"
          image = "mysql:8.0"

          port {
            container_port = 3306
          }

          env {
            name = "MYSQL_ROOT_PASSWORD"
            value_from {
              secret_key_ref {
                name = kubernetes_secret.mysql_secret.metadata[0].name
                key  = "MYSQL_ROOT_PASSWORD"
              }
            }
          }

          env {
            name  = "MYSQL_DATABASE"
            value = var.database_name
          }

          volume_mount {
            name       = "mysql-config"
            mount_path = "/etc/mysql/conf.d"
          }

          volume_mount {
            name       = "mysql-initdb"
            mount_path = "/docker-entrypoint-initdb.d"
          }

          volume_mount {
            name       = "mysql-data"
            mount_path = "/var/lib/mysql"
          }

          resources {
            requests = {
              cpu    = "500m"
              memory = "1Gi"
            }
            limits = {
              cpu    = "1"
              memory = "2Gi"
            }
          }

          liveness_probe {
            exec {
              command = ["mysqladmin", "ping", "-h", "localhost"]
            }
            initial_delay_seconds = 30
            period_seconds        = 10
          }

          readiness_probe {
            exec {
              command = ["mysql", "-h", "localhost", "-u", "root", "-p${var.root_password}", "-e", "SELECT 1"]
            }
            initial_delay_seconds = 15
            period_seconds        = 5
          }
        }

        volume {
          name = "mysql-config"
          config_map {
            name = kubernetes_config_map.mysql_config.metadata[0].name
          }
        }

        volume {
          name = "mysql-initdb"
          config_map {
            name = kubernetes_config_map.mysql_initdb.metadata[0].name
          }
        }

        volume {
          name = "mysql-data"
          persistent_volume_claim {
            claim_name = kubernetes_persistent_volume_claim.mysql_data.metadata[0].name
          }
        }
      }
    }
  }
}

# ---------- Service (ClusterIP for internal + LoadBalancer for local simulator) ----------
resource "kubernetes_service" "mysql_internal" {
  metadata {
    name      = "mysql"
    namespace = kubernetes_namespace.lightspeed.metadata[0].name
  }

  spec {
    selector = {
      app = "mysql"
    }

    port {
      port        = 3306
      target_port = 3306
    }

    type = "ClusterIP"
  }
}

# LoadBalancer pour le simulateur Go local
resource "kubernetes_service" "mysql_external" {
  metadata {
    name      = "mysql-external"
    namespace = kubernetes_namespace.lightspeed.metadata[0].name
  }

  spec {
    selector = {
      app = "mysql"
    }

    port {
      port        = 3306
      target_port = 3306
    }

    type = "LoadBalancer"
  }
}
