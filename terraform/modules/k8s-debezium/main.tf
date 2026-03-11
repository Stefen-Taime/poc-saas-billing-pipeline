###############################################################################
# Module: k8s-debezium — Debezium Server 2.4 → Pub/Sub direct (no Kafka)
###############################################################################

# ---------- Debezium Server configuration (application.properties) ----------
resource "kubernetes_config_map" "debezium_config" {
  metadata {
    name      = "debezium-config"
    namespace = var.namespace
  }

  data = {
    "application.properties" = <<-EOT
      # ---------------------------------------------------------------
      # Debezium Server — MySQL source → Cloud Pub/Sub sink (direct)
      # ---------------------------------------------------------------

      # --- Source connector: MySQL CDC ---
      debezium.source.connector.class=io.debezium.connector.mysql.MySqlConnector
      debezium.source.database.hostname=${var.mysql_host}
      debezium.source.database.port=${var.mysql_port}
      debezium.source.database.user=root
      debezium.source.database.password=${var.mysql_password}
      debezium.source.database.server.id=184054
      debezium.source.database.server.name=lightspeed
      debezium.source.database.include.list=${var.mysql_database}
      debezium.source.topic.prefix=lightspeed
      debezium.source.table.include.list=${var.mysql_database}.merchants,${var.mysql_database}.subscriptions,${var.mysql_database}.invoices,${var.mysql_database}.payment_attempts,${var.mysql_database}.plan_changes
      debezium.source.include.schema.changes=false

      # Data type handling
      debezium.source.decimal.handling.mode=string
      debezium.source.time.precision.mode=connect

      # --- Topic routing: lightspeed.lightspeed_db.X -> lightspeed.X.cdc ---
      debezium.transforms=route
      debezium.transforms.route.type=org.apache.kafka.connect.transforms.RegexRouter
      debezium.transforms.route.regex=lightspeed\\.${var.mysql_database}\\.(.*)
      debezium.transforms.route.replacement=lightspeed.$1.cdc

      # --- Sink: Cloud Pub/Sub (native, no Kafka) ---
      debezium.sink.type=pubsub
      debezium.sink.pubsub.project.id=${var.project_id}

      # --- Format: JSON without schemas ---
      debezium.format.key=json
      debezium.format.value=json
      debezium.format.key.schemas.enable=false
      debezium.format.value.schemas.enable=false

      # --- Offset storage: local file (persisted on PVC) ---
      debezium.source.offset.storage=org.apache.kafka.connect.storage.FileOffsetBackingStore
      debezium.source.offset.storage.file.filename=/debezium/data/offsets.dat
      debezium.source.offset.flush.interval.ms=10000

      # --- Schema history: local file (persisted on PVC) ---
      debezium.source.schema.history.internal=io.debezium.storage.file.history.FileSchemaHistory
      debezium.source.schema.history.internal.file.filename=/debezium/data/schema-history.dat

      # --- Quarkus HTTP port (health checks) ---
      quarkus.http.port=8080
    EOT
  }
}

# ---------- PVC for offset + schema history persistence ----------
resource "kubernetes_persistent_volume_claim" "debezium_data" {
  metadata {
    name      = "debezium-data"
    namespace = var.namespace
  }

  # WaitForFirstConsumer storage class: PVC binds only when a pod mounts it
  wait_until_bound = false

  spec {
    access_modes = ["ReadWriteOnce"]
    resources {
      requests = {
        storage = "1Gi"
      }
    }
  }
}

# ---------- K8s Service Account with Workload Identity ----------
resource "kubernetes_service_account" "debezium" {
  metadata {
    name      = "debezium"
    namespace = var.namespace

    annotations = {
      "iam.gke.io/gcp-service-account" = "debezium-sa@${var.project_id}.iam.gserviceaccount.com"
    }
  }
}

# ---------- Deployment ----------
resource "kubernetes_deployment" "debezium" {
  metadata {
    name      = "debezium"
    namespace = var.namespace
    labels = {
      app       = "debezium"
      component = "cdc"
    }
  }

  spec {
    replicas = 1

    selector {
      match_labels = {
        app = "debezium"
      }
    }

    template {
      metadata {
        labels = {
          app = "debezium"
        }
      }

      spec {
        service_account_name = kubernetes_service_account.debezium.metadata[0].name

        # debezium/server runs as UID 1001 — fsGroup ensures PVC is writable
        security_context {
          fs_group = 1001
        }

        container {
          name  = "debezium-server"
          image = "debezium/server:2.4"

          port {
            container_port = 8080
          }

          # Mount application.properties into the conf directory
          volume_mount {
            name       = "config"
            mount_path = "/debezium/conf"
          }

          # Persistent storage for offsets + schema history
          volume_mount {
            name       = "data"
            mount_path = "/debezium/data"
          }

          resources {
            requests = {
              cpu    = "250m"
              memory = "512Mi"
            }
            limits = {
              cpu    = "500m"
              memory = "1Gi"
            }
          }

          liveness_probe {
            http_get {
              path = "/q/health/live"
              port = 8080
            }
            initial_delay_seconds = 30
            period_seconds        = 30
          }

          readiness_probe {
            http_get {
              path = "/q/health/ready"
              port = 8080
            }
            initial_delay_seconds = 30
            period_seconds        = 10
          }
        }

        volume {
          name = "config"
          config_map {
            name = kubernetes_config_map.debezium_config.metadata[0].name
          }
        }

        volume {
          name = "data"
          persistent_volume_claim {
            claim_name = kubernetes_persistent_volume_claim.debezium_data.metadata[0].name
          }
        }
      }
    }
  }
}

# ---------- Service (health check only — no REST API) ----------
resource "kubernetes_service" "debezium" {
  metadata {
    name      = "debezium"
    namespace = var.namespace
  }

  spec {
    selector = {
      app = "debezium"
    }

    port {
      name        = "health"
      port        = 8080
      target_port = 8080
    }

    type = "ClusterIP"
  }
}
