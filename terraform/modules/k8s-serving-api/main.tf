###############################################################################
# Module: k8s-serving-api — Flask API reading from Firestore
###############################################################################

# ---------- K8s Service Account with Workload Identity ----------
resource "kubernetes_service_account" "serving_api" {
  metadata {
    name      = "serving-api"
    namespace = var.namespace

    annotations = {
      "iam.gke.io/gcp-service-account" = "serving-api-sa@${var.project_id}.iam.gserviceaccount.com"
    }
  }
}

# ---------- Deployment ----------
resource "kubernetes_deployment" "serving_api" {
  metadata {
    name      = "serving-api"
    namespace = var.namespace
    labels = {
      app       = "serving-api"
      component = "serving-layer"
    }
  }

  # Don't block terraform apply — image may not exist yet on first run
  wait_for_rollout = false

  spec {
    replicas = 2

    selector {
      match_labels = {
        app = "serving-api"
      }
    }

    template {
      metadata {
        labels = {
          app = "serving-api"
        }
      }

      spec {
        service_account_name = kubernetes_service_account.serving_api.metadata[0].name

        container {
          name  = "flask-api"
          image = "gcr.io/${var.project_id}/serving-api:latest"

          port {
            container_port = 5000
          }

          env {
            name  = "GCP_PROJECT"
            value = var.project_id
          }

          env {
            name  = "FLASK_ENV"
            value = "production"
          }

          env {
            name  = "FIRESTORE_COLLECTION"
            value = "merchants_state"
          }

          resources {
            requests = {
              cpu    = "100m"
              memory = "128Mi"
            }
            limits = {
              cpu    = "250m"
              memory = "256Mi"
            }
          }

          liveness_probe {
            http_get {
              path = "/health"
              port = 5000
            }
            initial_delay_seconds = 10
            period_seconds        = 15
          }

          readiness_probe {
            http_get {
              path = "/health"
              port = 5000
            }
            initial_delay_seconds = 5
            period_seconds        = 5
          }
        }
      }
    }
  }
}

# ---------- Service — LoadBalancer ----------
resource "kubernetes_service" "serving_api" {
  metadata {
    name      = "serving-api"
    namespace = var.namespace
  }

  spec {
    selector = {
      app = "serving-api"
    }

    port {
      port        = 80
      target_port = 5000
    }

    type = "LoadBalancer"
  }
}
