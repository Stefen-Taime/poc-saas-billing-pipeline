###############################################################################
# Module: k8s-datahub — DataHub quickstart (all-in-one) on GKE
###############################################################################

# ---------- Deployment (DataHub GMS + Frontend + dependencies) ----------
resource "kubernetes_deployment" "datahub" {
  metadata {
    name      = "datahub"
    namespace = var.namespace
    labels = {
      app       = "datahub"
      component = "catalog"
    }
  }

  spec {
    replicas = 1

    selector {
      match_labels = {
        app = "datahub"
      }
    }

    template {
      metadata {
        labels = {
          app = "datahub"
        }
      }

      spec {
        # Disable K8s auto-injected service env vars (MYSQL_PORT, etc.)
        # They conflict with DataHub's own MYSQL_PORT/MYSQL_HOST env vars
        enable_service_links = false

        # ---------- MySQL backend for DataHub ----------
        container {
          name  = "datahub-mysql"
          image = "mysql:5.7"

          port {
            container_port = 3306
          }

          env {
            name  = "MYSQL_ROOT_PASSWORD"
            value = "datahub"
          }

          env {
            name  = "MYSQL_DATABASE"
            value = "datahub"
          }

          volume_mount {
            name       = "datahub-mysql-init"
            mount_path = "/docker-entrypoint-initdb.d"
          }

          resources {
            requests = {
              cpu    = "100m"
              memory = "256Mi"
            }
            limits = {
              cpu    = "250m"
              memory = "512Mi"
            }
          }
        }

        # ---------- Elasticsearch ----------
        container {
          name  = "elasticsearch"
          image = "elasticsearch:7.10.1"

          port {
            container_port = 9200
          }

          env {
            name  = "discovery.type"
            value = "single-node"
          }

          env {
            name  = "ES_JAVA_OPTS"
            value = "-Xms512m -Xmx512m"
          }

          resources {
            requests = {
              cpu    = "250m"
              memory = "768Mi"
            }
            limits = {
              cpu    = "750m"
              memory = "1536Mi"
            }
          }
        }

        # ---------- DataHub GMS ----------
        container {
          name  = "datahub-gms"
          image = "linkedin/datahub-gms:${var.datahub_version}"

          # Override entrypoint: wait for Schema Registry before starting GMS.
          # The built-in dockerize in start.sh only waits for MySQL/Kafka/ES,
          # but GMS bootstrap (IngestPoliciesStep) needs Schema Registry for Avro.
          command = ["/bin/bash", "-c"]
          args = [
            <<-EOT
              echo "Waiting for Schema Registry on localhost:8081 ..."
              while ! wget -q --spider http://localhost:8081/subjects 2>/dev/null; do
                sleep 3
              done
              echo "Schema Registry is ready. Launching GMS ..."
              exec /datahub/datahub-gms/scripts/start.sh
            EOT
          ]

          port {
            container_port = 8080
          }

          env {
            name  = "MYSQL_HOST"
            value = "localhost"
          }

          env {
            name  = "MYSQL_PORT"
            value = "3306"
          }

          env {
            name  = "MYSQL_USERNAME"
            value = "root"
          }

          env {
            name  = "MYSQL_PASSWORD"
            value = "datahub"
          }

          env {
            name  = "ELASTICSEARCH_HOST"
            value = "localhost"
          }

          env {
            name  = "ELASTICSEARCH_PORT"
            value = "9200"
          }

          env {
            name  = "KAFKA_BOOTSTRAP_SERVER"
            value = "localhost:9092"
          }

          env {
            name  = "GRAPH_SERVICE_IMPL"
            value = "elasticsearch"
          }

          env {
            name  = "DATAHUB_SECRET"
            value = "YouKnowNothing"
          }

          env {
            name  = "EBEAN_DATASOURCE_URL"
            value = "jdbc:mysql://localhost:3306/datahub?verifyServerCertificate=false&useSSL=false&useUnicode=yes&characterEncoding=UTF-8"
          }

          env {
            name  = "EBEAN_DATASOURCE_USERNAME"
            value = "root"
          }

          env {
            name  = "EBEAN_DATASOURCE_PASSWORD"
            value = "datahub"
          }

          env {
            name  = "EBEAN_DATASOURCE_DRIVER"
            value = "com.mysql.jdbc.Driver"
          }

          env {
            name  = "EBEAN_DATASOURCE_HOST"
            value = "localhost:3306"
          }

          env {
            name  = "ENTITY_REGISTRY_CONFIG_PATH"
            value = "/datahub/datahub-gms/resources/entity-registry.yml"
          }

          env {
            name  = "SKIP_ELASTICSEARCH_CHECK"
            value = "false"
          }

          env {
            name  = "ELASTICSEARCH_USE_SSL"
            value = "false"
          }

          env {
            name  = "JAVA_OPTS"
            value = "-Xms512m -Xmx1536m"
          }

          env {
            name  = "KAFKA_SCHEMAREGISTRY_URL"
            value = "http://localhost:8081"
          }

          resources {
            requests = {
              cpu    = "750m"
              memory = "1536Mi"
            }
            limits = {
              cpu    = "2000m"
              memory = "2560Mi"
            }
          }
        }

        # ---------- DataHub Frontend ----------
        container {
          name  = "datahub-frontend"
          image = "linkedin/datahub-frontend-react:${var.datahub_version}"

          # Wait for GMS to be up before starting frontend
          command = ["/bin/sh", "-c"]
          args = [
            <<-EOT
              echo "Waiting for DataHub GMS on localhost:8080 ..."
              while ! wget -q --spider http://localhost:8080/health 2>/dev/null; do
                sleep 5
              done
              echo "GMS is ready. Launching Frontend ..."
              exec /start.sh
            EOT
          ]

          port {
            container_port = 9002
          }

          env {
            name  = "SERVER_PORT"
            value = "9002"
          }

          env {
            name  = "DATAHUB_GMS_HOST"
            value = "localhost"
          }

          env {
            name  = "DATAHUB_GMS_PORT"
            value = "8080"
          }

          env {
            name  = "DATAHUB_SECRET"
            value = "YouKnowNothing"
          }

          env {
            name  = "DATAHUB_APP_VERSION"
            value = "1.0"
          }

          env {
            name  = "DATAHUB_PLAY_MEM_BUFFER_SIZE"
            value = "10MB"
          }

          env {
            name  = "KAFKA_BOOTSTRAP_SERVER"
            value = "localhost:9092"
          }

          env {
            name  = "ELASTICSEARCH_HOST"
            value = "localhost"
          }

          env {
            name  = "ELASTICSEARCH_PORT"
            value = "9200"
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
        }

        # ---------- Kafka (for DataHub internal messaging) ----------
        container {
          name  = "kafka"
          image = "confluentinc/cp-kafka:7.5.0"

          port {
            container_port = 9092
          }

          env {
            name  = "KAFKA_NODE_ID"
            value = "1"
          }

          env {
            name  = "KAFKA_PROCESS_ROLES"
            value = "broker,controller"
          }

          env {
            name  = "KAFKA_LISTENERS"
            value = "PLAINTEXT://0.0.0.0:9092,CONTROLLER://0.0.0.0:9093"
          }

          env {
            name  = "KAFKA_ADVERTISED_LISTENERS"
            value = "PLAINTEXT://localhost:9092"
          }

          env {
            name  = "KAFKA_CONTROLLER_LISTENER_NAMES"
            value = "CONTROLLER"
          }

          env {
            name  = "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP"
            value = "CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT"
          }

          env {
            name  = "KAFKA_CONTROLLER_QUORUM_VOTERS"
            value = "1@localhost:9093"
          }

          env {
            name  = "KAFKA_LOG_DIRS"
            value = "/tmp/kraft-logs"
          }

          env {
            name  = "CLUSTER_ID"
            value = "RGF0YUh1YktyYWZ0Q2x1cw"
          }

          # Single-broker overrides (default replication factor = 3 fails with 1 broker)
          env {
            name  = "KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR"
            value = "1"
          }

          env {
            name  = "KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR"
            value = "1"
          }

          env {
            name  = "KAFKA_TRANSACTION_STATE_LOG_MIN_ISR"
            value = "1"
          }

          resources {
            requests = {
              cpu    = "100m"
              memory = "256Mi"
            }
            limits = {
              cpu    = "250m"
              memory = "512Mi"
            }
          }
        }

        # ---------- Schema Registry (required by DataHub for Avro serialization) ----------
        container {
          name  = "schema-registry"
          image = "confluentinc/cp-schema-registry:7.5.0"

          port {
            container_port = 8081
          }

          env {
            name  = "SCHEMA_REGISTRY_HOST_NAME"
            value = "localhost"
          }

          env {
            name  = "SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS"
            value = "PLAINTEXT://localhost:9092"
          }

          resources {
            requests = {
              cpu    = "100m"
              memory = "256Mi"
            }
            limits = {
              cpu    = "250m"
              memory = "512Mi"
            }
          }
        }

        volume {
          name = "datahub-mysql-init"
          config_map {
            name = kubernetes_config_map.datahub_mysql_init.metadata[0].name
          }
        }
      }
    }
  }
}

# ---------- MySQL init SQL for DataHub schema ----------
resource "kubernetes_config_map" "datahub_mysql_init" {
  metadata {
    name      = "datahub-mysql-init"
    namespace = var.namespace
  }

  data = {
    "init.sql" = <<-SQL
      CREATE DATABASE IF NOT EXISTS datahub CHARACTER SET latin1;
      USE datahub;

      CREATE TABLE IF NOT EXISTS metadata_aspect_v2 (
        urn VARCHAR(500) NOT NULL,
        aspect VARCHAR(200) NOT NULL,
        version BIGINT NOT NULL,
        metadata LONGTEXT NOT NULL,
        systemMetadata LONGTEXT,
        createdOn DATETIME(6) NOT NULL,
        createdBy VARCHAR(255) NOT NULL,
        createdFor VARCHAR(255),
        PRIMARY KEY (urn, aspect, version),
        INDEX idx_urn (urn),
        INDEX idx_aspect (aspect)
      ) ENGINE=InnoDB;

      CREATE TABLE IF NOT EXISTS metadata_index (
        id BIGINT AUTO_INCREMENT PRIMARY KEY,
        urn VARCHAR(500) NOT NULL,
        aspect VARCHAR(200) NOT NULL,
        path VARCHAR(200) NOT NULL,
        longVal BIGINT,
        stringVal VARCHAR(500),
        doubleVal DOUBLE,
        INDEX idx_long_val (aspect, path, longVal, urn),
        INDEX idx_string_val (aspect, path, stringVal, urn),
        INDEX idx_double_val (aspect, path, doubleVal, urn),
        INDEX idx_urn (urn)
      ) ENGINE=InnoDB;
    SQL
  }
}

# ---------- Service — LoadBalancer for DataHub UI ----------
resource "kubernetes_service" "datahub" {
  metadata {
    name      = "datahub"
    namespace = var.namespace
  }

  spec {
    selector = {
      app = "datahub"
    }

    port {
      name        = "frontend"
      port        = 9002
      target_port = 9002
    }

    port {
      name        = "gms"
      port        = 8080
      target_port = 8080
    }

    type = "LoadBalancer"
  }
}
