###############################################################################
# main.tf — POC Stripe Lightspeed — Orchestrateur de modules
###############################################################################

terraform {
  required_version = ">= 1.5"

  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
    kubernetes = {
      source  = "hashicorp/kubernetes"
      version = "~> 2.25"
    }
  }
}

provider "google" {
  project = var.project_id
  region  = var.region
  zone    = var.zone
}

# ---------- GKE cluster (doit exister avant le provider kubernetes) ----------
module "gke" {
  source = "./modules/gke"

  project_id    = var.project_id
  region        = var.region
  zone          = var.zone
  cluster_name  = var.gke_cluster_name
  machine_type  = var.gke_node_machine_type
  node_count    = var.gke_node_count
  environment   = var.environment
}

# ---------- Provider Kubernetes configuré via le cluster GKE ----------
provider "kubernetes" {
  host                   = "https://${module.gke.endpoint}"
  token                  = data.google_client_config.default.access_token
  cluster_ca_certificate = base64decode(module.gke.ca_certificate)
}

data "google_client_config" "default" {}

# ---------- GCP managed services ----------
module "pubsub" {
  source = "./modules/pubsub"

  project_id = var.project_id
  topics     = var.pubsub_topics
}

module "bigquery" {
  source = "./modules/bigquery"

  project_id        = var.project_id
  region            = var.region
  dataset_analytics = var.bq_dataset_analytics
  dataset_batch     = var.bq_dataset_batch
}

module "firestore" {
  source = "./modules/firestore"

  project_id = var.project_id
  region     = var.region
}

module "gcs" {
  source = "./modules/gcs"

  project_id  = var.project_id
  region      = var.region
  bucket_name = "${var.bootstrap_bucket_name}-${var.project_id}"
}

module "iam" {
  source = "./modules/iam"

  project_id = var.project_id
}

# ---------- Kubernetes workloads on GKE ----------
module "k8s_mysql" {
  source = "./modules/k8s-mysql"

  namespace      = var.gke_namespace
  root_password  = var.mysql_root_password
  database_name  = var.mysql_database

  depends_on = [module.gke]
}

module "k8s_debezium" {
  source = "./modules/k8s-debezium"

  namespace      = var.gke_namespace
  mysql_host     = module.k8s_mysql.service_host
  mysql_port     = module.k8s_mysql.service_port
  mysql_password = var.mysql_root_password
  mysql_database = var.mysql_database
  project_id     = var.project_id

  depends_on = [module.k8s_mysql, module.pubsub]
}

module "k8s_serving_api" {
  source = "./modules/k8s-serving-api"

  namespace  = var.gke_namespace
  project_id = var.project_id

  depends_on = [module.gke, module.firestore]
}
