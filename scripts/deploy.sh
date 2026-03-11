#!/usr/bin/env bash
# =============================================================================
# deploy.sh — Deploy the full POC Stripe Lightspeed pipeline
# =============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(dirname "$SCRIPT_DIR")"
TF_DIR="$ROOT_DIR/terraform"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

log()  { echo -e "${GREEN}[deploy]${NC} $1"; }
warn() { echo -e "${YELLOW}[deploy]${NC} $1"; }
err()  { echo -e "${RED}[deploy]${NC} $1"; exit 1; }

# --- Pre-flight checks ---
command -v terraform >/dev/null 2>&1 || err "terraform not found"
command -v gcloud >/dev/null 2>&1    || err "gcloud not found"

# --- Get project ID ---
PROJECT_ID=$(gcloud config get-value project 2>/dev/null)
[ -z "$PROJECT_ID" ] && err "No GCP project set. Run: gcloud config set project YOUR_PROJECT"
log "GCP project: $PROJECT_ID"

# --- Check MySQL password ---
if [ -z "${TF_VAR_mysql_root_password:-}" ]; then
    warn "TF_VAR_mysql_root_password not set"
    read -sp "Enter MySQL root password: " TF_VAR_mysql_root_password
    echo
    export TF_VAR_mysql_root_password
fi

# --- Enable required APIs ---
log "Enabling GCP APIs..."
gcloud services enable \
    container.googleapis.com \
    pubsub.googleapis.com \
    bigquery.googleapis.com \
    firestore.googleapis.com \
    dataflow.googleapis.com \
    dataproc.googleapis.com \
    cloudbuild.googleapis.com \
    --project="$PROJECT_ID" \
    --quiet

# --- Terraform ---
log "Running Terraform init..."
cd "$TF_DIR"
terraform init

log "Running Terraform plan..."
terraform plan -out=tfplan

log "Applying Terraform..."
terraform apply tfplan
rm -f tfplan

# --- Get GKE credentials ---
CLUSTER_NAME=$(terraform output -raw gke_cluster_name)
ZONE=$(terraform output -raw gke_zone 2>/dev/null || echo "us-east1-b")

log "Getting GKE credentials..."
gcloud container clusters get-credentials "$CLUSTER_NAME" --zone "$ZONE" --project "$PROJECT_ID"

# --- Build and push serving API image ---
log "Building serving API Docker image..."
cd "$ROOT_DIR/serving-api"
gcloud builds submit --tag "gcr.io/$PROJECT_ID/serving-api:latest" --quiet

# --- Wait for pods ---
log "Waiting for pods to be ready..."
kubectl -n lightspeed wait --for=condition=ready pod -l app=mysql --timeout=120s 2>/dev/null || warn "MySQL pod not ready yet"
kubectl -n lightspeed wait --for=condition=ready pod -l app=debezium --timeout=120s 2>/dev/null || warn "Debezium pod not ready yet"

# --- Debezium Server starts automatically from application.properties ---
# No connector registration needed (unlike Kafka Connect, Debezium Server
# reads its config from application.properties at startup)
log "Debezium Server will auto-start CDC from application.properties config"

# --- Output ---
log "=========================================="
log "Deployment complete!"
log "=========================================="
cd "$TF_DIR"
terraform output

log ""
log "Next steps:"
log "  1. Run Go simulator locally:"
log "     cd go-simulator"
log "     export MYSQL_PASSWORD=\$TF_VAR_mysql_root_password"
log "     go run main.go --mysql-host=\$(terraform -chdir=$TF_DIR output -raw mysql_external_ip)"
log ""
log "  2. Start Dataflow pipeline:"
log "     python dataflow/beam_pipeline/pipeline.py --project=$PROJECT_ID --runner=DataflowRunner"
log ""
log "  3. Emit lineage to DataHub:"
log "     export DATAHUB_GMS_URL=\$(terraform -chdir=$TF_DIR output -raw datahub_url | sed 's/:9002/:8080/')"
log "     python datahub/emitters/datahub_emitter.py"
