#!/usr/bin/env bash
# =============================================================================
# teardown.sh — Destroy all POC resources (GKE + GCP services)
# Run after your 3-4h POC session to avoid costs.
# =============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(dirname "$SCRIPT_DIR")"
TF_DIR="$ROOT_DIR/terraform"

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

log()  { echo -e "${GREEN}[teardown]${NC} $1"; }
warn() { echo -e "${YELLOW}[teardown]${NC} $1"; }

PROJECT_ID=$(gcloud config get-value project 2>/dev/null)
log "Tearing down POC for project: $PROJECT_ID"

# --- Confirmation ---
echo ""
echo -e "${RED}WARNING: This will destroy ALL POC resources:${NC}"
echo "  - GKE cluster (MySQL, Debezium, DataHub, Serving API)"
echo "  - Pub/Sub topics and subscriptions"
echo "  - BigQuery datasets (lightspeed_analytics, lightspeed_batch)"
echo "  - Firestore database"
echo "  - GCS bootstrap bucket"
echo "  - IAM service accounts"
echo ""
read -p "Type 'destroy' to confirm: " confirm
[ "$confirm" != "destroy" ] && { log "Aborted."; exit 0; }

# --- Stop Dataflow jobs if running ---
log "Stopping Dataflow jobs..."
gcloud dataflow jobs list --project="$PROJECT_ID" --status=active --format="value(id)" 2>/dev/null | while read -r job_id; do
    log "  Cancelling Dataflow job: $job_id"
    gcloud dataflow jobs cancel "$job_id" --project="$PROJECT_ID" --quiet 2>/dev/null || true
done

# --- Delete GCR images ---
log "Deleting container images..."
gcloud container images delete "gcr.io/$PROJECT_ID/serving-api:latest" --force-delete-tags --quiet 2>/dev/null || true

# --- Terraform destroy ---
log "Running Terraform destroy..."
cd "$TF_DIR"

if [ -f "terraform.tfstate" ] || [ -d ".terraform" ]; then
    terraform destroy -auto-approve
else
    warn "No Terraform state found — skipping"
fi

# --- Cleanup local files ---
log "Cleaning up local Terraform files..."
rm -rf "$TF_DIR/.terraform"
rm -f "$TF_DIR/terraform.tfstate"*
rm -f "$TF_DIR/tfplan"
rm -f "$TF_DIR/.terraform.lock.hcl"

log "=========================================="
log "Teardown complete — all resources destroyed"
log "=========================================="
