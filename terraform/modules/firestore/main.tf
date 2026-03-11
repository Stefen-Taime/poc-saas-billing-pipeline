###############################################################################
# Module: firestore — Serving layer (document par merchant_id)
###############################################################################

resource "google_firestore_database" "serving" {
  project     = var.project_id
  name        = "(default)"
  location_id = var.region
  type        = "FIRESTORE_NATIVE"

  # POC — pas de backup
  delete_protection_state = "DELETE_PROTECTION_DISABLED"
}
