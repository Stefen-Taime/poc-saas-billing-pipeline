"""
app.py — Flask Serving API
Simulates the Stripe Subscriptions API.
Reads from Firestore serving layer — < 5ms per query.
"""

import os
import time
from datetime import datetime

from flask import Flask, jsonify
from google.cloud import firestore

app = Flask(__name__)

PROJECT_ID = os.getenv("GCP_PROJECT")
COLLECTION = os.getenv("FIRESTORE_COLLECTION", "merchants_state")

db_client = firestore.Client(project=PROJECT_ID)


@app.route("/health", methods=["GET"])
def health():
    """Health check endpoint."""
    return jsonify({"status": "healthy", "service": "serving-api", "timestamp": datetime.utcnow().isoformat()}), 200


@app.route("/merchant/<merchant_id>/subscription", methods=["GET"])
def get_subscription(merchant_id: str):
    """
    GET /merchant/{merchant_id}/subscription
    Simulates Stripe GET /v1/subscriptions
    Reads from Firestore — O(1) lookup by merchant_id
    """
    start = time.monotonic()

    doc = db_client.collection(COLLECTION).document(merchant_id).get()

    latency_ms = (time.monotonic() - start) * 1000

    if not doc.exists:
        return jsonify({"error": "merchant not found", "merchant_id": merchant_id}), 404

    data = doc.to_dict()
    data["_latency_ms"] = round(latency_ms, 2)

    return jsonify(data), 200


@app.route("/merchant/<merchant_id>/invoices", methods=["GET"])
def get_invoices(merchant_id: str):
    """
    GET /merchant/{merchant_id}/invoices
    Returns the last 12 invoices for the merchant.
    """
    start = time.monotonic()

    invoices = (
        db_client.collection("invoices_state")
        .where("merchant_id", "==", merchant_id)
        .order_by("date_emission", direction=firestore.Query.DESCENDING)
        .limit(12)
        .get()
    )

    latency_ms = (time.monotonic() - start) * 1000

    results = [inv.to_dict() for inv in invoices]

    return jsonify({
        "merchant_id": merchant_id,
        "invoices": results,
        "count": len(results),
        "_latency_ms": round(latency_ms, 2),
    }), 200


@app.route("/health/mrr", methods=["GET"])
def get_mrr_snapshot():
    """
    GET /health/mrr
    MRR total from Firestore (light aggregation for reconciliation tests).
    """
    start = time.monotonic()

    merchants = (
        db_client.collection(COLLECTION)
        .where("statut_abonnement", "==", "active")
        .get()
    )

    total_mrr = sum(m.to_dict().get("montant_mensuel_cad", 0) for m in merchants)
    count = len(merchants)

    latency_ms = (time.monotonic() - start) * 1000

    return jsonify({
        "mrr_firestore_cad": round(total_mrr, 2),
        "active_merchants": count,
        "as_of": datetime.utcnow().isoformat(),
        "_latency_ms": round(latency_ms, 2),
    }), 200


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=False)
