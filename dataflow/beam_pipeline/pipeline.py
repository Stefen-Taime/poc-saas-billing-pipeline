"""
pipeline.py — Dataflow Streaming Pipeline
CDC events from Pub/Sub → Firestore (serving) + BigQuery (analytics)
"""

import argparse
import base64
import json
import logging
from datetime import date, datetime, timedelta

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from apache_beam.io.gcp.pubsub import ReadFromPubSub
from google.cloud import firestore, bigquery

logger = logging.getLogger(__name__)

# Epoch reference for Debezium date fields (io.debezium.time.Date = days since epoch)
_EPOCH = date(1970, 1, 1)

# Fields that Debezium emits as epoch-day integers (MySQL DATE type)
_DATE_FIELDS = {
    "date_debut", "date_prochain_paiement", "periode_essai_fin",
    "date_emission", "date_echeance", "date_effet", "date_inscription",
}

# Fields that Debezium emits as DECIMAL (base64 when decimal.handling.mode=bytes,
# string when decimal.handling.mode=string). Scale per field.
_DECIMAL_FIELDS = {
    "montant_mensuel_cad": 2,
    "montant_cad": 2,
    "ancien_montant_cad": 2,
    "nouveau_montant_cad": 2,
}

# Fields that Debezium emits as 0/1 for MySQL TINYINT(1) / BOOLEAN
_BOOLEAN_FIELDS = {"annulation_schedulee"}


def _decode_debezium_decimal(value, scale):
    """Decode a Debezium DECIMAL value to a float.

    Handles both base64-encoded bytes (decimal.handling.mode=bytes, the default)
    and string representation (decimal.handling.mode=string).
    """
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        # Try parsing as a numeric string first (decimal.handling.mode=string)
        try:
            return float(value)
        except ValueError:
            pass
        # Fall back to base64-encoded bytes (decimal.handling.mode=bytes)
        try:
            raw_bytes = base64.b64decode(value)
            unscaled = int.from_bytes(raw_bytes, byteorder="big", signed=True)
            return unscaled / (10 ** scale)
        except Exception:
            logger.warning(f"Could not decode decimal value: {value!r}")
            return None
    return None


def _decode_debezium_date(value):
    """Convert Debezium epoch-day integer to 'YYYY-MM-DD' string."""
    if value is None:
        return None
    if isinstance(value, int):
        return (_EPOCH + timedelta(days=value)).isoformat()
    # Already a string date
    if isinstance(value, str):
        return value
    return None


def _normalise_after(after):
    """Normalise Debezium-encoded field values in an 'after' dict."""
    if not after:
        return after
    result = dict(after)
    for field in _DATE_FIELDS:
        if field in result:
            result[field] = _decode_debezium_date(result[field])
    for field, scale in _DECIMAL_FIELDS.items():
        if field in result:
            result[field] = _decode_debezium_decimal(result[field], scale)
    for field in _BOOLEAN_FIELDS:
        if field in result:
            result[field] = bool(result[field])
    return result


# =============================================================================
# Transforms
# =============================================================================

class ParseCDCEvent(beam.DoFn):
    """Parse JSON CDC event from Debezium via Pub/Sub."""

    def process(self, element):
        try:
            raw = json.loads(element.decode("utf-8"))
            payload = raw.get("payload", raw)

            after = _normalise_after(payload.get("after", {}))
            before = payload.get("before", {})
            op = payload.get("op", "u")  # c=create, u=update, d=delete

            # Extract table name from source
            source = payload.get("source", {})
            table = source.get("table", "unknown")

            yield {
                "table": table,
                "operation": op,
                "before": before,
                "after": after,
                "timestamp": datetime.utcnow().isoformat(),
                "ts_ms": payload.get("ts_ms", 0),
            }
        except Exception as e:
            logger.error(f"Failed to parse CDC event: {e}")


class WriteToFirestore(beam.DoFn):
    """Write merchant state to Firestore serving layer."""

    def __init__(self, project_id):
        self.project_id = project_id
        self.client = None

    def setup(self):
        self.client = firestore.Client(project=self.project_id)

    def process(self, element):
        table = element["table"]
        after = element["after"]

        if not after:
            return

        merchant_id = after.get("merchant_id")
        if not merchant_id:
            return

        if table == "subscriptions":
            doc_ref = self.client.collection("merchants_state").document(merchant_id)
            doc_ref.set(
                {
                    "merchant_id": merchant_id,
                    "plan_actuel": after.get("plan"),
                    "statut_abonnement": after.get("statut"),
                    "montant_mensuel_cad": float(after.get("montant_mensuel_cad", 0)),
                    "date_prochain_paiement": after.get("date_prochain_paiement"),
                    "annulation_schedulee": after.get("annulation_schedulee", False),
                    "updated_at": element["timestamp"],
                },
                merge=True,
            )

        elif table == "merchants":
            doc_ref = self.client.collection("merchants_state").document(merchant_id)
            doc_ref.set(
                {
                    "merchant_id": merchant_id,
                    "nom": after.get("nom"),
                    "email": after.get("email"),
                    "plan_actuel": after.get("plan"),
                    "statut_merchant": after.get("statut"),
                    "province": after.get("province"),
                    "updated_at": element["timestamp"],
                },
                merge=True,
            )

        elif table == "payment_attempts":
            doc_ref = self.client.collection("merchants_state").document(merchant_id)
            doc_ref.set(
                {
                    "derniere_tentative_paiement": after.get("created_at"),
                    "statut_derniere_tentative": after.get("statut"),
                    "nb_echecs_paiement_consecutifs": (
                        firestore.Increment(1) if after.get("statut") == "failed" else 0
                    ),
                    "updated_at": element["timestamp"],
                },
                merge=True,
            )

        yield element


class WriteToBigQuery(beam.DoFn):
    """Write CDC events to BigQuery analytics tables."""

    TABLE_MAP = {
        "subscriptions": "subscriptions_stream",
        "invoices": "invoices_stream",
        "payment_attempts": "payment_attempts_stream",
        "plan_changes": "plan_changes_stream",
    }

    def __init__(self, project_id, dataset):
        self.project_id = project_id
        self.dataset = dataset
        self.client = None

    def setup(self):
        self.client = bigquery.Client(project=self.project_id)

    def process(self, element):
        table = element["table"]
        bq_table = self.TABLE_MAP.get(table)

        if not bq_table or not element["after"]:
            return

        row = dict(element["after"])
        row["cdc_operation"] = element["operation"]
        row["cdc_timestamp"] = element["timestamp"]

        table_ref = f"{self.project_id}.{self.dataset}.{bq_table}"

        errors = self.client.insert_rows_json(table_ref, [row])
        if errors:
            logger.error(f"BigQuery insert errors for {bq_table}: {errors}")

        yield element


# =============================================================================
# Pipeline
# =============================================================================

def run(argv=None):
    parser = argparse.ArgumentParser(description="CDC Streaming Pipeline")
    parser.add_argument("--project", required=True, help="GCP project ID")
    parser.add_argument("--dataset", default="lightspeed_analytics", help="BigQuery dataset")
    parser.add_argument(
        "--subscriptions",
        nargs="+",
        default=[
            "lightspeed.subscriptions.cdc-dataflow-sub",
            "lightspeed.merchants.cdc-dataflow-sub",
            "lightspeed.invoices.cdc-dataflow-sub",
            "lightspeed.payment_attempts.cdc-dataflow-sub",
            "lightspeed.plan_changes.cdc-dataflow-sub",
        ],
        help="Pub/Sub subscription names",
    )

    known_args, pipeline_args = parser.parse_known_args(argv)

    # Re-inject --project into pipeline_args so DataflowRunner sees it
    pipeline_args.extend(["--project", known_args.project])

    options = PipelineOptions(pipeline_args)
    options.view_as(StandardOptions).streaming = True

    with beam.Pipeline(options=options) as p:
        for sub_name in known_args.subscriptions:
            subscription_path = f"projects/{known_args.project}/subscriptions/{sub_name}"

            events = (
                p
                | f"Read_{sub_name}" >> ReadFromPubSub(subscription=subscription_path)
                | f"Parse_{sub_name}" >> beam.ParDo(ParseCDCEvent())
            )

            # Serving layer — Firestore
            events | f"Firestore_{sub_name}" >> beam.ParDo(
                WriteToFirestore(known_args.project)
            )

            # Analytics layer — BigQuery
            events | f"BigQuery_{sub_name}" >> beam.ParDo(
                WriteToBigQuery(known_args.project, known_args.dataset)
            )


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    run()
