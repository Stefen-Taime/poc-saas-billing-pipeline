"""
datahub_emitter.py — Shared lineage emitter for all pipeline components.
Emits dataset lineage to DataHub GMS REST API.
"""

import logging
import os

from datahub.emitter.mce_builder import make_dataset_urn, make_data_job_urn
from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.metadata.schema_classes import (
    DatasetLineageTypeClass,
    UpstreamClass,
    UpstreamLineageClass,
)
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.metadata.schema_classes import DataJobInfoClass

logger = logging.getLogger(__name__)

DATAHUB_GMS_URL = os.getenv("DATAHUB_GMS_URL", "http://localhost:8080")


def get_emitter() -> DatahubRestEmitter:
    """Create a DataHub REST emitter."""
    return DatahubRestEmitter(gms_server=DATAHUB_GMS_URL)


def emit_lineage(
    source_platform: str,
    source_tables: list[str],
    dest_platform: str,
    dest_table: str,
    job_name: str,
    job_description: str = "",
):
    """
    Emit lineage from source tables to a destination table via DataHub.

    Args:
        source_platform: Source platform (e.g., "mysql", "pubsub")
        source_tables: List of source table/topic names
        dest_platform: Destination platform (e.g., "bigquery", "firestore")
        dest_table: Destination table name
        job_name: DataHub job identifier
        job_description: Optional job description
    """
    emitter = get_emitter()

    # Build upstream lineage
    upstreams = [
        UpstreamClass(
            dataset=make_dataset_urn(source_platform, table),
            type=DatasetLineageTypeClass.TRANSFORMED,
        )
        for table in source_tables
    ]

    lineage = UpstreamLineageClass(upstreams=upstreams)

    # Emit lineage on destination dataset
    dest_urn = make_dataset_urn(dest_platform, dest_table)
    mcp = MetadataChangeProposalWrapper(
        entityUrn=dest_urn,
        aspect=lineage,
    )

    try:
        emitter.emit(mcp)
        logger.info(f"Lineage emitted: {source_tables} → {dest_table} (job: {job_name})")
    except Exception as e:
        logger.error(f"Failed to emit lineage: {e}")
    finally:
        emitter.close()


# =============================================================================
# Pre-configured lineage for each pipeline component
# =============================================================================

def emit_debezium_lineage():
    """Debezium CDC: MySQL → Pub/Sub topics."""
    tables = ["merchants", "subscriptions", "invoices", "payment_attempts", "plan_changes"]

    for table in tables:
        emit_lineage(
            source_platform="mysql",
            source_tables=[f"lightspeed_db.{table}"],
            dest_platform="pubsub",
            dest_table=f"lightspeed.{table}.cdc",
            job_name="debezium-cdc-ingestion",
            job_description="Debezium MySQL CDC to Pub/Sub",
        )


def emit_dataflow_streaming_lineage():
    """Dataflow streaming: Pub/Sub → Firestore + BigQuery."""
    tables = ["subscriptions", "invoices", "payment_attempts", "plan_changes"]

    for table in tables:
        # Pub/Sub → BigQuery
        emit_lineage(
            source_platform="pubsub",
            source_tables=[f"lightspeed.{table}.cdc"],
            dest_platform="bigquery",
            dest_table=f"lightspeed_analytics.{table}_stream",
            job_name="dataflow-cdc-to-analytics",
            job_description="Dataflow CDC streaming to BigQuery",
        )

    # Pub/Sub → Firestore (merchants + subscriptions)
    emit_lineage(
        source_platform="pubsub",
        source_tables=[
            "lightspeed.merchants.cdc",
            "lightspeed.subscriptions.cdc",
            "lightspeed.payment_attempts.cdc",
        ],
        dest_platform="firestore",
        dest_table="merchants_state",
        job_name="dataflow-cdc-to-serving",
        job_description="Dataflow CDC streaming to Firestore serving layer",
    )


def emit_dataproc_bootstrap_lineage():
    """Dataproc batch: GCS (MySQL dump) → BigQuery batch."""
    emit_lineage(
        source_platform="gcs",
        source_tables=["poc-stripe-bootstrap/subscriptions.csv", "poc-stripe-bootstrap/invoices.csv"],
        dest_platform="bigquery",
        dest_table="lightspeed_batch.subscriptions_batch",
        job_name="dataproc-historical-bootstrap",
        job_description="Dataproc Spark bootstrap from MySQL dump to BigQuery",
    )


def emit_reconciliation_lineage():
    """Reconciliation: BigQuery stream + batch → reconciliation log."""
    emit_lineage(
        source_platform="bigquery",
        source_tables=[
            "lightspeed_analytics.subscriptions_stream",
            "lightspeed_batch.subscriptions_batch",
        ],
        dest_platform="bigquery",
        dest_table="lightspeed_analytics.reconciliation_log",
        job_name="dataflow-reconciliation",
        job_description="Daily batch reconciliation streaming vs batch MRR",
    )


def emit_all_lineage():
    """Emit the complete pipeline lineage to DataHub."""
    logger.info("Emitting full pipeline lineage to DataHub...")
    emit_debezium_lineage()
    emit_dataflow_streaming_lineage()
    emit_dataproc_bootstrap_lineage()
    emit_reconciliation_lineage()
    logger.info("Full lineage emission complete")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    emit_all_lineage()
