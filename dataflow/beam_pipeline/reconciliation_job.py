"""
reconciliation_job.py — Batch reconciliation: streaming vs batch BigQuery
Runs daily at UTC 02:00 via Cloud Scheduler or manually.
Compares MRR from streaming pipeline vs batch bootstrap.
"""

import argparse
import logging
import os
from datetime import datetime, timedelta

from google.cloud import bigquery

logger = logging.getLogger(__name__)


def run_reconciliation(project_id: str, date: str, slack_webhook: str = None):
    """
    Compare BigQuery streaming vs batch MRR for a given date.
    Alerts if ecart > 0.1%.
    """
    client = bigquery.Client(project=project_id)

    # Single reconciliation query: INNER JOIN on subscription_id
    # Compares latest streaming state vs batch for common subscriptions only.
    # Streaming is deduplicated via ROW_NUMBER (multiple CDC events per sub).
    reconciliation_query = f"""
        WITH latest_streaming AS (
            SELECT subscription_id, montant_mensuel_cad, statut,
                   ROW_NUMBER() OVER (
                       PARTITION BY subscription_id
                       ORDER BY updated_at DESC, montant_mensuel_cad DESC
                   ) AS rn
            FROM `{project_id}.lightspeed_analytics.subscriptions_stream`
            WHERE DATE(updated_at) <= '{date}'
        ),
        streaming_active AS (
            SELECT subscription_id, montant_mensuel_cad
            FROM latest_streaming
            WHERE rn = 1 AND statut = 'active'
        ),
        batch_active AS (
            SELECT subscription_id, montant_mensuel_cad
            FROM `{project_id}.lightspeed_batch.subscriptions_batch`
            WHERE statut = 'active'
              AND DATE(updated_at) <= '{date}'
        )
        SELECT
            COALESCE(SUM(s.montant_mensuel_cad), 0) AS mrr_streaming,
            COALESCE(SUM(b.montant_mensuel_cad), 0) AS mrr_batch,
            COUNT(*) AS common_subs
        FROM streaming_active s
        INNER JOIN batch_active b ON s.subscription_id = b.subscription_id
    """

    result = list(client.query(reconciliation_query).result())[0]
    mrr_streaming = result.mrr_streaming
    mrr_batch = result.mrr_batch

    if mrr_batch == 0:
        logger.warning(f"Batch MRR is 0 for {date} — skipping reconciliation")
        return

    ecart_pct = abs(float(mrr_streaming) - float(mrr_batch)) / float(mrr_batch)
    status = "OK" if ecart_pct <= 0.001 else "KO"

    logger.info(
        f"Reconciliation {date}: streaming={mrr_streaming:.2f} CAD, "
        f"batch={mrr_batch:.2f} CAD, ecart={ecart_pct:.4%}, status={status}"
    )

    # Write to reconciliation log
    rows = [
        {
            "date": date,
            "mrr_streaming_cad": float(mrr_streaming),
            "mrr_batch_cad": float(mrr_batch),
            "ecart_pct": ecart_pct,
            "status": status,
            "checked_at": datetime.utcnow().isoformat(),
        }
    ]

    table_ref = f"{project_id}.lightspeed_analytics.reconciliation_log"
    errors = client.insert_rows_json(table_ref, rows)
    if errors:
        logger.error(f"Failed to write reconciliation log: {errors}")

    # Alert if KO
    if status == "KO" and slack_webhook:
        _send_slack_alert(
            slack_webhook,
            f":x: Reconciliation KO — {date} | "
            f"Streaming: {mrr_streaming:,.2f} CAD | "
            f"Batch: {mrr_batch:,.2f} CAD | "
            f"Ecart: {ecart_pct:.4%}",
        )
    elif status == "OK":
        logger.info(f"Reconciliation OK — {date}")
        if slack_webhook:
            _send_slack_alert(
                slack_webhook,
                f":white_check_mark: Reconciliation OK — {date} | "
                f"MRR: {mrr_streaming:,.2f} CAD | "
                f"Ecart: {ecart_pct:.4%}",
            )


def _send_slack_alert(webhook_url: str, message: str):
    """Send alert to Slack channel."""
    import urllib.request

    import json

    payload = json.dumps({"text": message}).encode("utf-8")
    req = urllib.request.Request(
        webhook_url,
        data=payload,
        headers={"Content-Type": "application/json"},
    )
    try:
        urllib.request.urlopen(req)
    except Exception as e:
        logger.error(f"Slack alert failed: {e}")


def main():
    parser = argparse.ArgumentParser(description="MRR Reconciliation Job")
    parser.add_argument("--project", required=True, help="GCP project ID")
    parser.add_argument(
        "--date",
        default=(datetime.utcnow() - timedelta(days=1)).strftime("%Y-%m-%d"),
        help="Date to reconcile (default: yesterday)",
    )
    parser.add_argument(
        "--slack-webhook",
        default=os.getenv("SLACK_WEBHOOK_URL"),
        help="Slack webhook URL for alerts",
    )
    args = parser.parse_args()

    run_reconciliation(args.project, args.date, args.slack_webhook)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
