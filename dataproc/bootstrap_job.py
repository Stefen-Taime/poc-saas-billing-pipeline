"""
bootstrap_job.py — Dataproc Spark job
MySQL full dump (from GCS) → BigQuery batch dataset
Used as the batch oracle for reconciliation against streaming.

Explicit schema definitions to match BigQuery table schemas
(inferSchema causes type mismatches: int vs BOOLEAN, double vs NUMERIC).
"""

import argparse
import logging

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType,
    DecimalType, DateType, TimestampType, IntegerType
)
from pyspark.sql import functions as F

logger = logging.getLogger(__name__)


# ---------- Schema definitions matching BigQuery tables ----------

SUBSCRIPTIONS_SCHEMA = StructType([
    StructField("subscription_id", StringType(), False),
    StructField("merchant_id", StringType(), False),
    StructField("plan", StringType(), True),
    StructField("statut", StringType(), True),
    StructField("montant_mensuel_cad", StringType(), True),   # read as string, cast to decimal
    StructField("devise", StringType(), True),
    StructField("date_debut", DateType(), True),
    StructField("date_prochain_paiement", DateType(), True),
    StructField("periode_essai_fin", DateType(), True),
    StructField("annulation_schedulee", StringType(), True),  # read as string, cast to boolean
    StructField("updated_at", TimestampType(), True),
])

INVOICES_SCHEMA = StructType([
    StructField("invoice_id", StringType(), False),
    StructField("merchant_id", StringType(), False),
    StructField("subscription_id", StringType(), True),
    StructField("montant_cad", StringType(), True),           # read as string, cast to decimal
    StructField("statut", StringType(), True),
    StructField("date_emission", DateType(), True),
    StructField("date_echeance", DateType(), True),
    StructField("date_paiement", TimestampType(), True),
    StructField("tentatives_paiement", IntegerType(), True),
    StructField("updated_at", TimestampType(), True),
])


def main():
    parser = argparse.ArgumentParser(description="Historical Bootstrap: MySQL -> BigQuery")
    parser.add_argument("--project", required=True, help="GCP project ID")
    parser.add_argument("--gcs-input", required=True, help="GCS path to MySQL dump CSVs (gs://...)")
    parser.add_argument("--bq-dataset", default="lightspeed_batch", help="BigQuery batch dataset")
    args = parser.parse_args()

    spark = (
        SparkSession.builder
        .appName("poc-stripe-bootstrap")
        .config("spark.jars.packages",
                "com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.34.0")
        .getOrCreate()
    )

    tables = [
        ("subscriptions", "subscriptions_batch", SUBSCRIPTIONS_SCHEMA),
        ("invoices", "invoices_batch", INVOICES_SCHEMA),
    ]

    gcs_bucket = args.gcs_input.split("/")[2]

    for mysql_table, bq_table, schema in tables:
        input_path = f"{args.gcs_input}/{mysql_table}.csv"
        output_table = f"{args.project}.{args.bq_dataset}.{bq_table}"

        logger.info(f"Loading {input_path} -> {output_table}")

        df = (
            spark.read
            .option("header", "true")
            .schema(schema)
            .csv(input_path)
        )

        # Post-read casts for columns that need special handling
        if mysql_table == "subscriptions":
            df = (
                df
                .withColumn("montant_mensuel_cad",
                            F.col("montant_mensuel_cad").cast(DecimalType(10, 2)))
                .withColumn("annulation_schedulee",
                            F.col("annulation_schedulee").cast("int").cast("boolean"))
            )
        elif mysql_table == "invoices":
            df = (
                df
                .withColumn("montant_cad",
                            F.col("montant_cad").cast(DecimalType(10, 2)))
            )

        logger.info(f"  rows: {df.count()}, columns: {df.columns}")
        df.printSchema()

        (
            df.write
            .format("bigquery")
            .option("table", output_table)
            .option("temporaryGcsBucket", gcs_bucket)
            .mode("overwrite")
            .save()
        )

        logger.info(f"  written to {output_table}")

    spark.stop()
    logger.info("Bootstrap complete")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
