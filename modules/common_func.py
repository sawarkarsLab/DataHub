"""
modules/common_func.py
──────────────────────
Reusable helpers for all AWS Glue Spark jobs in the DataHub Migration project.
Each function is intentionally minimal; flesh out auth/error handling as needed.
"""

from __future__ import annotations

import os
import json
import re
from typing import Dict, Optional, List

import boto3
import requests
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from awsglue.context import GlueContext

# local util
from utils.config_loader import load_yaml_with_env


# ──────────────────────────────────────────────────────────────────────────────
#  Spark / Glue session helpers
# ──────────────────────────────────────────────────────────────────────────────
def get_glue_context() -> GlueContext:
    """Return (or create) a GlueContext and SparkSession."""
    spark = SparkSession.builder.getOrCreate()
    return GlueContext(spark.sparkContext)


# ──────────────────────────────────────────────────────────────────────────────
#  Ingestors
# ──────────────────────────────────────────────────────────────────────────────
def load_from_sqlserver(
    spark: SparkSession,
    table: str,
    config_path: str = "config/sqlserver_config.yaml",
    query: Optional[str] = None,
) -> DataFrame:
    """Read a table (or SQL query) from SQL Server via JDBC into a Spark DF."""
    cfg = load_yaml_with_env(config_path)
    jdbc_url = (
        f"jdbc:sqlserver://{cfg['host']}:{cfg['port']};"
        f"databaseName={cfg['database']}"
    )
    reader = (
        spark.read.format("jdbc")
        .option("url", jdbc_url)
        .option("user", cfg["user"])
        .option("password", cfg["password"])
        .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
    )
    if query:
        reader = reader.option("query", query)
    else:
        reader = reader.option("dbtable", table)
    return reader.load()


def load_from_salesforce(
    spark: SparkSession,
    soql: str,
    config_path: str = "config/salesforce_config.yaml",
) -> DataFrame:
    """
    Pull Salesforce objects with REST API (simple-salesforce is optional).
    Creates a Spark DataFrame from the JSON records.
    """
    cfg = load_yaml_with_env(config_path)

    # Obtain access token (using refresh_token flow)
    token_resp = requests.post(
        f"{cfg['instance_url']}/services/oauth2/token",
        data={
            "grant_type": "refresh_token",
            "client_id": cfg["client_id"],
            "client_secret": cfg["client_secret"],
            "refresh_token": cfg["refresh_token"],
        },
        timeout=30,
    )
    token_resp.raise_for_status()
    access_token = token_resp.json()["access_token"]

    # Query endpoint
    query_url = f"{cfg['instance_url']}/services/data/v58.0/query"
    resp = requests.get(
        query_url, params={"q": soql}, headers={"Authorization": f"Bearer {access_token}"}
    )
    resp.raise_for_status()
    records = resp.json()["records"]

    # Convert to Spark DF
    return spark.createDataFrame(records)


def load_from_onestream(
    spark: SparkSession,
    endpoint: str,
    params: Dict[str, str] | None = None,
    config_path: str = "config/onestream_config.yaml",
) -> DataFrame:
    """Generic OneStream REST call → Spark DF."""
    cfg = load_yaml_with_env(config_path)

    # OAuth2 token
    token_resp = requests.post(
        f"{cfg['base_url']}/connect/token",
        data={
            "grant_type": "refresh_token",
            "client_id": cfg["client_id"],
            "client_secret": cfg["client_secret"],
            "refresh_token": cfg["refresh_token"],
        },
        timeout=30,
    )
    token_resp.raise_for_status()
    token = token_resp.json()["access_token"]

    resp = requests.get(
        f"{cfg['base_url']}{endpoint}",
        params=params,
        headers={"Authorization": f"Bearer {token}"},
        timeout=60,
    )
    resp.raise_for_status()
    data = resp.json()

    return spark.read.json(spark.sparkContext.parallelize([json.dumps(data)]))


def load_from_qlik_replicate_files(
    spark: SparkSession,
    s3_path: str,
    file_type: str = "parquet",
) -> DataFrame:
    """
    Qlik Replicate often lands CDC files to S3.  Point Spark at that path.
    """
    if file_type == "parquet":
        return spark.read.parquet(s3_path)
    if file_type == "csv":
        return spark.read.option("header", True).csv(s3_path)
    raise ValueError("Unsupported file_type")


def load_from_csv(
    spark: SparkSession,
    config_path: str = "config/csv_config.yaml",
) -> DataFrame:
    """Load CSVs from the directory specified in csv_config.yaml."""
    cfg = load_yaml_with_env(config_path)
    return (
        spark.read.option("header", True)
        .option("delimiter", cfg.get("delimiter", ","))
        .csv(cfg["data_directory"])
    )


def load_from_sharepoint(
    spark: SparkSession,
    relative_url: str,
    config_path: str = "config/sharepoint_config.yaml",
) -> DataFrame:
    """
    Download a SharePoint file via MS Graph API and create a Spark DF.
    Note: for large files prefer copying to S3 first and loading directly.
    """
    cfg = load_yaml_with_env(config_path)

    # Get token with client-credential flow
    token_resp = requests.post(
        f"https://login.microsoftonline.com/{cfg['tenant_id']}/oauth2/v2.0/token",
        data={
            "grant_type": "client_credentials",
            "client_id": cfg["client_id"],
            "client_secret": cfg["client_secret"],
            "scope": "https://graph.microsoft.com/.default",
        },
        timeout=30,
    )
    token_resp.raise_for_status()
    token = token_resp.json()["access_token"]

    # Download file content
    url = f"{cfg['graph_url']}/sites/{cfg['site_id']}/{relative_url}:/content"
    resp = requests.get(url, headers={"Authorization": f"Bearer {token}"}, timeout=120)
    resp.raise_for_status()

    # Save to /tmp in Glue container
    local_path = "/tmp/sharepoint_download"
    with open(local_path, "wb") as f:
        f.write(resp.content)

    return spark.read.option("header", True).csv(local_path)


# ──────────────────────────────────────────────────────────────────────────────
#  Transformation helpers
# ──────────────────────────────────────────────────────────────────────────────
def snake_case(col_name: str) -> str:
    """Convert CamelCase or mixedCase names to snake_case."""
    s1 = re.sub("(.)([A-Z][a-z]+)", r"\1_\2", col_name)
    s2 = re.sub("([a-z0-9])([A-Z])", r"\1_\2", s1)
    return s2.lower()


def normalize_column_names(df: DataFrame) -> DataFrame:
    """Return a DF with all column names converted to snake_case."""
    for col_name in df.columns:
        df = df.withColumnRenamed(col_name, snake_case(col_name))
    return df


def add_ingestion_metadata(df: DataFrame, source: str) -> DataFrame:
    """Add generic metadata columns."""
    return (
        df.withColumn("ingestion_timestamp", F.current_timestamp())
        .withColumn("source_system", F.lit(source))
    )


# ──────────────────────────────────────────────────────────────────────────────
#  Writers
# ──────────────────────────────────────────────────────────────────────────────
def write_parquet(
    df: DataFrame,
    s3_path: str,
    mode: str = "overwrite",
    partition_cols: Optional[List[str]] = None,
) -> None:
    """Write DF to S3 in Parquet format."""
    writer = df.write.mode(mode).format("parquet")
    if partition_cols:
        writer = writer.partitionBy(*partition_cols)
    writer.save(s3_path)


def write_iceberg_table(
    df: DataFrame,
    catalog_name: str,
    table: str,
    mode: str = "overwrite",
) -> None:
    """
    Write to an Iceberg table registered in Glue Catalog.
    Requires Spark configs for Iceberg (AWS Glue 4.0+ supports Iceberg out of box).
    """
    df.writeTo(f"{catalog_name}.{table}").using("iceberg").mode(mode).createOrReplace()


# ──────────────────────────────────────────────────────────────────────────────
#  Example end-to-end helper
# ──────────────────────────────────────────────────────────────────────────────
def etl_salesforce_to_iceberg(
    soql: str,
    iceberg_catalog: str,
    iceberg_table: str,
) -> None:
    """
    One-liner you can call from a Glue job: pull Salesforce → transform → Iceberg.
    """
    glue_ctx = get_glue_context()
    spark = glue_ctx.spark_session

    df = (
        load_from_salesforce(spark, soql)
        .transform(normalize_column_names)
        .transform(lambda d: add_ingestion_metadata(d, "salesforce"))
    )

    write_iceberg_table(df, iceberg_catalog, iceberg_table, mode="append")
