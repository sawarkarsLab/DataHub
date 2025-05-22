"""
Glue Job: etl_salesforce.py
Purpose : Ingest Salesforce data via REST API, transform, and load into Iceberg.
"""

import sys
from awsglue.utils import getResolvedOptions
from modules.common_func import (
    get_glue_context,
    load_from_salesforce,
    normalize_column_names,
    add_ingestion_metadata,
    write_iceberg_table,
)

# ─── Parse Glue job arguments ────────────────────────────────────────────────
args = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
        "soql",              # SOQL query string (URL-encoded in Glue console)
        "iceberg_catalog",   # Glue catalog name
        "iceberg_table",     # target Iceberg table  (e.g. raw_salesforce_account)
        "write_mode",        # overwrite | append  (default append)
    ],
)
soql            = args["soql"]
ice_catalog     = args["iceberg_catalog"]
ice_table       = args["iceberg_table"]
write_mode      = args.get("write_mode", "append")

# ─── ETL ─────────────────────────────────────────────────────────────────────
glue_ctx = get_glue_context()
spark    = glue_ctx.spark_session

df = (
    load_from_salesforce(spark, soql)
    .transform(normalize_column_names)
    .transform(lambda d: add_ingestion_metadata(d, "salesforce"))
)

write_iceberg_table(df, ice_catalog, ice_table, mode=write_mode)
