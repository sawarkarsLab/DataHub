"""
Glue Job: etl_onestream.py
Purpose : Extract data from OneStream REST endpoint, land to Iceberg.
"""

import sys
from awsglue.utils import getResolvedOptions
from modules.common_func import (
    get_glue_context,
    load_from_onestream,
    normalize_column_names,
    add_ingestion_metadata,
    write_iceberg_table,
)

args = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
        "endpoint",          # e.g. /api/v1/reports/SomeReport
        "iceberg_catalog",
        "iceberg_table",
        "write_mode",
    ],
)
endpoint       = args["endpoint"]
ice_catalog    = args["iceberg_catalog"]
ice_table      = args["iceberg_table"]
mode           = args.get("write_mode", "append")

glue_ctx = get_glue_context()
spark    = glue_ctx.spark_session

df = (
    load_from_onestream(spark, endpoint)
    .transform(normalize_column_names)
    .transform(lambda d: add_ingestion_metadata(d, "onestream"))
)

write_iceberg_table(df, ice_catalog, ice_table, mode=mode)
