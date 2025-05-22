"""
Glue Job: etl_sqlserver.py
Purpose : Extract table (or query) from SQL Server via JDBC and write to S3 Parquet.
"""

import sys
from awsglue.utils import getResolvedOptions
from modules.common_func import (
    get_glue_context,
    load_from_sqlserver,
    normalize_column_names,
    add_ingestion_metadata,
    write_parquet,
)

args = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
        "dbtable",        # table name OR
        "query",          #   SQL query (one of dbtable/query required)
        "s3_target",      # s3://bucket/prefix/
        "partition_cols", # comma-separated list or empty
        "write_mode",     # overwrite | append
    ],
)
dbtable        = args.get("dbtable")
query          = args.get("query")
s3_target      = args["s3_target"]
partition_cols = [c.strip() for c in args.get("partition_cols", "").split(",") if c]
mode           = args.get("write_mode", "overwrite")

glue_ctx = get_glue_context()
spark    = glue_ctx.spark_session

df = (
    load_from_sqlserver(spark, table=dbtable, query=query)
    .transform(normalize_column_names)
    .transform(lambda d: add_ingestion_metadata(d, "sqlserver"))
)

write_parquet(df, s3_target, mode=mode, partition_cols=partition_cols)
