"""
Glue Job: etl_csv.py
Purpose : Load local/shared CSVs (path in csv_config.yaml) to Parquet.
"""

import sys
from awsglue.utils import getResolvedOptions
from modules.common_func import (
    get_glue_context,
    load_from_csv,
    normalize_column_names,
    add_ingestion_metadata,
    write_parquet,
)

args = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
        "s3_target",
        "write_mode",
    ],
)
s3_target = args["s3_target"]
mode      = args.get("write_mode", "overwrite")

glue_ctx = get_glue_context()
spark    = glue_ctx.spark_session

df = (
    load_from_csv(spark)
    .transform(normalize_column_names)
    .transform(lambda d: add_ingestion_metadata(d, "csv"))
)

write_parquet(df, s3_target, mode=mode)
