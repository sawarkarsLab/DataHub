"""
Glue Job: etl_qlik.py
Purpose : Read Qlik Replicate landing files (Parquet/CSV) from S3 and store to Iceberg.
"""

import sys
from awsglue.utils import getResolvedOptions
from modules.common_func import (
    get_glue_context,
    load_from_qlik_replicate_files,
    normalize_column_names,
    add_ingestion_metadata,
    write_iceberg_table,
)

args = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
        "s3_path",          # path containing Replicate files
        "file_type",        # parquet | csv
        "iceberg_catalog",
        "iceberg_table",
        "write_mode",
    ],
)
s3_path    = args["s3_path"]
file_type  = args.get("file_type", "parquet")
catalog    = args["iceberg_catalog"]
table      = args["iceberg_table"]
mode       = args.get("write_mode", "append")

glue_ctx = get_glue_context()
spark    = glue_ctx.spark_session

df = (
    load_from_qlik_replicate_files(spark, s3_path, file_type=file_type)
    .transform(normalize_column_names)
    .transform(lambda d: add_ingestion_metadata(d, "qlik_replicate"))
)

write_iceberg_table(df, catalog, table, mode=mode)
