"""
Glue Job: etl_sharepoint.py
Purpose : Download a file from SharePoint via Graph API into Parquet.
"""

import sys
from awsglue.utils import getResolvedOptions
from modules.common_func import (
    get_glue_context,
    load_from_sharepoint,
    normalize_column_names,
    add_ingestion_metadata,
    write_parquet,
)

args = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
        "relative_url",    # e.g. /drive/items/{file-id}
        "s3_target",
        "write_mode",
    ],
)
rel_url   = args["relative_url"]
s3_target = args["s3_target"]
mode      = args.get("write_mode", "overwrite")

glue_ctx = get_glue_context()
spark    = glue_ctx.spark_session

df = (
    load_from_sharepoint(spark, rel_url)
    .transform(normalize_column_names)
    .transform(lambda d: add_ingestion_metadata(d, "sharepoint"))
)

write_parquet(df, s3_target, mode=mode)
