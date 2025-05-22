"""
Minimal orchestrator to trigger multiple Glue jobs with boto3.
Run this as a Python shell job or locally with proper AWS creds.
"""

import boto3
import datetime

glue = boto3.client("glue")
JOB_NAMES = [
    "etl_salesforce",
    "etl_sqlserver",
    "etl_onestream",
    "etl_qlik",
    "etl_csv",
    "etl_sharepoint",
]

for job in JOB_NAMES:
    run_id = glue.start_job_run(JobName=job, Arguments={"trigger_ts": str(datetime.datetime.utcnow())})
    print(f"Started {job} → run ID: {run_id['JobRunId']}")
