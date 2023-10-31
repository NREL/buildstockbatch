import os
import io
import json
import boto3
from pprint import pprint


def lambda_handler(event, context):
    # some prep work needed for this - check your security groups - there may default groups if any EMR cluster
    # was launched from the console - also prepare a bucket for logs

    # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/emr.html

    session = boto3.Session(region_name=os.environ["REGION"])

    s3 = session.client("s3")
    with io.BytesIO() as f:
        s3.download_fileobj(os.environ["BUCKET"], os.environ["EMR_CONFIG_JSON_KEY"], f)
        args = json.loads(f.getvalue())

    emr = session.client("emr")

    response = emr.run_job_flow(**args)
    pprint(response)
