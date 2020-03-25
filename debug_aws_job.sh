#!/bin/bash

docker run -it --rm \
    -v $(pwd):/buildstock-batch \
    -e AWS_BATCH_JOB_ARRAY_INDEX=2 \
    -e S3_BUCKET=resbldg-datasets \
    -e S3_PREFIX=testing/noeltest31k \
    -e JOB_NAME=noeltest31 \
    -e REGION=us-west-2 \
    --name debugbsb \
    nrel/buildstockbatch \
    python3 -m pdb /buildstock-batch/buildstockbatch/aws/aws.py
