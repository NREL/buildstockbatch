#!/bin/bash

docker run -it --rm \
    -v $(pwd):/buildstock-batch \
    -v ~/.aws:/root/.aws \
    -e AWS_PROFILE=dev \
    -e AWS_BATCH_JOB_ARRAY_INDEX=5 \
    -e S3_BUCKET=buildstockbatch-test8 \
    -e S3_PREFIX=noeltest11a \
    -e JOB_NAME=noeltest11 \
    -e REGION=us-west-2 \
    --name debugbsb \
    nrel/buildstockbatch \
    python3 /buildstock-batch/buildstockbatch/aws/aws.py
