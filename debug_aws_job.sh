#!/bin/bash

docker run -it --rm \
    -v $(pwd):/buildstock-batch \
    -v ~/.aws:/root/.aws \
    -e AWS_PROFILE=dev \
    -e AWS_BATCH_JOB_ARRAY_INDEX=5 \
    -e S3_BUCKET=buildstockbatch-test \
    -e S3_PREFIX=resstock_national_3pwindows \
    --name debugbsb \
    nrel/buildstockbatch \
    python3 -m pdb /buildstock-batch/buildstockbatch/aws.py
