#!/bin/bash

echo "begin eagle_postprocessing.sh"

echo "Job ID: $SLURM_JOB_ID"
echo "Hostname: $HOSTNAME"

module load conda singularity-container
source activate "$MY_CONDA_ENV"

export POSTPROCESS=1

echo "UPLOADONLY: ${UPLOADONLY}"

SCHEDULER_FILE=$OUT_DIR/dask_scheduler.json

echo "head node"
echo $SLURM_JOB_NODELIST_PACK_GROUP_0
echo "workers"
echo $SLURM_JOB_NODELIST_PACK_GROUP_1

pdsh -w $SLURM_JOB_NODELIST_PACK_GROUP_1 "free -h"

$MY_CONDA_ENV/bin/dask-scheduler --scheduler-file $SCHEDULER_FILE &> $OUT_DIR/dask_scheduler.out &
pdsh -w $SLURM_JOB_NODELIST_PACK_GROUP_1 "$MY_CONDA_ENV/bin/dask-worker --scheduler-file $SCHEDULER_FILE --local-directory /tmp/scratch/dask --nprocs 9" &> $OUT_DIR/dask_workers.out &

time python -u -m buildstockbatch.eagle "$PROJECTFILE"
