#!/bin/bash

echo "Job ID: $SLURM_JOB_ID"
echo "Hostname: $HOSTNAME"

module load conda singularity-container
source activate "$MY_CONDA_ENV"

SCHEDULER_FILE=$OUT_DIR/dask_scheduler.json

srun --pack-group=0 $MY_CONDA_ENV/bin/dask-scheduler --bokeh-whitelist="0.0.0.0" --scheduler-file $SCHEDULER_FILE --local-directory /tmp/scratch/dask &
srun --pack-group=1 $MY_CONDA_ENV/bin/dask-worker --scheduler-file $SCHEDULER_FILE --local-directory /tmp/scratch/dask --nprocs 18 &

time python -u -m buildstockbatch.eagle "$PROJECTFILE"
