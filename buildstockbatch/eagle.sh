#!/bin/bash
#SBATCH --nodes=1
#SBATCH --ntasks=1

echo "Job ID: $SLURM_JOB_ID"
echo "Hostname: $HOSTNAME"
echo "QOS: $SLURM_JOB_QOS"

module load conda singularity-container/3.2.1
source activate "$MY_CONDA_ENV"

time python -u -m buildstockbatch.eagle "$PROJECTFILE"
