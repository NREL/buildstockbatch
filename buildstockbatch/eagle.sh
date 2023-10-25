#!/bin/bash
#SBATCH --nodes=1
#SBATCH --ntasks=1

echo "Job ID: $SLURM_JOB_ID"
echo "Hostname: $HOSTNAME"
echo "QOS: $SLURM_JOB_QOS"

df -i
df -h

module load conda singularity-container
source activate "$MY_CONDA_ENV"

time python -u -m buildstockbatch.hpc "$PROJECTFILE"
