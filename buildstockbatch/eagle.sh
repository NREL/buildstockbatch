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
source /shared-projects/buildstock/aws_credentials.sh

time python -u -m buildstockbatch.hpc eagle "$PROJECTFILE"
