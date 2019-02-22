#!/bin/bash
#SBATCH --nodes=1
#SBATCH --ntasks=1

echo "Job ID: $SLURM_JOB_ID"
echo "Hostname: $HOSTNAME"

module load conda singularity-container
source activate buildstock

time python -u -m buildstockbatch.eagle "$PROJECTFILE"
