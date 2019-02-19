#!/bin/bash
#SBATCH --nodes=1
#SBATCH --ntasks=1

echo "Job ID: $SLURM_JOB_ID"
echo "Hostname: $HOSTNAME"

CONDA_DIR=/shared-projects/buildstock/
NAME=buildstock

module load conda singularity-container
source activate $CONDA_DIR/$NAME

time python -u -m buildstockbatch.eagle "$PROJECTFILE"
