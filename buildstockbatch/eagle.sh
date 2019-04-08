#!/bin/bash
#SBATCH --nodes=1
#SBATCH --ntasks=1

export PYTHONWARNINGS=ignore::yaml.YAMLLoadWarning

echo "Job ID: $SLURM_JOB_ID"
echo "Hostname: $HOSTNAME"

module load conda singularity-container
source activate "$MY_CONDA_ENV"

time python -u -m buildstockbatch.eagle "$PROJECTFILE"
