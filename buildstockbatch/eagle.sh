#!/bin/bash
#SBATCH --nodes=1
#SBATCH --ntasks=1

module load conda singularity-container
source activate buildstock

time python -u -m buildstockbatch.eagle "$PROJECTFILE"
