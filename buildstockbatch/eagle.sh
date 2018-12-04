#!/bin/bash
#SLURM --nodes=1
#SLURM --ntasks=1

module load conda singularity-container
source activate buildstock

time python -u -m buildstockbatch.eagle "$PROJECTFILE"
