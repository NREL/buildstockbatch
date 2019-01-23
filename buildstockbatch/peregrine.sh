#!/bin/bash
#PBS -l nodes=1
#PBS -l walltime=1:00:00
#PBS -q short
#PBS -j oe

module load conda singularity-container
source activate buildstock

time python -u -m buildstockbatch.peregrine "$PROJECTFILE"
