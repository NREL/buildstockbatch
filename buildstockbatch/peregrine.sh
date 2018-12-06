#!/bin/bash
#PBS -l nodes=1
#PBS -l walltime=1:00:00
#PBS -q short
#PBS -j oe
#PBS -A la100

module load conda singularity-container
source activate buildstock

time buildstock_peregrine "$PROJECTFILE"
