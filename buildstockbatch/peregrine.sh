#!/bin/bash
#PBS -l nodes=1
#PBS -l walltime=1:00:00
#PBS -l qos=high
#PBS -q short
#PBS -j oe
#PBS -A eedr

module load conda singularity-container
source activate buildstock

time buildstock_peregrine "$PROJECTFILE"