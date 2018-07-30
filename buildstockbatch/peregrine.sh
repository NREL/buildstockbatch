#!/bin/bash
#PBS -l nodes=1
#PBS -l walltime=1:00:00
#PBS -q short
#PBS -j oe
#PBS -A res_stock

module load conda singularity-container
source activate buildstock

buildstock_peregrine "$PROJECTFILE"