#!/bin/bash

module load conda singularity-container

export SLURM_ARRAY_TASK_ID=1

source activate /shared-projects/buildstock/envs/dev_nmerket/

pip install --upgrade -e .[dev]

python -m pdb -m buildstockbatch.eagle project_resstock_national.yml
