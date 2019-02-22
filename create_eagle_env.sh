#!/bin/bash

DEV=0
while getopts dev option
do
case "${option}"
in
d) DEV=1
esac
done

MY_CONDA_ENV_NAME=${@:$OPTIND:1}

CONDA_ENVS_DIR=/shared-projects/buildstock/envs
MY_CONDA_PREFIX="$CONDA_ENVS_DIR/$MY_CONDA_ENV_NAME"
module load conda
conda remove -y --prefix "$MY_CONDA_PREFIX" --all
conda create -y --prefix "$MY_CONDA_PREFIX" python=3.6 pandas hdf5 pytables
source activate "$MY_CONDA_PREFIX"
pip install --upgrade pip
if [ $DEV -eq 1 ]
then
    pip install -e .
else
    pip install .
fi
