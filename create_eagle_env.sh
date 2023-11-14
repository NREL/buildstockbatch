#!/bin/bash

DEV=0
while getopts de: option
do
case "${option}"
in
d) DEV=1;;
e) CONDA_ENVS_DIR=${OPTARG};;
esac
done

if [ -z "$CONDA_ENVS_DIR" ]
then
    CONDA_ENVS_DIR=/shared-projects/buildstock/envs
fi

MY_CONDA_ENV_NAME=${@:$OPTIND:1}
if [ -z "$MY_CONDA_ENV_NAME" ]
then
    echo "Environment name not provided"
    exit 1
fi

MY_CONDA_PREFIX="$CONDA_ENVS_DIR/$MY_CONDA_ENV_NAME"
echo "Creating $MY_CONDA_PREFIX"
module load conda
conda remove -y --prefix "$MY_CONDA_PREFIX" --all
mamba create -y --prefix "$MY_CONDA_PREFIX" -c conda-forge "python=3.10" "pyarrow" "numpy" "pandas" "dask>=2022.10.0" "distributed" "ruby"
conda deactivate
conda activate "$MY_CONDA_PREFIX"
which pip
if [ $DEV -eq 1 ]
then
    pip install --no-cache-dir -e ".[dev]"
else
    pip install --no-cache-dir .
fi
