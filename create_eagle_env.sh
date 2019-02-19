#!/bin/bash
#SBATCH --job-name=create_buildstock_env
#SBATCH --output=create_env.out
#SBATCH --ntasks=1
#SBATCH --nodes=1
#SBATCH --time=15:00

CONDA_DIR=/shared-projects/buildstock/
NAME=buildstock

module load conda
if [[ ! -d $CONDA_DIR/$NAME ]]; then
    conda remove -y --prefix $CONDA_DIR/$NAME --all
    conda create -y --prefix $CONDA_DIR/$NAME python=3.6 pandas hdf5 pytables
fi
source activate $CONDA_DIR/$NAME
pip install --upgrade pip
pip install -e .
