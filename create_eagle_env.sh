#!/bin/bash
#SBATCH --job-name=create_buildstock_env
#SBATCH --output=create_env.out
#SBATCH --ntasks=1
#SBATCH --nodes=1
#SBATCH --time=15:00

module load conda
conda remove -y --name buildstock --all
conda create -y --name buildstock python=3.6 pandas
source activate buildstock
pip install --upgrade pip
pip install -e .
