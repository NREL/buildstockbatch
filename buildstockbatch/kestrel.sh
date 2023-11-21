#!/bin/bash
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --tmp=1000000

echo "Job ID: $SLURM_JOB_ID"
echo "Hostname: $HOSTNAME"
echo "QOS: $SLURM_JOB_QOS"

df -i
df -h

module load python apptainer
source "$MY_PYTHON_ENV/bin/activate"
source /kfs2/shared-projects/buildstock/aws_credentials.sh

time python -u -m buildstockbatch.hpc kestrel "$PROJECTFILE"
