#!/bin/bash
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --tmp=1000000
#SBATCH --reservation=jobdebugging
 
echo "Job ID: $SLURM_JOB_ID"
echo "Hostname: $HOSTNAME"
echo "QOS: $SLURM_JOB_QOS"
 
df -i
df -h
 
module load python apptainer
source "$MY_PYTHON_ENV/bin/activate"
# Default LOCAL_SCRATCH = $TMPDIR
# Setting to user-specific dir to avoid
# issues with deleting previous buildstock run debris
export LOCAL_SCRATCH=$TMPDIR
source /kfs2/shared-projects/buildstock/aws_credentials.sh
 
time python -u -m buildstockbatch.hpc kestrel "$PROJECTFILE"