#!/bin/bash

DEV=0
while getopts de: option
do
case "${option}"
in
d) DEV=1;;
e) PYTHON_ENVS_DIR=${OPTARG};;
esac
done

if [ -z "$PYTHON_ENVS_DIR" ]
then
    PYTHON_ENVS_DIR=/kfs2/shared-projects/buildstock/envs
fi

MY_PYTHON_ENV_NAME=${@:$OPTIND:1}
if [ -z "$MY_PYTHON_ENV_NAME" ]
then
    echo "Environment name not provided"
    exit 1
fi

MY_PYTHON_ENV="$PYTHON_ENVS_DIR/$MY_PYTHON_ENV_NAME"
echo "Creating $MY_PYTHON_ENV"
module load python
python -m venv --clear --upgrade-deps --prompt "$MY_PYTHON_ENV_NAME" "$MY_PYTHON_ENV"
source "$MY_PYTHON_ENV/bin/activate"
which pip
if [ $DEV -eq 1 ]
then
    pip install --no-cache-dir -e ".[dev]"
else
    pip install --no-cache-dir .
fi
