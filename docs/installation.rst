Installation
------------

Both the local and Peregrine installations depend on the
`OpenStudio-BuildStock <https://github.com/NREL/OpenStudio-BuildStock>`__
repository. Either ``git clone`` it or download a copy of it or your
fork or branch of it with your projects.

Local
~~~~~

This method works for running the simulations locally through Docker.

`Download <http://docker.io>`__ and install Docker for your platform.

Install Python 3.6 or greater for your platform. Either the official
distribution from python.org or the Anaconda distribution.

Get a copy of this code either by downloading the zip file from GitHub
or cloning the repository.

Install the library by doing the following:

::

   cd /path/to/buildstockbatch
   pip install -e .

.. _eagle_install:

Eagle
~~~~~

BuildStock Batch is preinstalled on Eagle. To use it, activate the appropriate conda environment:

::

   ssh eagle.hpc.nrel.gov
   module load conda
   source activate /shared-projects/buildstock/envs/buildstock-X.X

You can get a list of installed environments by looking in the envs directory

::

   ls /shared-projects/buildstock/envs

Developer installation
......................

For those doing development work on BuildStock Batch (not most users), a new conda environment
is that includes buildstock batch is created with the bash script `create_eagle_env.sh`
in the git repo that will need to be cloned onto Eagle. The script is called as follows:

::

   bash create_eagle_env.sh envname

This will create a directory ``/shared-projects/buildstock/envs/env-name`` that contains the conda
environment with BuildStock Batch installed. This environment can then be used by any user.

If you pass the ``-d`` flag to that script, it will install the buildstock-batch package in development mode
meaning that any changes you make in your cloned repo will immediately be available to that environment.
However, it means that only the user who installed the environment can use it.

If you pass the flag ``-e /projects/someproject/envs``, it will install the environment there instead
of the default location. This is useful if you need a specific installation for a particular project.

The ``-d`` and ``-e`` flags can also be combined if desired

::

   bash create_eagle_env.sh -d -e /projects/enduse/envs mydevenv

Peregrine
~~~~~~~~~

.. warning:: 

   The use of BuildStock Batch on Peregrine is deprecated and not really maintained anymore. 
   We recommend using Eagle. 


To use this library you will need to have access to NREL's HPC system.
Instructions can be found on `NREL's High Performance Computing
website <http://www.nrel.gov/hpc>`__. Once you have access, ssh into
Peregrine, and do the following:

::

   git clone <repo_url>
   cd buildstockbatch
   qsub -A res_stock create_peregrine_env.sh

This assumes you have access to the allocation ``res_stock``. If you are
using another allocation replace it in the ``-A`` argument above.

This submits a job that will create the environment. It may take several
minutes to complete. When it is done, a file ``create_environment.out``
will be in the directory with the output of the installation process.
