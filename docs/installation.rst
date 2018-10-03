Installation
------------

Both the local and Peregrine installations depend on the
`OpenStudio-BuildStock <https://github.com/NREL/OpenStudio-BuildStock>`__
repository. Either ``git clone`` it or download a copy of it or your
fork or branch of it with your projects.

Local and AWS
~~~~~~~~~~~~~

This method works for running the simulations locally through Docker and
will be the installation method for using the platform on AWS.

`Download <http://docker.io>`__ and install Docker for your platform.

Install Python 3.6 or greater for your platform. Either the official
distribution from python.org or the Anaconda distribution.

Get a copy of this code either by downloading the zip file from GitHub
or cloning the repository.

Install the library by doing the following:

::

   cd /path/to/buildstockbatch
   pip install -e .

Peregrine
~~~~~~~~~

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
