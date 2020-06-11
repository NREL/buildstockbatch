Installation
------------

BuildStock-Batch installations depend on the
`OpenStudio-BuildStock <https://github.com/NREL/OpenStudio-BuildStock>`__
repository. Either ``git clone`` it or download a copy of it or your
fork or branch of it with your projects.

.. _local-install:

Local
~~~~~

This method works for running the simulations locally through Docker. BuildStock-Batch simulations are
computationally intensive. Local use is only recommended for very small testing runs.

`Download <http://docker.io>`_ and install Docker for your platform.

Install Python 3.6 or greater for your platform. Either the official
distribution from python.org or the `Anaconda distribution
<https://www.anaconda.com/distribution/>`_ (recommended).

Get a copy of this code either by downloading the zip file from GitHub or
cloning the repository.

Optional, but highly recommended, is to create a new `python virtual
environment`_ if you're using python from python.org, or to create a new `conda
environment`_ if you're using Anaconda. Then activate your environment. 

.. _python virtual environment: https://docs.python.org/3/library/venv.html
.. _conda environment: https://conda.io/projects/conda/en/latest/user-guide/tasks/manage-environments.html

Install the library by doing the following:

::

   cd /path/to/buildstockbatch
   pip install -e .

.. _aws-user-config-local:

AWS User Configuration
......................

To use the automatic upload of processed results to AWS Athena, you'll need to
configure your user account with your AWS credentials. This setup only needs to
be done once.

1. `Install the AWS CLI`_ into a *separate* `conda environment`_ or `python
   virtual environment`_ from your buildstock conda environment.
2. `Configure the AWS CLI`_ from that environment. (Don't type the ``$`` in the example.)

.. _Install the AWS CLI: https://docs.aws.amazon.com/cli/latest/userguide/cli-chap-install.html
.. _Configure the AWS CLI: https://docs.aws.amazon.com/cli/latest/userguide/cli-chap-configure.html#cli-quick-configuration

.. _eagle_install:

Eagle
~~~~~

BuildStock Batch is preinstalled on Eagle. To use it, `ssh into Eagle`_,
activate the appropriate conda environment:

.. _ssh into Eagle: https://www.nrel.gov/hpc/eagle-user-basics.html

::

   module load conda
   source activate /shared-projects/buildstock/envs/buildstock-X.X

You can get a list of installed environments by looking in the envs directory

::

   ls /shared-projects/buildstock/envs

.. _aws-user-config-eagle:

AWS User Configuration
......................

To use the automatic upload of processed results to AWS Athena, you'll need to
configure your user account with your AWS credentials. This setup only needs to
be done once.

First, `ssh into Eagle`_, then
issue the following commands

::

   module load conda
   source activate /shared-projects/buildstock/envs/awscli
   aws configure

Follow the on screen instructions to enter your AWS credentials. When you are
done:

::

   source deactivate

Developer installation
......................

For those doing development work on BuildStock Batch (not most users), a new
conda environment is that includes buildstock batch is created with the bash
script `create_eagle_env.sh` in the git repo that will need to be cloned onto
Eagle. The script is called as follows:

::

   bash create_eagle_env.sh envname

This will create a directory ``/shared-projects/buildstock/envs/env-name`` that
contains the conda environment with BuildStock Batch installed. This environment
can then be used by any user.

If you pass the ``-d`` flag to that script, it will install the buildstock-batch
package in development mode meaning that any changes you make in your cloned
repo will immediately be available to that environment. However, it means that
only the user who installed the environment can use it.

If you pass the flag ``-e /projects/someproject/envs``, it will install the
environment there instead of the default location. This is useful if you need a
specific installation for a particular project.

The ``-d`` and ``-e`` flags can also be combined if desired

::

   bash create_eagle_env.sh -d -e /projects/enduse/envs mydevenv


Amazon Web Services (Beta)
~~~~~~~~~~~~~~~~~~~~~~~~~~

The installation instructions are the same as the :ref:`local-install`
installation. You will need to use an AWS account with appropriate permissions.

The first time you run ``buildstock_aws`` it may take several minutes,
especially over a slower internet connection as it is downloading and building a docker image. If you
make changes to buildstockbatch, run this again before submitting another job.
