Installation
------------

BuildStockBatch installations depend on the `ResStock`_ or `ComStock`_
repository. Either ``git clone`` it or download a copy of it or your fork or
branch of it with your projects.

.. _ResStock: https://github.com/NREL/resstock
.. _ComStock: https://github.com/NREL/comstock

.. _local-install:

Local
~~~~~

This method works for running the simulations locally. BuildStockBatch simulations are
computationally intensive. Local use is only recommended for small testing runs.

OpenStudio Installation
.......................

Download and install the `OpenStudio release`_ that corresponds to your
operating system and the release of ResStock or ComStock you are using.

It's common to need a couple different versions of OpenStudio available for
different analyses. This is best achieved by downloading the ``.tar.gz`` package
for your operating system and unzipping it into a folder rather than installing
it. To let BuildStockBatch know which OpenStudio to use, pass the path as the
``OPENSTUDIO_EXE`` environment variable.

For example to get OpenStudio 3.5.1 on an Apple Silicon Mac

.. code-block:: bash

   # Make a directory for your openstudio installations to live in
   mkdir ~/openstudio
   cd ~/openstudio

   # Download the .tar.gz version for your operating system, x86_64 for an Intel mac
   # This can also done using a browser from the OpenStudio releases page
   curl -O -L https://github.com/NREL/OpenStudio/releases/download/v3.5.1/OpenStudio-3.5.1+22e1db7be5-Darwin-arm64.tar.gz

   # Extract it
   tar xvzf OpenStudio-3.5.1+22e1db7be5-Darwin-arm64.tar.gz

   # Optionally remove the tar file
   rm OpenStudio-3.5.1+22e1db7be5-Darwin-arm64.tar.gz

   # Set your environment variable to point to the correct version
   # This will only work for the current terminal session
   # You can also set this in ~/.zshrc to make it work for every terminal session
   export OPENSTUDIO_EXE="~/openstudio/OpenStudio-3.5.1+22e1db7be5-Darwin-arm64/bin/openstudio"

For Windows, the process is similar.

   1. Download the Windows `OpenStudio release`_ for windows with the ``.tar.gz`` extension.
      For OpenStudio 3.5.1 that is ``OpenStudio-3.5.1+22e1db7be5-Windows.tar.gz``.
   2. Extract it to a folder that you know.
   3. Set the ``OPENSTUDIO_EXE`` environment variable to the path.
      ``C:\path\to\OpenStudio-3.5.1+22e1db7be5-Windows/bin/openstudio.exe``
      Here's how to `set a Windows environment Variable`_.


.. _set a Windows environment Variable: https://www.computerhope.com/issues/ch000549.htm
.. _OpenStudio release: https://github.com/NREL/OpenStudio/releases

.. _python:

BuildStockBatch Python Library
..............................

Install Python 3.8 or greater for your platform.

Get a copy of BuildStockBatch either by downloading the zip file from GitHub or
`cloning the repository <https://github.com/NREL/buildstockbatch>`_.

Optional, but highly recommended, is to create a new `python virtual
environment`_ if you're using python from python.org, or to create a new `conda
environment`_ if you're using conda. Make sure you configure your virtual
environment to use Python 3.8 or greater. Then activate your environment.

.. _python virtual environment: https://docs.python.org/3/library/venv.html
.. _conda environment: https://conda.io/projects/conda/en/latest/user-guide/tasks/manage-environments.html

Standard Install
................

If you are just going to be using buildstockbatch, not working on it, install like so:

::

   cd /path/to/buildstockbatch
   python -m pip install -e .

Developer Install
.................

If you are going to be working on and contributing back to buildstockbatch,
install as follows after cloning the repository and creating and activating a
new python or conda environment.

::

   cd /path/to/buildstockbatch
   python -m pip install -e ".[dev]"
   pre-commit install

.. _aws-user-config-local:

AWS User Configuration
......................

To upload BuildStockBatch data to AWS at the end of your run and send results to AWS Athena, you'll need to
configure your user account with your AWS credentials. This setup only needs to be done once.

1. `Install the AWS CLI`_ version 2
2. `Configure the AWS CLI`_. (Don't type the ``$`` in the example.)
3. You may need to `change the Athena Engine version`_ for your query workgroup to v2 or v3.

.. _Install the AWS CLI: https://docs.aws.amazon.com/cli/latest/userguide/cli-chap-install.html
.. _Configure the AWS CLI: https://docs.aws.amazon.com/cli/latest/userguide/cli-chap-configure.html#cli-quick-configuration
.. _change the Athena Engine version: https://docs.aws.amazon.com/athena/latest/ug/engine-versions-changing.html

.. _kestrel_install:

Kestrel
~~~~~~~

The most common way to run buildstockbatch on Kestrel will be to use a pre-built
python environment. This is done as follows:

::

   module load python
   source /kfs2/shared-projects/buildstock/envs/buildstock-20XX.XX.X/bin/activate

You can get a list of installed environments by looking in the envs directory

::

   ls /kfs2/shared-projects/buildstock/envs

Developer Installaion
......................

For those doing development work on buildstockbatch (not most users), a new
python environment that includes buildstockbatch is created with the bash
script `create_kestrel_env.sh` in the git repo that will need to be cloned onto
Kestrel. The script is called as follows:

::

   module load git
   git clone git@github.com:NREL/buildstockbatch.git
   cd buildstockbatch
   bash create_kestrel_env.sh env-name

This will create a directory ``/kfs2/shared-projects/buildstock/envs/env-name``
that contains the python environment with buildstockbatch installed. This
environment can then be used by any user.

If you pass the ``-d`` flag to that script, it will install the buildstockbatch
package in development mode meaning that any changes you make in your cloned
repo will immediately be available to that environment. However, it means that
only the user who installed the environment can use it.

If you pass the flag ``-e /projects/someproject/envs``, it will install the
environment there instead of the default location. This is useful if you need a
specific installation for a particular project.

The ``-d`` and ``-e`` flags can also be combined if desired

::

   bash create_kestrel_env.sh -d -e /projects/enduse/envs mydevenv

.. _eagle_install:

Eagle
~~~~~

buildstockbatch is preinstalled on Eagle. To use it, `ssh into Eagle`_,
activate the appropriate conda environment:

.. _ssh into Eagle: https://www.nrel.gov/hpc/eagle-user-basics.html

::

   module load conda
   source activate /shared-projects/buildstock/envs/buildstock-X.X

You can get a list of installed environments by looking in the envs directory

::

   ls /shared-projects/buildstock/envs

Developer installation
......................

For those doing development work on buildstockbatch (not most users), a new
conda environment that includes buildstockbatch is created with the bash
script `create_eagle_env.sh` in the git repo that will need to be cloned onto
Eagle. The script is called as follows:

::

   bash create_eagle_env.sh env-name

This will create a directory ``/shared-projects/buildstock/envs/env-name`` that
contains the conda environment with BuildStock Batch installed. This environment
can then be used by any user.

If you pass the ``-d`` flag to that script, it will install the buildstockbatch
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

.. warning::

   The AWS version of buildstockbatch is currently broken. A remedy is in
   progress. Thanks for your patience.

The installation instructions are the same as the :ref:`local-install`
installation. You will need to use an AWS account with appropriate permissions.
The first time you run ``buildstock_aws`` it may take several minutes,
especially over a slower internet connection as it is downloading and building a docker image.


Google Cloud Platform
~~~~~~~~~~~~~~~~~~~~~

Shared, one-time GCP setup
..........................
One-time GCP setup that can be shared by multiple users.

1. If needed, create a GCP Project. The following steps will occur in that project.
2. Set up the following resources in your GCP projects. You can either do this manually or
   using terraform.

    * **Option 1**: Manual setup

      * `Create a Google Cloud Storage Bucket`_ (that will store simulation and postprocessing output).
        Alternatively, each user can create and use their own bucket.
      * `Create a repository`_ in Artifact Registry (to store Docker images).
        This is expected to be in the same region as the storage bucket.

    * **Option 2**: Terraform

      * Follow the :ref:`per-user-gcp` instructions below to install BuildStockBatch and the Google Cloud CLI.
      * Install `Terraform`_.
      * From the buildstockbatch/gcp/ directory, run the following with your chosen GCP project and region
        (e.g. "us-central1"). You can optionally specify the names of the storage bucket and
        artifact registery repository. See `main.tf` for more details.

        ::

            terraform init
            terraform apply -var="gcp_project=PROJECT" -var="region=REGION"

3. Optionally, create a shared Service Account. Alternatively, each user can create their own service account,
   or each user can install the `gcloud CLI`_. The following documentation will assume use of a Service
   Account.

.. _Create a repository:
   https://cloud.google.com/artifact-registry/docs/repositories/create-repos
.. _Create a Google Cloud Storage Bucket:
   https://cloud.google.com/storage/docs/creating-buckets
.. _gcloud CLI: https://cloud.google.com/sdk/docs/install
.. _Terraform: https://developer.hashicorp.com/terraform/tutorials/aws-get-started/install-cli


.. _per-user-gcp:

Per-user setup
..............
One-time setup that each user needs to do on the workstation from which they'll launch and
manage BuildStockBatch runs.

1. Install `Docker`_. This is needed by the script to manage Docker images (pull, push, etc).
2. Get BuildStockBatch and set up a Python environment for it using the :ref:`python` instructions
   above (i.e., create a Python virtual environment, activate the venv, and install buildstockbatch
   to it).
3. Download/Clone ResStock or ComStock.
4. Set up GCP authentication.

   * **Option 1**: Create and download a `Service Account Key`_.

     * Add the location of the key file as an environment variable; e.g.,
       ``export GOOGLE_APPLICATION_CREDENTIALS="~/path/to/service-account-key.json"``. This can be
       done at the command line (in which case it will need to be done for every shell session that
       will run BuildStockBatch, and it will only be in effect for only that session), or added to a
       shell startup script (in which case it will be available to all shell sessions).

   * **Option 2**: Install the `Google Cloud CLI`_ and run the following:

      ::

        gcloud config set project PROJECT
        gcloud auth application-default login

        gcloud auth login
        gcloud auth configure-docker REGION-docker.pkg.dev



.. _Docker: https://www.docker.com/get-started/
.. _Service Account Key: https://cloud.google.com/iam/docs/keys-create-delete
.. _Google Cloud CLI: https://cloud.google.com/sdk/docs/install-sdk
