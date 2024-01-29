Running a Project
-----------------

Local
~~~~~

Running a project file is straightforward. Call the ``buildstock_local`` command line tool as follows:

.. command-output:: buildstock_local --help
   :ellipsis: 0,8

.. warning::

    In general, you should omit the ``-j`` argument, which will use all the cpus you made available to docker.
    Setting the ``-j`` flag for a number greater than the number of CPUs you made available in Docker
    will cause the simulations to run *slower* as the concurrent simulations will compete for CPUs.

.. warning::

    Running the simulation with ``--postprocessonly`` when there is already postprocessed results from previous run will
    overwrite those results.

NREL HPC (Eagle or Kestrel)
~~~~~~~~~~~~~~~~~~~~~~~~~~~
After you have :ref:`activated the appropriate conda environment on Eagle <eagle_install>`,
you can submit a project file to be simulated by passing it to the ``buildstock_eagle`` command.

.. command-output:: buildstock_eagle --help
   :ellipsis: 0,8

.. command-output:: buildstock_kestrel --help
   :ellipsis: 0, 8

.. warning::

    Running the simulation with ``postprocessonly`` when there is already postprocessed results from previous run will
    overwrite those results.


Project configuration
.....................

To run a project on Kestrel or Eagle, you will need to make a few changes to
your :doc:`project_defn`. First, the ``output_directory`` should be in
``/scratch/your_username/some_directory`` or in ``/projects`` somewhere.
Building stock simulations generate a lot of output quickly and the ``/scratch``
or ``/projects`` filesystem are equipped to handle that kind of I/O throughput
where your ``/home`` directory is not.

Next, you will need to add a :ref:`kestrel-config` or :ref:`eagle-config` top level key to the
project file, which will look something like this:

.. code-block:: yaml

    kestrel:  # or eagle
      account: your_hpc_allocation
      n_jobs: 100  # the number of concurrent nodes to use
      minutes_per_sim: 2
      sampling:
        time: 60  # the number of minutes you expect sampling to take
      postprocessing:
        time: 180  # the number of minutes you expect post processing to take

In general, be conservative on the time estimates. It can be helpful to run a
small batch with pretty conservative estimates and then look at the output logs
to see how long things really took before submitting a full batch simulation.

Re-running failed array jobs
............................

Running buildstockbatch on HPC breaks the simulation into an array of jobs that
you set with the ``n_jobs`` configuration parameter. Each of those jobs runs a
batch of simulations on a single compute node. Sometimes a handful of jobs will
fail. If most of the jobs succeeded, rather than rerun everything you can
resubmit just the jobs that failed with the ``--rerun_failed`` command line
argument. This will also clear out and rerun the postprocessing.

Amazon Web Services
~~~~~~~~~~~~~~~~~~~

Running a batch on AWS is done by calling the ``buildstock_aws`` command line
tool.

.. command-output:: buildstock_aws --help
   :ellipsis: 0,8

AWS Specific Project configuration
..................................

For the project to run on AWS, you will need to add a section to your config
file, something like this:

.. code-block:: yaml

    aws:
      # The job_identifier should be unique, start with alpha, and limited to 10 chars or data loss can occur
      job_identifier: national01
      s3:
        bucket: myorg-resstock
        prefix: national01_run01
      region: us-west-2
      use_spot: true
      batch_array_size: 10000
      # To receive email updates on job progress accept the request to receive emails that will be sent from Amazon
      notifications_email: your_email@somewhere.com

See :ref:`aws-config` for details.

Cleaning up after yourself
..........................

When the simulation and postprocessing is all complete, run ``buildstock_aws
--clean your_project_file.yml``. This will clean up all the AWS resources that
were created on your behalf to run the simulations. Your results will still be
on S3 and queryable in Athena.

Google Cloud Platform
~~~~~~~~~~~~~~~~~~~~~

Run a project on GCP by calling the ``buildstock_gcp`` command line tool.

.. command-output:: buildstock_gcp --help
   :ellipsis: 0,8

The first time you run ``buildstock_gcp`` it may take several minutes,
especially over a slower internet connection as it is downloading and building a docker image.

GCP specific project configuration
..................................

For the project to run on GCP, you will need to add a ``gcp`` section to your project
file, something like this:

.. code-block:: yaml

    gcp:
      job_identifier: national01
      # The project, Artifact Registry repo, and GCS bucket must already exist.
      project: myorg_project
      region: us-central1
      artifact_registry:
        repository: buildstockbatch-docker
      gcs:
        bucket: buildstockbatch
        prefix: national01_run01
      job_environment:
        use_spot: true
      batch_array_size: 10000

See :ref:`gcp-config` for details and other optional settings.

You can optionally override the ``job_identifier`` from the command line
(``buildstock_gcp project.yml [job_identifier]``). Note that each job you run must have a unique ID
(unless you delete a previous job with the ``--clean`` option), so this option makes it easier to
quickly assign a new ID with each run without updating the config file.


Show existing jobs
..................

Run ``buildstock_gcp your_project_file.yml [job_identifier] --show_jobs`` to see the existing
jobs matching the project specified. This can show you whether a previously-started job
has completed, is still running, or has already been cleaned up.


Post-processing only
.....................

If ``buildstock_gcp`` is interrupted after the simulations are kicked off (i.e. the Batch job is
running), the simulations will finish, but post-processing will not be started. You can run only
the post-processing steps later with the ``--postprocessonly`` flag.


Cleaning up after yourself
..........................

When the simulations and postprocessing are complete, run ``buildstock_gcp
your_project_file.yml [job_identifier] --clean``. This will clean up all the GCP resources that
were created to run the specified project, other than files in Cloud Storage. If the project is
still running, it will be cancelled. Your output files will still be available in GCS.

You can clean up files in Cloud Storage from the `GCP Console`_.

If you make code changes between runs, you may want to occasionally clean up the docker
images created for each run with ``docker image prune``.

.. _GCP Console: https://console.cloud.google.com/storage/browser
