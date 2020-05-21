Running a Project
-----------------

Local (Docker)
~~~~~~~~~~~~~~

Running the simulations locally uses Docker. Docker needs to be configured to use all
(or most) of the CPUs on your machine. To do so, click on the Docker icon by the clock. It
looks like a little whale with boxes on its back. Select "Preferences..." and "Advanced".
Slide the CPUs available to Docker to the number of concurrent simulations you want to run
(probably all of them).

Running a project file is straightforward. Call the ``buildstock_docker`` command line tool as follows:

.. command-output:: buildstock_docker --help
   :ellipsis: 0,8

.. warning::

    In general, you should omit the ``-j`` argument, which will use all the cpus you made available to docker.
    Setting the ``-j`` flag for a number greater than the number of CPUs you made available in Docker
    will cause the simulations to run *slower* as the concurrent simulations will compete for CPUs.

.. warning::

    Running the simulation with ``postprocessonly`` when there is already postprocessed results from previous run will
    overwrite those results.

Eagle
~~~~~
After you have :ref:`activated the appropriate conda environment on Eagle <eagle_install>`, 
you can submit a project file to be simulated by passing it to the ``buildstock_eagle`` command.

.. command-output:: buildstock_eagle --help
   :ellipsis: 0,8

.. warning::

    Running the simulation with ``postprocessonly`` when there is already postprocessed results from previous run will
    overwrite those results.


Eagle specific project configuration
....................................

To get a project to run on Eagle, you will need to make a few changes to your :doc:`project_defn`.
First, the ``output_directory`` should be in ``/scratch/your_username/some_directory`` or in ``/projects`` somewhere.
Building stock simulations generate a lot of output quickly and the ``/scratch`` or ``/projects`` filesystem are
equipped to handle that kind of I/O throughput where your ``/home`` directory is not and may cause 
stability issues across the whole system. 

Next, you will need to add an ``eagle`` top level key to the project file, which will look something like this

.. code-block:: yaml

    eagle:
      account: your_hpc_allocation
      n_jobs: 100  # the number of concurrent nodes to use on Eagle, typically 100-500
      minutes_per_sim: 2
      sampling:
        time: 60  # the number of minutes you expect sampling to take
      postprocessing:
        time: 180  # the number of minutes you expect post processing to take

In general, be conservative on the time estimates. It can be helpful to run a small batch with
pretty conservative estimates and then look at the output logs to see how long things really took
before submitting a full batch simulation. 


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
