Running a Project
-----------------

Local (Docker)
~~~~~~~~~~~~~~

Running the simulations locally uses Docker. Docker needs to be configured to use all
(or most) of the CPUs on your machine. To do so, click on the Docker icon up by the clock. It
looks like a little whale with boxes on its back. Select "Preferences..." and "Advanced".
Slide the CPUs available to Docker to the number of concurrent simulations you want to run
(probably all of them).

Running a project file is straightforward. Call the ``buildstock_docker`` command line tool as follows:

.. code-block:: none

    usage: buildstock_docker [-h] [-j J] [--postprocessonly] [--uploadonly] project_filename

    positional arguments:
    project_filename

    optional arguments:
    -h, --help          show this help message and exit.
    -j J                Number of parallel simulations, -1 is all cores, -2 is all.
                        cores except one.
    --postprocessonly   Skip simulating buildings and directly jump to postprocessing step, which involves aggregating
                        and uploading (if configured in the project settings YAML file). Useful for (and can only be
                        used) when the simulations are already done.
    --uploadonly        Directly jump to the upload (to AWS s3) step, skipping both simulation and the aggregation part
                        of postprocessing. Useful for (and can only be used) when the simulation and aggregation are
                        already done. s3 configuration must be present in the project definition yaml file. Cannot be
                        used with --postprocessonly

.. warning::

    Setting the ``-j`` flag for a number greater than the number of CPUs you made available in Docker
    will cause the simulations to run *slower* as the concurrent simulations will compete for CPUs.

.. warning::

    Running the simulation with ``postprocessonly`` when there is already postprocessed results from previous run will
    overwrite those results.

Eagle
~~~~~
After you have :ref:`activated the appropriate conda environment on Eagle <eagle_install>`, 
you can submit a project file to be simulated by passing it to the ``buildstock_eagle`` command.

.. code-block:: none

    usage: buildstock_eagle [--postprocessonly] [--uploadonly] my_projectfile.yml

    --postprocessonly   Skip simulating buildings and directly jump to postprocessing step, which involves aggregating
                        and uploading (if configured in the project settings YAML file). Useful for (and can only be
                        used) when the simulations are already done.
    --uploadonly        Directly jump to the upload (to AWS s3) step, skipping both simulation and the aggregation part
                        of postprocessing. Useful for (and can only be used) when the simulation and aggregation are
                        already done. s3 configuration must be present in the project definition yaml file. Cannot be
                        used with --postprocessonly

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

Peregrine
~~~~~~~~~

.. warning::

    Use of BuildStock Batch on Peregrine is deprecated. It is no longer being maintained for that platform.
    We recommend you use Eagle if you need to run on an HPC environment.

Running on Peregrine, NREL's HPC system is done through 
`submitting a job <https://www.nrel.gov/hpc/peregrine-batch-jobs.html>`_ 
using ``qsub``. First, `ssh into Peregrine <https://www.nrel.gov/hpc/user-basics-peregrine.html>`_
and then do the following:

.. code-block:: bash

    cd buildstockbatch
    qsub -A res_stock -v PROJECTFILE=/path/to/your_project.yml buildstockbatch/peregrine.sh

.. note::

    Use the appropriate allocation for your project in the ``-A`` parameter to ``qsub``.

This will queue your job. Once it runs, it will queue several other jobs which will be the bulk of
the simulations. To check on the status of your jobs, use the command ``qstat -u $USER`` or one of
the commands `here <https://www.nrel.gov/hpc/peregrine-monitor-control-commands.html>`_.

When the simulations and postprocessing are complete the aggregated results will be in 
``<output_directory>/results/results.csv`` along with folders for each simulation. If the results.csv 
file is missing, see :ref:`run-out-of-walltime`.

Project configuration specific to Peregrine
...........................................

In the project file, the ``output_directory`` should be in ``/scratch/your_username/some_directory``.
Building stock simulations generate a lot of output quickly and the ``/scratch`` filesystem is 
equipped to handle that kind of I/O throughput where your ``/home`` directory is not and may cause 
stability issues across the whole system. 

Additionally project files can contain an additional ``peregrine`` key in the project file that 
specifies particulars about how to run the batch simulation on Peregrine. These are optional and 
have sensible defaults for the most part.

- ``n_jobs``: The number of nodes to request on Peregrine for this batch. 
- ``nodetype``: What `kind of node <https://www.nrel.gov/hpc/peregrine-node-requests.html>`_ to run the simulations on. 
- ``queue``: Which `queue <https://www.nrel.gov/hpc/peregrine-job-queues-scheduling.html>`_ to schedule the simulations in.
- ``allocation``: Which allocation to use.
- ``minutes_per_sim``: A conservative estimate of how long an average simulation is expected to take. 
  This is used to set the wall time in the job submission.

.. _run-out-of-walltime:

What to do if your job runs out of wall time
............................................

If your results.csv file is missing, you can check to see if walltime was exceeded by doing
the following:

.. code-block:: bash

    cd /scratch/$USER/output_directory
    grep "PBS: job killed: walltime" job.out-*

That will tell you which (if any) jobs exceeded their wall time. To finish the simulations that didn't complete,
resubmit the job as follows:

.. code-block:: bash

    cd ~/buildstockbatch
    qsub -A res_stock -v PICKUP=1,PROJECTFILE=/path/to/your_project.yml buildstockbatch/peregrine.sh

