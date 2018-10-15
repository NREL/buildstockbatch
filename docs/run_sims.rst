Running a Project
-----------------

Local (Docker)
~~~~~~~~~~~~~~

Running the simulations locally uses Docker. On the Mac, Docker needs to be configured to use all
(or most) of the CPUs on your machine. To do so, click on the Docker icon up by the clock. It
looks like a little whale with boxes on its back. Select "Preferences..." and "Advanced".
Slide the CPUs available to Docker to the number of concurrent simulations you want to run
(probably all of them).

.. todo::

    Determine if Windows needs a similar configuration.

Running a project file is straightforward. Call the ``buildstock_docker`` command line tool as follows:

.. code-block:: none

    usage: buildstock_docker [-h] [-j J] [--skipsims] project_filename

    positional arguments:
    project_filename

    optional arguments:
    -h, --help        show this help message and exit
    -j J              Number of parallel simulations, -1 is all cores, -2 is all
                        cores except one
    --skipsims        Skip simulating buildings, useful for when the simulations
                        are already done

.. warning::

    Setting the ``-j`` flag for a number greater than the number of CPUs you made available in Docker
    will cause the simulations to run *slower* as the concurrent simulations will compete for CPUs.

Peregrine
~~~~~~~~~

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


Amazon Web Services
~~~~~~~~~~~~~~~~~~~

Coming soon.

