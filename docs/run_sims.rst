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

.. command-output:: buildstock_docker --help
   :ellipsis: 0,8

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
