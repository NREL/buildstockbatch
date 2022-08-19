Residential Default Workflow Generator
--------------------------------------

Configuration Example
~~~~~~~~~~~~~~~~~~~~~

.. code-block:: yaml

    workflow_generator:
      type: residential_default
      args:
        timeseries_csv_export:
          reporting_frequency: Hourly
          include_enduse_subcategories: true
        server_directory_cleanup:
          retain_in_osm: true
          retain_in_idf: false
          retain_eplusout_rdd: true

Arguments
~~~~~~~~~

- ``measures_to_ignore`` (optional): **ADVANCED FEATURE (USE WITH
  CAUTION--ADVANCED USERS/WORKFLOW DEVELOPERS ONLY)** to optionally
  not run one or more measures (specified as a list) that are referenced in the
  options_lookup.tsv but should be skipped during model creation. The measures
  are referenced by their directory name.

- ``residential_simulation_controls`` update the arguments to the
  `ResidentialSimulationControls`_ measure. See
  :ref:`residential-sim-ctrl-defaults` for current defaults.

- ``measures`` (optional): Add these optional measures to the end of your workflow.

    - ``measure_dir_name``: Name of measure directory.
    - ``arguments``: map of key, value arguments to pass to the measure.

- ``simulation_output`` (optional): optionally include annual totals for end use
  subcategories (i.e., interior equipment broken out by end use) along with the
  usual annual simulation results. This argument is passed directly into the
  `SimulationOutputReport`_ measure in resstock. Please refer to
  the measure argument there to determine what to set it to in your config file.
  Note that this measure and presence of any arguments may be different
  depending on which version of resstock you're using. The best
  thing you can do is to verify that it works with what is in your branch.

- ``timeseries_csv_export`` (optional): include hourly or subhourly results
  along with the usual annual simulation results. These arguments are passed
  directly to the `TimeseriesCSVExport`_ measure in resstock. Please refer to
  the measure arguments there to determine what to set them to in your config
  file. Note that this measure and arguments may be different depending on which
  version of resstock you're using. The best thing you can do is to
  verify that it works with what is in your branch.

- ``server_directory_cleanup`` (optional): optionally preserve or delete
  various simulation output files. These arguments are passed directly to
  the `ServerDirectoryCleanup`_ measure in resstock. Please refer to the
  measure arguments there to determine what to set them to in your config file.
  Note that the default behavior is to retain some files and remove others.
  See :ref:`server-dir-cleanup-defaults` for current defaults.

- ``reporting_measures`` (optional): a list of reporting measures to apply
  to the workflow. Any columns reported by these additional measures will be
  appended to the results csv. Note: For upgrade runs, do not add
  ``ApplyUpgrade`` to the list of reporting measures, doing so will cause run
  to fail prematurely. ``ApplyUpgrade`` is applied automatically when the
  ``upgrades`` key is supplied.

  - ``measure_dir_name``: Name of measure directory.
  - ``arguments``: map of key, value arguments to pass to the measure.


.. _ResidentialSimulationControls: https://github.com/NREL/resstock/blob/develop/measures/ResidentialSimulationControls/measure.xml
.. _SimulationOutputReport: https://github.com/NREL/resstock/blob/develop/measures/SimulationOutputReport/measure.xml
.. _TimeseriesCSVExport: https://github.com/NREL/resstock/blob/develop/measures/TimeseriesCSVExport/measure.xml
.. _ServerDirectoryCleanup: https://github.com/NREL/resstock/blob/develop/measures/ServerDirectoryCleanup/measure.xml

.. _residential-sim-ctrl-defaults:

Residential Simulation Control Defaults
.......................................

.. include:: ../../buildstockbatch/workflow_generator/residential.py
   :code: python
   :start-after: res_sim_ctl_args = {
   :end-before: }

.. _server-dir-cleanup-defaults:

Server Directory Cleanup Defaults
.................................

.. include:: ../../buildstockbatch/workflow_generator/residential.py
   :code: python
   :start-after: server_dir_cleanup_args = {
   :end-before: }
