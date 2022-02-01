Residential HPXML Workflow Generator
------------------------------------

Configuration Example
~~~~~~~~~~~~~~~~~~~~~

.. code-block:: yaml

    workflow_generator:
      type: residential_hpxml
      args:
        build_existing_model:
          simulation_control_run_period_calendar_year: 2010

        emissions:
          - scenario_name: Scenario1
            type: CO2
            elec_folder: data/cambium/LRMER_MidCase_15
            gas_value: 117.6
            propane_value: 136.6
            oil_value: 161.0
            wood_value: 200.0

        simulation_output_report:
          timeseries_frequency: hourly
          include_timeseries_fuel_consumptions: true
          include_timeseries_end_use_consumptions: true
          include_timeseries_emissions: true

        server_directory_cleanup:
          retain_in_osm: true
          retain_in_idf: false
          retain_eplusout_rdd: true

Arguments
~~~~~~~~~

- ``build_existing_model``: Update the simulation control arguments to the `BuildExistingModel`_ measure. See
  :ref:`build-existing-model-defaults` for current defaults.

- ``emissions`` (optional): Add these arguments to the `BuildExistingModel`_ measure for performing emissions calculations.

    - ``scenario_name``: Name of the emission scenario.
    - ``type``: Type of emission (e.g., CO2, NOx, etc.).
    - ``elec_folder``: Folder of schedule files with hourly electricity emissions factors values. Units are kg/MWh. Path is relative to buildstock_directory's resources folder. File names must contain GEA region names.
    - ``gas_value``: Annual emissions factor for natural gas. Units are lb/MBtu (million Btu).
    - ``propane_value``: Annual emissions factor for propane. Units are lb/MBtu (million Btu).
    - ``oil_value``: Annual emissions factor for fuel oil. Units are lb/MBtu (million Btu).
    - ``wood_value``: Annual emissions factor for wood. Units are lb/MBtu (million Btu).

- ``simulation_output_report``: Update the arguments to the `ReportSimulationOutput`_ measure. See
  :ref:`sim-output-report-defaults` for current defaults.

- ``measures`` (optional): Add these optional measures to the end of your workflow.

    - ``measure_dir_name``: Name of measure directory.
    - ``arguments``: map of key, value arguments to pass to the measure.

- ``reporting_measures`` (optional): a list of reporting measure names to apply
  additional reporting measures (that require no arguments) to the workflow. Any
  columns reported by these additional measures will be appended to the results
  csv. Note: For upgrade runs, do not add ``ApplyUpgrade`` to the list of
  reporting measures, doing so will cause run to fail prematurely.
  ``ApplyUpgrade`` is applied automatically when the ``upgrades`` key is supplied.

- ``server_directory_cleanup`` (optional): optionally preserve or delete
  various simulation output files. These arguments are passed directly to
  the `ServerDirectoryCleanup`_ measure in resstock. Please refer to the
  measure arguments there to determine what to set them to in your config file.
  Note that the default behavior is to retain some files and remove others.
  See :ref:`server-dir-cleanup-defaults` for current defaults.

.. _BuildExistingModel: https://github.com/NREL/resstock/blob/restructure-v3/measures/BuildExistingModel/measure.xml
.. _ReportSimulationOutput: https://github.com/NREL/resstock/blob/restructure-v3/resources/hpxml-measures/ReportSimulationOutput/measure.xml
.. _ServerDirectoryCleanup: https://github.com/NREL/resstock/blob/restructure-v3/measures/ServerDirectoryCleanup/measure.xml

.. _build-existing-model-defaults:

Build Existing Model Defaults
.............................

.. include:: ../../buildstockbatch/workflow_generator/residential_hpxml.py
   :code: python
   :start-after: sim_ctl_args = {
   :end-before: }

.. _sim-output-report-defaults:

Simulation Output Report Defaults
..................................

.. include:: ../../buildstockbatch/workflow_generator/residential_hpxml.py
   :code: python
   :start-after: sim_out_rep_args = {
   :end-before: }

.. _server-dir-cleanup-defaults:

Server Directory Cleanup Defaults
.................................

.. include:: ../../buildstockbatch/workflow_generator/residential_hpxml.py
   :code: python
   :start-after: server_dir_cleanup_args = {
   :end-before: }
