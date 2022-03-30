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
            type: CO2e
            elec_folder: data/cambium/LRMER_MidCase_15
            gas_value: 147.3
            propane_value: 177.8
            oil_value: 195.9
            wood_value: 0.0

        simulation_output_report:
          timeseries_frequency: hourly
          include_timeseries_total_consumptions: true
          include_timeseries_fuel_consumptions: true
          include_timeseries_end_use_consumptions: true
          include_timeseries_emissions: true
          output_variables:
            - name: Zone People Occupant Count

        reporting_measures:
          - measure_dir_name: QOIReport

        server_directory_cleanup:
          retain_in_osm: true
          retain_in_idf: false
          retain_eplusout_rdd: true

Arguments
~~~~~~~~~

- ``build_existing_model``: Update the simulation control arguments to the `BuildExistingModel`_ measure.
  See :ref:`build-existing-model-defaults` for current defaults.

  - ``simulation_control_timestep``: Value must be a divisor of 60.
  - ``simulation_control_run_period_begin_month``: This numeric field should contain the starting month number (1 = January, 2 = February, etc.) for the annual run period desired.
  - ``simulation_control_run_period_begin_day_of_month``: This numeric field should contain the starting day of the starting month (must be valid for month) for the annual run period desired.
  - ``simulation_control_run_period_end_month``: This numeric field should contain the end month number (1 = January, 2 = February, etc.) for the annual run period desired.
  - ``simulation_control_run_period_end_day_of_month``: This numeric field should contain the ending day of the ending month (must be valid for month) for the annual run period desired.
  - ``simulation_control_run_period_calendar_year``: This numeric field should contain the calendar year that determines the start day of week. If you are running simulations using AMY weather files, the value entered for calendar year will not be used; it will be overridden by the actual year found in the AMY weather file.
  - ``debug``: If true: 1) Writes in.osm file, 2) Generates additional log output, and 3) Creates all EnergyPlus output files.
  - ``add_component_loads``: If true, output the annual heating/cooling component loads. Using this comes with a small runtime performance penalty.

- ``emissions`` (optional): Add these arguments to the `BuildExistingModel`_ measure for performing emissions calculations.

  - ``scenario_name``: Name of the emission scenario.
  - ``type``: Type of emission (e.g., CO2e, NOx, etc.).
  - ``elec_folder``: Folder of schedule files with hourly electricity emissions factors values. Units are kg/MWh. Path is relative to buildstock_directory's resources folder. File names must contain GEA region names.
  - ``gas_value``: Annual emissions factor for natural gas. Units are lb/MBtu (million Btu).
  - ``propane_value``: Annual emissions factor for propane. Units are lb/MBtu (million Btu).
  - ``oil_value``: Annual emissions factor for fuel oil. Units are lb/MBtu (million Btu).
  - ``wood_value``: Annual emissions factor for wood. Units are lb/MBtu (million Btu).

- ``measures`` (optional): Add these optional measures to the end of your workflow.

  - ``measure_dir_name``: Name of measure directory.
  - ``arguments``: map of key, value arguments to pass to the measure.

- ``simulation_output_report``: Update the arguments to the `ReportSimulationOutput`_ measure.
  See :ref:`sim-output-report-defaults` for current defaults.

  - ``timeseries_frequency``: The frequency at which to report timeseries output data. Using 'none' will disable timeseries outputs. Valid choices are 'none', 'timestep', 'hourly', 'daily', and 'monthly'.
  - ``include_timeseries_total_consumptions``: Generates timeseries energy consumptions for building total.
  - ``include_timeseries_fuel_consumptions``: Generates timeseries energy consumptions for each fuel type (in kBtu for fossil fuels and kWh for electricity).
  - ``include_timeseries_end_use_consumptions``: Generates timeseries energy consumptions for each end use type (in kBtu for fossil fuels and kWh for electricity).
  - ``include_timeseries_emissions``: Generates timeseries emissions (e.g., CO2). Requires the appropriate HPXML inputs to be specified.
  - ``include_timeseries_hot_water_uses``: Generates timeseries hot water usages for each end use type (in gallons).
  - ``include_timeseries_total_loads``: Generates timeseries total heating, cooling, and hot water loads (in kBtu) for the building.
  - ``include_timeseries_component_loads``: Generates timeseries heating and cooling loads (in kBtu) disaggregated by component type (e.g., Walls, Windows, Infiltration, Ducts, etc.).
  - ``include_timeseries_zone_temperatures``: Generates timeseries average temperatures (in deg-F) for each space modeled (e.g., living space, attic, garage, basement, crawlspace, etc.). 
  - ``include_timeseries_airflows``: Generates timeseries airflow rates (in cfm) for infiltration, mechanical ventilation (including clothes dryer exhaust), natural ventilation, whole house fans.
  - ``include_timeseries_weather``: Generates timeseries weather file data including outdoor temperatures, relative humidity, wind speed, and solar.
  - ``add_timeseries_dst_column``: Optionally add, in addition to the default local standard Time column, a local clock TimeDST column. Requires that daylight saving time is enabled.
  - ``add_timeseries_utc_column``: Optionally add, in addition to the default local standard Time column, a local clock TimeUTC column. If the time zone UTC offset is not provided in the HPXML file, the time zone in the EPW header will be used.
  - ``output_variables``: Optionally request EnergyPlus output variables. Do not include key values; by default all key values will be requested.

- ``reporting_measures`` (optional): A list of additional reporting measures to apply to the workflow.
  Any columns reported by these additional measures will be appended to the results csv.
  Note: For upgrade runs, do not add ``ApplyUpgrade`` to the list of reporting measures, doing so will cause run to fail prematurely.
  ``ApplyUpgrade`` is applied automatically when the ``upgrades`` key is supplied.

  - ``measure_dir_name``: Name of measure directory.
  - ``arguments``: map of key, value arguments to pass to the measure.

- ``server_directory_cleanup`` (optional): Optionally preserve or delete various simulation output files.
  These arguments are passed directly to the `ServerDirectoryCleanup`_ measure in resstock.
  Please refer to the measure arguments there to determine what to set them to in your config file.
  Note that the default behavior is to retain some files and remove others.
  See :ref:`server-dir-cleanup-defaults` for current defaults.

.. _BuildExistingModel: https://github.com/NREL/resstock/blob/develop/measures/BuildExistingModel/measure.xml
.. _ReportSimulationOutput: https://github.com/NREL/resstock/blob/develop/resources/hpxml-measures/ReportSimulationOutput/measure.xml
.. _ServerDirectoryCleanup: https://github.com/NREL/resstock/blob/develop/measures/ServerDirectoryCleanup/measure.xml

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
