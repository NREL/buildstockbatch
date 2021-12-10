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
        simulation_output_report:
          timeseries_frequency: hourly
          include_timeseries_fuel_consumptions: true
          include_timeseries_end_use_consumptions: true

Arguments
~~~~~~~~~

- ``build_existing_model``: Update the arguments to the `BuildExistingModel`_ measure. See
  :ref:`build-existing-model-defaults` for current defaults.

- ``simulation_output_report``: Update the arguments to the `SimulationOutputReport`_ measure. See
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

.. _BuildExistingModel: https://github.com/NREL/resstock/blob/restructure-v3/measures/BuildExistingModel/measure.xml
.. _ReportSimulationOutput: https://github.com/NREL/resstock/blob/restructure-v3/resources/hpxml-measures/ReportSimulationOutput/measure.xml

.. _build-existing-model-defaults:

Build Existing Model Defaults
.............................

.. include:: ../../buildstockbatch/workflow_generator/residential_hpxml.py
   :code: python
   :start-after: sim_ctl_run_prd_args = {
   :end-before: }

.. _sim-output-report-defaults:

Simulation Output Report Defaults
..................................

.. include:: ../../buildstockbatch/workflow_generator/residential_hpxml.py
   :code: python
   :start-after: sim_out_rep_args = {
   :end-before: }
