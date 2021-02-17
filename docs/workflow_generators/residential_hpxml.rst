Residential HPXML Workflow Generator
------------------------------

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

- ``build_existing_model`` (optional): TODO. See
  :ref:`build-existing-model-defaults` for current defaults.

- ``simulation_output_report`` (optional): TODO. See
  :ref:`sim-output-report-defaults` for current defaults.

.. _BuildExistingModel: https://github.com/NREL/resstock/blob/restructure-v3/measures/BuildExistingModel/measure.xml
.. _SimulationOutputReport: https://github.com/NREL/resstock/blob/restructure-v3/resources/hpxml-measures/SimulationOutputReport/measure.xml

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
