Commercial Default Workflow Generator
-------------------------------------

Configuration Example
~~~~~~~~~~~~~~~~~~~~~

.. code-block:: yaml

    workflow_generator:
      type: commercial_default
      args:
        reporting_measures:
        - measure_dir_name: f8e23017-894d-4bdf-977f-37e3961e6f42 # OpenStudio Results
          arguments:
            building_summary_section: true
            annual_overview_section: true
            monthly_overview_section: true
            utility_bills_rates_section: true
            envelope_section_section: true
            space_type_breakdown_section: true
            space_type_details_section: true
            interior_lighting_section: true
            plug_loads_section: true
            exterior_light_section: true
            water_use_section: true
            hvac_load_profile: true
            zone_condition_section: true
            zone_summary_section: true
            zone_equipment_detail_section: true
            air_loops_detail_section: true
            plant_loops_detail_section: true
            outdoor_air_section: true
            cost_summary_section: true
            source_energy_section: true
            schedules_overview_section: true
        - measure_dir_name: comstock_sensitivity_reports
        - measure_dir_name: qoi_report
        - measure_dir_name: la_100_qaqc
          arguments:
            run_qaqc: true
        - measure_dir_name: simulation_settings_check
          arguments:
            run_sim_settings_checks: true


Arguments
~~~~~~~~~
- ``measures`` (optional): Add these optional measures to the end of your workflow.
  Not typically used by ComStock.

    - ``measure_dir_name``: Name of measure directory.
    - ``arguments``: map of key, value arguments to pass to the measure.

- ``reporting_measures`` (optional): Add these optional reporting measures to the end of your workflow.

    - ``measure_dir_name``: Name of measure directory.
    - ``arguments``: map of key, value arguments to pass to the measure.

.. note::

   The registerValues created by the OpenStudio Results measure
   (measure_dir_name: `f8e23017-894d-4bdf-977f-37e3961e6f42`, class name: `OpenStudioResults`)
   have been hard-coded to be disabled in the workflow because most of the values are not useful in a
   ComStock results.csv. If you wish to have these values show up, simply make a copy of the measure and
   use a different class name in the measure.rb and measure.xml file.
