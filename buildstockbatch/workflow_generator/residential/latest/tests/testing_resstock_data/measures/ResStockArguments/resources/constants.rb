# frozen_string_literal: true

class Constants
  def self.build_residential_hpxml_excludes
    # don't make these BuildResidentialHPXML arguments into ResStockArguments arguments
    return ['hpxml_path',
            'existing_hpxml_path',
            'whole_sfa_or_mf_building_sim',
            'software_info_program_used',
            'software_info_program_version',
            'schedules_filepaths',
            'simulation_control_timestep',
            'simulation_control_run_period',
            'simulation_control_run_period_calendar_year',
            'simulation_control_daylight_saving_period',
            'simulation_control_temperature_capacitance_multiplier',
            'simulation_control_defrost_model_type',
            'unit_multiplier',
            'geometry_unit_left_wall_is_adiabatic',
            'geometry_unit_right_wall_is_adiabatic',
            'geometry_unit_front_wall_is_adiabatic',
            'geometry_unit_back_wall_is_adiabatic',
            'geometry_unit_num_floors_above_grade',
            'air_leakage_has_flue_or_chimney_in_conditioned_space',
            'heating_system_airflow_defect_ratio',
            'cooling_system_airflow_defect_ratio',
            'cooling_system_charge_defect_ratio',
            'heat_pump_airflow_defect_ratio',
            'heat_pump_charge_defect_ratio',
            'hvac_control_heating_weekday_setpoint',
            'hvac_control_heating_weekend_setpoint',
            'hvac_control_cooling_weekday_setpoint',
            'hvac_control_cooling_weekend_setpoint',
            'pv_system_num_bedrooms_served',
            'battery_num_bedrooms_served',
            'emissions_scenario_names',
            'emissions_types',
            'emissions_electricity_units',
            'emissions_electricity_values_or_filepaths',
            'emissions_electricity_number_of_header_rows',
            'emissions_electricity_column_numbers',
            'emissions_fossil_fuel_units',
            'emissions_natural_gas_values',
            'emissions_propane_values',
            'emissions_fuel_oil_values',
            'emissions_wood_values',
            'emissions_coal_values',
            'emissions_wood_pellets_values',
            'utility_bill_scenario_names',
            'utility_bill_electricity_filepaths',
            'utility_bill_electricity_fixed_charges',
            'utility_bill_electricity_marginal_rates',
            'utility_bill_natural_gas_fixed_charges',
            'utility_bill_natural_gas_marginal_rates',
            'utility_bill_propane_fixed_charges',
            'utility_bill_propane_marginal_rates',
            'utility_bill_fuel_oil_fixed_charges',
            'utility_bill_fuel_oil_marginal_rates',
            'utility_bill_wood_fixed_charges',
            'utility_bill_wood_marginal_rates',
            'utility_bill_coal_fixed_charges',
            'utility_bill_coal_marginal_rates',
            'utility_bill_wood_pellets_fixed_charges',
            'utility_bill_wood_pellets_marginal_rates',
            'utility_bill_pv_compensation_types',
            'utility_bill_pv_net_metering_annual_excess_sellback_rate_types',
            'utility_bill_pv_net_metering_annual_excess_sellback_rates',
            'utility_bill_pv_feed_in_tariff_rates',
            'utility_bill_pv_monthly_grid_connection_fee_units',
            'utility_bill_pv_monthly_grid_connection_fees',
            'additional_properties',
            'combine_like_surfaces',
            'apply_defaults',
            'apply_validation']
  end

  def self.build_residential_schedule_file_excludes
    # don't make these BuildResidentialScheduleFile arguments into ResStockArguments arguments
    return ['hpxml_path',
            'schedules_column_names',
            'schedules_random_seed',
            'output_csv_path',
            'hpxml_output_path',
            'append_output',
            'debug',
            'building_id']
  end

  def self.other_excludes
    # these are ResStockArguments that haven't made their way into options_lookup.tsv yet
    return ['heating_system_actual_cfm_per_ton',
            'heating_system_rated_cfm_per_ton']
  end

  def self.Auto
    return 'auto'
  end
end
