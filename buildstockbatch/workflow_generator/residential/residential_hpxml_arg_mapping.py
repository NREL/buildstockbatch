# Some entries in workflow_generator args need to be mapped to the build_existing_model measures
# The mapping involves changing the key names and aggregating the values across multiple entries
# for example, in the workflow generator args, multiple uitility bill scenario are specified using
# mulitple blocks (which translates to a list of dictionaries in the yaml file). The argument to the
# build_existing_model measure is utility_bill_scenario_names with a comma separated list of scenario names
# This ARG_MAP dictionary specifies the mapping between the workflow_generator args and the build_existing_model
# and how to aggregate the values


BUILD_EXISTING_MODEL_ARG_MAP = {
    "utility_bills": {
        "scenario_name": "utility_bill_scenario_names",
        "simple_filepath": "utility_bill_simple_filepaths",
        "detailed_filepath": "utility_bill_detailed_filepaths",
        "elec_fixed_charge": "utility_bill_electricity_fixed_charges",
        "elec_marginal_rate": "utility_bill_electricity_marginal_rates",
        "gas_fixed_charge": "utility_bill_natural_gas_fixed_charges",
        "gas_marginal_rate": "utility_bill_natural_gas_marginal_rates",
        "propane_fixed_charge": "utility_bill_propane_fixed_charges",
        "propane_marginal_rate": "utility_bill_propane_marginal_rates",
        "oil_fixed_charge": "utility_bill_fuel_oil_fixed_charges",
        "oil_marginal_rate": "utility_bill_fuel_oil_marginal_rates",
        "wood_fixed_charge": "utility_bill_wood_fixed_charges",
        "wood_marginal_rate": "utility_bill_wood_marginal_rates",
        "pv_compensation_type": "utility_bill_pv_compensation_types",
        "pv_net_metering_annual_excess_sellback_rate_type": "utility_bill_pv_net_metering_annual_excess_sellback_rate_types",
        "pv_net_metering_annual_excess_sellback_rate": "utility_bill_pv_net_metering_annual_excess_sellback_rates",
        "pv_feed_in_tariff_rate": "utility_bill_pv_feed_in_tariff_rates",
        "pv_monthly_grid_connection_fee_units": "utility_bill_pv_monthly_grid_connection_fee_units",
        "pv_monthly_grid_connection_fee": "utility_bill_pv_monthly_grid_connection_fees",
    },
    "emissions": {
        "scenario_name": "emissions_scenario_names",
        "type": "emissions_types",
        "elec_folder": "emissions_electricity_folders",
        "gas_value": "emissions_natural_gas_values",
        "propane_value": "emissions_propane_values",
        "oil_value": "emissions_fuel_oil_values",
        "wood_value": "emissions_wood_values",
    },
}
