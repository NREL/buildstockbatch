"""
Default values for various measure arguments used in the residential HPXML workflow generator is defined here.
This default values provide a means to use different defaults than the one defined in the measure itself.

Structure of DEFAULT_MEASURE_ARGS:
DEFAULT_MEASURE_ARGS = {
    "MeasureDirName": {  <-- Directory name only, path will be automatically found
        "measure_arg_name": "default_value",
        ...
    },
    ...
}
"""

DEFAULT_MEASURE_ARGS = {
    "BuildExistingModel": {
        "simulation_control_timestep": 60,
        "simulation_control_run_period_begin_month": 1,
        "simulation_control_run_period_begin_day_of_month": 1,
        "simulation_control_run_period_end_month": 12,
        "simulation_control_run_period_end_day_of_month": 31,
        "simulation_control_run_period_calendar_year": 2007,
    },
    "HPXMLtoOpenStudio": {
        "hpxml_path": "../../run/home.xml",
        "output_dir": "../../run",
        "debug": False,
        "add_component_loads": False,
        "skip_validation": True,
    },
    "ReportSimulationOutput": {
        "timeseries_frequency": "none",
        "include_timeseries_total_consumptions": False,
        "include_timeseries_fuel_consumptions": False,
        "include_timeseries_end_use_consumptions": True,
        "include_timeseries_emissions": False,
        "include_timeseries_emission_fuels": False,
        "include_timeseries_emission_end_uses": False,
        "include_timeseries_hot_water_uses": False,
        "include_timeseries_total_loads": True,
        "include_timeseries_component_loads": False,
        "include_timeseries_zone_temperatures": False,
        "include_timeseries_airflows": False,
        "include_timeseries_weather": False,
        "timeseries_timestamp_convention": "end",
        "add_timeseries_dst_column": True,
        "add_timeseries_utc_column": True,
        "include_annual_total_consumptions": True,
        "include_annual_fuel_consumptions": True,
        "include_annual_end_use_consumptions": True,
        "include_annual_system_use_consumptions": False,
        "include_annual_emissions": True,
        "include_annual_emission_fuels": True,
        "include_annual_emission_end_uses": True,
        "include_annual_total_loads": True,
        "include_annual_unmet_hours": True,
        "include_annual_peak_fuels": True,
        "include_annual_peak_loads": True,
        "include_annual_component_loads": True,
        "include_annual_hot_water_uses": True,
        "include_annual_hvac_summary": True,
        "include_annual_resilience": True,
        "include_timeseries_system_use_consumptions": False,
        "include_timeseries_unmet_hours": False,
        "include_timeseries_resilience": False,
        "timeseries_num_decimal_places": 3,
        "user_output_variables": "",
    },
    "ReportUtilityBills": {
        "include_annual_bills": False,
        "include_monthly_bills": False,
        "register_annual_bills": True,
        "register_monthly_bills": False,
    },
    "UpgradeCosts": {"debug": False},
    "ServerDirectoryCleanup": {
        "retain_in_osm": False,
        "retain_in_idf": True,
        "retain_pre_process_idf": False,
        "retain_eplusout_audit": False,
        "retain_eplusout_bnd": False,
        "retain_eplusout_eio": False,
        "retain_eplusout_end": False,
        "retain_eplusout_err": False,
        "retain_eplusout_eso": False,
        "retain_eplusout_mdd": False,
        "retain_eplusout_mtd": False,
        "retain_eplusout_rdd": False,
        "retain_eplusout_shd": False,
        "retain_eplusout_msgpack": False,
        "retain_eplustbl_htm": False,
        "retain_stdout_energyplus": False,
        "retain_stdout_expandobject": False,
        "retain_schedules_csv": True,
        "debug": False,
    },
}
