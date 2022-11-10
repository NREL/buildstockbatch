from buildstockbatch.workflow_generator.base import WorkflowGeneratorBase
from buildstockbatch.workflow_generator.residential import ResidentialDefaultWorkflowGenerator
from buildstockbatch.workflow_generator.residential_hpxml import ResidentialHpxmlWorkflowGenerator
from buildstockbatch.workflow_generator.residential_hpxml_hes import ResidentialHpxmlHesWorkflowGenerator
from buildstockbatch.workflow_generator.commercial import CommercialDefaultWorkflowGenerator
import pytest


def test_apply_logic_recursion():

    apply_logic = WorkflowGeneratorBase.make_apply_logic_arg(['one', 'two', 'three'])
    assert apply_logic == '(one&&two&&three)'

    apply_logic = WorkflowGeneratorBase.make_apply_logic_arg({
        'and': ['one', 'two', 'three']
    })
    assert apply_logic == '(one&&two&&three)'

    apply_logic = WorkflowGeneratorBase.make_apply_logic_arg({
        'or': ['four', 'five', 'six']
    })
    assert apply_logic == '(four||five||six)'

    apply_logic = WorkflowGeneratorBase.make_apply_logic_arg({
        'not': 'seven'
    })
    assert apply_logic == '!seven'

    apply_logic = WorkflowGeneratorBase.make_apply_logic_arg({
        'and': [
            {'not': 'abc'},
            {'or': [
                'def',
                'ghi'
            ]},
            'jkl',
            'mno'
        ]
    })
    assert apply_logic == '(!abc&&(def||ghi)&&jkl&&mno)'


def test_residential_package_apply(mocker):
    mocker.patch.object(ResidentialDefaultWorkflowGenerator, 'validate_measures_and_arguments', return_value=True)
    sim_id = 'bldg1up1'
    building_id = 1
    upgrade_idx = 0
    cfg = {
        'baseline': {
            'n_buildings_represented': 100
        },
        'workflow_generator': {
            'type': 'residential_default',
            'args': {}
        },
        'upgrades': [
            {
                'upgrade_name': 'upgrade 1',
                'options': [
                    {
                        'option': 'option name',
                        'lifetime': 10,
                        'apply_logic': [
                            'one',
                            {'not': 'two'}
                        ],
                        'costs': [{
                            'value': 10,
                            'multiplier': 'sq.ft.'
                        }]
                    }
                ],
                'package_apply_logic': ['three', 'four', {'or': ['five', 'six']}]
            }
        ]
    }
    osw_gen = ResidentialDefaultWorkflowGenerator(cfg, 10)
    osw = osw_gen.create_osw(sim_id, building_id, upgrade_idx)
    upg_step = osw['steps'][2]
    assert upg_step['measure_dir_name'] == 'ApplyUpgrade'
    assert upg_step['arguments']['option_1'] == cfg['upgrades'][0]['options'][0]['option']
    assert upg_step['arguments']['package_apply_logic'] == WorkflowGeneratorBase.make_apply_logic_arg(cfg['upgrades'][0]['package_apply_logic'])  # noqa E501


def test_residential_simulation_controls_config(mocker):
    mocker.patch.object(ResidentialDefaultWorkflowGenerator, 'validate_measures_and_arguments', return_value=True)
    sim_id = 'bldb1up1'
    building_id = 1
    upgrade_idx = None
    cfg = {
        'baseline': {
            'n_buildings_represented': 100
        },
        'workflow_generator': {
            'type': 'residential_default',
            'args': {}
        },
    }
    n_datapoints = 10
    osw_gen = ResidentialDefaultWorkflowGenerator(cfg, n_datapoints)
    osw = osw_gen.create_osw(sim_id, building_id, upgrade_idx)
    step0 = osw['steps'][0]
    assert step0['measure_dir_name'] == 'ResidentialSimulationControls'
    default_args = {
        'timesteps_per_hr': 6,
        'begin_month': 1,
        'begin_day_of_month': 1,
        'end_month': 12,
        'end_day_of_month': 31,
        'calendar_year': 2007
    }
    for k, v in default_args.items():
        assert step0['arguments'][k] == v
    cfg['workflow_generator']['args']['residential_simulation_controls'] = {
        'timesteps_per_hr': 2,
        'begin_month': 7
    }
    osw_gen = ResidentialDefaultWorkflowGenerator(cfg, n_datapoints)
    osw = osw_gen.create_osw(sim_id, building_id, upgrade_idx)
    args = osw['steps'][0]['arguments']
    assert args['timesteps_per_hr'] == 2
    assert args['begin_month'] == 7
    for argname in ('begin_day_of_month', 'end_month', 'end_day_of_month', 'calendar_year'):
        assert args[argname] == default_args[argname]


def test_residential_simulation_weight(mocker):
    cfg = {
        'baseline': {
            'n_buildings_represented': 1000
        },
        'workflow_generator': {
            'type': 'residential_default',
            'args': {}
        }
    }
    n_datapoints = 10
    sim_id = 'bldb1up1'
    building_id = 1
    upgrade_idx = None
    osw_gen = ResidentialDefaultWorkflowGenerator(cfg, n_datapoints)
    osw = osw_gen.create_osw(sim_id, building_id, upgrade_idx)
    assert osw['steps'][1]['arguments']['sample_weight'] == pytest.approx(100, abs=1e-6)


def test_timeseries_csv_export(mocker):
    mocker.patch.object(ResidentialDefaultWorkflowGenerator, 'validate_measures_and_arguments', return_value=True)
    sim_id = 'bldb1up1'
    building_id = 1
    upgrade_idx = None
    cfg = {
        'baseline': {
            'n_buildings_represented': 100
        },
        'workflow_generator': {
            'type': 'residential_default',
            'args': {
                'timeseries_csv_export': {
                    'reporting_frequency': 'Timestep',
                    'output_variables': 'Zone Mean Air Temperature'
                }
            }
        }
    }
    n_datapoints = 10
    osw_gen = ResidentialDefaultWorkflowGenerator(cfg, n_datapoints)
    osw = osw_gen.create_osw(sim_id, building_id, upgrade_idx)
    timeseries_step = osw['steps'][-2]
    assert timeseries_step['measure_dir_name'] == 'TimeseriesCSVExport'
    default_args = {
        'reporting_frequency': 'Hourly',
        'include_enduse_subcategories': False,
        'output_variables': ''
    }
    osw_gen = ResidentialDefaultWorkflowGenerator(cfg, n_datapoints)
    osw = osw_gen.create_osw(sim_id, building_id, upgrade_idx)
    args = osw['steps'][-2]['arguments']
    assert args['reporting_frequency'] == 'Timestep'
    assert args['output_variables'] == 'Zone Mean Air Temperature'
    for argname in ('include_enduse_subcategories',):
        assert args[argname] == default_args[argname]


def test_additional_reporting_measures(mocker):
    mocker.patch.object(ResidentialDefaultWorkflowGenerator, 'validate_measures_and_arguments', return_value=True)
    sim_id = 'bldb1up1'
    building_id = 1
    upgrade_idx = None
    cfg = {
        'baseline': {
            'n_buildings_represented': 100
        },
        'workflow_generator': {
            'type': 'residential_default',
            'args': {
                'reporting_measures': [
                    {'measure_dir_name': 'ReportingMeasure1'},
                    {'measure_dir_name': 'ReportingMeasure2', 'arguments': {'arg1': 'asdf', 'arg2': 'jkl'}}
                ]
            }
        }
    }
    ResidentialDefaultWorkflowGenerator.validate(cfg)
    osw_gen = ResidentialDefaultWorkflowGenerator(cfg, 10)
    osw = osw_gen.create_osw(sim_id, building_id, upgrade_idx)
    reporting_measure_1_step = osw['steps'][-3]
    assert reporting_measure_1_step['measure_dir_name'] == 'ReportingMeasure1'
    assert reporting_measure_1_step['arguments'] == {}
    assert reporting_measure_1_step['measure_type'] == 'ReportingMeasure'
    reporting_measure_2_step = osw['steps'][-2]
    assert reporting_measure_2_step['measure_dir_name'] == 'ReportingMeasure2'
    assert reporting_measure_2_step['arguments']['arg1'] == 'asdf'
    assert reporting_measure_2_step['arguments']['arg2'] == 'jkl'
    assert len(reporting_measure_2_step['arguments'])
    assert reporting_measure_2_step['measure_type'] == 'ReportingMeasure'


def test_ignore_measures_argument(mocker):
    mocker.patch.object(ResidentialDefaultWorkflowGenerator, 'validate_measures_and_arguments', return_value=True)
    sim_id = 'bldb1up1'
    building_id = 1
    upgrade_idx = None
    cfg = {
        'baseline': {
            'n_buildings_represented': 100,
        },
        'workflow_generator': {
            'type': 'residential_default',
            'args': {
                'measures_to_ignore': [
                    'ResidentialApplianceCookingRange',
                    'ResidentialApplianceDishwasher',
                    'ResidentialApplianceClothesWasher',
                    'ResidentialApplianceClothesDryer',
                    'ResidentialApplianceRefrigerator'
                ]
            }
        }
    }
    osw_gen = ResidentialDefaultWorkflowGenerator(cfg, 10)
    osw = osw_gen.create_osw(sim_id, building_id, upgrade_idx)
    measure_step = None
    for step in osw['steps']:
        if step['measure_dir_name'] == 'BuildExistingModel':
            measure_step = step
            break
    assert measure_step is not None, osw
    assert 'measures_to_ignore' in measure_step['arguments'], measure_step
    assert measure_step['arguments']['measures_to_ignore'] == 'ResidentialApplianceCookingRange|' + \
        'ResidentialApplianceDishwasher|ResidentialApplianceClothesWasher|' + \
        'ResidentialApplianceClothesDryer|ResidentialApplianceRefrigerator', measure_step


def test_default_apply_upgrade(mocker):
    mocker.patch.object(ResidentialDefaultWorkflowGenerator, 'validate_measures_and_arguments', return_value=True)
    sim_id = 'bldg1up1'
    building_id = 1
    upgrade_idx = 0
    cfg = {
        'baseline': {
            'n_buildings_represented': 100
        },
        'workflow_generator': {
            'type': 'residential_default',
            'args': {}
        },
        'upgrades': [
            {
                'options': [
                    {
                        'option': 'Parameter|Option',
                    }
                ],
            }
        ]
    }
    osw_gen = ResidentialDefaultWorkflowGenerator(cfg, 10)
    osw = osw_gen.create_osw(sim_id, building_id, upgrade_idx)
    for step in osw['steps']:
        if step['measure_dir_name'] == 'ApplyUpgrade':
            break
    assert step['measure_dir_name'] == 'ApplyUpgrade'
    assert len(step['arguments']) == 2
    assert step['arguments']['run_measure'] == 1
    assert step['arguments']['option_1'] == 'Parameter|Option'


def test_simulation_output(mocker):
    mocker.patch.object(ResidentialDefaultWorkflowGenerator, 'validate_measures_and_arguments', return_value=True)
    sim_id = 'bldb1up1'
    building_id = 1
    upgrade_idx = None
    cfg = {
        'baseline': {
            'n_buildings_represented': 100
        },
        'workflow_generator': {
            'type': 'residential_default',
            'args': {
                'simulation_output': {
                    'include_enduse_subcategories': True
                }
            }
        }
    }
    n_datapoints = 10
    osw_gen = ResidentialDefaultWorkflowGenerator(cfg, n_datapoints)
    osw = osw_gen.create_osw(sim_id, building_id, upgrade_idx)
    simulation_output_step = osw['steps'][-2]
    assert simulation_output_step['measure_dir_name'] == 'SimulationOutputReport'
    default_args = {
        'include_enduse_subcategories': False
    }
    osw_gen = ResidentialDefaultWorkflowGenerator(cfg, n_datapoints)
    osw = osw_gen.create_osw(sim_id, building_id, upgrade_idx)
    args = osw['steps'][-2]['arguments']
    for argname in ('include_enduse_subcategories',):
        assert args[argname] != default_args[argname]


def test_residential_hpxml(mocker):
    sim_id = 'bldb1up1'
    building_id = 1
    upgrade_idx = 0
    cfg = {
        'baseline': {
            'n_buildings_represented': 100
        },
        'workflow_generator': {
            'type': 'residential_hpxml',
            'args': {
                'build_existing_model': {
                    'simulation_control_run_period_begin_month': 2,
                    'simulation_control_run_period_begin_day_of_month': 1,
                    'simulation_control_run_period_end_month': 2,
                    'simulation_control_run_period_end_day_of_month': 28,
                    'simulation_control_run_period_calendar_year': 2010,
                },
                'simulation_output_report': {
                    'timeseries_frequency': 'hourly',
                    'include_timeseries_total_consumptions': True,
                    'include_timeseries_end_use_consumptions': True,
                    'include_timeseries_total_loads': True,
                    'include_timeseries_zone_temperatures': False,
                }
            }
        },
        'upgrades': [
            {
                'options': [
                    {
                        'option': 'Parameter|Option',
                    }
                ],
            }
        ]
    }
    n_datapoints = 10
    osw_gen = ResidentialHpxmlWorkflowGenerator(cfg, n_datapoints)
    osw = osw_gen.create_osw(sim_id, building_id, upgrade_idx)

    steps = osw['steps']
    assert len(steps) == 8

    build_existing_model_step = steps[0]
    assert build_existing_model_step['measure_dir_name'] == 'BuildExistingModel'
    assert build_existing_model_step['arguments']['simulation_control_run_period_begin_month'] == 2
    assert build_existing_model_step['arguments']['simulation_control_run_period_begin_day_of_month'] == 1
    assert build_existing_model_step['arguments']['simulation_control_run_period_end_month'] == 2
    assert build_existing_model_step['arguments']['simulation_control_run_period_end_day_of_month'] == 28
    assert build_existing_model_step['arguments']['simulation_control_run_period_calendar_year'] == 2010

    apply_upgrade_step = steps[1]
    assert apply_upgrade_step['measure_dir_name'] == 'ApplyUpgrade'

    hpxml_to_os_step = steps[2]
    assert hpxml_to_os_step['measure_dir_name'] == 'HPXMLtoOpenStudio'

    simulation_output_step = steps[3]
    assert simulation_output_step['measure_dir_name'] == 'ReportSimulationOutput'
    assert simulation_output_step['arguments']['timeseries_frequency'] == 'hourly'
    assert simulation_output_step['arguments']['include_timeseries_total_consumptions'] is True
    assert simulation_output_step['arguments']['include_timeseries_fuel_consumptions'] is False
    assert simulation_output_step['arguments']['include_timeseries_end_use_consumptions'] is True
    assert simulation_output_step['arguments']['include_timeseries_emissions'] is False
    assert simulation_output_step['arguments']['include_timeseries_emission_fuels'] is False
    assert simulation_output_step['arguments']['include_timeseries_emission_end_uses'] is False
    assert simulation_output_step['arguments']['include_timeseries_hot_water_uses'] is False
    assert simulation_output_step['arguments']['include_timeseries_total_loads'] is True
    assert simulation_output_step['arguments']['include_timeseries_component_loads'] is False
    assert simulation_output_step['arguments']['include_timeseries_zone_temperatures'] is False
    assert simulation_output_step['arguments']['include_timeseries_airflows'] is False
    assert simulation_output_step['arguments']['include_timeseries_weather'] is False
    assert simulation_output_step['arguments']['timeseries_timestamp_convention'] == 'end'
    assert simulation_output_step['arguments']['add_timeseries_dst_column'] is True
    assert simulation_output_step['arguments']['add_timeseries_utc_column'] is True

    hpxml_output_step = steps[4]
    assert hpxml_output_step['measure_dir_name'] == 'ReportHPXMLOutput'

    hpxml_output_step = steps[5]
    assert hpxml_output_step['measure_dir_name'] == 'ReportUtilityBills'

    upgrade_costs_step = steps[6]
    assert upgrade_costs_step['measure_dir_name'] == 'UpgradeCosts'

    server_dir_cleanup_step = steps[7]
    assert server_dir_cleanup_step['measure_dir_name'] == 'ServerDirectoryCleanup'


def test_residential_hpxml_hes(mocker):
    sim_id = 'bldb1up1'
    building_id = 1
    upgrade_idx = 0
    cfg = {
        'baseline': {
            'n_buildings_represented': 100
        },
        'workflow_generator': {
            'type': 'residential_hpxml',
            'args': {
                'build_existing_model': {
                    'simulation_control_run_period_begin_month': 2,
                    'simulation_control_run_period_begin_day_of_month': 1,
                    'simulation_control_run_period_end_month': 2,
                    'simulation_control_run_period_end_day_of_month': 28,
                    'simulation_control_run_period_calendar_year': 2010,
                    'os_hescore_directory': '../OpenStudio-HEScore'
                },

                'simulation_output_report': {
                    'timeseries_frequency': 'hourly',
                    'include_timeseries_end_use_consumptions': True,
                    'include_timeseries_total_loads': True,
                    'include_timeseries_zone_temperatures': False,
                }
            }
        },
        'upgrades': [
            {
                'options': [
                    {
                        'option': 'Parameter|Option',
                    }
                ],
            }
        ]
    }
    n_datapoints = 10
    osw_gen = ResidentialHpxmlHesWorkflowGenerator(cfg, n_datapoints)
    osw = osw_gen.create_osw(sim_id, building_id, upgrade_idx)

    steps = osw['steps']
    assert(len(steps) == 6)

    build_existing_model_step = steps[0]
    assert(build_existing_model_step['measure_dir_name'] == 'BuildExistingModel')
    assert(build_existing_model_step['arguments']['simulation_control_run_period_begin_month'] == 2)
    assert(build_existing_model_step['arguments']['simulation_control_run_period_begin_day_of_month'] == 1)
    assert(build_existing_model_step['arguments']['simulation_control_run_period_end_month'] == 2)
    assert(build_existing_model_step['arguments']['simulation_control_run_period_end_day_of_month'] == 28)
    assert(build_existing_model_step['arguments']['simulation_control_run_period_calendar_year'] == 2010)
    assert(build_existing_model_step['arguments']['os_hescore_directory'] == "../../OpenStudio-HEScore")

    apply_upgrade_step = steps[1]
    assert(apply_upgrade_step['measure_dir_name'] == 'ApplyUpgrade')

    simulation_output_step = steps[2]
    assert(simulation_output_step['measure_dir_name'] == 'ReportSimulationOutput')
    assert(simulation_output_step['arguments']['timeseries_frequency'] == 'hourly')
    assert(simulation_output_step['arguments']['include_timeseries_end_use_consumptions'] is True)
    assert(simulation_output_step['arguments']['include_timeseries_total_loads'] is True)
    assert(simulation_output_step['arguments']['include_timeseries_zone_temperatures'] is False)

    hpxml_output_step = steps[3]
    assert(hpxml_output_step['measure_dir_name'] == 'ReportHPXMLOutput')

    upgrade_costs_step = steps[4]
    assert(upgrade_costs_step['measure_dir_name'] == 'UpgradeCosts')

    server_dir_cleanup_step = steps[5]
    assert(server_dir_cleanup_step['measure_dir_name'] == 'ServerDirectoryCleanup')


def test_com_default_workflow_generator_basic(mocker):
    sim_id = 'bldb1up1'
    building_id = 1
    upgrade_idx = None
    cfg = {
        'baseline': {
            'n_buildings_represented': 100
        },
        'workflow_generator': {
            'type': 'commercial_default',
            'args': {
            }
        }
    }
    CommercialDefaultWorkflowGenerator.validate(cfg)
    osw_gen = CommercialDefaultWorkflowGenerator(cfg, 10)
    osw = osw_gen.create_osw(sim_id, building_id, upgrade_idx)

    # Should always get SimulationOutputReport
    reporting_measure_step = osw['steps'][1]
    assert reporting_measure_step['measure_dir_name'] == 'SimulationOutputReport'
    assert reporting_measure_step['arguments'] == {}
    assert reporting_measure_step['measure_type'] == 'ReportingMeasure'
    # Should not get TimeseriesCSVExport if excluded in args
    assert len(osw['steps']) == 2


def test_com_default_workflow_generator_with_timeseries(mocker):
    sim_id = 'bldb1up1'
    building_id = 1
    upgrade_idx = None
    cfg = {
        'baseline': {
            'n_buildings_represented': 100
        },
        'workflow_generator': {
            'type': 'commercial_default',
            'args': {
                'timeseries_csv_export': {
                    'reporting_frequency': 'Hourly',
                    'inc_output_variables': 'true'
                }
            }
        }
    }
    CommercialDefaultWorkflowGenerator.validate(cfg)
    osw_gen = CommercialDefaultWorkflowGenerator(cfg, 10)
    osw = osw_gen.create_osw(sim_id, building_id, upgrade_idx)

    # Should always get SimulationOutputReport
    reporting_measure_step = osw['steps'][1]
    assert reporting_measure_step['measure_dir_name'] == 'SimulationOutputReport'
    assert reporting_measure_step['measure_type'] == 'ReportingMeasure'
    assert reporting_measure_step['arguments'] == {}
    # Should get TimeseriesCSVExport if included in args
    reporting_measure_step = osw['steps'][2]
    assert reporting_measure_step['measure_dir_name'] == 'TimeseriesCSVExport'
    assert reporting_measure_step['measure_type'] == 'ReportingMeasure'
    assert reporting_measure_step['arguments']['reporting_frequency'] == 'Hourly'
    assert reporting_measure_step['arguments']['inc_output_variables'] == 'true'


def test_com_default_workflow_generator_extended(mocker):
    sim_id = 'bldb1up1'
    building_id = 1
    upgrade_idx = None
    cfg = {
        'baseline': {
            'n_buildings_represented': 100
        },
        'workflow_generator': {
            'type': 'commercial_default',
            'args': {
                'reporting_measures': [
                    {'measure_dir_name': 'f8e23017-894d-4bdf-977f-37e3961e6f42', 'arguments': {
                        'building_summary_section': 'true',
                        'annual_overview_section': 'true',
                        'monthly_overview_section': 'true',
                        'utility_bills_rates_section': 'true',
                        'envelope_section_section': 'true',
                        'space_type_breakdown_section': 'true',
                        'space_type_details_section': 'true',
                        'interior_lighting_section': 'true',
                        'plug_loads_section': 'true',
                        'exterior_light_section': 'true',
                        'water_use_section': 'true',
                        'hvac_load_profile': 'true',
                        'zone_condition_section': 'true',
                        'zone_summary_section': 'true',
                        'zone_equipment_detail_section': 'true',
                        'air_loops_detail_section': 'true',
                        'plant_loops_detail_section': 'true',
                        'outdoor_air_section': 'true',
                        'cost_summary_section': 'true',
                        'source_energy_section': 'true',
                        'schedules_overview_section': 'true'
                    }},
                    {'measure_dir_name': 'SimulationOutputReport'},
                    {'measure_dir_name': 'comstock_sensitivity_reports'},
                    {'measure_dir_name': 'qoi_report'},
                    {'measure_dir_name': 'la_100_qaqc', 'arguments': {'run_qaqc': 'true'}},
                    {'measure_dir_name': 'simulation_settings_check', 'arguments': {'run_sim_settings_checks': 'true'}},
                    {'measure_dir_name': 'run_directory_cleanup'},
                ],
                'timeseries_csv_export': {
                    'reporting_frequency': 'Hourly',
                    'inc_output_variables': 'true'
                }
            }
        }
    }

    CommercialDefaultWorkflowGenerator.validate(cfg)
    CommercialDefaultWorkflowGenerator.validate(cfg)
    osw_gen = CommercialDefaultWorkflowGenerator(cfg, 10)
    osw = osw_gen.create_osw(sim_id, building_id, upgrade_idx)

    # Should always get SimulationOutputReport
    reporting_measure_step = osw['steps'][1]
    assert reporting_measure_step['measure_dir_name'] == 'SimulationOutputReport'
    assert reporting_measure_step['measure_type'] == 'ReportingMeasure'
    assert reporting_measure_step['arguments'] == {}
    # Should get TimeseriesCSVExport if included in args
    reporting_measure_step = osw['steps'][2]
    assert reporting_measure_step['measure_dir_name'] == 'TimeseriesCSVExport'
    assert reporting_measure_step['measure_type'] == 'ReportingMeasure'
    assert reporting_measure_step['arguments']['reporting_frequency'] == 'Hourly'
    assert reporting_measure_step['arguments']['inc_output_variables'] == 'true'
    # Should have the openstudio report
    reporting_measure_step = osw['steps'][3]
    assert reporting_measure_step['measure_dir_name'] == 'f8e23017-894d-4bdf-977f-37e3961e6f42'
    assert reporting_measure_step['measure_type'] == 'ReportingMeasure'
    assert reporting_measure_step['arguments']['building_summary_section'] == 'true'
    assert reporting_measure_step['arguments']['schedules_overview_section'] == 'true'
    # Should have 1 workflow measure plus 9 reporting measures
    assert len(osw['steps']) == 10
