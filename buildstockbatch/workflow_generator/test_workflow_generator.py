from buildstockbatch.workflow_generator.base import WorkflowGeneratorBase
from buildstockbatch.workflow_generator.residential import ResidentialDefaultWorkflowGenerator


def test_apply_logic_recursion():

    apply_logic = WorkflowGeneratorBase.make_apply_logic_arg(['one', 'two', 'three'])
    assert(apply_logic == '(one&&two&&three)')

    apply_logic = WorkflowGeneratorBase.make_apply_logic_arg({
        'and': ['one', 'two', 'three']
    })
    assert(apply_logic == '(one&&two&&three)')

    apply_logic = WorkflowGeneratorBase.make_apply_logic_arg({
        'or': ['four', 'five', 'six']
    })
    assert(apply_logic == '(four||five||six)')

    apply_logic = WorkflowGeneratorBase.make_apply_logic_arg({
        'not': 'seven'
    })
    assert(apply_logic == '!seven')

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
    assert(apply_logic == '(!abc&&(def||ghi)&&jkl&&mno)')


def test_residential_package_apply():
    sim_id = 'bldg1up1'
    building_id = 1
    upgrade_idx = 0
    cfg = {
        'baseline': {
            'n_datapoints': 10,
            'n_buildings_represented': 100
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
    osw_gen = ResidentialDefaultWorkflowGenerator(cfg)
    osw = osw_gen.create_osw(sim_id, building_id, upgrade_idx)
    upg_step = osw['steps'][2]
    assert(upg_step['measure_dir_name'] == 'ApplyUpgrade')
    assert(upg_step['arguments']['option_1'] == cfg['upgrades'][0]['options'][0]['option'])
    assert(upg_step['arguments']['package_apply_logic'] == WorkflowGeneratorBase.make_apply_logic_arg(cfg['upgrades'][0]['package_apply_logic']))  # noqa E501


def test_residential_simulation_controls_config():
    sim_id = 'bldb1up1'
    building_id = 1
    upgrade_idx = None
    cfg = {
        'baseline': {
            'n_datapoints': 10,
            'n_buildings_represented': 100
        },
    }
    osw_gen = ResidentialDefaultWorkflowGenerator(cfg)
    osw = osw_gen.create_osw(sim_id, building_id, upgrade_idx)
    step0 = osw['steps'][0]
    assert(step0['measure_dir_name'] == 'ResidentialSimulationControls')
    default_args = {
        'timesteps_per_hr': 6,
        'begin_month': 1,
        'begin_day_of_month': 1,
        'end_month': 12,
        'end_day_of_month': 31
    }
    for k, v in default_args.items():
        assert(step0['arguments'][k] == v)
    cfg['residential_simulation_controls'] = {
        'timesteps_per_hr': 2,
        'begin_month': 7
    }
    osw_gen = ResidentialDefaultWorkflowGenerator(cfg)
    osw = osw_gen.create_osw(sim_id, building_id, upgrade_idx)
    args = osw['steps'][0]['arguments']
    assert(args['timesteps_per_hr'] == 2)
    assert(args['begin_month'] == 7)
    for argname in ('begin_day_of_month', 'end_month', 'end_day_of_month'):
        assert(args[argname] == default_args[argname])


def test_timeseries_csv_export():
    sim_id = 'bldb1up1'
    building_id = 1
    upgrade_idx = None
    cfg = {
        'baseline': {
            'n_datapoints': 10,
            'n_buildings_represented': 100
        },
        'timeseries_csv_export': {
            'reporting_frequency': 'Timestep',
            'output_variables': 'Zone Mean Air Temperature'
        }
    }
    osw_gen = ResidentialDefaultWorkflowGenerator(cfg)
    osw = osw_gen.create_osw(sim_id, building_id, upgrade_idx)
    timeseries_step = osw['steps'][-2]
    assert(timeseries_step['measure_dir_name'] == 'TimeseriesCSVExport')
    default_args = {
        'reporting_frequency': 'Hourly',
        'include_enduse_subcategories': False,
        'output_variables': ''
    }
    osw_gen = ResidentialDefaultWorkflowGenerator(cfg)
    osw = osw_gen.create_osw(sim_id, building_id, upgrade_idx)
    args = osw['steps'][-2]['arguments']
    assert(args['reporting_frequency'] == 'Timestep')
    assert(args['output_variables'] == 'Zone Mean Air Temperature')
    for argname in ('include_enduse_subcategories',):
        assert(args[argname] == default_args[argname])


def test_additional_reporting_measures():
    sim_id = 'bldb1up1'
    building_id = 1
    upgrade_idx = None
    cfg = {
        'baseline': {
            'n_datapoints': 10,
            'n_buildings_represented': 100
        },
        'reporting_measures': [
            'ReportingMeasure1',
            'ReportingMeasure2'
        ]
    }
    osw_gen = ResidentialDefaultWorkflowGenerator(cfg)
    osw = osw_gen.create_osw(sim_id, building_id, upgrade_idx)
    reporting_measure_1_step = osw['steps'][-3]
    assert(reporting_measure_1_step['measure_dir_name'] == 'ReportingMeasure1')
    reporting_measure_2_step = osw['steps'][-2]
    assert(reporting_measure_2_step['measure_dir_name'] == 'ReportingMeasure2')


def test_ignore_measures_argument():
    sim_id = 'bldb1up1'
    building_id = 1
    upgrade_idx = None
    cfg = {
        'baseline': {
            'n_datapoints': 10,
            'n_buildings_represented': 100,
            'measures_to_ignore': [
                'ResidentialApplianceCookingRange',
                'ResidentialApplianceDishwasher',
                'ResidentialApplianceClothesWasher',
                'ResidentialApplianceClothesDryer',
                'ResidentialApplianceRefrigerator']
        }
    }
    osw_gen = ResidentialDefaultWorkflowGenerator(cfg)
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


def test_default_apply_upgrade():
    sim_id = 'bldg1up1'
    building_id = 1
    upgrade_idx = 0
    cfg = {
        'baseline': {
            'n_datapoints': 10,
            'n_buildings_represented': 100
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
    osw_gen = ResidentialDefaultWorkflowGenerator(cfg)
    osw = osw_gen.create_osw(sim_id, building_id, upgrade_idx)
    for step in osw['steps']:
        if step['measure_dir_name'] == 'ApplyUpgrade':
            break
    assert(step['measure_dir_name'] == 'ApplyUpgrade')
    assert(len(step['arguments']) == 2)
    assert(step['arguments']['run_measure'] == 1)
    assert(step['arguments']['option_1'] == 'Parameter|Option')


def test_simulation_output():
    sim_id = 'bldb1up1'
    building_id = 1
    upgrade_idx = None
    cfg = {
        'baseline': {
            'n_datapoints': 10,
            'n_buildings_represented': 100
        },
        'simulation_output': {
            'include_enduse_subcategories': True
        }
    }
    osw_gen = ResidentialDefaultWorkflowGenerator(cfg)
    osw = osw_gen.create_osw(sim_id, building_id, upgrade_idx)
    simulation_output_step = osw['steps'][-2]
    assert(simulation_output_step['measure_dir_name'] == 'SimulationOutputReport')
    default_args = {
        'include_enduse_subcategories': False
    }
    osw_gen = ResidentialDefaultWorkflowGenerator(cfg)
    osw = osw_gen.create_osw(sim_id, building_id, upgrade_idx)
    args = osw['steps'][-2]['arguments']
    for argname in ('include_enduse_subcategories',):
        assert(args[argname] != default_args[argname])
