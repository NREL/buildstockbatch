import json
import os
import pathlib
import requests
import shutil
from unittest.mock import patch

from buildstockbatch.eagle import user_cli, EagleBatch


@patch('buildstockbatch.eagle.subprocess')
def test_hpc_run_building(mock_subprocess, monkeypatch, basic_residential_project_file):

    here = pathlib.Path(__file__).resolve().parent
    test_osw_path = here / 'test_results' / 'simulation_output' / 'up00' / 'bldg0000001' / 'in.osw'
    with open(test_osw_path, 'r', encoding='utf-8') as f:
        osw_dict = json.load(f)

    project_filename, results_dir = basic_residential_project_file()
    tmp_path = pathlib.Path(results_dir).parent
    sim_path = tmp_path / 'output' / 'simulation_output' / 'up00' / 'bldg0000001'
    shutil.rmtree(sim_path)
    os.makedirs(sim_path)

    cfg = EagleBatch.get_project_configuration(project_filename)

    with patch.object(EagleBatch, 'weather_dir', None), \
            patch.object(EagleBatch, 'singularity_image', '/path/to/singularity.simg'), \
            patch.object(EagleBatch, 'create_osw', return_value=osw_dict), \
            patch.object(EagleBatch, 'make_sim_dir', return_value=('bldg0000001up00', sim_path)):

        # Normal run
        run_bldg_args = [
            results_dir,
            cfg,
            1,
            None
        ]
        EagleBatch.run_building(*run_bldg_args)
        expected_singularity_args = [
            'singularity',
            'exec',
            '--contain',
            '-e',
            '--pwd',
            '/var/simdata/openstudio',
            '-B', f'{sim_path}:/var/simdata/openstudio',
            '-B', '/tmp/scratch/buildstock/resources:/lib/resources',
            '-B', '/tmp/scratch/housing_characteristics:/lib/housing_characteristics',
            '-B', '/tmp/scratch/buildstock/measures:/measures:ro',
            '-B', '/tmp/scratch/project/seeds:/seeds:ro',
            '-B', '/tmp/scratch/weather:/weather:ro',
            '/tmp/scratch/openstudio.simg',
            'bash', '-x'
        ]
        mock_subprocess.run.assert_called_once()
        assert(mock_subprocess.run.call_args[0][0] == expected_singularity_args)
        called_kw = mock_subprocess.run.call_args[1]
        assert(called_kw.get('check') is True)
        assert('input' in called_kw)
        assert('stdout' in called_kw)
        assert('stderr' in called_kw)
        assert(str(called_kw.get('cwd')) == '/tmp/scratch/output')
        assert(called_kw['input'].decode('utf-8').find(' --measures_only') == -1)

        # Measures only run
        mock_subprocess.reset_mock()
        shutil.rmtree(sim_path)
        os.makedirs(sim_path)
        monkeypatch.setenv('MEASURESONLY', '1')
        EagleBatch.run_building(*run_bldg_args)
        mock_subprocess.run.assert_called_once()
        assert(mock_subprocess.run.call_args[0][0] == expected_singularity_args)
        called_kw = mock_subprocess.run.call_args[1]
        assert(called_kw.get('check') is True)
        assert('input' in called_kw)
        assert('stdout' in called_kw)
        assert('stderr' in called_kw)
        assert(str(called_kw.get('cwd')) == '/tmp/scratch/output')
        assert(called_kw['input'].decode('utf-8').find(' --measures_only') > -1)


def test_singularity_image_download_url():
    url = EagleBatch.singularity_image_url()
    r = requests.head(url)
    assert r.status_code == requests.codes.ok


@patch('buildstockbatch.base.BuildStockBatchBase.validate_measures_and_arguments')
@patch('buildstockbatch.base.BuildStockBatchBase.validate_options_lookup')
@patch('buildstockbatch.eagle.subprocess')
def test_user_cli(mock_subprocess, mock_validate_options, mock_validate_measures, basic_residential_project_file,
                  monkeypatch):
    mock_validate_measures.return_value = True
    mock_validate_options.return_value = True

    project_filename, results_dir = basic_residential_project_file()
    shutil.rmtree(results_dir)
    monkeypatch.setenv('CONDA_PREFIX', 'something')
    argv = [project_filename]
    user_cli(argv)
    mock_subprocess.run.assert_called_once()
    eagle_sh = os.path.abspath(os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'eagle.sh'))
    assert mock_subprocess.run.call_args[0][0][-1] == eagle_sh
    assert '--time=20' in mock_subprocess.run.call_args[0][0]
    assert '--account=testaccount' in mock_subprocess.run.call_args[0][0]
    assert '--nodes=1' in mock_subprocess.run.call_args[0][0]
    assert '--export=PROJECTFILE,MY_CONDA_ENV,MEASURESONLY,SAMPLINGONLY' in mock_subprocess.run.call_args[0][0]
    assert '--output=sampling.out' in mock_subprocess.run.call_args[0][0]
    assert '--qos=high' not in mock_subprocess.run.call_args[0][0]
    assert '0' == mock_subprocess.run.call_args[1]['env']['MEASURESONLY']

    mock_subprocess.reset_mock()
    shutil.rmtree(results_dir)
    argv = ['--hipri', project_filename]
    user_cli(argv)
    mock_subprocess.run.assert_called_once()
    assert '--time=20' in mock_subprocess.run.call_args[0][0]
    assert '--account=testaccount' in mock_subprocess.run.call_args[0][0]
    assert '--nodes=1' in mock_subprocess.run.call_args[0][0]
    assert '--export=PROJECTFILE,MY_CONDA_ENV,MEASURESONLY,SAMPLINGONLY' in mock_subprocess.run.call_args[0][0]
    assert '--output=sampling.out' in mock_subprocess.run.call_args[0][0]
    assert '--qos=high' in mock_subprocess.run.call_args[0][0]
    assert '0' == mock_subprocess.run.call_args[1]['env']['MEASURESONLY']
    assert '0' == mock_subprocess.run.call_args[1]['env']['SAMPLINGONLY']

    mock_subprocess.reset_mock()
    shutil.rmtree(results_dir)
    argv = ['--measures_only', project_filename]
    user_cli(argv)
    mock_subprocess.run.assert_called_once()
    assert '--time=20' in mock_subprocess.run.call_args[0][0]
    assert '--account=testaccount' in mock_subprocess.run.call_args[0][0]
    assert '--nodes=1' in mock_subprocess.run.call_args[0][0]
    assert '--export=PROJECTFILE,MY_CONDA_ENV,MEASURESONLY,SAMPLINGONLY' in mock_subprocess.run.call_args[0][0]
    assert '--output=sampling.out' in mock_subprocess.run.call_args[0][0]
    assert '--qos=high' not in mock_subprocess.run.call_args[0][0]
    assert '1' == mock_subprocess.run.call_args[1]['env']['MEASURESONLY']
    assert '0' == mock_subprocess.run.call_args[1]['env']['SAMPLINGONLY']

    mock_subprocess.reset_mock()
    shutil.rmtree(results_dir)
    argv = ['--samplingonly', project_filename]
    user_cli(argv)
    mock_subprocess.run.assert_called_once()
    assert '--time=20' in mock_subprocess.run.call_args[0][0]
    assert '--account=testaccount' in mock_subprocess.run.call_args[0][0]
    assert '--nodes=1' in mock_subprocess.run.call_args[0][0]
    assert '--export=PROJECTFILE,MY_CONDA_ENV,MEASURESONLY,SAMPLINGONLY' in mock_subprocess.run.call_args[0][0]
    assert '--output=sampling.out' in mock_subprocess.run.call_args[0][0]
    assert '--qos=high' not in mock_subprocess.run.call_args[0][0]
    assert '1' == mock_subprocess.run.call_args[1]['env']['SAMPLINGONLY']
    assert '0' == mock_subprocess.run.call_args[1]['env']['MEASURESONLY']


@patch('buildstockbatch.eagle.subprocess')
def test_qos_high_job_submit(mock_subprocess, basic_residential_project_file, monkeypatch):
    mock_subprocess.run.return_value.stdout = 'Submitted batch job 1\n'
    mock_subprocess.PIPE = None
    project_filename, results_dir = basic_residential_project_file()
    shutil.rmtree(results_dir)
    monkeypatch.setenv('CONDA_PREFIX', 'something')
    monkeypatch.setenv('SLURM_JOB_QOS', 'high')

    with patch.object(EagleBatch, 'weather_dir', None), \
            patch.object(EagleBatch, 'singularity_image', '/path/to/singularity.simg'):
        batch = EagleBatch(project_filename)
        for i in range(1, 11):
            pathlib.Path(results_dir, 'job{:03d}.json'.format(i)).touch()
        with open(os.path.join(results_dir, 'job001.json'), 'w') as f:
            json.dump({'batch': list(range(100))}, f)
        batch.queue_jobs()
        mock_subprocess.run.assert_called_once()
        assert '--qos=high' in mock_subprocess.run.call_args[0][0]

    mock_subprocess.reset_mock()
    mock_subprocess.run.return_value.stdout = 'Submitted batch job 1\n'
    mock_subprocess.PIPE = None

    with patch.object(EagleBatch, 'weather_dir', None), \
            patch.object(EagleBatch, 'singularity_image', '/path/to/singularity.simg'):
        batch = EagleBatch(project_filename)
        batch.queue_post_processing()
        mock_subprocess.run.assert_called_once()
        assert '--qos=high' in mock_subprocess.run.call_args[0][0]
