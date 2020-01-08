import json
import pathlib
import shutil
import requests
from unittest.mock import patch

from buildstockbatch.hpc import HPCBatchBase

def test_singularity_image_download_url(basic_residential_project_file):
    project_filename, results_dir = basic_residential_project_file()
    with patch.object(HPCBatchBase, 'weather_dir', None), \
            patch.object(HPCBatchBase, 'output_dir', results_dir), \
            patch.object(HPCBatchBase, 'singularity_image', '/path/to/singularity.simg') as singularity_image:
        hpc_batch_base = HPCBatchBase(project_filename)
        url = hpc_batch_base.singularity_image_url
        r = requests.head(url)
        assert r.status_code == requests.codes.ok


@patch('buildstockbatch.hpc.subprocess')
def test_hpc_run_building(mock_subprocess, monkeypatch, basic_residential_project_file):

    here = pathlib.Path(__file__).resolve().parent
    test_osw_path = here / 'test_results' / 'simulation_output' / 'up00' / 'bldg0000001' / 'in.osw'
    with open(test_osw_path, 'r', encoding='utf-8') as f:
        osw_dict = json.load(f)

    project_filename, results_dir = basic_residential_project_file()
    tmp_path = pathlib.Path(results_dir).parent
    sim_path = tmp_path / 'output' / 'simulation_output' / 'up00' / 'bldg0000001'
    shutil.rmtree(sim_path)

    cfg = HPCBatchBase.get_project_configuration(project_filename)

    with patch.object(HPCBatchBase, 'weather_dir', None), \
            patch.object(HPCBatchBase, 'singularity_image', '/path/to/singularity.simg') as singularity_image, \
            patch.object(HPCBatchBase, 'create_osw', return_value=osw_dict):

        # Normal run
        run_bldg_args = [
            str(tmp_path / 'project_resstock_national'),
            str(tmp_path / 'openstudio_buildstock'),
            str(tmp_path / 'weather'),
            results_dir,
            singularity_image,
            cfg,
            1,
            None
        ]
        HPCBatchBase.run_building(*run_bldg_args)
        expected_singularity_args = [
            'singularity',
            'exec',
            '--contain',
            '-e',
            '--pwd',
            '/var/simdata/openstudio',
            '-B', f'{sim_path}:/var/simdata/openstudio',
            '-B', f'{tmp_path}/openstudio_buildstock/resources:/lib/resources',
            '-B', f'{tmp_path}/output/housing_characteristics:/lib/housing_characteristics',
            '-B', f'{tmp_path}/openstudio_buildstock/measures:/measures:ro',
            '-B', f'{tmp_path}/project_resstock_national/seeds:/seeds:ro',
            '-B', f'{tmp_path}/weather:/weather:ro',
            singularity_image,
            'bash', '-x'
        ]
        mock_subprocess.run.assert_called_once()
        assert(mock_subprocess.run.call_args[0][0] == expected_singularity_args)
        called_kw = mock_subprocess.run.call_args[1]
        assert(called_kw.get('check') is True)
        assert('input' in called_kw)
        assert('stdout' in called_kw)
        assert('stderr' in called_kw)
        assert(called_kw.get('cwd') == results_dir)
        assert(called_kw['input'].decode('utf-8').find(' --measures_only') == -1)

        # Measures only run
        mock_subprocess.reset_mock()
        shutil.rmtree(sim_path)
        monkeypatch.setenv('MEASURESONLY', '1')
        HPCBatchBase.run_building(*run_bldg_args)
        mock_subprocess.run.assert_called_once()
        assert(mock_subprocess.run.call_args[0][0] == expected_singularity_args)
        called_kw = mock_subprocess.run.call_args[1]
        assert(called_kw.get('check') is True)
        assert('input' in called_kw)
        assert('stdout' in called_kw)
        assert('stderr' in called_kw)
        assert(called_kw.get('cwd') == results_dir)
        assert(called_kw['input'].decode('utf-8').find(' --measures_only') > -1)
