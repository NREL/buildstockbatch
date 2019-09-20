import json
import os
import pathlib
import shutil
from unittest.mock import patch

from buildstockbatch.eagle import user_cli, EagleBatch


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
    assert '--export=PROJECTFILE,MY_CONDA_ENV,MEASURESONLY' in mock_subprocess.run.call_args[0][0]
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
    assert '--export=PROJECTFILE,MY_CONDA_ENV,MEASURESONLY' in mock_subprocess.run.call_args[0][0]
    assert '--output=sampling.out' in mock_subprocess.run.call_args[0][0]
    assert '--qos=high' in mock_subprocess.run.call_args[0][0]
    assert '0' == mock_subprocess.run.call_args[1]['env']['MEASURESONLY']

    mock_subprocess.reset_mock()
    shutil.rmtree(results_dir)
    argv = ['--measures_only', project_filename]
    user_cli(argv)
    mock_subprocess.run.assert_called_once()
    assert '--time=20' in mock_subprocess.run.call_args[0][0]
    assert '--account=testaccount' in mock_subprocess.run.call_args[0][0]
    assert '--nodes=1' in mock_subprocess.run.call_args[0][0]
    assert '--export=PROJECTFILE,MY_CONDA_ENV,MEASURESONLY' in mock_subprocess.run.call_args[0][0]
    assert '--output=sampling.out' in mock_subprocess.run.call_args[0][0]
    assert '--qos=high' not in mock_subprocess.run.call_args[0][0]
    assert '1' == mock_subprocess.run.call_args[1]['env']['MEASURESONLY']


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
