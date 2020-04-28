from fsspec.implementations.local import LocalFileSystem
import gzip
import json
import pandas as pd
import pathlib
import re
import tarfile

from buildstockbatch import postprocessing
from buildstockbatch.base import BuildStockBatchBase


def test_report_additional_results_csv_columns(basic_residential_project_file):
    reporting_measures = [
        'ReportingMeasure1',
        'ReportingMeasure2'
    ]
    project_filename, results_dir = basic_residential_project_file({
        'reporting_measures': reporting_measures
    })

    fs = LocalFileSystem()

    results_dir = pathlib.Path(results_dir)
    sim_out_dir = results_dir / 'simulation_output'
    with tarfile.open(sim_out_dir / 'simulations_job0.tar.gz', 'r') as tarf:
        tarf.extractall(sim_out_dir)

    dpouts2 = []
    for filename in sim_out_dir.rglob('data_point_out.json'):
        with filename.open('rt', encoding='utf-8') as f:
            dpout = json.load(f)
        dpout['ReportingMeasure1'] = {'column_1': 1, 'column_2': 2}
        dpout['ReportingMeasure2'] = {'column_3': 3, 'column_4': 4}
        with filename.open('wt', encoding='utf-8') as f:
            json.dump(dpout, f)

        sim_dir = str(filename.parent.parent)
        upgrade_id = int(re.search(r'up(\d+)', sim_dir).group(1))
        building_id = int(re.search(r'bldg(\d+)', sim_dir).group(1))
        dpouts2.append(
            postprocessing.read_simulation_outputs(fs, reporting_measures, sim_dir, upgrade_id, building_id)
        )

    with gzip.open(sim_out_dir / 'results_job0.json.gz', 'wt', encoding='utf-8') as f:
        json.dump(dpouts2, f)

    cfg = BuildStockBatchBase.get_project_configuration(project_filename)

    postprocessing.combine_results(fs, results_dir, cfg, do_timeseries=False)

    for upgrade_id in (0, 1):
        df = pd.read_csv(str(results_dir / 'results_csvs' / f'results_up{upgrade_id:02d}.csv.gz'))
        assert (df['reporting_measure1.column_1'] == 1).all()
        assert (df['reporting_measure1.column_2'] == 2).all()
        assert (df['reporting_measure2.column_3'] == 3).all()
        assert (df['reporting_measure2.column_4'] == 4).all()
