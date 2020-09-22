'''
This is just a utility script to regenerate the results_job0.json.gz
from the simulation outputs in simulations_job0.tar.gz so you don't
have to resimulate everything.
'''

from buildstockbatch import postprocessing
from fsspec.implementations.local import LocalFileSystem
import pathlib
import re
import json
import gzip
import tempfile
import tarfile


def main():
    here = pathlib.Path(__file__).resolve().parent
    with tempfile.TemporaryDirectory() as tmpdir:
        simtarpath = here / 'test_results' / 'simulation_output' / 'simulations_job0.tar.gz'
        with tarfile.open(simtarpath, 'r') as simtar:
            simtar.extractall(tmpdir)
        fs = LocalFileSystem()
        sim_dirs = list(map(str, pathlib.Path(tmpdir).glob('up*/bldg*')))
        dpouts = []
        for sim_dir in sim_dirs:
            print(sim_dir)
            upgrade_id, building_id = map(int, re.search(r'up(\d+)/bldg(\d+)', sim_dir).groups())
            dpouts.append(postprocessing.read_simulation_outputs(fs, [], sim_dir, upgrade_id, building_id))
    with gzip.open(here / 'test_results' / 'simulation_output' / 'results_job0.json.gz', 'wt', encoding='utf-8') as f:
        json.dump(dpouts, f, indent=4)


if __name__ == '__main__':
    main()
