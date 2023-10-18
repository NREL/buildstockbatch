"""
buildstockbatch.sampler.residential_quota
~~~~~~~~~~~~~~~
This object contains the code required for generating the set of simulations to execute

:author: Noel Merket, Ry Horsey
:copyright: (c) 2020 by The Alliance for Sustainable Energy
:license: BSD-3
"""
import logging
import os
import pathlib
import shutil
import subprocess
import time
import pathlib

from .base import BuildStockSampler
from .downselect import DownselectSamplerBase
from .residential_sampler import sample_all
from buildstockbatch.exc import ValidationError

logger = logging.getLogger(__name__)


class ResidentialQuotaSampler(BuildStockSampler):

    def __init__(self, parent, n_datapoints):
        """Residential Quota Sampler

        :param parent: BuildStockBatchBase object
        :type parent: BuildStockBatchBase (or subclass)
        :param n_datapoints: number of datapoints to sample
        :type n_datapoints: int
        """
        super().__init__(parent)
        self.validate_args(self.parent().project_filename, n_datapoints=n_datapoints)
        self.n_datapoints = n_datapoints

    @classmethod
    def validate_args(cls, project_filename, **kw):
        expected_args = set(['n_datapoints'])
        for k, v in kw.items():
            expected_args.discard(k)
            if k == 'n_datapoints':
                if not isinstance(v, int):
                    raise ValidationError('n_datapoints needs to be an integer')
                if v <= 0:
                    raise ValidationError('n_datapoints need to be >= 1')
            else:
                raise ValidationError(f'Unknown argument for sampler: {k}')
        if len(expected_args) > 0:
            raise ValidationError('The following sampler arguments are required: ' + ', '.join(expected_args))
        return True

    def _run_sampling(self):
        tick = time.time()
        buildstock_directory = pathlib.Path(self.cfg['buildstock_directory'])
        sample_df = sample_all(buildstock_directory / self.cfg['project_directory'], self.n_datapoints)
        print(self.csv_path)
        sample_df.to_csv(self.csv_path, index=False)
        logger.debug('Sampling took {:.1f} seconds'.format(time.time() - tick))
        return self.csv_path

    def _run_sampling_docker(self):
        return self._run_sampling()

    def _run_sampling_singularity(self):
        return self._run_sampling()

    def _run_sampling_local_openstudio(self):
        subprocess.run(
            [
                self.parent().openstudio_exe(),
                str(pathlib.Path('resources', 'run_sampling.rb')),
                '-p', self.cfg['project_directory'],
                '-n', str(self.n_datapoints),
                '-o', 'buildstock.csv'
            ],
            cwd=self.buildstock_dir,
            check=True
        )
        destination_filename = pathlib.Path(self.csv_path)
        if destination_filename.exists():
            os.remove(destination_filename)
        shutil.move(
            pathlib.Path(self.buildstock_dir, 'resources', 'buildstock.csv'),
            destination_filename
        )
        return destination_filename


class ResidentialQuotaDownselectSampler(DownselectSamplerBase):
    SUB_SAMPLER_CLASS = ResidentialQuotaSampler
