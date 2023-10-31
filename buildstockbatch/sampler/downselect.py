"""
buildstockbatch.sampler.residential_quota_downselect
~~~~~~~~~~~~~~~
This object contains the code required for generating the set of simulations to execute

:author: Noel Merket
:copyright: (c) 2020 by The Alliance for Sustainable Energy
:license: BSD-3
"""
import gzip
import logging
import math
import numpy as np
import os
import shutil

from .base import BuildStockSampler
from buildstockbatch.exc import ValidationError
from buildstockbatch.utils import read_csv

logger = logging.getLogger(__name__)


class DownselectSamplerBase(BuildStockSampler):
    SUB_SAMPLER_CLASS = None

    def __init__(self, parent, n_datapoints, logic, resample=True, **kw):
        """Downselect Sampler

        This sampler performs a downselect of another sampler based on
        building characteristics.

        :param parent: BuildStockBatchBase object
        :type parent: BuildStockBatchBase (or subclass)
        :param n_datapoints: number of datapoints to sample
        :type n_datapoints: int
        :param logic: downselection logic
        :type logic: dict, list, or str
        :param resample: do we resample , defaults to True
        :type resample: bool, optional
        :param **kw: args to pass through to sub sampler
        """
        super().__init__(parent)
        self.validate_args(
            self.parent().project_filename, n_datapoints=n_datapoints, logic=logic, resample=resample, **kw
        )
        self.logic = logic
        self.resample = resample
        self.n_datapoints = n_datapoints
        self.sub_kw = kw
        sampler = self.SUB_SAMPLER_CLASS(self.parent(), n_datapoints=n_datapoints, **kw)
        self.csv_path = sampler.csv_path

    @classmethod
    def validate_args(cls, project_filename, **kw):
        expected_args = set(["logic"])
        extra_kw = {}
        for k, v in kw.items():
            expected_args.discard(k)
            if k == "logic":
                # TODO: do some validation of the logic here.
                pass
            elif k == "resample":
                pass
            else:
                extra_kw[k] = v
        if len(expected_args) > 0:
            raise ValidationError("The following sampler arguments are required: " + ", ".join(expected_args))
        cls.SUB_SAMPLER_CLASS.validate_args(project_filename, **extra_kw)
        return True

    @classmethod
    def downselect_logic(cls, df, logic):
        if isinstance(logic, dict):
            assert len(logic) == 1
            key = list(logic.keys())[0]
            values = logic[key]
            if key == "and":
                retval = cls.downselect_logic(df, values[0])
                for value in values[1:]:
                    retval &= cls.downselect_logic(df, value)
                return retval
            elif key == "or":
                retval = cls.downselect_logic(df, values[0])
                for value in values[1:]:
                    retval |= cls.downselect_logic(df, value)
                return retval
            elif key == "not":
                return ~cls.downselect_logic(df, values)
        elif isinstance(logic, list):
            retval = cls.downselect_logic(df, logic[0])
            for value in logic[1:]:
                retval &= cls.downselect_logic(df, value)
            return retval
        elif isinstance(logic, str):
            key, value = logic.split("|")
            return df[key] == value

    def run_sampling(self):
        if self.resample:
            logger.debug("Performing initial sampling to figure out number of samples for downselect")
            n_samples_init = 350000
            init_sampler = self.SUB_SAMPLER_CLASS(self.parent(), n_datapoints=n_samples_init, **self.sub_kw)
            buildstock_csv_filename = init_sampler.run_sampling()
            df = read_csv(buildstock_csv_filename, index_col=0, dtype=str)
            df_new = df[self.downselect_logic(df, self.logic)]
            downselected_n_samples_init = df_new.shape[0]
            n_samples = math.ceil(self.n_datapoints * n_samples_init / downselected_n_samples_init)
            os.remove(buildstock_csv_filename)
            del init_sampler
        else:
            n_samples = self.n_datapoints
        sampler = self.SUB_SAMPLER_CLASS(self.parent(), n_datapoints=n_samples, **self.sub_kw)
        buildstock_csv_filename = sampler.run_sampling()
        with gzip.open(os.path.splitext(buildstock_csv_filename)[0] + "_orig.csv.gz", "wb") as f_out:
            with open(buildstock_csv_filename, "rb") as f_in:
                shutil.copyfileobj(f_in, f_out)
        df = read_csv(buildstock_csv_filename, index_col=0, dtype="str")
        df_new = df[self.downselect_logic(df, self.logic)]
        if len(df_new.index) == 0:
            raise RuntimeError("There are no buildings left after the down select!")
        if self.resample:
            old_index_name = df_new.index.name
            df_new.index = np.arange(len(df_new)) + 1
            df_new.index.name = old_index_name
        df_new.to_csv(buildstock_csv_filename)
        return buildstock_csv_filename
