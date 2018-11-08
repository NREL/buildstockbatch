# -*- coding: utf-8 -*-

"""
buildstockbatch.sampler.commercial_sobol
~~~~~~~~~~~~~~~
This object contains the code required for generating the set of simulations to execute

:author: Noel Merket, Ry Horsey
:copyright: (c) 2018 by The Alliance for Sustainable Energy
:license: BSD-3
"""

from copy import deepcopy
from itertools import compress
from joblib import Parallel, delayed
import logging
from multiprocessing import Manager, cpu_count
import os
import pandas as pd
from warnings import warn

from .sobol_lib import i4_sobol_generate
from .base import BuildStockSampler

logger = logging.getLogger(__name__)


class CommercialBaseSobolSampler(BuildStockSampler):

    def __init__(self, *args, **kwargs):
        """
        Initialize the sampler.

        :param output_dir: Directory in which to place buildstock.csv
        :param cfg: YAML configuration specified by the user for the analysis
        :param buildstock_dir: The location of the OpenStudio-BuildStock repo
        :param project_dir: The project directory within the OpenStudio-BuildStock repo
        """
        super().__init__(*args, **kwargs)

    def run_sampling(self, n_datapoints=None):
        """
        Execute the sampling generating the specified number of datapoints.

        This is a stub. It needs to be implemented in the child classes for each deployment environment.

        :param n_datapoints: Number of datapoints to sample from the distributions.
        """
        raise NotImplementedError

    def run_sobol_sampling(self, n_datapoints, csv_path):
        """
        Run the commercial sampling.

        This sampling method executes a sobol sequence to pre-compute optimally space-filling sample locations in the\
        unit hyper-cube defined by the set of TSV files & then spawns processes to evaluate each point in the sample\
        space given the input TSV set.

        :param n_datapoints: Number of datapoints to sample from the distributions.
        :param csv_path: Where to write the output CSV to - this is deployment dependent
        :return: Absolute path to the output buildstock.csv file
        """
        sample_number = self.cfg['baseline']['n_datapoints']
        if isinstance(n_datapoints, int):
            sample_number = n_datapoints
        logging.debug('Sampling, n_datapoints={}'.format(sample_number))

        tsv_hash = {}
        for tsv_file in os.listdir(self.buildstock_dir):
            if '.tsv' in tsv_file:
                tsv_df = pd.read_csv(os.path.join(self.buildstock_dir, tsv_file), sep='\t')
                dependency_columns = [item for item in list(tsv_df) if 'Dependency=' in item]
                tsv_df[dependency_columns] = tsv_df[dependency_columns].astype('str')
                tsv_hash[tsv_file.replace('.tsv', '')] = tsv_df
        dependency_hash, attr_order = self._com_order_tsvs(tsv_hash)
        sample_matrix = self._com_execute_sobol_sampling(attr_order.__len__(), sample_number)
        header = 'Building,'
        for item in attr_order:
            header += str(item) + ','
        header = header[0:-1] + '\n'
        with open(csv_path, 'w') as fd:
            fd.write(header)
        manager = Manager()
        lock = manager.Lock()
        logger.info('Beginning sampling process')
        n_jobs = cpu_count() * 2
        Parallel(n_jobs=n_jobs, verbose=5)(
            delayed(self._com_execute_sample)(tsv_hash, dependency_hash, attr_order, sample_matrix, index, csv_path,
                                              lock)
            for index in range(sample_number)
        )
        return csv_path

    @staticmethod
    def _com_execute_sobol_sampling(n_dims, n_samples):
        """
        Execute a low discrepancy sampling of the unit hyper-cube defined by the n_dims input using the sobol sequence\
        methodology implemented by Corrado Chisari. Please refer to the sobol_lib.py file for license & attribution\
        details.
        :param n_dims: Number of dimensions, equivalent to the number of TSV files to be sampled from
        :param n_samples: Number of samples to calculate
        :return: Pandas DataFrame object which contains the low discrepancy result of the sobol algorithm
        """
        return pd.DataFrame(i4_sobol_generate(n_dims, n_samples, 0)).replace(1.0, 0.999999)

    @staticmethod
    def _com_order_tsvs(tsv_hash):
        """
        This method orders the TSV files to ensure that no TSV is sampled before its dependencies are. It also returns\
        a has of dependencies which are used in subsequent code to down-select TSVs based on previous sample results.
        :param tsv_hash: Dictionary structure containing each TSV file as a Pandas DataFrame
        :return: A dictionary defining each TSVs required inputs, as well as the ordered list of TSV files for sampling
        """
        dependency_hash = {}
        for attr in tsv_hash.keys():
            dependency_hash[attr] = [item.replace('Dependency=', '') for item in list(tsv_hash[attr]) if
                                     'Dependency=' in item]
        attr_order = []
        for attr in dependency_hash.keys():
            if dependency_hash[attr]:
                attr_order.append(attr)
        max_iterations = 5
        while True:
            for attr in dependency_hash.keys():
                if attr in attr_order:
                    continue
                dependencies_met = True
                for dependency in dependency_hash[attr]:
                    if dependency not in attr_order:
                        dependencies_met = False
                if dependencies_met:
                    attr_order.append(attr)
            if dependency_hash.keys().__len__() == attr_order.__len__():
                break
            elif max_iterations > 0:
                max_iterations -= 1
            else:
                raise RuntimeError('Unable to resolve the dependency tree within the set iteration limit')
        return dependency_hash, attr_order

    @staticmethod
    def _com_execute_sample(tsv_hash, dependency_hash, attr_order, sample_matrix, sample_index, csv_path, lock):
        """
        This function evaluates a single point in the sample matrix with the provided TSV files & persists the result\
        of the sample to the CSV file specified. The provided lock ensures the file is not corrupted by multiple\
        instances of this method running in parallel.
        :param tsv_hash: Dictionary structure containing each TSV file as a Pandas DataFrame
        :param dependency_hash: Dictionary defining each TSVs required inputs
        :param attr_order: List defining the order in which to sample TSVs in the tsv_hash
        :param sample_matrix: Pandas DataFrame specifying the points in the sample space to sample
        :param sample_index: Integer specifying which sample in the sample_matrix to evaluate
        :param csv_path: Absolute path of the buildstock.csv file to write to
        :param lock: Cross-pool mutex lock provided by the multiprocessing.Manager class
        """
        sample_vector = list(sample_matrix.loc[:, sample_index])
        sample_dependency_hash = deepcopy(dependency_hash)
        result_vector = []
        for attr_index in range(attr_order.__len__()):
            attr = attr_order[attr_index]
            tsv_lkup = tsv_hash[attr]
            tsv_dist_val = sample_vector[attr_index]
            for dependency in sample_dependency_hash[attr]:
                tsv_lkup = tsv_lkup.loc[tsv_lkup.loc[:, 'Dependency=' + dependency] ==
                                        sample_dependency_hash[dependency]]
                tsv_lkup = tsv_lkup.drop('Dependency=' + dependency, axis=1)
            if tsv_lkup.shape[0] is 0:
                warn('TSV lookup reduced to 0 for {}, index {}, dep hash {}'.format(attr, sample_index,
                                                                                    sample_dependency_hash))
                return
            if tsv_lkup.shape[0] is not 1:
                raise RuntimeError('Unable to reduce tsv for {} to 1 row, index {}'.format(attr, sample_index))
            tsv_lkup_cdf = tsv_lkup.values.cumsum() > tsv_dist_val
            option_values = [item.replace('Option=', '') for item in list(tsv_lkup) if 'Option=' in item]
            attr_result = list(compress(option_values, tsv_lkup_cdf))[0]
            sample_dependency_hash[attr] = attr_result
            result_vector.append(attr_result)
        csv_row = str(sample_index + 1) + ','
        for item in result_vector:
            csv_row += str(item) + ','
        csv_row = csv_row[0:-1] + '\n'
        lock.acquire()
        try:
            with open(csv_path, 'a') as fd:
                fd.write(csv_row)
        finally:
            lock.release()


class CommercialSobolSingularitySampler(CommercialBaseSobolSampler):

    def __init__(self, output_dir, *args, **kwargs):
        """
        This class uses the Commercial Sobol Sampler to execute samples for Peregrine Singularity deployments
        """
        self.output_dir = output_dir
        super().__init__(*args, **kwargs)

    def run_sampling(self, n_datapoints=None):
        """
        Execute the sampling for use in Peregrine Singularity deployments

        :param n_datapoints: Number of datapoints to sample from the distributions.
        :return: Path to the sample CSV file
        """
        csv_path = os.path.join(self.output_dir, 'buildstock.csv')
        return self.run_sobol_sampling(n_datapoints, csv_path)


class CommercialSobolDockerSampler(CommercialBaseSobolSampler):

    def __init__(self, *args, **kwargs):
        """
        This class uses the Commercial Sobol Sampler to execute samples for local Docker deployments
        """
        super().__init__(*args, **kwargs)

    def run_sampling(self, n_datapoints=None):
        """
        Execute the sampling for use in local Docker deployments

        :param n_datapoints: Number of datapoints to sample from the distributions.
        :return: Path to the sample CSV file
        """
        csv_path = os.path.join(self.project_dir, 'housing_characteristics', 'buildstock.csv')
        return self.run_sobol_sampling(n_datapoints, csv_path)

