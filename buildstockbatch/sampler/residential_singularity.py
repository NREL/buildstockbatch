# -*- coding: utf-8 -*-

"""
buildstockbatch.sampler.residential_singularity
~~~~~~~~~~~~~~~
This object contains the code required for generating the set of simulations to execute

:author: Noel Merket, Ry Horsey
:copyright: (c) 2018 by The Alliance for Sustainable Energy
:license: BSD-3
"""

import logging
import os
import subprocess

import pandas as pd

from .base import BuildStockSampler

logger = logging.getLogger(__name__)


class ResidentialSingularitySampler(BuildStockSampler):

    def __init__(self, singularity_image, output_dir, *args, **kwargs):
        """
        Initialize the sampler.

        :param singularity_image: path to the singularity image to use
        :param output_dir: Simulation working directory
        :param cfg: YAML configuration specified by the user for the analysis
        :param buildstock_dir: The location of the OpenStudio-BuildStock repo
        :param project_dir: The project directory within the OpenStudio-BuildStock repo
        """
        super().__init__(*args, **kwargs)
        self.singularity_image = singularity_image
        self.output_dir = output_dir
        self.csv_path = os.path.join(self.output_dir, 'housing_characteristics', 'buildstock.csv')
        
        # if there are subproject directories, create run-specific OpenStudio-BuildStock folder
        # - most items there are just links to the items needed
        # - but measures and .tsv files are added in
        # - and the resources/options_lookup.tsv is a concatenation
        if self.subproject_directories:
            self.create_run_buildstock_dir()

    def create_run_buildstock_dir(self):
        # create run-specific OpenStudio-BuildStock directory
        new_buildstock_dir = os.path.join(self.output_dir,'OpenStudio-BuildStock')
        new_resources_dir = os.path.join(new_buildstock_dir, 'resources')
        new_measures_dir = os.path.join(new_resources_dir, 'measures')

        # this object gets made more than once. if the directory already 
        # exists, just point to it and move on.
        if os.path.exists(new_buildstock_dir):
            # point to new buildstock and project directories
            self.buildstock_dir = new_buildstock_dir
            self.project_dir = os.path.join(new_buildstock_dir, os.path.basename(self.project_dir))
            return

        os.mkdir(new_buildstock_dir)
        os.mkdir(new_resources_dir)
        os.mkdir(new_measures_dir)        

        def copy_item(original_item, new_folder):
            # Initially this was "create_softlink" and used ln -s, but that did 
            # not play well with singularity.
            sep = '/' if os.path.isdir(original_item) else ''
            args = [
                'rsync',
                '-avP',
                original_item + sep,
                os.path.join(new_folder,os.path.basename(original_item)) + sep
            ]
            subprocess.run(args, check=True, env=os.environ, cwd=self.output_dir)

        # soft link files to be retained from source OpenStudio-BuildStock and 
        # the project directory
        resources_found = False
        project_found = False
        options_tsv = None
        for item in os.listdir(self.buildstock_dir):
            if item.startswith('.'):
                continue
            item = os.path.join(self.buildstock_dir, item)
            if os.path.isdir(item):
                dirname = os.path.basename(item)
                if dirname == 'resources':
                    resources_found = True
                    for r_item in os.listdir(item):
                        r_item = os.path.join(item, r_item)
                        if os.path.isdir(r_item):
                            assert os.path.basename(r_item) == 'measures'
                            for r_m_item in os.listdir(r_item):
                                r_m_item = os.path.join(r_item, r_m_item)
                                copy_item(r_m_item, new_measures_dir)
                        else:
                            assert os.path.isfile(r_item)
                            if os.path.basename(r_item) == 'options_lookup.tsv':
                                options_tsv = pd.read_csv(r_item, sep='\t')
                            else:
                                copy_item(r_item, new_resources_dir)
                elif dirname.startswith('project'):
                    if dirname == os.path.basename(self.project_dir):
                        project_found = True
                        new_project_dir = os.path.join(new_buildstock_dir, dirname)
                        os.mkdir(new_project_dir)
                        new_tsv_dir = os.path.join(new_project_dir, 'housing_characteristics')
                        os.mkdir(new_tsv_dir)
                        for p_item in os.listdir(item):
                            p_item = os.path.join(item, p_item)
                            if os.path.isdir(p_item):
                                if os.path.basename(p_item) == 'housing_characteristics':
                                    for p_hc_item in os.listdir(p_item):
                                        p_hc_item = os.path.join(p_item, p_hc_item)
                                        copy_item(p_hc_item, new_tsv_dir)
                                    continue
                            copy_item(p_item, new_project_dir)
                    else:
                        # project we don't need -- skip
                        continue
                elif dirname in ['test','docs']:
                    # don't need these folders
                    continue
                else:
                    # soft-link the folder
                    copy_item(item, new_buildstock_dir)
            elif os.path.isfile(item):
                # soft-link file
                copy_item(item, new_buildstock_dir)

        assert resources_found
        assert project_found
        assert isinstance(options_tsv,pd.DataFrame)

        # soft link measures and tsvs from subproject directories, collect options_lookup.tsv files
        for subproject_directory in self.subproject_directories:
            for item in os.listdir(subproject_directory):
                item = os.path.join(subproject_directory, item)
                if os.path.isdir(item):
                    assert os.path.basename(item) == 'measures'
                    for m_item in os.listdir(item):
                        m_item = os.path.join(item, m_item)
                        copy_item(m_item, new_measures_dir)
                else:
                    assert os.path.isfile(item)
                    assert os.path.splitext(item)[1] == '.tsv'
                    if os.path.basename(item) == 'options_lookup.tsv':
                        tmp = pd.read_csv(item, sep='\t')
                        options_tsv = pd.concat([options_tsv, tmp],sort=False)
                    else:
                        copy_item(item, new_tsv_dir)

        # save options_lookup.tsv file
        options_tsv.to_csv(os.path.join(new_resources_dir, 'options_lookup.tsv'), sep='\t', index=False)

        # point to new buildstock and project directories
        self.buildstock_dir = new_buildstock_dir
        self.project_dir = new_project_dir

    def run_sampling(self, n_datapoints):
        """
        Run the residential sampling in a singularity container.

        :param n_datapoints: Number of datapoints to sample from the distributions.
        :return: Absolute path to the output buildstock.csv file
        """
        logging.debug('Sampling, n_datapoints={}'.format(n_datapoints))
        args = [
            'singularity',
            'exec',
            '--contain',
            '--home', '{}:/buildstock'.format(self.buildstock_dir),
            '--bind', '{}:/outbind'.format(os.path.dirname(self.csv_path)),
            self.singularity_image,
            'ruby',
            'resources/run_sampling.rb',
            '-p', self.cfg['project_directory'],
            '-n', str(n_datapoints),
            '-o', '../../outbind/{}'.format(os.path.basename(self.csv_path))
        ]
        subprocess.run(args, check=True, env=os.environ, cwd=self.output_dir)
        return self.csv_path
