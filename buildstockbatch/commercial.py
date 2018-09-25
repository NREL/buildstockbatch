# -*- coding: utf-8 -*-

"""
buildstockbatch.commercial
~~~~~~~~~~~~~~~
This module contains commercial building specific implementations for use in other classes

:author: Henry R Horsey III
:copyright: (c) 2018 by The Alliance for Sustainable Energy
:license: BSD-3
"""


def com_create_osw(cls, sim_id, cfg, i, upgrade_idx):
    raise NotImplementedError


def com_run_peregrine_sampling(cls, n_datapoints):
    raise NotImplementedError


def com_run_local_sampling(cls, n_datapoints):
    raise NotImplementedError
