# -*- coding: utf-8 -*-

"""
buildstockbatch.workflow_generator.base
~~~~~~~~~~~~~~~
This object contains the base class for generating OSW files from individual samples

:author: Noel Merket, Ry Horsey
:copyright: (c) 2018 by The Alliance for Sustainable Energy
:license: BSD-3
"""

import logging

logger = logging.getLogger(__name__)


class WorkflowGeneratorBase(object):
    def __init__(self, cfg, n_datapoints):
        self.cfg = cfg
        self.n_datapoints = n_datapoints

    def create_osw(self, sim_id, building_id, upgrade_idx):
        """
        Generate and return the osw as a python dict

        :param sim_id: simulation id, looks like 'bldg0000001up01'
        :param building_id: integer building id to use from the sampled buildstock.csv
        :param upgrade_idx: integer index of the upgrade scenario to apply, None if baseline

        This is a stub. It needs to be defined in the subclass.
        """
        raise NotImplementedError

    @classmethod
    def make_apply_logic_arg(cls, logic):
        """
        Convert the parameter|option logic in the yaml file into the format the apply upgrade measure understands.

        :param logic: dict, list, or string with downselection logic in it
        :returns: str of logic
        """
        if isinstance(logic, dict):
            assert len(logic) == 1
            key = list(logic.keys())[0]
            val = logic[key]
            if key == "and":
                return cls.make_apply_logic_arg(val)
            elif key == "or":
                return "(" + "||".join(map(cls.make_apply_logic_arg, val)) + ")"
            elif key == "not":
                return "!" + cls.make_apply_logic_arg(val)
        elif isinstance(logic, list):
            return "(" + "&&".join(map(cls.make_apply_logic_arg, logic)) + ")"
        elif isinstance(logic, str):
            return logic

    @classmethod
    def validate(cls, cfg):
        """Validate the workflor generator arguments

        Replace this in your subclass.

        :param cfg: project configuration
        :type cfg: dict
        """
        return True

    def reporting_measures(self):
        """Return a list of reporting measures to include in the outputs

        Replace this in your subclass
        """
        return []
