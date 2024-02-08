Precomputed Sampler
-------------------

The Precomputed Sampler provides a way to directly provide buildstockbatch a sample of buildings to simulate. This can be useful for a variety of cases, including where you previously ran sampling for ResStock or ComStock and want to rerun the same set of buildings with a different set of upgrades.

This sampler cannot be used with a downselect (i.e. there is no precomputed downselect sampler). To downselect the buildings in a precomputed sample, simply remove the buildings you don't want to run from the sample file (buildstock.csv).

Configuration Example
~~~~~~~~~~~~~~~~~~~~~

.. code-block:: yaml

    sampler:
      type: precomputed
      args:
        sample_file: path/to/buildstock.csv

Arguments
~~~~~~~~~

- ``sample_file``: A csv file containing the building sample--one row per building. The format is that the first column is the building_id, usually starting at one and incrementing from there, and following columns each represent a building characteristic. The characteristic columns expected depend on the workflow generator to be used (ResStock or ComStock).
