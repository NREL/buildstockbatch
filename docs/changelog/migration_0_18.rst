===================================
What's new in buildstockbatch 0.18?
===================================

.. admonition:: About this Document

    This document describes changes between buildstockbatch version 0.17 and buildstockbatch version 0.18

Introduction
============

This guide introduces what's new in buildstockbatch version 0.18 and also documents changes which affect users migrating
their analyses from 0.17.

Please carefully review the section on schema changes as the minimum schema version has been updated to the new version,
0.2, and there are non-backwards-compatible changes in the schema update.

General
=======

Changelog and Migration Documentation
-------------------------------------

We have added in changelogs and migration guides to assist folks updating from one version of buildstockbatch to the
next! Please review these as they will significantly minimize annoyance and churn. All new pull requests will
additionally need to add a changelog entry to the changelog_dev.rst file (a template exists in said file) so that upon
release of the next version we can drag and drop in the relevant updates and create a migration guide.

Schema Version Update
---------------------

.. _migration-0-18-schema-label:

Schema version 0.2 has been release and is required for version 0.18. There are several non-backwards-compatible changes
that have been implemented. Happily these do not require significant changes in configuration yamls! So, what changes
were made?

- The ``schema_version`` key, previously not required, is now required. This is important since we now have more than a
  single schema version to keep track of.
- The ``n_datapoints`` key in the ``baseline`` section, previously not required, is now required.
- The minimum version for the ``schema_version`` key for this release (0.18) has been updated to ``0.2`` which will
  force all project yamls to be updated.
- The precomputed sampling interface has been overhauled and the desired sampler now must be specified. When trying to
  use a precomputed buildstock.csv file use the ``precomputed`` sampler; for the default residential quota sampler use
  ``quota``; for commercial dynamic sampling (please don't do this here btw ~Ry) use ``sobol``.

All users should fall into one of two camps at this point: running the residential quota sampler or specifying the
buildstock.csv file to be executed. Three before and afters are presented:

Using the quota sampler
^^^^^^^^^^^^^^^^^^^^^^^

The relevant *before* section should look something like:

.. sourcecode:: yaml

    baseline:
      n_datapoints: 1234


This needs to be updated to include the ``schema_version`` key, and the ``sampling_algorithm`` key. The new schema would
now look like:

.. sourcecode:: yaml

    baseline:
      sampling_algorithm: quota
      n_datapoints: 1234
      n_buildings_represented: 12340
    schema_version: 0.2

Using a precomputed sample - Residential
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The relevant *before* section should look something like:

.. sourcecode:: yaml

    baseline:
      buildstock_csv: /path/to/buildstock.csv


This needs to be updated to include the ``schema_version`` key, the ``sampling_algorithm`` key, the ``n_datapoints``
key, the ``n_buildings_represented`` key, and ``buildstock_csv`` needs to be changed to ``precomputed_sample``. The new schema would now look like:

.. sourcecode:: yaml

    baseline:
      sampling_algorithm: precomputed
      precomputed_sample: /path/to/buildstock.csv
      n_datapoints: 1234
      n_buildings_represented: 12340
    schema_version: 0.2

Using a precomputed sample - Commercial
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The relevant *before* section should look something like:

.. sourcecode:: yaml

    baseline:
      sampling_algorithm: precomputed
      precomputed_sample: /path/to/buildstock.csv


This needs to be updated to include the ``schema_version`` key and the ``n_datapoints`` key. The new schema would now
look like:

.. sourcecode:: yaml

    baseline:
      sampling_algorithm: precomputed
      precomputed_sample: /path/to/buildstock.csv
      n_datapoints: 1234
    schema_version: 0.2

Samplers - Precomputed and Quota
--------------------------------

Previously the file specified by the ``buildstock_csv`` key was dealt with by the
:func:`~.BuildStockBatchBase.run_sampling` function in the base :class:`~.BuildStockBatchBase` class. This is no longer
the case, but the associated functionality has been retained, now in the :class:`~.sampler.PrecomputedSampler` class
which inherits from the :class:`~.sampler.BuildStockSampler` class. This cleans up some structural inconsistencies and
allows for a more standard interface.

New Features and Improvements
=============================

.. _change_65:

ComStock
--------

Release 0.18 merges in the long-running ComStock support branch, allowing (finally) for ComStock runs to be performed
using the major release packages. At this time ComStock is still in a Beta stage and not nearly as 'fire-and-forget' as
ResStock and is not currently supported AT ALL outside of the ComStock team. Additional documentation is being built
out in the ComStock repo - please review and update as issues and resolutions and common mistakes are determined. As we
continue to uncover issues we will delegate them to either the ComStock repo or the repo on an ad-hoc basis.

No specific actions are required for using release 0.18 with ComStock, except for the need to (as always) correctly
set up your YAML configuration file!
