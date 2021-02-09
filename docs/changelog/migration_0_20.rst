===================================
What's new in buildstockbatch 0.20?
===================================

.. admonition:: About this Document

    This document describes changes between buildstockbatch version 0.19 and buildstockbatch version 0.20

Introduction
============

This guide introduces what's new in buildstockbatch version 0.20 and also
documents changes which affect users migrating their analyses from 0.19.

Please carefully review the section on schema changes as the minimum schema
version has been updated to the new version, 0.3, and there are
non-backwards-compatible changes in the schema update.

General
=======

Schema Version Update
---------------------

The input schema has been updated to version 0.3. Project configuration files
will need to be upgraded to use the new format. The ``schema_version`` key now
needs to be a string, so put some quotes around the version number.

.. code-block:: yaml

    schema_version: '0.3'

There are no more ``stock_type``, ``sampling_algorithm``, or ``downselect``
keys. This information is defined in the new ``sampler`` and ``workflow`` keys.

Sampler Specification
---------------------

The sampler now has its own section in the configuration file. In it, you
specify which kind of sampler to use and the arguments to pass to it. These
arguments used to be scattered all around the project configuration file and it
caused some confusion when arguments would only apply to some kinds of samplers.
They are now consolidated under the ``sampler`` key and each sampler type
specifies its own options. Complete documentation of the available samplers and
their arguments is at :doc:`../samplers/index`.


Here are a few examples for common scenarios:

Residential Quota Sampling
~~~~~~~~~~~~~~~~~~~~~~~~~~

See :doc:`../samplers/residential_quota`.

Old Spec:

.. code-block:: yaml

    schema_version: 0.2
    stock_type: residential
    baseline:
      sampling_algorithm: quota
      n_datapoints: 350000

New Spec:

.. code-block:: yaml

    schema_version: '0.3'
    sampler:
      type: residential_quota
      args:
        n_datapoints: 350000

Residential Quota with Downselecting
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Downselecting is no longer a separate config item, it is implemented in a
different sampler type. See :doc:`../samplers/residential_quota_downselect`.

Old Spec:

.. code-block:: yaml

    schema_version: 0.2
    stock_type: residential
    baseline:
      sampling_algorithm: quota
      n_datapoints: 350000
    downselect:
      logic:
        - Geometry Building Type RECS|Single-Family Detached
        - Vacancy Status|Occupied
      resample: false

New Spec:

.. code-block:: yaml

    schema_version: '0.3'
    sampler:
      type: residential_quota_downselect
      args:
        n_datapoints: 350000
        logic:
          - Geometry Building Type RECS|Single-Family Detached
          - Vacancy Status|Occupied
        resample: false

Precomputed Sampling
~~~~~~~~~~~~~~~~~~~~

The ``baseline.precomputed_sample`` key has been removed and the precomputed
sampling is its own sampler type. Downselecting is no longer permitted with a
precomputed sample. If you want to downselect, please modify your buildstock.csv
file to only include the buildings you want in your sample. See
:doc:`../samplers/precomputed`.

Old Spec:

.. code-block:: yaml

    schema_version: 0.2
    stock_type: residential
    baseline:
      precomputed_sample: path/to/buildstock.csv
      n_datapoints: 350000

New Spec:

.. code-block:: yaml

    schema_version: '0.3'
    sampler:
      type: precomputed
      args:
        sample_file: path/to/buildstock.csv # n_datapoints determined from csv file

Workflow Generator Specification
--------------------------------

The workflow generator has changed as well. Many inputs to the workflow were
previously at the top level in the project configuration file. This was again
confusing because different config entries only applied to certain workflow
generators. In previous versions, the appropriate workflow generator was
inferred based on the ``stock_type`` key. Now it is explicitly defined and the
arguments to be passed to it are defined under the ``workflow_generator`` key.
See :doc:`../workflow_generators/index`.

Old Spec:

.. code-block:: yaml

    schema_version: 0.2
    stock_type: residential
    timeseries_csv_export:
      reporting_frequency: Hourly
      include_enduse_subcategories: true

New Spec:

.. code-block:: yaml

    schema_version: '0.3'
    workflow_generator:
      type: residential_default
      args:
        timeseries_csv_export:
          reporting_frequency: Hourly
          include_enduse_subcategories: true
