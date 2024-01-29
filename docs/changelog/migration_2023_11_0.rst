.. |version| replace:: v2023.11.0

=======================================
What's new in buildstockbatch |version|
=======================================

.. admonition:: About this Document

    This document describes changes between buildstockbatch version 2022.10.0 and
    buildstockbatch version |version|

General
=======

This version should be backwards compatible with previous versions of
buildstockbatch.

See :doc:`changelog_2023_11_0` for details of this change.

.. _kestrel-availability:

Kestrel Availability
====================

This version adds support for `Kestrel`_. See the sections on :ref:`installation
<kestrel_install>` and :ref:`configuration <kestrel-config>` for details.

.. _Kestrel: https://www.nrel.gov/hpc/kestrel-computing-system.html

Most users will be able to migrate their project files by adding a ``kestrel``
top level key, which can be copied and modified from the ``eagle``
configuration.

.. code-block:: yaml

    kestrel:
      n_jobs: 10
      minutes_per_sim: 0.8
      account: your_account
      sampling:
        time: 30
      postprocessing:
        time: 30
        n_workers: 4

You will want to decrease the ``kestrel.n_jobs`` and
``kestrel.postprocessing.n_workers`` values by a factor of ~2.8 because the
Kestrel compute nodes have 104 cores whereas the Eagle nodes only had 36. As
such you should generally need fewer Kestrel nodes to do the same work in the
same time.

We're using python's venv for environment management instead of conda on
Kestrel. That means that activating the environment will be slightly different:

.. code-block:: bash

    module load python
    source /kfs2/shared-projects/buildstock/envs/buildstock-20XX.XX.X/bin/activate

Calling buildstockbatch uses the ``buildstock_kestrel`` command line interface
is very similar to Eagle. A few of the optional args were renamed in this
version for consistency.

AWS Keys on Kestrel and Eagle
=============================

You no longer need to manage AWS keys on Kestrel or Eagle. A service account has
been created for each and the software knows where to find those keys.


Schema Updates
==============

The ``kestrel`` key was added to the schema. See :ref:`kestrel-availability` for
details.
