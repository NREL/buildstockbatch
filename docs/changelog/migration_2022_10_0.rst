=======================================
What's new in buildstockbatch 2022.10.0
=======================================

.. admonition:: About this Document

    This document describes changes between buildstockbatch version 0.21 and
    buildstockbatch version 2022.10.0

General
=======

Buildstockbatch is changing versioning schemes to use a year.month.patch format.
It has been a while since our last formal release. A lot has changed in both
ResStock and ComStock in that time. This is the first release that formally
supports the new HPXML based workflow for ResStock. It is also the first formal
version that supports ComStock. (Prior to this ComStock was being run from a
separate branch.)

Most users have been using their own branches or environments built from
``develop`` to get the features they need. Hopefully this allows most users to
use a release instead. We intend to make more frequent releases to get features
in users hands without the need to support your own development environment.

See :doc:`changelog_2022_10_0` for details of changes since the last version.

Schema Updates
==============

Most input changes are in the Residential HPXML and Commercial workflow
generators. See :doc:`../workflow_generators/index` for more information.

There are some new, non breaking postprocessing inputs. The number of processors
per worker and parquet memory size on Eagle are now configurable. There is also
a new configuration capability to partition the postprocessed output for more
efficient querying in Athena. See :ref:`eagle-config` and
:ref:`post-config-opts` for details.
