=======================================
What's new in buildstockbatch 2023.05.0
=======================================

.. admonition:: About this Document

    This document describes changes between buildstockbatch version 2022.1.0 and
    buildstockbatch version 2023.05.0

See :doc:`changelog_2023_05_0` for full details of changes since the last version.

General
=======

This version should be backwards compatible with previous versions of
buildstockbatch. The default version of OpenStudio was changed to 3.6.1, so if
you require an older version, make sure it is called out in your project file.

Docker no longer required for local runs
========================================

Docker is no longer a required dependency for running a local run through
buildstockbatch. As such, the ``buildstock_docker`` cli was replaced with
``buildstock_local``. The appropriate version of OpenStudio will need to be
available. See :ref:`local-install` for details.

Residential HPXML Workflow
==========================

Updates were made to workflow generator arguments, see
:doc:`../workflow_generators/residential_hpxml` for details.

Residential Default Workflow Removed
====================================

The ``residential_default`` workflow generator was removed. To use this version
of buildstockbatch, please update your version of ResStock to on that uses the
``residential_hpxml`` workflow generator.

Schema Updates
==============

``output_directory`` is now required in the schema and will not be defaulted.

``postprocessing.aws.athena.database`` limits the database name to be letters,
numbers, and the underscore (``_`` ) character.

A ``references`` section can now be included in the project yaml file. This can
be used to define other parts of the file that you want to reuse using yaml
anchors and references.

``eagle.minutes_per_sim`` can now be extended up to 8 hours.
