=======================================
What's new in buildstockbatch 2023.01.0
=======================================

.. admonition:: About this Document

    This document describes changes between buildstockbatch version 2022.10.1 and
    buildstockbatch version 2023.01.0

General
=======

This version should be backwards compatible with previous versions of
buildstockbatch. The default version of OpenStudio was changed to 3.5.1, so if
you require an older version, make sure it is called out in your project file.

See :doc:`changelog_2023_01_0` for details of changes since the last version.

Residential HPXML Workflow Changes
==================================

Two new optional arguments were added to the Residential HPXML Workflow
Generator. It is recommended to add the ``timeseries_num_decimal_places``
argument and set it to at least ``5`` to reduce cumulative rounding errors in
the timeseries outputs.

Schema Updates
==============

No schema updates this time. 
