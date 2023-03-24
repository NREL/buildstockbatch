=======================================
What's new in buildstockbatch 2023.01.0
=======================================

.. admonition:: About this Document

    This document describes changes between buildstockbatch version 2022.01.0 and
    buildstockbatch version 2023.03.0

General
=======

This version should be backwards compatible with previous versions of
buildstockbatch. The constraint on the ``minutes_per_sim`` key under the
``eagle`` section of the schema was relaxed from two hours to eight hours.
This change is backwards compatible as it makes an existing validation
constraint less restrictive.

See :doc:`changelog_2023_03_0` for details of this change.

Schema Updates
==============

No schema updates this time. 
