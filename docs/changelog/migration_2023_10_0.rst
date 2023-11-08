.. |version| replace:: v2023.10.0

=======================================
What's new in buildstockbatch |version|
=======================================

.. admonition:: About this Document

    This document describes changes between buildstockbatch version 2022.06.0 and
    buildstockbatch version |version|

General
=======

This version should be backwards compatible with previous versions of
buildstockbatch.

See :doc:`changelog_2023_10_0` for details of this change.

This update primarily addresses bugfixes and issues with the Local and Eagle
implementations. Updates to support Kestrel and AWS will be part of
future versions.

Schema Updates
==============

The constraint on the ``minutes_per_sim`` key under the ``eagle`` section of the
schema was relaxed from two hours to eight hours. This change is backwards
compatible as it makes an existing validation constraint less restrictive. It is
now a required inputs, whereas it used to default to 3.

An optional ``max_minutes_per_sim`` argument was added. If it is present, if any
particular simulation takes longer than the amount of time specified, it will be
terminated.

Residential HPXML Workflor Generator Changes
============================================

Updates were made to workflow generator arguments to add new ``detailed_filepath``
and ``include_timeseries_resilience`` arguments, see
:doc:`../workflow_generators/residential_hpxml` for details.
