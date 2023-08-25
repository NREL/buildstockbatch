.. |version| replace:: development

=======================================
What's new in buildstockbatch |version|
=======================================

.. admonition:: About this Document

    This document describes changes between buildstockbatch version 2022.01.0 and
    buildstockbatch version |version|

General
=======

This version should be backwards compatible with previous versions of
buildstockbatch. 

See :doc:`changelog_dev` for details of this change.

Schema Updates
==============

The constraint on the ``minutes_per_sim`` key under the
``eagle`` section of the schema was relaxed from two hours to eight hours.
This change is backwards compatible as it makes an existing validation
constraint less restrictive.
