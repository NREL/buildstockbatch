===================================
What's new in buildstockbatch 0.20?
===================================

.. admonition:: About this Document

    This document describes changes between buildstockbatch version 0.20 and buildstockbatch version 0.21

Introduction
============

This guide introduces what's new in buildstockbatch version 0.21 and also
documents changes which affect users migrating their analyses from 0.20.

Please note that schema version 0.4 has been released however it is not required.
The only breaking change in this release is that projects relying on the ``custom_gems``
functionality that alters the openstudio cli commands must now use openstudio versions
in the 3.X series - 2.X series versions of openstudio are no longer supported with use
of the ``custom_gems`` functionality.

General
=======

Schema Version Update
---------------------

The input schema has been updated to version 0.4. Project configuration files
may update to the new format if they chose to. The only addition is a override
that forces the timeseries postprocessing code to execute:

.. code-block:: yaml

    postprocessing:
      do_timeseries: True

There are no breaking changes and the new key (and the postprocessing key as well)
remain optional.

Custom Gems & OX 3.X Support
----------------------------

The optional capabilities that allow for execution of custom gem builds in the
openstudio simulations now has been updated to support openstudio 3.X, and in
the process support for openstudio 2.X builds has been removed. The new series
of builds requires the ``--bundle_without native_ext`` string to be passed in 
to the openstudio cli, however this functionality is not supported (to the best
of Ry's knowledge) in the openstudio 2.X series.

Cheers & Happy Modeling!!!
==========================
