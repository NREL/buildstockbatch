===================================
What's new in buildstockbatch 0.19?
===================================

.. admonition:: About this Document

    This document describes changes between buildstockbatch version 0.18 and buildstockbatch version 0.19

General
=======

Changes between these versions focused on bugfixes, performance improvements,
and documentation updates. See :doc:`changelog_0_19` for details.

Schema Updates
==============

Only one change was made to the schema, which is to require
``n_buildings_represented``. Previously that field wasn't *required* in the
schema, but the simulation would fail without it. Therefore, no changes should
be required to existing working project files.
