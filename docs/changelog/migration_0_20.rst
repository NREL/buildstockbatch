===================================
What's new in buildstockbatch 0.20?
===================================

.. admonition:: About this Document

    This document describes changes between buildstockbatch version 0.19 and buildstockbatch version 0.20

Schema Updates
==============

Two schema changes were made, both of which make ``reporting_measures`` more flexible
and consistent between residential and commercial workflows.

First, ``reporting_measures`` changed from a list of measure names
to a list of measures, each of which has a ``measure_dir_name`` and an ``arguments`` map which
contains ``argument_name: argument_value`` pairs.  This allows users to dynamically add reporting
measures with arguments to the workflow.  The following shows an example.

.. code-block:: yaml

  reporting_measures:
    - measure_dir_name: super_duper_report
      arguments:
        some_arg_name: True
        other_arg_name: 100.0

To migrate existing workflows with ``reporting_measures``, change this (0.19):

.. code-block:: yaml

  reporting_measures:
    - ReportingMeasure2

To this (0.20):

.. code-block:: yaml

  reporting_measures:
    - measure_dir_name: ReportingMeasure2

Second, the ``include_qaqc`` field was removed.  The previously described
``reporting_measures`` feature can be used to add qaqc or other measures dynamically as desired.
To migrate, remove this field from the file and manually add the qaqc
measures to the workflow using the syntax shown above.
