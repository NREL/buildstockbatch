Commercial Default Workflow Generator
-------------------------------------

Configuration Example
~~~~~~~~~~~~~~~~~~~~~

.. code-block:: yaml

    workflow_generator:
      type: commercial_default
      args:
        include_qaqc: true
        measures:
          - measure_dir_name: MyMeasure
            arguments:
              arg1: val1
              arg2: val2
          - measure_dir_name: MyOtherMeasure
            arguments:
              arg3: val3


Arguments
~~~~~~~~~
- ``measures`` (optional): Add these optional measures to the end of your workflow.

    - ``measure_dir_name``: Name of measure directory.
    - ``arguments``: map of key, value arguments to pass to the measure.

- ``include_qaqc``: (optional), when set to ``True`` runs some additional
  measures that check a number of key (and often incorrectly configured) part of
  the simulation inputs as well as providing additional model QAQC data streams
  on the output side. Recommended for test runs but not production analyses.
