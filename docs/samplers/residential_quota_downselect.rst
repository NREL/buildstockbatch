Residential Quota Downselect Sampler
------------------------------------

Sometimes it is desirable to run a stock simulation of a subset of what is
included in a project. For instance one might want to run the simulation only in
one climate region or for certain vintages. However, it can be a considerable
effort to create a new project and modify the housing characteristic
distributions. The Residential Quota Downselect sampler adds a downselection
capability to the :doc:`residential_quota`.

Downselecting can be performed in one of two ways: with and without resampling.
Downselecting with resampling samples twice, once to determine how much smaller
the set of sampled buildings becomes when it is filtered down and again with a
larger sample so the final set of sampled buildings is at or near the number
specified in ``n_datapoints``.

Downselecting without resampling skips that step. In this case the total sampled
buildings returned will be the number left over after sampling the entire stock
and then filtering down to the buildings that meet the criteria.



Configuration Example
~~~~~~~~~~~~~~~~~~~~~

.. code-block:: yaml

    sampler:
      type: residential_quota_downselect
      args:
        n_datapoints: 350000
        logic:
          - Geometry Building Type RECS|Single-Family Detached
          - Vacancy Status|Occupied
        resample: false

Arguments
~~~~~~~~~

- ``n_datapoints``: The number of datapoints to sample.
- ``logic``: The criteria to apply to remove buildings from the simulation. For details on how to specify the filters, see :ref:`filtering-logic`.
- ``resample``: boolean, whether to run the sampling twice with a goal to have the downselected number of datapoints be equal to ``n_datapoints``.
