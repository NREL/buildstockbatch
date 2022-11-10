====================
v2022.10.1 Changelog
====================

.. changelog::
    :version: v2022.10.1
    :released: 2022-10-31

    .. change::
        :tags: workflow, feature
        :pullreq: 311

        Add ability to use start-of-timestep or end-of-timestep convention for timeseries timestamps using the ``residential_hpxml`` workflow.

    .. change::
        :tags: bugfix, eagle
        :pullreq: 324

        Using new style CLI for dask scheduler and dask workers. Requiring dask >= 2022.10.
