===============
v0.21 Changelog
===============

.. changelog::
    :version: 0.21
    :released: Feb 8, 2022

    .. change::
        :tags: bugfix
        :pullreq: 232
        :tickets:

        There was a few days there when the version of some sublibrary (click)
        of dask was incompatible with the latest version of dask. We temporarily
        pinned the sublibrary so that new installs would work. They have fixed
        that problem now, so this removes the restriction on that library.

    .. change::
        :tags: bugfix
        :pullreq: 234
        :tickets:

        For ResStock the ``build_existing_model.sample_weight`` was inverse to what we would expect. The bug was
        identified in the residential workflow generator.

    .. change::
        :tags: general, feature
        :pullreq: 240
        :tickets:

        For ResStock the OpenStudio version has changed to v3.2.1. Also, the residential workflow generator has changed
        slightly. Simulation output files retention and deletion can be controlled through arguments to the
        ServerDirectoryCleanup measure.

    .. change::
        :tags: general, feature, eagle
        :pullreq: 246
        :tickets:

        The buildstock.csv is trimmed for each batch job to hold only the rows corresponding to buildings in the batch.
        This improves speed and memory consumption when the file is loaded in ResStock.

    .. change::
        :tags: general, postprocessing
        :pullreq: 247

        The output partition size of 4GB was making downstream data processing
        difficult. Both spark and dask clusters were failing due to out of
        memory errors. I'm changing it back to 1GB, which will make more files,
        but each will be more manageable.

    .. change::
        :tags: general, feature
        :pullreq: 251
        :tickets:

        For ResStock the OpenStudio version has changed to v3.3.0.

    .. change::
        :tags: bugfix
        :pullreq: 258, 262
        :tickets: 253

        Fixes an issue that caused out of memory error when postprocessing large run with many upgrades.

    .. change::
        :tags: bugfix
        :pullreq: 263
        :tickets: 261

        Fixes a bug that caused postprocessing to crash when there is only one datapoint.

    .. change::
        :tags: bugfix
        :pullreq: 266
        :tickets: 265

        Fixes a bug that caused postprocessing to crash on small runs where some jobs have failed sims.
