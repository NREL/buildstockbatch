=====================
Development Changelog
=====================

.. changelog::
    :version: development
    :released: It has not been

    .. change::
        :tags: general, feature
        :pullreq: 101
        :tickets: 101

        This is an example change. Please copy and paste it - for valid tags please refer to ``conf.py`` in the docs
        directory. ``pullreq`` should be set to the appropriate pull request number and ``tickets`` to any related
        github issues. These will be automatically linked in the documentation.

    .. change::
        :tags: general, housekeeping
        :pullreq: 267
        :tickets: 223

        Migrated CI to GitHub Actions.

    .. change::
        :tags: workflow
        :pullreq: 208
        :tickets:

        Add ``residential_hpxml`` workflow generator.

    .. change::
        :tags: bugfix
        :pullreq: 271
        :tickets:

        Postprocessing can correctly handle assortment of upgrades with overlaping set of columns with missing and
        non-missing values.

    .. change::
        :tags: postprocessing, feature
        :pullreq: 275
        :tickets:

        Postprocessing can partition the data before uploading to s3 and Athena. This allows for faster and cheaper
        queries.
        ``n_procs argument`` is added to ``eagle`` spec to allow users to pick number of CPUs in each node. Default: 18
        ``partition_columns`` argument is added to ``postprocessing`` spec to allow the partitioning. Default: []

    .. change::
        :tags: bugfix
        :pullreq: 282
        :tickets:

        Fixes bug that would cause sample weight to be incorrect on the HPXML workflow.

    .. change::
        :tags: general, feature
        :pullreq: 281
        :tickets:

        For ResStock the OpenStudio version has changed to v3.4.0.

    .. change::
        :tags: general, feature
        :pullreq: 295
        :tickets:

        Add basic logic validation that checks for incorrect use of 'and' and 'not' block.
        BSB requires at least python 3.8.

    .. change::
        :tags: general, feature, eagle
        :pullreq: 304

        Added ability to resubmit failed array jobs on Eagle.

    .. change::
        :tags: workflow, feature
        :pullreq: 259

        Add ability to calculate emissions using the ``residential_hpxml`` workflow.

    .. change::
        :tags: workflow, feature
        :pullreq: 303

        Add ability to calculate simple utility bills using the ``residential_hpxml`` workflow.

    .. change::
        :tags: general, feature, eagle
        :pullreq: 306
        :tickets: 305

        Now reruns jobs where the job.out-x is missing entirely.

    .. change::
        :tags: bugfix, eagle
        :pullreq: 291

        Mounts a temp dir into the container to avoid using the RAM disk.
        Especially helpful for large schedules. Fixes `NREL/OpenStudio-HPXML#1070 <https://github.com/NREL/OpenStudio-HPXML/issues/1070>`_.

    .. change::
        :tags: comstock, local
        :pullreq: 238

        Changes the default commercial workflow generator to mimic the residential workflow generator,
        where a new timeseries_csv_export key was added to the workflow schema in order to trigger timeseries postprocessing.
        Changes the CLI commands to work with OpenStudio 3.X when custom_gems=True.
        Enables use of custom gems in local docker runs by installing to local docker volume.
