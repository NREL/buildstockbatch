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
