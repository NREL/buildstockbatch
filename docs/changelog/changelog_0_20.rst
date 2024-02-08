===============
v0.20 Changelog
===============

.. changelog::
    :version: 0.20
    :released: May 13, 2021

    .. change::
        :tags: general, feature
        :pullreq: 187
        :tickets: 147

        Changed the project configuration for samplers and workflow generators.
        See migration guide for details.

    .. change::
        :tags: documentation
        :pullreq: 203

        https://github.com/NREL/OpenStudio-BuildStock was renamed to https://github.com/NREL/resstock so references to
        OpenStudio-BuildStock in docs were replaced with references to resstock and/or comstock.

    .. change::
        :tags: general
        :pullreq: 205
        :tickets: 164

        Removing master/slave language from AWS EMR configuration in project schema.

    .. change::
        :tags: bugfix
        :pullreq: 197
        :tickets: 196

        Fixing issue where the postprocessing fails when a building simulation crashes in buildstockbatch.

    .. change::
        :tags: postprocessing
        :pullreq: 212
        :tickets:

        Use a map of dask delayed function to combine parquets instead of a giant dask df to avoid memory issues.
        Default to 85GB memory nodes in eagle with single process and single thread in each node to avoid memory issues.

    .. change::
        :tags: postprocessing
        :pullreq: 202
        :tickets: 159

        The glue crawler was failing when there was a trailing ``/`` character.
        This fixes that as well as checks to make sure files were uploaded
        before running the crawler.

    .. change::
        :tags: bugfix
        :pullreq: 224
        :tickets: 221

        Defaults to the newer datetime encoding in the parquet files now that
        Athena can understand it.

    .. change::
        :tags: workflow
        :pullreq: 219
        :tickets: 189

        Adding measure arguments for reporting measures in the workflow generator.

    .. change::
        :tags: general, eagle
        :pullreq: 226
        :tickets:

        Fix for create_eagle_env.sh not creating environment.

    .. change::
        :tags: postprocessing
        :pullreq: 228
        :tickets: 182

        Moves the ``eagle.postprocessing.keep_intermediate_files`` to
        ``postprocessing.keep_individual_timeseries`` and changes behavior to
        keep only the timeseries parquet files. Also, removes the deprecated
        ``aggregate_timeseries`` key as that aggregation always happens.

    .. change::
        :tags: documentation
        :pullreq: 229
        :tickets: 225

        Modifies docs to specify that the ``eagle.postprocessing.n_workers`` key
        is for how many Eagle nodes are used and indicates the default of 2.

    .. change::
        :tags: postprocessing, bugfix
        :pullreq: 230
        :tickets: 199

        Previously the postprocessing would fail if an upgrade scenario didn't
        have any timeseries simulation output. Now it will skip it and post a
        warning message. This was fixed previously, but now we have tests for
        it.
