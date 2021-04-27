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
