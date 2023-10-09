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
        :pullreq: 387
        :tickets: 385

        Removing broken postprocessing tests.

    .. change::
        :tags: bugfix
        :pullreq: 386
        :tickets: 256

        No longer automatically downloads the appropriate singularity image from
        S3. Also added validation to ensure the image is in the correct location.

    .. change::
        :tags: general, feature
        :pullreq: 383

        For the Residential HPXML Workflow Generator, fixes new ``include_annual_resilience`` argument to true and
        adds a new optional ``include_timeseries_resilience`` argument that defaults to false. Also fixes new
        ``include_annual_bills`` argument to true and ``include_monthly_bills`` argument to false.
