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
        :tags: general, bugfix
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
        :pullreq: 382

        For the Residential HPXML Workflow Generator, add a new ``detailed_filepath`` argument
        for pointing to user-specified TSV file of electricity tariff file paths. The TSV file can contain
        utility rates mapped by State, or any other parameter.

    .. change::
        :tags: general, feature
        :pullreq: 383

        For the Residential HPXML Workflow Generator, fixes new ``include_annual_resilience`` argument to true and
        adds a new optional ``include_timeseries_resilience`` argument that defaults to false. Also fixes new
        ``include_annual_bills`` argument to true and ``include_monthly_bills`` argument to false.

    .. change::
        :tags: postprocessing, feature
        :pullreq: 365

        Upload buildstock.csv to S3 during postprocessing

    .. change::
        :tags: eagle, bugfix
        :pullreq: 398
        :tickets: 390

        No longer errors out with a "no space left on device" when using the
        ``weather_files_url`` on Eagle.
