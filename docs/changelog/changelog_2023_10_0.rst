====================
v2023.10.0 Changelog
====================

.. changelog::
    :version: v2023.10.0
    :released: 2023-10-17

    .. change::
        :tags: general, bugfix
        :pullreq: 387
        :tickets: 385

        Removing broken postprocessing tests.

    .. change::
        :tags: general, bugfix
        :pullreq: 384

        Introduce '*' as a valid option name in options_lookup.tsv to indicate a
        parameter that can take any option and don't need to pass arguments to 
        measures. Enables buildstock.csv validation for ComStock without blowing
        up the size of the options_lookup.tsv file.

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
        :tags: feature
        :pullreq: 396
        :tickets: 377

        Allow fractional ``eagle.minutes_per_sim`` for simulations that run less
        than a minute. Making that it a required input.

    .. change::
        :tags: comstock, workflow
        :pullreq: 399

        Remove default addition of SimulationOutputReport from ComStock workflow generator to avoid multiple instances
        when also included in YML. SimulationOutputReport measure must be included in YML to be added to workflow.

    .. change::
        :tags: eagle, bugfix
        :tickets: 393
        :pullreq: 397

        Updating validation for Eagle output directory to include
        ``/lustre/eaglefs`` directories.

    .. change::
        :tags: eagle, bugfix
        :pullreq: 398
        :tickets: 390

        No longer errors out with a "no space left on device" when using the
        ``weather_files_url`` on Eagle.
