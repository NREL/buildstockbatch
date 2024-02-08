====================
v2023.01.0 Changelog
====================

.. changelog::
    :version: v2023.01.0
    :released: 2023-01-30

    .. change::
        :tags: general, feature
        :pullreq: 337
        :tickets: 340

        Updates to support the latest OpenStudio-HPXML and OpenStudio 3.5.1.
        Adds ``include_timeseries_unmet_hours`` and
        ``timeseries_num_decimal_places`` arguments to the Residential HPXML
        Workflow Generator.

    .. change::
        :tags: bugfix, validation
        :pulreq: 342
        :tickets: 328

        Checks ``versions`` instead of the dict type. Adds test and rearranges
        ``resstock_required`` condition so it can be used across the testing
        suite.
