====================
v2023.05.0 Changelog
====================

.. changelog::
    :version: v2023.05.0
    :released: 2023-05-24

    .. change::
        :tags: general, feature
        :pullreq: 349
        :tickets: 300

        Remove docker dependency for local runs.

    .. change::
        :tags: general, bugfix
        :pullreq: 355
        :tickets: 352

        Fix an issue with schedules datatype that was causing the crash of postporcessing at the final step.

    .. change::
        :tags: workflow, feature
        :pullreq: 353

        Avoid unnecessarily validating the HPXML file twice after having slightly changed the ``residential_hpxml`` workflow.

    .. change::
        :tags: validation, feature
        :pullreq: 362

        Enforce Athena database name and table name to follow strict alphanumeric only naming convention.

    .. change::
        :tags: validation, feature
        :pullreq: 366

        Add a references section in the yaml schema to allow defining the anchors at a single place.

    .. change::
        :tags: comstock, changed, validation, eagle
        :pullreq: 350

        Allows up to 8 hours per simulation in the ``minutes_per_sim`` validator
        under the ``eagle`` section of a configuation YAML. This is required to
        allow long-running ComStock models to be segmented into their own YAML
        to allow for more efficient use of HPC resources.

    .. change::
        :tags: general, feature
        :pullreq: 363

        For the Residential HPXML Workflow Generator, fixes all ``include_annual_foo`` arguments to true (except
        for ``include_annual_system_use_consumptions`` which is fixed to false). Also fixes
        ``include_timeseries_system_use_consumptions`` to false.

    .. change::
        :tags: resstock, workflow generator, deprecated
        :pullreq: 370

        Removing the ``residential_default`` workflow generator and adding a
        validator to eagle.py to ensure the output directory is on a Lustre
        filesystem directory.

    .. change::

        :tags: resstock, openstudio
        :pullreq: 368

        Updating default OpenStudio version to 3.6.1
