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
        :tags: resstock, workflow generator, deprecated
        :pullreq: 370

        Removing the ``residential_default`` workflow generator and adding a
        validator to eagle.py to ensure the output directory is on a Lustre
        filesystem directory.
