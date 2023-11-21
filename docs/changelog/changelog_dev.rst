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
        :tags: eagle, bugfix
        :pullreq: 406
        :tickets: 404

        Cleans out the ``/tmp/scratch`` folder on Eagle at the end of each array job.

    .. change::
        :tags: documentation
        :pullreq: 410
        :tickets: 408

        Update cost multiplier link in upgrade scenarios documentation.

    .. change::
        :tags: bugfix
        :pullreq: 418
        :tickets: 411

        Fixing ``started_at`` and ``completed_at`` timestamps in parquet files
        to that when read by AWS Glue/Athena they show up as dates rather than
        bigints.
