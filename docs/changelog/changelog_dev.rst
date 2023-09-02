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
