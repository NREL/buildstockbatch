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
        :tags: general, feature
        :pullreq: 387
        :tickets: 385

        Removing broken postprocessing tests.

    .. change::
        :tags: general, bugfix
        :pullreq: 384

        Split the the precomputed sampler into residential and commerical version because only the residential version
        needs the buildstock_csv validation. Also add deprecation warning if 'precomputed' sampler is used instead of
        'residential_precomputed' or 'commercial_precomputed'.

    .. change::
        :tags: bugfix
        :pullreq: 386
        :tickets: 256

        No longer automatically downloads the appropriate singularity image from
        S3. Also added validation to ensure the image is in the correct location.
