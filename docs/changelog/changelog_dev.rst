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
        :tags: bugfix
        :pullreq: 232
        :tickets: 

        There was a few days there when the version of some sublibrary (click)
        of dask was incompatible with the latest version of dask. We temporarily
        pinned the sublibrary so that new installs would work. They have fixed
        that problem now, so this removes the restriction on that library. 
