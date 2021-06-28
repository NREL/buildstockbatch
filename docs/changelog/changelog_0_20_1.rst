================
0.20.1 Changelog
================

.. changelog::
    :version: 0.20
    :released: June 28, 2021

    .. change::
        :tags: bugfix
        :pullreq: 232
        :tickets: 

        There was a few days there when the version of some sublibrary (click)
        of dask was incompatible with the latest version of dask. We temporarily
        pinned the sublibrary so that new installs would work. They have fixed
        that problem now, so this removes the restriction on that library. 

    .. change::
        :tags: bugfix
        :pullreq: 234
        :tickets:

        For ResStock the ``build_existing_model.sample_weight`` was inverse to what we would expect. The bug was 
        identified in the residential workflow generator.
