================
0.19.1 Changelog
================

.. changelog::
    :version: 0.19.1
    :released: March 22, 2021

    .. change::
        :tags: postprocessing
        :pullreq: 212
        :tickets:

        Use a map of dask delayed function to combine parquets instead of a giant dask df to avoid memory issues.
        Default to 85GB memory nodes in eagle with single process and single thread in each node to avoid memory issues.
