====================
v2023.01.0 Changelog
====================

.. changelog::
    :version: v2023.03.0
    :released: 2023-03-28

    .. change::
        :tags: comstock, changed, validation, eagle
        :pullreq: 350

        Allows up to 8 hours per simulation in the ``minutes_per_sim`` validator
        under the ``eagle`` section of a configuation YAML. This is required to
        allow long-running ComStock models to be segmented into their own YAML
        to allow for more efficient use of HPC resources.

