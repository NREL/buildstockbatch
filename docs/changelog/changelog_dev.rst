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
        :tags: bugfix, feature
        :pullreq: 374
        :tickets: 373

        Add read_csv function to utils to handle parsing "None" correctly with pandas > 2.0+. Also add a validator for
        buildstock_csv that checks if all the entries are available in the options_lookup.tsv.
