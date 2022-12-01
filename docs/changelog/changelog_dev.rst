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
        :tags: general, bugfix
        :pullreq: 323

        Updates and simplifies python dependencies.

    .. change::
        :tags: general
        :pullreq: 326

        Adds some integration tests with the latest ResStock.

    .. change::
        :tags: validation
        :pullreq: 330
        :tickets: 329

        Adds a validation step to verify the ApplyUpgrade measure has enough
        options specified to support the upgrades in the current project file.
        Instructs the user how to add more options to the ApplyUpgrade measure
        if not.

    .. change::
        :tags: bugfix
        :pullreq: 332
        :tickets: 331

        Sort the results by ``building_id``.
