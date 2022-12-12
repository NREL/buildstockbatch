====================
v2022.12.0 Changelog
====================

.. changelog::
    :version: v2022.12.0
    :released: 2022-12-12

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

    .. change::
        :tags: documentation
        :pullreq: 334
        :tickets: 333

        Fixes the indenting and bullets in the documentation.
