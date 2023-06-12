====================
v2023.06.0 Changelog
====================

.. changelog::
    :version: v2023.06.0
    :released: 2023-06-12

    .. change::
        :tags: bugfix, feature
        :pullreq: 374
        :tickets: 373

        Add read_csv function to utils to handle parsing "None" correctly with pandas > 2.0+. Also add a validator for
        buildstock_csv that checks if all the entries are available in the options_lookup.tsv.

    .. change::
        :tags: general, feature
        :pullreq: 351

        For the Residential HPXML Workflow Generator, add a new ``simple_filepath`` argument
        for pointing to user-specified CSV file of utility rates. The CSV file can contain utility rates
        mapped by State, or any other parameter.
