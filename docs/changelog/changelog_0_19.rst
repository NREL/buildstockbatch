==============
0.19 Changelog
==============

.. changelog::
    :version: 0.19
    :released: October 26, 2020

    .. change::
        :tags: bugfix, aws
        :pullreq: 188

        The most recent version of s3fs was causing dependency problems in the
        EMR bootstrap script. This pins it to an older, working version.

    .. change::
        :tags: postprocessing, feature
        :pullreq: 186
        :tickets:

        Concatenate the schedules.csv (if they exists) to the enduse_timeseries.csv before converting them to parquet

    .. change::
        :tags: documentation, examples
        :pullreq: 185

        Some changes to the example config file to ``num_buildings_represented``
        and ``downselect`` to more accurately do single family detached housing
        with a downselect.

    .. change::
        :tags: bugfix, eagle
        :pullreq: 181
        :tickets: 158

        Fixing silent errors in simulation job that left an empty output directory.

    .. change::
        :tags: documentation
        :pullreq: 167
        :tickets: 168

        Minor updates to documentation.

    .. change::
        :tags: bugfix, eagle
        :pullreq: 179
        :tickets: 178

        Fixing dependency conflict in installation.

    .. change::
        :tags: schema, change
        :pullreq: 177
        :tickets: 158

        Updated the yml schema to require the ``n_buildings_represented`` in schema version 0.2.

    .. change::
        :tags: aws, bugfix
        :pullreq: 176

        Fixing dependency problem with ruamal.yaml for AWS workflow.

    .. change::
        :tags: aws, documentation
        :pullreq: 174

        Adding AWS demo project yaml file.

    .. change::
        :tags: aws, documentation
        :pullreq: 167

        Updating docs for AWS

    .. change::
        :tags: aws, bugfix
        :pullreq: 163

        Fix bug in AWS postprocessing.

    .. change::
        :tags: bugfix, aws
        :pullreq: 162

        Avoiding bad pandas version on AWS.

    .. change::
        :tags: bugfix
        :pullreq: 161

        Avoiding bad pandas version.

    .. change::
        :tags: documentation
        :pullreq: 160
        :tickets: 157

        Specifying workable Docker version for Windows in documentation.

    .. change::
        :tags: postprocessing, bugfix
        :pullreq: 152
        :tickets: 151

        Throws a more descriptive error in post-processing when no simulation
        results are found.
