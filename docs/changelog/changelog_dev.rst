=====================
Development Changelog
=====================

.. changelog::
    :version: development
    :released: It has not been released

    .. change::
        :tags: general, feature
        :pullreq: 101
        :tickets: 101

        This is an example change. Please copy and paste it - for valid tags please refer to ``conf.py`` in the docs
        directory. ``pullreq`` should be set to the appropriate pull request number and ``tickets`` to any related
        github issues. These will be automatically linked in the documentation.


    .. change::
        :tags: WorkflowGenerator, feature
        :pullreq: 480

        This PR adds a new version of the WorkflowGenerator for ResStock and ComStock that passes the buildstock_directory
        argument to BuildExistingModel and ApplyUpgrade measure. This is in support of the change in ResStock
        (and potentially in ComStock) to get rid of the lib folder.


    .. change::
        :tags: WorkflowGenerator, feature
        :pullreq: 483

        This PR adds the necessary copying of project_direcotry in HPC so that BSB can work in Kestrel.


    .. change::
        :tags: general, feature
        :pullreq: 490

        When running ``buildstock_local``, especially in CI, it is useful to be able to use minimal disk space.
        This PR adds a ``low-disk`` flag to use minimal disk space.


    .. change::
        :tags: postprocessing, feature
        :pullreq: 492


        Added support for publishing annual results in the postprocessing step. When enabled via the ``publish_annual_results``
        configuration option, the system will generate additional processed results in both CSV and Parquet formats.
        For resstock projects, this functionality leverages the ``resstockpostproc`` module's publishing functions.

    .. change::
        :tags: postprocessing, feature
        :pullreq: 499


        Added eplusout_err column in the results_csv to provide visibility into EnergyPlus warnings and errors. Especially useful
        for failed simulations.

    .. change::
        :tags: postprocessing, feature
        :pullreq: 500

        Added support for replacing existing results in s3 for buildstock_local. When --replace_existing is passed to buildstock_local,
        it will replace existing results in s3.

    .. change::
        :tags: postprocessing, feature
        :pullreq: 501

        Introduce a bunch of changes originating from issues in SDR run.
        1. Intermediate files are not cleaned up after postprocessing. This allows for re-running postprocessing.
        2. eplusout_err and step_failures are truncated to 100000 chars. eplusout_err is only collected if E+ terminated.
        3. Default for include_annual_emission_end_uses is changed to False.
        4. Default for timeseries_num_decimal_places is changed to 5.
        5. When SimulationOutputReport or ReportSimulationOutput is both missing in data_point_out.json (perhaps due to failure),
        add a key of for ReportSimulationOutput instead of SimulationOutputReport as the later is outdated.

    .. change::
        :tags: general, feature
        :pullreq: 517

        Adds ``include_annual_foo`` arguments to the Residential HPXML Workflow Generator.

    .. change::
        :tags: WorkflowGenerator, feature
        :pullreq: 528

        Adds Residential HPXML Workflow Generator version ``2026.10.05`` with
        support for constant electricity emissions factors using ``elec_value``.
        Each scenario must specify exactly one electricity factor source: an hourly
        schedule folder or a constant value. A project can include
        both scenarios that use hourly schedules and scenarios that use constant
        values. The previous
        ``2025.12.03`` generator remains available for existing projects.


    .. change::
        :tags: validation, feature

        Adds project schema version ``0.7``, in which ``baseline`` and ``baseline: n_buildings_represented`` are
        optional. The ``residential_stratified`` sampler requires schema version ``0.7`` or later and rejects
        ``n_buildings_represented``: that sampler's buildings are weighted by allocating the ACS housing unit
        catalogue to them in ``publish_annual_results``, so the catalogue, not this key, sets the size of the
        stock, and the key was being silently ignored. ``n_buildings_represented`` is still required with every
        other sampler, and schema versions ``0.3`` to ``0.6`` are unchanged. The latest residential workflow
        generator omits ``sample_weight`` when the key is absent.
