==============
0.18 Changelog
==============

.. changelog::
    :version: 0.18
    :released: May 8, 2020

    .. change::
        :tags: schema, changed
        :pullreq: 65
        :tickets: 63, 6

        Updated to schema version 0.2.


    .. change::
        :tags: schema, changed
        :pullreq: 65
        :tickets: 63, 6

        Updated the schema yml schema to require the ``schema_version`` in schema version 0.2. Please refer to
        :ref:`the migration document for 0.18<migration-0-18-schema-label>` for additional details.


    .. change::
        :tags: schema, sampler, changed
        :pullreq: 65
        :tickets: 63, 6

        Updated the schema yml schema to require the ``sampling_algorithm`` key in the ``baseline`` section of the
        configuration yaml to be set. The schema validator will only accept the strings ``precomputed``, ``quota`` (the
        default residential configuration) and ``sobol`` which at present only has been tested for commercial.


    .. change::
        :tags: sampler, schema, changed
        :tickets: 30

        Implemented a :class:`~.sampler.PrecomputedSampler` class which inherits from the
        :class:`~.sampler.BuildStockSampler` class and replaces the previous workaround in
        :class:`~.BuildStockBatchBase`. To activate this sampler set the ``sampling_algorithm`` key in the ``baseline``
        section of the configuration yaml to ``precomputed`` and add in a key ``precomputed_sample`` to the ``baseline``
        section of the configuration yaml specifying the absolute path to the buildstock CSV file to use. The method
        :func:`~.BuildStockBatchBase.run_sampling` no longer contains any special logic and fully delegates the
        sampler classes.


    .. change::
        :tags: comstock, feature
        :tickets: 63
        :pullreq: 65

        ComStock capabilities have been merged into buildstockbatch.


    .. change::
        :tags: comstock, eagle, feature, schema
        :pullreq: 65

        Added option to activate custom gems in the singularity container in eagle-based simulations by setting the
        ``custom_gem`` key in the ``baseline`` section of the configuration yaml to ``True``. This is implemented via
        the ``bundle`` and ``bundle_path`` options in the OpenStudio CLI. See :func:`~.EagleBatch.run_building` for
        additional implementation details.


    .. change::
        :tags: change, schema
        :pullreq: 65

        Added a new validator function ``validate_precomputed_sample`` to allow for enforcing the existence of
        the ``precomputed_sample`` file and ensuring that the file has ``n_datapoints`` number of enteries. The function
        is :func:`~.BuildStockBatchBase.validate_precomputed_sample`.


    .. change::
        :tags: change, schema, sampler
        :pullreq: 65

        Added nor in the schema validator function ``validate_xor_schema_keys`` to allow for enforcing dual existence of
        the ``precomputed_sample`` and ``sampling_algotithm: precomputed`` keys in the configuration sample. The
        function is now called :func:`~.BuildStockBatchBase.validate_xor_nor_schema_keys`.


    .. change::
        :tags: change, schema, sampler
        :pullreq: 65

        Updated the schema validator function ``validate_misc_constraints`` to ensure that resampling will not be
        allowed when using the ``downselect`` functionality if the precomputed sampler is being used.


    .. change::
        :tags: change, schema
        :pullreq: 65

        Added a new validator function ``validate_precomputed_sample`` to allow for enforcing the existence of
        the ``precomputed_sample`` file and ensuring that the file has ``n_datapoints`` number of enteries. The function
        is :func:`~.BuildStockBatchBase.validate_precomputed_sample`.


    .. change::
        :tags: comstock, feature, sampler
        :pullreq: 65

        Added :class:`~.sampler.CommercialSobolSingularitySampler` and :class:`~.sampler.CommercialSobolDockerSampler`.


    .. change::
        :tags: comstock, workflow, feature
        :pullreq: 65

        Added :class:`~.workflow_generator.CommercialDefaultWorkflowGenerator`
