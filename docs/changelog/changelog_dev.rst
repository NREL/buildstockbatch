=====================
Development Changelog
=====================

.. changelog::
    :version: 0.21
    :released: It has not been

    .. change::
        :tags: comstock, schema, workflow, postprocessing, bug
        :pullreq: 236
        :tickets: 237

        The timeseries postprocessing isn't triggered in ComStock due to misalignment of the workflow generators for
        commercial and residential (see issue #222). To patch this pending a fix an override key ``do_timeseries``
        has been added to the ``postprocessing`` key in project yaml file. If the code fragment
        ``self.cfg.get('postprocessing', {}).get('do_timeseries', False)`` returns ``True`` then the timeseries
        postprocessing functionality will be triggered. This is implemented in the new schema version 0.4.

    .. change::
        :tags: comstock, changed
        :pullreq: 236
        :tickets: 

        Updated the functionality of the ``custom_gems`` openstudio cli command override to include the string
        ``--bundle_without native_ext`` which is nessecary in v3.X openstudio builds. This has something to do
        with pycall in particular I beleive but I'm not really sure why.
