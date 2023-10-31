Project Definition
------------------

Most of the project definition happens in a project folder in the checked out
copy of the ResStock or ComStock repo as well as a project configuration file
that defines how the project will be run. The project file (also known as the
"yaml" file) is the primary input for buildstockbatch. Some examples of project
files are:

- `ResStock National Baseline <https://github.com/NREL/resstock/blob/develop/project_national/national_baseline.yml>`_
- `ResStock National with Upgrades <https://github.com/NREL/resstock/blob/develop/project_national/national_upgrades.yml>`_

Similar project files can be found in the ComStock repo.

The next few paragraphs will describe each section of the project file and what it does.

Reference the project
~~~~~~~~~~~~~~~~~~~~~

First we tell it what project we're running with the following keys:

- ``buildstock_directory``: The absolute (or relative to this YAML file) path of the `ResStock`_ or ComStock
  repository.
- ``project_directory``: The relative (to the ``buildstock_directory``) path of the project.
- ``schema_version``: The version of the project yaml file to use and validate - currently the minimum version is ``0.3``.

.. _ResStock: https://github.com/NREL/resstock

Weather Files
~~~~~~~~~~~~~

Each batch of simulations depends on a number of weather files. These
are provided in a zip file. This can be done with **one** of the
following keys:

- ``weather_files_url``: Where the zip file of weather files can be downloaded from.
- ``weather_files_path``: Where on this machine to find the zipped weather files. This can be absolute or relative
  (to this file).

Weather files for Typical Meteorological Years (TMYs) can be obtained from the `NREL data catalog <https://data.nrel.gov/submissions/128>`_.

Historical weather data for Actual Meteorological Years (AMYs) can be purchased in EPW format from various private companies. NREL users of buildstock batch can use NREL-owned AMY datasets by setting ``weather_files_url`` to a zip file located on `Box <https://app.box.com/s/atyl2q9v74kssjx5n14lbyhs1j6rt8ry>`_.

Custom Weather Files
....................

To use your own custom weather files for a specific location, this can be done in **one** of two ways:

- Rename the filename references in your local `options_lookup.tsv <https://github.com/NREL/resstock/blob/master/resources/options_lookup.tsv>`_ in the ``resources`` folder to match your custom weather file names. For example, in the options_lookup tsv, the Location ``AL_Birmingham.Muni.AP.722280`` is matched to the ``weather_file_name=USA_AL_Birmingham.Muni.AP.722280.epw``. To update the weather file for this location, the `weather_file_name` field needs to be updated to match your new name specified.

- Rename your custom .epw weather file to match the references in your local `options_lookup.tsv <https://github.com/NREL/resstock/blob/master/resources/options_lookup.tsv>`_ in the ``resources`` folder.

References
~~~~~~~~~~
This is a throwaway section where you can define YAML anchors so that these can be used elsewhere in the yaml. Things defined here have no impact in the simulation and is purely used for anchor definitions.

Sampler
~~~~~~~

The ``sampler`` key defines the type of building sampler to be used for the batch of simulations. The purpose of the sampler is to enumerate the buildings and building characteristics to be run as part of the building stock simulation. It has two arguments, ``type`` and ``args``.

- ``type`` tells buildstockbatch which sampler class to use.
- ``args`` are passed to the sampler to define how it is run.

Different samplers are available for ResStock and ComStock and variations thereof. Details of what samplers are available and their arguments are available at :doc:`samplers/index`.

Workflow Generator
~~~~~~~~~~~~~~~~~~

The ``workflow_generator`` key defines which workflow generator will be used to transform a building description from a set of high level characteristics into an OpenStudio workflow that in turn generates the detailed EnergyPlus model. It has two arguments, ``type`` and ``args``.

- ``type`` tells buildstockbatch which workflow generator class to use.
- ``args`` are passed to the sampler to define how it is run.

Different workflow generators are available for ResStock and ComStock and variations thereof. Details of what workflow generators and their arguments are available at :doc:`workflow_generators/index`.

Baseline simulations incl. sampling algorithm
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Information about baseline simulations are listed under the ``baseline`` key.

- ``n_buildings_represented``: The number of buildings that this sample is meant to represent.
- ``skip_sims``: Include this key to control whether the set of baseline simulations are run. The default (i.e., when
  this key is not included) is to run all the baseline simulations. No results csv table with baseline characteristics
  will be provided when the baseline simulations are skipped.
- ``custom_gems``: true or false. **ONLY WORKS ON EAGLE AND LOCAL DOCKER** When true, buildstockbatch will
  call the OpenStudio CLI commands with the  ``bundle`` and ``bundle_path`` options. These options tell the CLI
  to load a custom set of gems rather than those included in the OpenStudio CLI. For both Eagle
  and local Docker runs, these gems are first specified in the ``buildstock\resources\Gemfile``.
  For Eagle, when the Singularity image is built, these gems are added to the image.
  For local Docker, when the containers are started, the gems specified in the Gemfile are installed into a Docker
  volume on the local computer. This volume is mounted by each container as models are run, so each run
  uses the custom gems.

OpenStudio Version Overrides
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

This is a feature only used by ComStock at the moment. Please refer to the ComStock HPC documentation for additional
details on the correct configuration. This is noted here to explain the presence of two keys in the version ``0.2``
schema: ``os_version`` and ``os_sha``.

Upgrade Scenarios
~~~~~~~~~~~~~~~~~

Under the ``upgrades`` key is a list of upgrades to apply with the
following properties:

-  ``upgrade_name``: (required) The name that will be in the outputs for this
   upgrade scenario.
-  ``options``: A list of options to apply as part of this upgrade.

   -  ``option``: (required) The option to apply, in the format ``parameter|option`` which can be found in
      `options_lookup.tsv <https://github.com/NREL/resstock/blob/master/resources/options_lookup.tsv>`_
      in `ResStock`_.
   -  ``apply_logic``: Logic that defines which buildings to apply the upgrade to. See
      :ref:`filtering-logic` for instructions.
   - ``costs``: A list of costs for the upgrade.
     Multiple costs can be entered and each is multiplied by a cost multiplier, described below.

        - ``value``: A cost for the measure, which will be multiplied by the multiplier.
        - ``multiplier``: The cost above is multiplied by this value, which is a function of the building.
          Since there can be multiple costs, this permits both fixed and variable costs for upgrades
          that depend on the properties of the baseline building.
          The multiplier needs to be from
          `this enumeration list in the resstock or comstock repo <https://github.com/NREL/resstock/blob/master/measures/ApplyUpgrade/measure.rb#L71-L87>`_
          or from the list in your branch of that repo.
   - ``lifetime``: Lifetime in years of the upgrade.

- ``package_apply_logic``: (optional) The conditions under which this package of upgrades should be performed.
  See :ref:`filtering-logic`.
- ``reference_scenario``: (optional) The `upgrade_name` which should act as a reference to this upgrade to calculate
  savings. All this does is that reference_scenario show up as a column in results csvs alongside the upgrade name;
  Buildstockbatch will not do the savings calculation.

Output Directory
~~~~~~~~~~~~~~~~

``output_directory``: specifies where the outputs of the simulation should be stored. The last folder in the path will be used as the table name in Athena (if aws configuration is present under postprocessing) so needs to be lowercase, start from letters and contain only letters, numbers and underscore character. `Athena requirement. <https://docs.aws.amazon.com/athena/latest/ug/glue-best-practices.html#schema-names>`_

.. _eagle-config:

Eagle Configuration
~~~~~~~~~~~~~~~~~~~

Under the ``eagle`` key is a list of configuration for running the batch job on
the Eagle supercomputer.

*  ``n_jobs``: Number of eagle jobs to parallelize the simulation into
*  ``minutes_per_sim``: Required. Maximum allocated simulation time in minutes.
*  ``account``: Required. Eagle allocation account to charge the job to.
*  ``sampling``: Configuration for the sampling in eagle

    *  ``time``: Maximum time in minutes to allocate to sampling job

*  ``postprocessing``: Eagle configuration for the postprocessing step

    *  ``time``: Maximum time in minutes to allocate postprocessing job
    *  ``n_workers``: Number of eagle nodes to parallelize the postprocessing
       job into. Max supported is 32. Default is 2.
    *  ``n_procs``: Number of CPUs to use within each eagle nodes. Max is 36.
       Default is 18. Try reducing this if you get OOM error.
    *  ``node_memory_mb``: The memory (in MB) to request for eagle node for
       postprocessing. The valid values are 85248, 180224 and 751616. Default is
       85248.
    *  ``parquet_memory_mb``: The size (in MB) of the combined parquet file in
       memory. Default is 1000.

.. _aws-config:

AWS Configuration
~~~~~~~~~~~~~~~~~

The top-level ``aws`` key is used to specify options for running the batch job
on the `AWS Batch <https://aws.amazon.com/batch/>`_ service.

.. note::

   Many of these options overlap with options specified in the
   :ref:`postprocessing` section. The options here take precedence when running
   on AWS. In a future version we will break backwards compatibility in the
   config file and have more consistent options.

*  ``job_identifier``: A unique string that starts with an alphabetical character,
   is up to 10 characters long, and only has letters, numbers or underscore.
   This is used to name all the AWS service objects to be created and
   differentiate it from other jobs.
*  ``s3``: Configuration for project data storage on s3. When running on AWS,
   this overrides the s3 configuration in the :ref:`post-config-opts`.

    *  ``bucket``: The s3 bucket this project will use for simulation output and processed data storage.
    *  ``prefix``: The s3 prefix at which the data will be stored.

*  ``region``: The AWS region in which the batch will be run and data stored.
*  ``use_spot``: true or false. Defaults to false if missing. This tells the project
   to use the `Spot Market <https://aws.amazon.com/ec2/spot/>`_ for data
   simulations, which typically yields about 60-70% cost savings.
*  ``spot_bid_percent``: Percent of on-demand price you're willing to pay for
   your simulations. The batch will wait to run until the price drops below this
   level.
*  ``batch_array_size``: Number of concurrent simulations to run. Max: 10000.
*  ``notifications_email``: Email to notify you of simulation completion.
   You'll receive an email at the beginning where you'll need to accept the
   subscription to receive further notification emails.
*  ``emr``: Optional key to specify options for postprocessing using an EMR cluster. Generally the defaults should work fine.

    * ``manager_instance_type``: The `instance type`_ to use for the EMR master node. Default: ``m5.xlarge``.
    * ``worker_instance_type``: The `instance type`_ to use for the EMR worker nodes. Default: ``r5.4xlarge``.
    * ``worker_instance_count``: The number of worker nodes to use. Same as ``eagle.postprocessing.n_workers``.
      Increase this for a large dataset. Default: 2.
    * ``dask_worker_vcores``: The number of cores for each dask worker. Increase this if your dask workers are running out of memory. Default: 2.
*  ``job_environment``: Specifies the computing requirements for each simulation.

    * ``vcpus``: Number of CPUs needed. default: 1.
    * ``memory``: Amount of RAM memory needed for each simulation in MiB. default 1024. For large multifamily buildings
      this works better if set to 2048.


.. _instance type: https://aws.amazon.com/ec2/instance-types/


.. _gcp-config:

GCP Configuration
~~~~~~~~~~~~~~~~~
The top-level ``gcp`` key is used to specify options for running the batch job
on the `GCP Batch <https://cloud.google.com/batch>`_ service.

.. note::
    When BuildStockBatch is run on GCP, it will only save results to GCP Cloud Storage (using the
    ``gcs`` configuration below); i.e., it currently cannot save to AWS S3 and Athena. Likewise,
    buildstock run locally, on Eagle, or on AWS cannot save to GCP.

*  ``job_identifier``: A unique string that starts with an alphabetical character,
   is up to 10 characters long, and only has letters, numbers or underscore.
   This is used to name all the GCP service objects to be created and
   differentiate it from other jobs.
*  ``project``: The GCP Project ID in which the batch will be run and of the Artifact Registry
   (where Docker images are stored).
*  ``gcs``: Configuration for project data storage on GCP Cloud Storage.

    *  ``bucket``: The Cloud Storage bucket this project will use for simulation output and
       processed data storage.
    *  ``prefix``: The Cloud Storage prefix at which the data will be stored.

*  ``region``: The GCP region in which the batch will be run and of the Artifact Registry.
*  ``use_spot``: true or false. Defaults to false if missing. This tells the project
   to use `Spot VMs <https://cloud.google.com/spot-vms>`_ for data
   simulations, which can reduce costs by up to 91%.
*  ``batch_array_size``: Number of concurrent simulations to run. Max: 10000.
*  ``artifact_registry``: Configuration for Docker image storage in GCP Artifact Registry

    * ``repository``: The name of the GCP Artifact Repository in which Docker images are stored.
      This will be combined with the ``project`` and ``region`` to build the full URL to the
      repository.

.. _postprocessing:

Postprocessing
~~~~~~~~~~~~~~

After a batch of simulation completes, to analyze BuildStock results the
individual simulation results are aggregated in a postprocessing step as
follows:

1. The inputs and annual outputs of each simulation are gathered together into
   one table for each upgrade scenario. In older versions that ran on PAT, this
   was known as the ``results.csv``. This table is now made available in both
   csv and parquet format.
2. Time series results for each simulation are gathered and concatenated into
   fewer larger parquet files that are better suited for querying using big data
   analysis tools.

For ResStock runs with the ResidentialScheduleGenerator, the generated schedules
are horizontally concatenated with the time series files before aggregation,
making sure the schedule values are properly lined up with the timestamps in the
`same way that EnergyPlus handles ScheduleFiles
<https://github.com/NREL/resstock/issues/469#issuecomment-697849076>`_.


Uploading to AWS Athena
.......................

BuildStock results can optionally be uploaded to AWS for further analysis using
Athena. This process requires appropriate access to an AWS account to be
configured on your machine. You will need to set this up wherever you use buildstockbatch.
If you don't have
keys, consult your AWS administrator to get them set up.

* :ref:`Local Docker AWS setup instructions <aws-user-config-local>`
* :ref:`Eagle AWS setup instructions <aws-user-config-eagle>`
* `Detailed instructions from AWS <https://boto3.amazonaws.com/v1/documentation/api/latest/guide/quickstart.html#configuration>`_

.. _post-config-opts:

Postprocessing Configuration Options
....................................

.. warning::

   The ``region_name`` and ``s3`` info here are **ignored** when running ``buildstock_aws``.
   The configuration is defined in :ref:`aws-config`.

The configuration options for postprocessing and AWS upload are:

*  ``postprocessing``: postprocessing configuration

    * ``keep_individual_timeseries``: (optional, bool) For some use cases it is useful to keep
      the timeseries output for each simulation as a separate parquet file.
      Setting this option to ``true`` allows that. Default is ``false``.

    * ``partition_columns``: (optional, list) Allows partitioning the output data based on some columns. The columns
      must match the parameters found in options_lookup.tsv. This allows for efficient athena queries. Only recommended
      for moderate or large sized runs (ndatapoints > 10K)

    * ``aws``: (optional) configuration related to uploading to and managing
      data in amazon web services. For this to work, please `configure aws`_.
      Including this key will cause your datasets to be uploaded to AWS,
      omitting it will cause them not to be uploaded.

        *  ``region_name``: The name of the aws region to use for database creation and other services.
        *  ``s3``: Configurations for data upload to Amazon S3 data storage service.

            * ``bucket``: The s3 bucket into which the postprocessed data is to be uploaded to
            * ``prefix``: S3 prefix at which the data is to be uploaded. The complete path will become: ``s3://bucket/prefix/output_directory_name``

        *  ``athena``: configurations for Amazon Athena database creation. If this section is missing/commented-out, no
           Athena tables are created.

            *  ``glue_service_role``: The data in s3 is catalogued using Amazon Glue data crawler. An IAM role must be
               present for the user that grants rights to Glue crawler to read s3 and create database catalogue. The
               name of that IAM role must be provided here. Default is: "service-role/AWSGlueServiceRole-default".
               For help, consult the `AWS documentation for Glue Service Roles <https://docs.aws.amazon.com/glue/latest/dg/create-an-iam-role.html>`_.
            *  ``database_name``: The name of the Athena database to which the data is to be placed. All tables in the database will be prefixed with the output directory name. Database name must be lowercase, start from letters and contain only letters, numbers and underscore character. `Athena requirement. <https://docs.aws.amazon.com/athena/latest/ug/glue-best-practices.html#schema-names>`_
            *  ``max_crawling_time``: The maximum time in seconds to wait for the glue crawler to catalogue the data
               before aborting it.

.. _configure aws: https://boto3.amazonaws.com/v1/documentation/api/latest/guide/quickstart.html#configuration
