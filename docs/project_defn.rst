Project Definition
------------------

Most of the project definition happens in a project folder in the
checked out copy of OpenStudio-BuildStock. However, for this library to
work, a separate project YAML file provides the details needed for the
batch run. An example file is in this repo as
``project_resstock_national.yml`` as shown below.

.. include:: ../project_resstock_national.yml
   :code: yaml

The next few paragraphs will describe each section of the file and what it does.

Reference the project
~~~~~~~~~~~~~~~~~~~~~

First we tell it what project we're running with the following keys:

-  ``buildstock_directory``: The absolute (or relative to this YAML
   file) path of the `OpenStudio-BuildStock`_ repository.
-  ``project_directory``: The relative (to the ``buildstock_directory``)
   path of the project.

.. _OpenStudio-BuildStock: https://github.com/NREL/OpenStudio-BuildStock

Weather Files
~~~~~~~~~~~~~

Each batch of simulations depends on a number of weather files. These
are provided in a zip file. This can be done with **one** of the
following keys:

-  ``weather_files_url``: Where the zip file of weather files can be
   downloaded from
-  ``weather_files_path``: Where on this machine to find the zipped
   weather files. This can be absolute or relative (to this file)

Baseline simulations
~~~~~~~~~~~~~~~~~~~~

Information about baseline simulations are listed under tyhe
``baseline`` key.

-  ``n_datapoints``: The number of buildings to sample and run for the
   baseline case.
-  ``n_buildings_represented``: The number of buildings that this sample
   is meant to represent.

Upgrade Scenarios
~~~~~~~~~~~~~~~~~

Under the ``upgrades`` key is a list of upgrades to apply with the
following properties;

-  ``upgrade_name``: The name that will be in the outputs for this
   upgrade scenario.
-  ``options``: A list of options to apply as part of this upgrade.

   -  ``option``: The option to apply, in the format ``parameter|option`` which can be found in 
      `options_lookup.tsv <https://github.com/NREL/OpenStudio-BuildStock/blob/master/resources/options_lookup.tsv>`_
      in `OpenStudio-BuildStock`_.
   -  ``apply_logic``: Logic that defines which buildings to apply the upgrade to. See 
      :ref:`filtering-logic` for instructions.
   - ``costs``: A list of costs for the upgrade. 
     Multiple costs can be entered and each is multiplied by a cost multiplier, described below.

        - ``value``: A cost for the measure, which will be multiplied by the multiplier.
        - ``multiplier``: The cost above is multiplied by this value, which is a function of the buiding.
          Since there can be multiple costs, this permits both fixed and variable costs for upgrades
          that depend on the properties of the baseline building.
          The multiplier needs to be from 
          `this enumeration list in OpenStudio-BuildStock <https://github.com/NREL/OpenStudio-BuildStock/blob/master/measures/ApplyUpgrade/measure.rb#L71-L87>`_
          or from the list in your branch of that repo.
   - ``lifetime``: Lifetime in years of the upgrade.

- ``package_apply_logic``: The conditions under which this package of upgrades should be performed.
  See :ref:`filtering-logic`.

Time Series Export Options
~~~~~~~~~~~~~~~~~~~~~~~~~~

Include the ``timeseries_csv_export`` key to include hourly or subhourly results along with the usual
annual simulation results. These arguments are passed directly to the 
`TimeseriesCSVExport measure <https://github.com/NREL/OpenStudio-BuildStock/blob/master/measures/TimeseriesCSVExport/measure.xml>`_
in OpenStudio-BuildStock. Please refer to the measure arguments there to determine what to set them to in your config file.
Note that this measure and arguments may be different depending on which version of OpenStudio-BuildStock you're using.
The best thing you can do is to verify that it works with what is in your branch.

Output Directory
~~~~~~~~~~~~~~~~

``output_directory`` specifies where the outputs of the simulation should be stored. 

Down Selecting the Sampling Space
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Sometimes it is desirable to run a stock simulation of a subset of what is included in a project. 
For instance one might want to run the simulation only in one climate region or for certain vintages.
However, it can be a considerable effort to create a new project. Adding the ``downselect`` key to
the project file permits a user to specify filters of what buildings should be simulated. 

Downselecting can be performed in one of two ways: with and without resampling.
Downselecting with resampling samples twice, once to determine how much smaller the 
set of sampled buildings becomes when it is filtered down and again with a larger sample so
the final set of sampled buildings is at or near the number specified in ``n_datapoints``.

Downselecting without resampling skips that step. In this case the total sampled buildings returned
will be the number left over after sampling the entire stock and then filtering down to the
buildings that meet the criteria.

The downselect block works as follows:

.. code-block:: yaml

  downselect:
    resample: true
    logic: 
      - Heating Fuel|Natural Gas
      - Location Region|CR02

For details on how to specify the filters, see :ref:`filtering-logic`.

.. _filtering-logic:

Filtering Logic
~~~~~~~~~~~~~~~

There are several places where logic is applied to filter simulations by the option values.
This is done by specifying the parameter|option criteria you want to include or exclude along
with the appropriate logical operator. This is done in the YAML syntax as follows:

And
...

To include certain parameter option combinations, specify them in a list or by using the ``and`` key.

.. code-block:: yaml

  - Vintage|1950s
  - Location Region|CR02

.. code-block:: yaml

  and:
    - Vintage|1950s
    - Location Region|CR02

The above example would include buildings in climate region 2 built in the 1950s.

Or
..

.. code-block:: yaml

  or:
    - Vintage|<1950
    - Vintage|1950s
    - Vintage|1960s

This example would include buildings built before 1970.

Not
...

.. code-block:: yaml

  not: Heating Fuel|Propane

Combining Logic
...............

These constructs can be combined to declare arbitrarily complex logic. Here is an example:

.. code-block:: yaml

  - or:
    - Vintage|<1950
    - Vintage|1950s
    - Vintage|1960s
  - not: Geometry Garage|3 Car
  - not: Geometry House Size|3500+
  - Geometry Stories|1

This will select homes that were built before 1970, don't have three car garages, are less 
than 3500 sq.ft., and have only one storey.
