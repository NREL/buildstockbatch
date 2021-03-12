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

The above example would include buildings in climate region 2 built in the 1950s. A list, except for that inside an
``or`` block is always interpreted as ``and`` block.

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

This will select buildings that does not have Propane Fuel type.

.. code-block:: yaml

    not:
      - Vintage|1950s
      - Location Region|CR02

This will select buildings that are not both Vintage 1950s **and** in location region CR02. It should be noted that this
**will** select buildings of 1950s vintage provided they aren't in region CR02. It will also select buildings in
location CR02 provided they aren't of vintage 1950s. If only those buildings that are neither of Vintage 1950s nor in
region CR02 needs to be selected, the following logic should be used:

.. code-block:: yaml

      - not: Vintage|1950s
      - not: Location Region|CR02

or,

.. code-block:: yaml

      and:
        - not: Vintage|1950s
        - not: Location Region|CR02

or,

.. code-block:: yaml

    not:
      or:
        - Vintage|1950s
        - Location Region|CR02


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
