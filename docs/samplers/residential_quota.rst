Residential Quota Sampler
-------------------------

The Residential Quota sampler utilizes a `quota-based sampling method <https://en.wikipedia.org/wiki/Quota_sampling>`_ to determine the buildings to simulate. It is the primary sampling algorithm used in ResStock.

Configuration Example
~~~~~~~~~~~~~~~~~~~~~

.. code-block:: yaml

    sampler:
      type: residential_quota
      args:
        n_datapoints: 350000

Arguments
~~~~~~~~~

- ``n_datapoints``: The number of datapoints to sample.
