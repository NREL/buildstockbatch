# -*- coding: utf-8 -*-

#   __               ___                __
#  /__)     . / _ / (_  _/_ _   _  /_  /__) _  _/_ _  /_
# /__) /_/ / / (_/ ___) /  (_) (_ /|  /__) (_| /  (_ / /
#

"""
Executing BuildStock projects on batch infrastructure
~~~~~~~~~~~~~~~~~~~~~
BuildStockBatch is a simulation runtime library, written in Python, to allow researchers to execute the very large scale
simulation sets required for BuildStock analyses. Basic Peregrine usage:
```
   [user@loginN ~]$ source ~/buildstockbatch/create_peregrine_env.sh
   [user@loginN ~]$ buildstock_peregrine ~/buildstockbatch/project_resstock_national.yml
```
... or locally using Docker:
```
   user$ pyenv activate buildstockbatch
   user$ pip install -e ./buildstockbatch
   user$ buildstock_docker -j -2 ~/buildstockbatch/project_resstock_multifamily.yml
```
Other batch simulation methods may be supported in future. Please refer to the to-be-written documentation for more
details regarding these features, and configuration via the project yaml configuration documentation.
:copyright: (c) 2018 by The Alliance for Sustainable Energy.
:license: BSD-3, see LICENSE for more details.
"""
