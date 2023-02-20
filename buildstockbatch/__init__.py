# -*- coding: utf-8 -*-

#   __               ___                __
#  /__)     . / _ / (_  _/_ _   _  /_  /__) _  _/_ _  /_
# /__) /_/ / / (_/ ___) /  (_) (_ /|  /__) (_| /  (_ / /
#

"""
Executing BuildStock projects on batch infrastructure
~~~~~~~~~~~~~~~~~~~~~
BuildStockBatch is a simulation runtime library, written in Python, to allow researchers to execute the very large scale
simulation sets required for BuildStock analyses. Basic Eagle usage:
```
   [user@loginN ~]$ module load conda
   [user@loginN ~]$ source activate /shared-projects/buildstock/envs/buildstock-X.X
   [user@loginN ~]$ buildstock_eagle ~/buildstockbatch/project_resstock_national.yml
```
... or locally using Docker:
```
   user$ pyenv activate buildstockbatch
   user$ pip install -e ./buildstockbatch
   user$ buildstock_local -j -2 ~/buildstockbatch/project_resstock_multifamily.yml
```
Other batch simulation methods may be supported in future. Please refer to the documentation for more
details regarding these features, and configuration via the project yaml configuration documentation.
:copyright: (c) 2018 by The Alliance for Sustainable Energy.
:license: BSD-3, see LICENSE for more details.
"""
