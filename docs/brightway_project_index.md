# Brightway project index

In order to easily allow setting the brightway project and allowing to share code between different machines, where the
project names might be different there is a local brightway project index file, which stores the names of brightway
projects of typical databases (ecoinvent)

## Getting started

Get a overview of all brightway projects on a system with:

```python
from enbios.bw2.project_index import print_bw_index

# see what is indexed:
print_bw_index()
# >>>
BWProjectIndex(ecovinvent391cutoff='uab_bw_ei39', ecovinvent391consequential=None, ecovinvent391apos=None)

# get an overview of brightway projects:
project_index_creation_helper()
# >>>
```

```
eco38:
  biosphere3:
    format: Ecoinvent XML
    number: 4427
  ei38apos:
    format: Ecospold2
    number: 19727
my_project_bw2_course:
  biosphere3:
    format: Ecoinvent XML
    number: 4709
uab_bw_ei39:
  biosphere3:
    format: Ecoinvent XML
    number: 4709
  ei39:
    format: Ecoinvent XML
    number: 21255
  ei391:
    format: Ecospold2
    number: 21238
```

To add a new project to the index:

```python
set_bw_index(bw_project_index: BWIndex, project_name: str)
```

To set the current brightway project based on an index.
Since BWIndex is an enum, there will be auto completion in the IDE.

```python
set_bw_current_project(bw_project_index: BWIndex)
# example:
set_bw_current_project(BWIndex.ecovinvent391cutoff)
```