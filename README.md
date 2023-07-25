# enbios2

This a ground up new implementation of enbios.

Compared to the original version of enbios, this version is more flexible does not make it's own LCA calculations, but
uses brightway2 for that.

The main functionality of enbios2 is currently to run experiments, which are described a set of (brightway) activities a
set of methods and a set of scenarios, which contain demand for the given activities.

In addition to get the results of the scenarios in the dendrogram (....) the user can also define a hierarchy, which
contains the activities at the bottom.

## Installation

Go into the enbios2 directory and create a new virtual environment (with pip or conda) of **python 3.9** or higher.

`conda create --name enbios2 python=3.9` or `python3.9 -m venv venv`

Activate the environment with `conda activate enbios2` or `source venv/bin/activate`

Then install the dependencies with
`python3 -m pip install -r requirements.txt`

