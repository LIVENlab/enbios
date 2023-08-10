# Enbios 2

This a ground up new implementation of enbios.

Compared to the original version of enbios, this version is more flexible does not make it's own LCA calculations, but
uses brightway2 for that.

The main functionality of enbios2 is currently to run experiments, which are described a set of (brightway) activities a
set of methods and a set of scenarios, which contain demand for the given activities.

In addition to get the results of the scenarios in the dendrogram (....) the user can also define a hierarchy, which
contains the activities at the bottom.

Enbios 2 is a python package (https://pypi.org/project/enbios2/), which can be installed with pip.

## Installation

Go into the enbios2 directory and create a new virtual environment (with pip or conda) of **python 3.9** or higher.

`python3.9 -m venv venv`

Activate the environment with

`source venv/bin/activate`

Install enbios2 with

`pip install enbios2`

## Demo

The repository contains a few notebooks (require jupyter notebook) in the demos folder.

[Getting started](https://github.com/LIVENlab/enbios/blob/main/enbios2/demos/intro.ipynb)