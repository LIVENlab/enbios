# ENBIOS2
## What is ENBIOS 2

ENBIOS2 (Environmental and Bioeconomic System Analysis)  is a [python-based](https://pypi.org/project/enbios/) simulation tool for the assessment of environmental impacts and resource requirements of energy system 
pathways according to policy scenarios. These pathways are typically calculated by Energy System Optimization Models (ESOMs). Currently, ENBIOS is coupled with the 
[Calliope](https://www.callio.pe/) framework, and we are working to couple it with the [TIMES](https://iea-etsap.org/index.php/etsap-tools/model-generators/times) framework.

ENBIOS2 is based on the integration of Life Cycle Assessment and the Multi-Scale Integrated Assessment of 
Socio-ecosystem framework (MuSIASEM) originally developed by C. Madrid-López 
([2019](https://zenodo.org/records/10252544) and [2020](https://zenodo.org/records/4916338))

ENBIOS2 is a ground up new computing implementation of the ENBIOS tool. You can see more information about 
this previous version below. Compared to the original version of enbios, this version is more flexible does not make 
its own LCA calculations, but uses [Brightway2](https://docs.brightway.dev/en/latest/) for that.

In ENBIOS2 you will implement an experiment. To do this you will need to have at hand:
 * a defined set of activities, that are typically the energy system technologies you would like to inlcude in the assessment
 * access to a life cycle inventory database that can be imported in Brightway2 (such as Ecoinvent) 
or the skills and data to create yours
 * a MuSIASEM (hierarchical) structuring of your energy system. This must be taylored to the speficis of your assessment
and include the structural components of the system (your activities) and functional components of your systems.
 * a set of non-linear assessment methods such as
   * life-cycle impact assessment methods (you can use Recipe for example, but you will need to correct its linearity) 
   * MuSIASEM methods
 * a set of ESOM-provided pathways, which contain energy supply, demand or transfer info for the given activities
 * If you like (optional!), you can couple ENBIOS2 with [PREMISE](https://www.sciencedirect.com/science/article/pii/S136403212200226X) for a prospective definition of 
life cycle inventories according to different climate scenarios and integrated assessment models.

What you get is results of impacts and resource demands by each activity (structure) and function at each level of the hierarchy.

ENBIOS is developed by the [LIVENlab](https://livenlab.org/), a research lab of the [SosteniPra](https://www.sostenipra.cat/) Research group, at [ICTA-UAB](https://www.uab.cat/icta/).

## Installation
We recommend you to run ENBIOS from a python IDE, such as Pycharm. 
But we also have a few Jupyter notebooks for you to use, see below.

You first need to create an environment. From your terminal, try this:

 * Windows  `python -m venv venv`
 * Linux   `python3.9 -m venv venv`

Activate the environment with

* (windows)
`venv\Scripts\activate`

* (linux)
`source venv/bin/activate`

Install enbios2 with

* (windows)
`python -m pip install enbios`

* (linux)
`python3 -m pip install enbios`

## Fundamentals

[Read fundamentals about setting up an enbios experiment](https://github.com/LIVENlab/enbios/blob/main/docs/Fundamentals.md)

## Experiment configuration json schema

The json schema for the experiment configuration can be found here:

https://github.com/LIVENlab/enbios/blob/main/data/schema/experiment.schema.gen.json

This schema, provides the structure of the experiment configuration, which is used to run the experiments.
Note, that there will be additional validations when the experiment is constructed (e.g. existence of brightway project,
databases, activities).

## Environment variables

2 environmental variables are used by enbios2:

- `CONFIG_FILE`: This variable can be used to specify the location of the configuration file (json). If experiment is
  initialized without any parameter (configuration data or file location), the configuration is read from this path.
- `RUN_SCENARIOS`: This variable can be used to specify the scenarios to run when `experiment_object.run()` is called.
  Setting the environmental variable overwrites the value set in the configuration file (`config.run_scenarios`). This
  variable should be formatted as a json array, with the aliases of the scenarios that should
  run. `e.g. '["Scenario 0"]'` (indexed default aliases, when no aliases are specified in the configuration file for the
  scenarios).

### Data inputs

- Outputs from your ESOM
- A dictionary that connects your ESOM taxonomy with your inventory taxonomy
- life cycle inventories in .spold format
- The basefile with the hierarchical structuring of the system
- your method file

### Outputs

For each system function and structure (activity):
- Environmental impact indicators from the most used LCIA methods (Recipe2016, CML, AWARE, etc.)
- Environmental externalization rates
- Forthcoming: Raw Material Recycling rates and Supply risk

### Features
- Integration of LCA and MuSIASEM evaluation methods
- Import of .spold LCA inventory data to a multi-level tree-like setting
- Library of impact assessment methods based on LCIA
- New impact assessment methods developed for raw materials and circularity
- Consideration of externalized environmental impacts
- Takes data from the friendly-data package (other formats under development)
- High level methods to quickly obtain/refresh analyses

## Demos

This repository contains a few notebooks (require jupyter notebook) in the demos folder, that can help you get started. 
We are updating and commenting these. Please bear with us while we do it and feel free to give us feedback on those (thanks).

[Getting started](https://github.com/LIVENlab/enbios/blob/main/enbios/demos/intro.ipynb)

[Plotting results](https://github.com/LIVENlab/enbios/blob/main/enbios/demos/plot_results.ipynb)

[Sorting the results in alternative hierarchies](https://github.com/LIVENlab/enbios/blob/main/enbios/demos/multiple_hierarchies.ipynb)

[Splitting the configuration](https://github.com/LIVENlab/enbios/blob/main/enbios/demos/multiple_config_files.ipynb)

[Working with trees](https://github.com/LIVENlab/enbios/blob/main/enbios/demos/trees.ipynb)

[Experiment with uncertainties](https://github.com/LIVENlab/enbios/blob/main/enbios/demos/uncertainty_experiment.ipynb)

## People

* [Ramin Soleymani](https://es.linkedin.com/in/ramin-soleymani-4703b17). -[ICTA-UAB](https://www.uab.cat/icta/)
* [Miquel Sierra Montoya](https://portalrecerca.uab.cat/en/persons/miquel-sierra-i-montoya). -[ICTA-UAB](https://www.uab.cat/icta/)
* [Alexander de Tomás](https://www.linkedin.com/in/alexander-de-tom%C3%A1s-pascual-a85348185/). -[ICTA-UAB](https://www.uab.cat/icta/)
* [Cristina Madrid-Lopez](https://portalrecerca.uab.cat/en/persons/cristina-madrid-lopez-3). - [ICTA-UAB](https://www.uab.cat/icta/)

## Contact

- For questions about the enbios framework, please contact the LIVENlab leader [cristina.madrid@uab.cat](mailto:cristina.madrid@uab.cat).

## Acknowledgements
### The first ENBIOS

The LCA-MuSIASEM integration that is the core of ENBIOS was born a few years back (2013 seems so far now!). 
The first prototype of the python package was built by [Rafa Nebot](https://github.com/rnebot) in a collaboration 
with the Technical Institute of the Canary Islands ([ITC](https://www.itccanarias.org/web/es/)) and based on the Nexus Information System developed within the
Horizon 2020 project [MAGIC-nexus](https://magic-nexus.eu/) and the LCA-MuSIASEM integration protocol developed in the
Marie Curie project [IANEX](https://cordis.europa.eu/project/id/623593). This early development was funded by the
Horizon 2020 project Sustainable Energy Transitions Laboratory ([SENTINEL](https://sentinel.energy>), GA 837089).

 ### Current development
ENBIOS2 is in development with funds from the Spanish Research Agency (AEI) and the European Commission (CINEA):

* [SEEDS](https://seeds-project.org/) project with AEI grant PCI2020-120710-2 funds the ENBIOS 2 build based on the
  Brightway2 LCA framework, adding inventory manipulation to match the mixes of the energy scenarios and the connection with MuSIASEM
* LIVEN project with AEI grant PID2020-119565RJ-I00 funds the regionalization of the analysis and connection with the TIMES energy model
* ETOS project with AEI grant TED2021-132032A-I00 funds the addition of externalization
* [JUSTWIND4ALL](https://justwind4all.eu/) project with Horizon Europe grant 101083936 funds the development of a higher
  resolution module for wind energy assessment, including new wind-specific holistic assessment methods.

## References
You can see some more info and results from ENBIOS here:
* More information on the roots of the framework and version 1 of the software can be found in [deliverable 2.2]() of
the [SENTINEL](https://sentinel.energy) project. 
* An application to the assessment of energy pathway option space (with 260+ pathways modelled with calliope) with ENBIOS2 can be consulted
in [deliverable 2.2](https://zenodo.org/record/7994038) of the [SEEDS](https://seeds-project.org/) project.
