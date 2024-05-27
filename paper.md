# Enbios 2

Enbios 2 (Environmental and Bioeconomic System Analysis) is a python based simulation tool for the assessment of
environmental impacts and resource requirements of energy system pathways according to policy scenarios.

Enbios is based on the integration of Life Cycle Assessment and the Multi-Scale Integrated Assessment of
Socio-ecosystem framework (MuSIASEM) originally developed by C.
Madrid-López ([2019](https://zenodo.org/records/10252544) and [2020](https://zenodo.org/records/4916338)).

The linked repository contains an extended documentation, example notebooks, built in plotting capabilities and more
than 170 tests.

Enbios main benefits are it's strict type-validation and unit and flexibility.

The behaviour of the MuSIASEM hierarchy nodes is not defined by fixed core implementation of
Enbios, but through adapters and aggregators in external python modules, which can dynamically be added.


An experiment can specify any number of scenarios. Each scenario can specify different outputs and configurations for the
nodes defined in the hierarchy.

<img src=data/docs_assets/base_experiment_diagram.png  width="500"/>