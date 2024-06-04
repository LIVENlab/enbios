# Enbios 2

Enbios 2 (Environmental and Bioeconomic System Analysis) is a python based tool for the assessment of
environmental impacts and resource requirements of energy system pathways according to policy scenarios.

Enbios is based on the integration of Life Cycle Assessment and the Multi-Scale Integrated Assessment of
Socio-ecosystem framework (MuSIASEM).

Enbios main benefits are it's strict type-validation and unit and flexibility for dendrogram processor calculations. The behaviour of the MuSIASEM hierarchy processors is not defined by fixed core implementation of
Enbios, but through adapters and aggregators in external python modules, which can dynamically be added. An experiment can specify any number of scenarios. Each scenario can specify different outputs and configurations for the
processor defined in the hierarchy.

A builtin adapter allows to make Life cycle assessment calculations based on brightway extended with capabilities for regionalized assessments and arbitrary (non-linear) characterization methods.

The linked repository contains an extended documentation, example notebooks, built in plotting capabilities and more
than 170 tests.

Code repository: https://github.com/LIVENlab/enbios/

pypi project: https://pypi.org/project/enbios/

## References

Madrid-López, C. (2019). Integrated Assessment of the Nexus: The case of Shale Gas. IANEX project summary Poster. (Version 1). Zenodo. https://doi.org/10.5281/zenodo.10252544

Giampietro, M., & Mayumi, K. (2000a). Multiple-scale integrated assessment of societal metabolism: Integrating biophysical and economic representations across scales. Population and Environment, 22(2), 155–210. https://doi.org/10.1023/A:1026643707370

Giampietro, M., & Mayumi, K. (2000b). Multiple-scale integrated assessment of societal metabolism: Introducing the approach. Population and Environment, 22(2), 109–153. https://doi.org/10.1023/A:1026691623300

Mutel, C. (2017). Brightway: An open source framework for Life Cycle Assessment. Journal of Open Source Software, 2(12), 236. https://doi.org/10.21105/JOSS.00236