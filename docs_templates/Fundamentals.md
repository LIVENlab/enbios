# Fundamentals

## Overview

- [Structure of an Enbios](#structure-of-enbios)
- [Initializing an experiment](#initializing-an-experiment)
- [Running an experiment](#running-an-experiment)
- [A first example](#a-first-simple-example)
- [How to configure Adapters and Aggregators](#how-to-configure-adapters-and-aggregators)
- [Hierarchy](#hierarchy)
- [Exporting results](#exporting-the-results)
- [Creating Adapters and Aggregators](#creating-adapters-and-aggregators)

This version is based on a very simple assumption. Calculating arbitrary (structural/terminal nodes) and aggregated
values (functional nodes) in
a MuSIASEM hierarchy for any type of scenarios (functional outputs).

The following diagram explains the main components and their interaction.
The main class is the Experiment. It requires a configuration object, which can come directly from a json file (or
a python dict object).

The behaviour of the MuSIASEM hierarchy elements (here called nodes) is not defined by fixed core implementation of
enbios, but through adapters and aggregators, which can dynamically be added.
We differentiate two types of nodes. Structural nodes (those at the bottom of the hierarchy) and functional nodes (all
nodes not on the bottom).
An experiment can specify any number of scenarios. Each scenario can specify different outputs for the nodes defined in
the hierarchy.

Structural nodes are each assigned an adapter, which calculates their result values based on their given output in a
scenario. Both outputs and resultvalues are passed up in the hierarchy until the root node (which is a functional node).

Functional nodes are each assigned an aggregator, which aggregates outputs and results and pass those up the hierarchy (
if there is a level up in the hierarchy for any given node).

## Structure of Enbios

```mermaid
graph
    Experiment --> Ada_Agg
    Ada_Agg[Adapters/Aggregators]
    Experiment --> Hierarchy
    Experiment --> Scenarios
    Hierarchy --> Nodes
    Nodes -. nodes refer and use\nadapter/aggregators .-> Ada_Agg
    Nodes -. scenario contain\nnode outputs .-> Scenarios
```

In creation of an enbios experiment. its configuration, is on strictly validated.
Afterward any scenario or all scenarios, defined in the config object can be executed.

[The full experiment API can be found here.](#Full-Experiment-API)

This version of enbios, is built with flexibility in mind. That means, the value calculation for structural nodes and
aggregation calculations for functional nodes is done in external python modules. Through this approach,
users can develop new arbitrary calculation (**Adapter**) and aggregation (**Aggregator**) modules and use them in
Enbios.

### Configuration

As seen above these are the main parts of an enbios config

- adapters: a list of adapter configurations, that should be used in this experiment
- aggregators: a list of aggregator configurations, that should be used in this experiment
- hierarchy: a tree-like structure, where each node in the tree needs a name, depending on its position (structural or
  functional) an association with an adapter or aggregator, and some specific configuration for that (e.g. how to
  identify the node in the adapter/aggregator, default outputs)
- scenarios: a list of scenario configuration, containing in particular the outputs of the structural units
- config: Some generic configurations

### Validation

Enbios uses pydantic (https://docs.pydantic.dev/latest/), as data validation library
for the structural validation of the configuration. The complete structural definitions of the data, is also given
as a JSON Schema (https://json-schema.org/)
file https://github.com/LIVENlab/enbios/blob/main/data/schema/experiment.schema.gen.json
Therefor the config data can also be validated with any JSON Schema validator (
e.g. https://www.jsonschemavalidator.net/)

## Initializing an experiment

An Experiment can be initialized in the 3 following ways.

1. Passing a python dictionary object, which contains the configuration
2. Passing a string to a JSON file, which contains the configuration
3. Not passing anything to the experiment, which will make enbios look for a JSON file at the path given for the
   environmental variable `CONFIG_FILE`.

This starts off the following steps of validation and preparation:

- check environment variables for link to experiment config file
- register additional units
- validate experiment data (structural validation)
- resolve experiment data links (eventual links to hierarchy and scenario files)
- validate full experiment data (structural validation)
- validate adapters
    - __For each defined adapter:__
        - load adapter module
        - <ada>_adapter.validate_definition_</ada>
        - <ada>_adapter.validate_config_</ada>
        - <ada>_adapter.validate_methods_</ada>
    - load builtin adapters
- validate aggregators
    - __For each defined aggregator:__
        - load aggregator module
        - <agg>_aggregator.validate_config_</agg>
    - load builtin aggregators
- validate hierarchy
    - basic structural validation
    - validate hierarchy nodes against their adapters, aggregators
      (<ada>_adapter.validate_node_</ada> <b>/</b> <agg>_aggregator.validate_node_</agg>)
- create template result-tree
- validate scenarios
    - create default scenario, if no scenario is defined
    - __for each defined scenario (or the default scenario)__:
        - validate scenario
            - __for each node that the scenario specifies:__
                - Validate the nodes scenario data against their adapter: <ada>adapter.validate_scenario_node</ada>)
        - prepare scenario result-tree
            - __for all structural nodes of the result-tree:__
                - Get the nodes output from its adapter: <ada>adapter.get_node_output</ada>
            - eventually remove exclude defaults (nodes with no output for a scenario) from the result-tree
            - from top to bottom aggregate the outputs within the result-tree (<agg>
              _aggregator.aggregate_node_output_</agg>)
- validate scenario settings: Check if the environmental settings, specify which scenarios to run

## Running an experiment

The function `Experiment.run` will run either

- all scenarios defined in the experiment config or
- run all scenarios specified in the config (a subset specifief in the experiment config or in through environmental
  variables )

Scenarios can also be run individually with: `Experiment.run_scenario`

### Running a scenario

For all adapters specified for the experiment:

- <ada>_adapter.run_scenario_</ada>
- add the results from the adapters to the result-tree

Propagate the results up in the result-tree:

From top to bottom aggregate the results within the result-tree (<agg>_aggregator.aggregate_node_result_</agg>)

## A first simple example

This example uses the brightway adapter and 4 activities of ecoinvent 3.9.1. The hierarchy contains 2 wind farms and 2
solar plants, which are in 2 functional nodes ('wind' and 'solar').
Additionally, the configuration contains 2 scenarios.

```mermaid
---
title: Example hierarchy
---
graph LR
    root("root")
    root --> wind("wind")
    root --> solar("solar")
    wind --> w1[wind turbine >3MW onshore]
    wind --> w2[wind turbine 1-3MW onshore]
    solar --> s1[solar tower power plant, 20 MW]
    solar --> s2[solar thermal parabolic trough, 50 MW]
```

_(structural nodes are rectangles and functional nodes are rounded rectangles)_

[This demo notebook](https://github.com/LIVENlab/enbios/blob/main/demos/intro.ipynb) shows how to build the
configuration step by step.

Full details are below the configuration

{{file:json:test/data/docs_data/simple_example.json}}

Upon running the experiment with the fiven configuration we get this result (if converted into a dict):
{{file:json:test/data/docs_data/gen/simple_example_result.json}}

### Configuration details

**adapters:** This list contains only the configuration for brightway-adapter. For each adapter, one of the given fields
must be given:

`module_path`: The absolute path of the module that contains the adapter.

`adapter_name`: The name of the builtin adapter.

Respectively for an aggregator (`module_path`, `aggregator_name`)

Besides the identification the adapter should have the fields `config` and `methods`.
For the `config` of the brightway-adapter it is crucial to include the field `bw_project`, so that enbios know which
brightway project to use.

For `methods` we need to pass a dictionary, where the keys are arbitrary names that we give to the method and the tuple
of strings, which are the names/identifiers of methods in brightway.

**aggregators**:

Since we only make use of the builtin _sum-aggregator_ which does not require any configuration, we can omit, this
field in the experiment configuration.

**hierarchy**:

Each node in the hierarchy has the following fields

_name_: An arbitrary name for that node (all names must be unique in the hierarchy)

_adapter_ | _aggregator_ : All structural nodes require an _adapter_ field and all functional nodes require an
_aggregator_ field. The values correspond to the name of the module, or it's specific node_indicator, which are 'sum'
for sum-aggregators and 'bw' for the brightway-adapter. (More details on that later)

_children_: A list of all the nodes children. This field is only required for functional nodes and must be omitted for
structural nodes.

_config_: A node specific config, that will be passed on to the Adapter ore Aggregator. The required fields depend on
the module at hand. In the case of brightway, we need some fields that help brightway to identify the activity.

**scenarios**:

A list of scenario configurations. Generally, we want to run multiple scenarios with different outputs for the
structural nodes.
Each configuration can have the following fields:

_name_: An arbitrary name for the scenario

_activities_: A dictionary where the keys are (structural) node names, as defined in the hierarchy and the values their
outputs. The outputs can have the form of a dictionary with the keys: `unit`, `magnitude` or list of 2 elements, where
the
first one defines the unit and the 2nd the amount. E.g. these two definitions are
equivalent: `{'unit':'kilowatt_hour','magnitude':5}` and `['kilowatt_hour', 5]`.

_config_: A dictionary with scenario specific configuration.

## How to configure Adapters and Aggregators

Before we look at how adapter and aggregator are created and work, we look at how users can look up how to specify them,
when making use of them.

There are 2 parts in the experiment file, where adapter/aggregator specific data has to be used. In the configuration of
the module (`adapters`/`aggregators`) in the experiment configuration and for each node, in the `config` field.

As seen in the example the adapter data for the _brightway-adapter_ looks like this:

```json
{
  "adapter_name": "brightway-adapter",
  "config": {
    "bw_project": "ecoinvent_391"
  },
  "methods": {
    "GWP1000": [
      "ReCiPe 2016 v1.03, midpoint (H)",
      "climate change",
      "global warming potential (GWP1000)"
    ],
    "FETP": [
      "ReCiPe 2016 v1.03, midpoint (H)",
      "ecotoxicity: freshwater",
      "freshwater ecotoxicity potential (FETP)"
    ]
  }
}
```

And the structural nodes in the example, have a config specific for the _brightway_adapter_

```json
{
  "name": "electricity production, wind, >3MW turbine, onshore",
  "adapter": "bw",
  "config": {
    "code": "0d48975a3766c13e68cedeb6c24f6f74",
    "default_output": {
      "unit": "kilowatt_hour",
      "magnitude": 3
    }
  }
}
```

All adapters/aggregators have the static function `get_config_schemas`, which gives us the corresponding json-schemas
for
configuration. Adapters return 3 schemas which are (by convention) called: `adapter`, `activity` and `method`, while
aggregators contain the schemas `aggregator`, `activity`.

Since the method `get_config_schemas` is static, we can call it directly on the class and do not need to initiate any
object before. So we can call
`BrightwayAdapter.get_config_schemas()` to get its config schemas.

However, there are some functions in Experiment for convenience of getting all configurations.
`get_builtin_adapters(details: bool = True)`
`get_builtin_aggregators(details: bool = True)`

will return dictionaries, which include `node_indicator` (the indicator to use for nodes) and (if the
parameter `details` is True (default:True)),
`config`, which will have the 2 (or the 3 in the case of aggregators) fields.

E.g. `get_builtin_adapters()['brightway-adapter']`

{{file:json:test/data/docs_data/gen/bw_adapter.json}}

On an experiment instance the following function can be
called: `get_all_configs(include_all_builtin_configs: bool = True)`, which
will return all configs for all adapters and aggregators specified in the experiment config data and all builtin
modules, when `include_all_builtin_configs` is set True (default).

The configs are in a dictionary in the fields `adapters`, `aggregators`

### Splitting the config file:

The configuration of an experiment can also be split into multiple files.
The hierarchy and scenarios can be separated in external files (and be provided in alternative formats).

See [this notebook](https://github.com/LIVENlab/enbios/blob/main/demos/multiple_config_files.ipynb) for details.

```json
  {
  "adapters": {
    "<adapter_name>": "<adapter_config>"
  },
  "aggregators": {
    "...": "..."
  }
}
```

### Builtin adapters and aggregators

There are a some builtin adapters and aggregators:

**Adapters:**

- SimpleAssignmentAdapter: For this Adapter, the outputs and impacts can be defined in the adapter configuration
- BrightwayAdapter: This Adapter, uses brightway2 (https://docs.brightway.dev) in order to calculate impacts,
  based on the outputs of activities (structural nodes)

See [this notebook](
https://github.com/LIVENlab/enbios/blob/main/demos/bw_adapter_config.ipynb
) for all possible configs in one structure

**Aggregators**

- SumAggregator: This Aggregator simply sums up the impact results of its children in the hierarchy.

## Hierarchy

Besides including the hierarchy in the configuration it is also possible to have a file location.
Enbios is able to read hierarchies in 3 formats.

1. as json, which should be the same format as it would be in the experiment config.

2. as csv file
   The following [jupyter notebook](https://github.com/LIVENlab/enbios/blob/main/demos/csv_hierarchy.ipynb) demonstrates
   how to use a csv file to specify the hierarchy:

3. as mermaid (mm) file
   As demonstrated in [this notebook](https://github.com/LIVENlab/enbios/blob/main/demos/mermaid.ipynb)

## Exporting the results

After running an experiment or individual scenarios, the results are stored in a tree structure.

The result can be turned into a dictionary (and dumped into a json file) with the Experiment function:

{{enbios.base.experiment.Experiment.results_to_dict}}

And for scenarios:

{{enbios.base.scenario.Scenario.result_to_dict}}

But can also be written to csv files with functions for Experiment (and scenarios respectively)

{{enbios.base.experiment.Experiment.results_to_csv}}

{{enbios.base.scenario.Scenario.result_to_csv}}

[This notebook](https://github.com/LIVENlab/enbios/blob/main/demos/csv_export.ipynb) demonstrates to usage of the
results_to_csv function.

### Aggegating the results into alternative hierarchies

After running an experiment (or individual scenarios) it is also possible to restructure the results into alternative
hierarchies. That means, that the results of the structural nodes, are not recalculated, but the functional nodes (which
use adapters) recalculate their results. When this is done, configurations for adapter nodes do not need to be provided
again. However, nodes with aggregators can redefine their config if wanted, or it might be needed, if the user
introduces new functional nodes.

[This notebook demonstrates](https://github.com/LIVENlab/enbios/blob/main/demos/multiple_hierarchies.ipynb) how this can
be done.

## Creating Adapters and Aggregators

In order to create new adapters and aggregators, one has to create module which contain a class, which inherits
from the abstract classes `enbios.base.adapters_aggregators.adapter.EnbiosAdapter`
or `enbios.base.adapters_aggregators.aggregator.EnbiosAggregator` respectively.

The fact that these parent classes are abstract, means they both specify a set of
abstract methods, which any subclass needs to implement.

The internals of these functions is mostly up to the developer, but they have to make sure, that they return data of the
types that enbios requires (these return types are already included as return type hints in the abstract methods).

Since adapters and aggregators have several functions in common, there is a common parent class, which defines these
functions. When creating a concrete adapter or aggregator, these functions have to be implemented as well.

{{enbios.base.adapters_aggregators.node_module.EnbiosNodeModule}}

This [demo notebook](https://github.com/LIVENlab/enbios/blob/main/demos/aggregator_extension.ipynb) shows how to build
and use a custom aggregator.

### Adapter

First, there are several validation functions. These functions serve to check the correctness of the input, but also to
store all configurations inside the adapter object as they are later needed for the execution. In case of invalid data,
they should raise an Exception. In the case some validation is not required, it is ok, they just contain `pass`.

{{enbios.base.adapters_aggregators.adapter.EnbiosAdapter}}

### Aggregator

{{enbios.base.adapters_aggregators.aggregator.EnbiosAggregator}}

## Environmental variables

In order to help executing enbios on remote systems it is also possible to make use of certain environmental variables,
for the enbios experiments. Environment variables can be set before a python script is executed.

Enbios reads two environmental variables:

- CONFIG_FILE: In case an experiment file is created without a config object, nor without a filepath for the
  configuration file, it will check for the existence of this variable and interpret is as the location of the
  configuration file.

- RUN_SCENARIOS: This variable is read as a json object (in this case as a list) and defines, which scenarios should be
  run.

The following [jupyter notebook](https://github.com/LIVENlab/enbios/blob/main/demos/environmental_variables.ipynb)
demonstrates the usage.

## Full Experiment API

{{enbios.base.experiment.Experiment}}

<style>
    ada {
        color: darkgreen;
        font-weight: bold;
    }

    agg {
        color: darkorange;
        font-weight: bold;
    }

</style>