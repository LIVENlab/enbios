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
        - <ada>adapter.validate_definition </ada>
        - <ada>adapter.validate_config </ada>
        - <ada>adapter.validate_methods </ada>
    - load builtin adapters
- validate aggregators
    - __For each defined aggregator:__
        - load aggregator module
        - <agg>aggregator.validate_config</agg>
    - load builtin aggregators
- validate hierarchy
    - basic structural validation
    - validate hierarchy nodes against their adapters, aggregators
      (<ada>adapter.validate_node</ada> <b>/</b> <agg>aggregator.validate_node</agg>)
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
              aggregator.aggregate_node_output</agg>)
- validate scenario settings: Check if the environmental settings, specify which scenarios to run

## Running an experiment

The function `Experiment.run` will run either

- all scenarios defined in the experiment config or
- run all scenarios specified in the config (a subset specifief in the experiment config or in through environmental
  variables )

Scenarios can also be run individually with: `Experiment.run_scenario`

### Running a scenario

For all adapters specified for the experiment:

- <ada>adapter.run_scenario</ada>
- add the results from the adapters to the result-tree

Propagate the results up in the result-tree:

From top to bottom aggregate the results within the result-tree (<agg>aggregator.aggregate_node_result</agg>)

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

Full details are below the configuration

```json
{
  "adapters": [
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
  ],
  "hierarchy": {
    "name": "root",
    "aggregator": "sum",
    "children": [
      {
        "name": "wind",
        "aggregator": "sum",
        "children": [
          {
            "name": "wind turbine >3MW onshore",
            "adapter": "bw",
            "config": {
              "code": "0d48975a3766c13e68cedeb6c24f6f74",
              "default_output": {
                "unit": "kilowatt_hour",
                "magnitude": 3
              }
            }
          },
          {
            "name": "wind turbine 1-3MW onshore",
            "adapter": "bw",
            "config": {
              "code": "ed3da88fc23311ee183e9ffd376de89b"
            }
          }
        ]
      },
      {
        "name": "solar",
        "aggregator": "sum",
        "children": [
          {
            "name": "solar tower power plant, 20 MW",
            "adapter": "bw",
            "config": {
              "code": "f2700b2ffcb6b32143a6f95d9cca1721"
            }
          },
          {
            "name": "solar thermal parabolic trough, 50 MW",
            "adapter": "bw",
            "config": {
              "code": "19040cdacdbf038e2f6ad59814f7a9ed"
            }
          }
        ]
      }
    ]
  },
  "scenarios": [
    {
      "name": "normal scenario",
      "nodes": {
        "wind turbine >3MW onshore": [
          "kilowatt_hour",
          1
        ],
        "wind turbine 1-3MW onshore": [
          "kilowatt_hour",
          1
        ],
        "solar tower power plant, 20 MW": [
          "kilowatt_hour",
          1
        ],
        "solar thermal parabolic trough, 50 MW": [
          "kilowatt_hour",
          1
        ]
      }
    },
    {
      "name": "randomized outputs",
      "nodes": {
        "wind turbine >3MW onshore": [
          "kilowatt_hour",
          6
        ],
        "wind turbine 1-3MW onshore": [
          "kilowatt_hour",
          1
        ],
        "solar tower power plant, 20 MW": [
          "kilowatt_hour",
          8
        ],
        "solar thermal parabolic trough, 50 MW": [
          "kilowatt_hour",
          6
        ]
      }
    }
  ]
}
```

Upon running the experiment with the fiven configuration we get this result (if converted into a dict):
```json
{
  "normal scenario": {
    "name": "root",
    "results": {
      "GWP1000": {
        "unit": "kg CO2-Eq",
        "magnitude": 0.142615182893779
      },
      "FETP": {
        "unit": "kg 1,4-DCB-Eq",
        "magnitude": 0.06303329465759974
      }
    },
    "output": [
      {
        "unit": "kilowatt_hour",
        "magnitude": 4.0,
        "label": null
      }
    ],
    "children": [
      {
        "name": "wind",
        "results": {
          "GWP1000": {
            "unit": "kg CO2-Eq",
            "magnitude": 0.04021441221464619
          },
          "FETP": {
            "unit": "kg 1,4-DCB-Eq",
            "magnitude": 0.0566978988042604
          }
        },
        "output": [
          {
            "unit": "kilowatt_hour",
            "magnitude": 2.0,
            "label": null
          }
        ],
        "children": [
          {
            "name": "wind turbine >3MW onshore",
            "results": {
              "GWP1000": {
                "unit": "kg CO2-Eq",
                "magnitude": 0.024970800870991273
              },
              "FETP": {
                "unit": "kg 1,4-DCB-Eq",
                "magnitude": 0.050272290729750875
              }
            },
            "output": [
              {
                "unit": "kilowatt_hour",
                "magnitude": 1.0,
                "label": null
              }
            ],
            "bw_activity_code": "0d48975a3766c13e68cedeb6c24f6f74"
          },
          {
            "name": "wind turbine 1-3MW onshore",
            "results": {
              "GWP1000": {
                "unit": "kg CO2-Eq",
                "magnitude": 0.015243611343654916
              },
              "FETP": {
                "unit": "kg 1,4-DCB-Eq",
                "magnitude": 0.006425608074509521
              }
            },
            "output": [
              {
                "unit": "kilowatt_hour",
                "magnitude": 1.0,
                "label": null
              }
            ],
            "bw_activity_code": "ed3da88fc23311ee183e9ffd376de89b"
          }
        ]
      },
      {
        "name": "solar",
        "results": {
          "GWP1000": {
            "unit": "kg CO2-Eq",
            "magnitude": 0.10240077067913281
          },
          "FETP": {
            "unit": "kg 1,4-DCB-Eq",
            "magnitude": 0.006335395853339337
          }
        },
        "output": [
          {
            "unit": "kilowatt_hour",
            "magnitude": 2.0,
            "label": null
          }
        ],
        "children": [
          {
            "name": "solar tower power plant, 20 MW",
            "results": {
              "GWP1000": {
                "unit": "kg CO2-Eq",
                "magnitude": 0.04820458190235993
              },
              "FETP": {
                "unit": "kg 1,4-DCB-Eq",
                "magnitude": 0.0032423199619428613
              }
            },
            "output": [
              {
                "unit": "kilowatt_hour",
                "magnitude": 1.0,
                "label": null
              }
            ],
            "bw_activity_code": "f2700b2ffcb6b32143a6f95d9cca1721"
          },
          {
            "name": "solar thermal parabolic trough, 50 MW",
            "results": {
              "GWP1000": {
                "unit": "kg CO2-Eq",
                "magnitude": 0.05419618877677287
              },
              "FETP": {
                "unit": "kg 1,4-DCB-Eq",
                "magnitude": 0.0030930758913964764
              }
            },
            "output": [
              {
                "unit": "kilowatt_hour",
                "magnitude": 1.0,
                "label": null
              }
            ],
            "bw_activity_code": "19040cdacdbf038e2f6ad59814f7a9ed"
          }
        ]
      }
    ]
  },
  "randomized outputs": {
    "name": "root",
    "results": {
      "GWP1000": {
        "unit": "kg CO2-Eq",
        "magnitude": 0.8758822044490706
      },
      "FETP": {
        "unit": "kg 1,4-DCB-Eq",
        "magnitude": 0.35255636749694047
      }
    },
    "output": [
      {
        "unit": "kilowatt_hour",
        "magnitude": 21.0,
        "label": null
      }
    ],
    "children": [
      {
        "name": "wind",
        "results": {
          "GWP1000": {
            "unit": "kg CO2-Eq",
            "magnitude": 0.1650684165695875
          },
          "FETP": {
            "unit": "kg 1,4-DCB-Eq",
            "magnitude": 0.30805935245300914
          }
        },
        "output": [
          {
            "unit": "kilowatt_hour",
            "magnitude": 7.0,
            "label": null
          }
        ],
        "children": [
          {
            "name": "wind turbine >3MW onshore",
            "results": {
              "GWP1000": {
                "unit": "kg CO2-Eq",
                "magnitude": 0.14982480522593258
              },
              "FETP": {
                "unit": "kg 1,4-DCB-Eq",
                "magnitude": 0.3016337443784996
              }
            },
            "output": [
              {
                "unit": "kilowatt_hour",
                "magnitude": 6.0,
                "label": null
              }
            ],
            "bw_activity_code": "0d48975a3766c13e68cedeb6c24f6f74"
          },
          {
            "name": "wind turbine 1-3MW onshore",
            "results": {
              "GWP1000": {
                "unit": "kg CO2-Eq",
                "magnitude": 0.015243611343654916
              },
              "FETP": {
                "unit": "kg 1,4-DCB-Eq",
                "magnitude": 0.006425608074509521
              }
            },
            "output": [
              {
                "unit": "kilowatt_hour",
                "magnitude": 1.0,
                "label": null
              }
            ],
            "bw_activity_code": "ed3da88fc23311ee183e9ffd376de89b"
          }
        ]
      },
      {
        "name": "solar",
        "results": {
          "GWP1000": {
            "unit": "kg CO2-Eq",
            "magnitude": 0.7108137878794831
          },
          "FETP": {
            "unit": "kg 1,4-DCB-Eq",
            "magnitude": 0.04449701504393135
          }
        },
        "output": [
          {
            "unit": "kilowatt_hour",
            "magnitude": 14.0,
            "label": null
          }
        ],
        "children": [
          {
            "name": "solar tower power plant, 20 MW",
            "results": {
              "GWP1000": {
                "unit": "kg CO2-Eq",
                "magnitude": 0.38563665521887946
              },
              "FETP": {
                "unit": "kg 1,4-DCB-Eq",
                "magnitude": 0.02593855969554289
              }
            },
            "output": [
              {
                "unit": "kilowatt_hour",
                "magnitude": 8.0,
                "label": null
              }
            ],
            "bw_activity_code": "f2700b2ffcb6b32143a6f95d9cca1721"
          },
          {
            "name": "solar thermal parabolic trough, 50 MW",
            "results": {
              "GWP1000": {
                "unit": "kg CO2-Eq",
                "magnitude": 0.3251771326606037
              },
              "FETP": {
                "unit": "kg 1,4-DCB-Eq",
                "magnitude": 0.018558455348388462
              }
            },
            "output": [
              {
                "unit": "kilowatt_hour",
                "magnitude": 6.0,
                "label": null
              }
            ],
            "bw_activity_code": "19040cdacdbf038e2f6ad59814f7a9ed"
          }
        ]
      }
    ]
  }
}
```

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

```json
{
  "node_indicator": "bw",
  "config": {
    "adapter": {
      "$defs": {
        "NonLinearCharacterizationConfig": {
          "properties": {
            "methods": {
              "additionalProperties": {
                "$ref": "#/$defs/NonLinearMethodConfig"
              },
              "description": "Non linear characterization. Nested Dictionary: method_name > NonLinearMethodConfig",
              "title": "Methods",
              "type": "object"
            }
          },
          "required": [
            "methods"
          ],
          "title": "NonLinearCharacterizationConfig",
          "type": "object"
        },
        "NonLinearMethodConfig": {
          "additionalProperties": false,
          "properties": {
            "name": {
              "default": null,
              "description": "bw method tuple name",
              "title": "Name",
              "type": "string"
            },
            "module_path_function_name": {
              "default": null,
              "description": "path to a module and a function name. which holds a function that returns a 'dict[tuple[str, str], Callable[[float], float]]'",
              "maxItems": 2,
              "minItems": 2,
              "prefixItems": [
                {
                  "type": "string"
                },
                {
                  "type": "string"
                }
              ],
              "title": "Module Path Function Name",
              "type": "array"
            },
            "get_defaults_from_original": {
              "anyOf": [
                {
                  "type": "boolean"
                },
                {
                  "type": "null"
                }
              ],
              "default": false,
              "description": "Method is already defined in BW and has characterization values. ",
              "title": "Get Defaults From Original"
            }
          },
          "title": "NonLinearMethodConfig",
          "type": "object"
        },
        "RegionalizationConfig": {
          "additionalProperties": false,
          "properties": {
            "run_regionalization": {
              "default": false,
              "title": "Run Regionalization",
              "type": "boolean"
            },
            "select_regions": {
              "default": null,
              "description": "regions to store the results for",
              "items": {
                "type": "string"
              },
              "title": "Select Regions",
              "type": "array",
              "uniqueItems": true
            },
            "set_node_regions": {
              "additionalProperties": {
                "items": {
                  "type": "string"
                },
                "type": "array"
              },
              "default": {},
              "description": "Set node regions",
              "title": "Set Node Regions",
              "type": "object"
            },
            "clear_all_other_node_regions": {
              "anyOf": [
                {
                  "type": "boolean"
                },
                {
                  "type": "null"
                }
              ],
              "default": false,
              "description": "Delete all regions not in 'hierarchy' and 'set_node_regions'",
              "title": "Clear All Other Node Regions"
            }
          },
          "title": "RegionalizationConfig",
          "type": "object"
        }
      },
      "additionalProperties": false,
      "properties": {
        "bw_project": {
          "title": "Bw Project",
          "type": "string"
        },
        "use_k_bw_distributions": {
          "default": 1,
          "description": "Number of samples to use for MonteCarlo",
          "title": "Use K Bw Distributions",
          "type": "integer"
        },
        "store_raw_results": {
          "default": false,
          "description": "If the numpy matrix of brightway should be stored in the adapter. Will be stored in `raw_results[scenario.name]`",
          "title": "Store Raw Results",
          "type": "boolean"
        },
        "store_lca_object": {
          "default": false,
          "description": "If the LCA object should be stored. Will be stored in `lca_objects[scenario.name]`",
          "title": "Store Lca Object",
          "type": "boolean"
        },
        "simple_regionalization": {
          "allOf": [
            {
              "$ref": "#/$defs/RegionalizationConfig"
            }
          ],
          "description": "Generate regionalized LCA"
        },
        "nonlinear_characterization": {
          "anyOf": [
            {
              "$ref": "#/$defs/NonLinearCharacterizationConfig"
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "description": "Nonlinear characterization"
        }
      },
      "required": [
        "bw_project"
      ],
      "title": "BWAdapterConfig",
      "type": "object"
    },
    "activity": {
      "$defs": {
        "NodeOutput": {
          "additionalProperties": false,
          "properties": {
            "unit": {
              "title": "Unit",
              "type": "string"
            },
            "magnitude": {
              "default": 1.0,
              "title": "Magnitude",
              "type": "number"
            },
            "label": {
              "anyOf": [
                {
                  "type": "string"
                },
                {
                  "type": "null"
                }
              ],
              "default": null,
              "title": "Label"
            }
          },
          "required": [
            "unit"
          ],
          "title": "NodeOutput",
          "type": "object"
        }
      },
      "properties": {
        "name": {
          "default": null,
          "description": "Search:Name of the brightway activity",
          "title": "Name",
          "type": "string"
        },
        "database": {
          "default": null,
          "description": "Search:Name of the database to search first",
          "title": "Database",
          "type": "string"
        },
        "code": {
          "default": null,
          "description": "Search:Brightway activity code",
          "title": "Code",
          "type": "string"
        },
        "location": {
          "default": null,
          "description": "Search:Location filter",
          "title": "Location",
          "type": "string"
        },
        "enb_location": {
          "default": null,
          "description": "Location for regionalization",
          "items": {
            "type": "string"
          },
          "title": "Enb Location",
          "type": "array"
        },
        "unit": {
          "default": null,
          "description": "Search: unit filter of results",
          "title": "Unit",
          "type": "string"
        },
        "default_output": {
          "allOf": [
            {
              "$ref": "#/$defs/NodeOutput"
            }
          ],
          "default": null,
          "description": "Default output of the activity for all scenarios"
        },
        "methods": {
          "default": null,
          "description": "Subset of all methods",
          "items": {
            "type": "string"
          },
          "title": "Methods",
          "type": "array"
        }
      },
      "title": "BrightwayActivityConfig",
      "type": "object"
    },
    "method": {
      "additionalProperties": {
        "items": {
          "type": "string"
        },
        "type": "array"
      },
      "description": "Simply a dict: name : BW method tuple",
      "title": "Method definition",
      "type": "object"
    }
  }
}
```

On an experiment instance the following function can be
called: `get_all_configs(include_all_builtin_configs: bool = True)`, which
will return all configs for all adapters and aggregators specified in the experiment config data and all builtin
modules, when `include_all_builtin_configs` is set True (default).

The configs are in a dictionary in the fields `adapters`, `aggregators`

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

## EnbiosAdapter Objects

#### validate\_methods

```python
@abstractmethod
def validate_methods(methods: Optional[dict[str, Any]]) -> list[str]
```

Validate the methods. The creator might store method specific data in the adapter through this method.

**Arguments**:

- `methods`: A dictionary of method names and their config (identifiers for the adapter).

**Returns**:

list of method names

#### get\_node\_output

```python
@abstractmethod
def get_node_output(node_name: str, scenario: str) -> list[NodeOutput]
```

The output of a node for a scenario. A list of NodeOutput objects.

**Arguments**:

- `node_name`: Name of the node in the hierarchy
- `scenario`: Name of the scenario

**Returns**:

Multiple NodeOutput objects.

#### get\_method\_unit

```python
@abstractmethod
def get_method_unit(method_name: str) -> str
```

Unit of a method

**Arguments**:

- `method_name`:

#### run\_scenario

```python
@abstractmethod
def run_scenario(scenario: Scenario) -> dict[str, dict[str, ResultValue]]
```

Run a specific scenario. The adapter should return a dictionary of the form:

{
        node_name: {
            method_name: ResultValue (unit, magnitude)
        }
    }

**Arguments**:

- `scenario`:

**Returns**:

Returns a dictionary node-name: (method-name: results)



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

#### results\_to\_dict

```python
def results_to_dict(
        scenarios: Optional[Union[str, list[str]]] = None,
        include_method_units: bool = True,
        include_output: bool = True,
        include_extras: Optional[bool] = True,
        alternative_hierarchy: Optional[dict] = None) -> list[dict[str, Any]]
```

Get the results of all scenarios as a list of dictionaries as dictionaries

**Arguments**:

- `scenarios`: A selection of scenarios to export. If None, all scenarios will be exported.
- `alternative_hierarchy`: If given, the results will be recalculated using the given alternative hierarchy.
In this alternative hierarchy, tho, already defined nodeds need no config and no adapter/aggregator.
- `include_method_units`: Include the units of the methods in the header (default: True)
- `include_output`: Include the output of each node in the tree (default: True)
- `include_extras`: Include extras from adapters and aggregators in the results (default: True)



And for scenarios:

#### result\_to\_dict

```python
def result_to_dict(
        include_output: bool = True,
        include_method_units: bool = True,
        include_extras: bool = True,
        warn_no_results: bool = True,
        alternative_hierarchy: Optional[dict] = None) -> dict[str, Any]
```

Return the results as a dictionary

**Arguments**:

- `include_method_units`: (Include the units of the methods in the header)
- `include_output`: Include the output of all nodes (default: True)
- `alternative_hierarchy`: An alternative hierarchy to use for the results,
which comes from Scenario.rearrange_results.
- `warn_no_results`: Write a warning, if the scenario has not run yet.



But can also be written to csv files with functions for Experiment (and scenarios respectively)

#### results\_to\_csv

```python
def results_to_csv(file_path: PathLike,
                   scenarios: Optional[Union[str, list[str]]] = None,
                   level_names: Optional[list[str]] = None,
                   include_method_units: bool = True,
                   include_output: bool = True,
                   flat_hierarchy: Optional[bool] = False,
                   include_extras: Optional[bool] = True,
                   repeat_parent_name: bool = False,
                   alternative_hierarchy: Optional[dict] = None)
```

Turn the results into a csv file. If no scenario name is given,

it will export all scenarios to the same file,

**Arguments**:

- `file_path`: File path to export to
- `scenarios`: string or list of strings. If no scenario name is given, it will export all scenarios
to the same file,
with an additional column for the scenario alias
- `level_names`: (list of strings) If given, the results will be exported with the given level names.
This is only effective when flat_hierarchy is False.
- `include_method_units`: (Include the units of the methods in the header)
- `include_output`: Include the output of all nodes (default: True)
- `flat_hierarchy`: If instead of representing each level of the hierarchy with its own column,
we just indicate the node levels.
- `include_extras`: Include extras from adapters and aggregators in the results (default: True)
- `repeat_parent_name`: If True, the parent name will be repeated for each child node in the
corresponding level column.  This is only effective when flat_hierarchy is False. (default: False)
- `alternative_hierarchy`: If given, the results will be recalculated using the given alternative hierarchy.
In this alternative hierarchy, tho, already defined nodeds need no config and no adapter/aggregator.



#### result\_to\_csv

```python
def result_to_csv(file_path: PathLike,
                  level_names: Optional[list[str]] = None,
                  include_output: bool = True,
                  include_method_units: bool = True,
                  alternative_hierarchy: Optional[dict] = None,
                  flat_hierarchy: Optional[bool] = False,
                  repeat_parent_name: bool = False,
                  include_extras: bool = True,
                  warn_no_results: bool = True)
```

Save the results (as tree) to a csv file

:param file_path:  path to save the results to
 :param level_names: names of the levels to include in the csv
 (must not match length of levels)
 :param include_method_units:  (Include the units of the methods in the header)

**Arguments**:

- `include_output`: Include the output of all nodes (default: True)
- `flat_hierarchy`: If instead of representing each level of the hierarchy with its own column,
we just indicate the node levels.
- `include_extras`: Include extras from adapters and aggregators in the results (default: True)
- `repeat_parent_name`: If True, the parent name will be repeated for each child node in the
corresponding level column.  This is only effective when flat_hierarchy is False. (default: False)
- `alternative_hierarchy`: If given, the results will be recalculated using the given alternative hierarchy.
In this alternative hierarchy, tho, already defined nodeds need no config and no adapter/aggregator.
- `warn_no_results`: Write a warning, if the scenario has not run yet.



[This notebook]((https://github.com/LIVENlab/enbios/blob/main/demos/csv_export.ipynb)) demonstrates to usage of the results_to_csv function.

## Creating Adapters and Aggregators

In order to create new adapters and aggregators, one has to create module which contain a class, which inherits
from the abstract classes `enbios.base.adapters_aggregators.adapter.EnbiosAdapter`
or `enbios.base.adapters_aggregators.aggregator.EnbiosAggregator` respectively.

The fact that these parent classes are abstract, means they both specify a set of
abstract methods, which any subclass needs to implement.

The internals of these functions is mostly up to the developer, but they have to make sure, that they return data of the
types that enbios requires (these return types are already included as return type hints in the abstract methods).

### Adapter

First, there are several validation functions. These functions serve to check the correctness of the input, but also to
store all configurations inside the adapter object as they are later needed for the execution. In case of invalid data,
they should raise an Exception. In the case some validation is not required, it is ok, they just contain `pass`.

`validate_definition(self, definition: AdapterModel)`

Validates the whole adapter definition, which is the whole dictionary (parse
as `enbios.models.models.AdapterModel`)

`validate_config(self, config: Optional[dict[str, Any]])`

Validates the configuration (the `config` value in the definition).

`validate_methods(self, methods: Optional[dict[str, Any]]) -> list[str]`

Validates the `methods` in the definition.

`validate_node(self, node_name: str, node_config: Any)`

Validates a node configuration, for each node that specifies this as its adapter.

`validate_node_output(self, node_name: str, target_output: ActivityOutput) -> float`

Validates the output of a node as part of a scenario validation in the experiment.

`get_node_output_unit(self, activity_name: str) -> str`

Get the output unit of a node.

`get_method_unit(self, method_name: str) -> str`

Get the unit of a method.

`get_default_output_value(self, activity_name: str) -> float`

Get the default output amount of a node (in its defined output unit).

`run_scenario(self, scenario: Scenario) -> dict[str, dict[str, ResultValue]]`

Run a scenario.

Additionally, there are some static method, which means, they can be called on the Adapter class.

`static node_indicator() -> str`

The indicator that can be used, to indicate that a node should use this adapter (alternatively, the name, as given in
name() can also be used).

`static get_config_schemas() -> dict[str, dict[str, Any]]`

Get the configuration schemas for `config`, `method` and  `activity`. The idea here, is that, these are generated from
Pydantic model classes, which are used `validate_config`, `validate_node` and `validate_methods`.

`static name() -> str`

Get the name of the adapter.

### Aggregator

`validate_config(self, config: Optional[dict[str, Any]])`

Validate the configuration of the aggregator.

`validate_node(self, node_name: str, node_config: Any)`

Validates a node configuration, for each node that specifies this as its aggregator.

`aggregate_node_output(self, node: BasicTreeNode[ScenarioResultNodeData], scenario_name: Optional[str] = "") -> Optional[NodeOutput]`

Aggregate the outputs of the children of a node. This method should return an optional NodeOutput, if the aggregation
was correct and there is some uniform output. This is in order to prevent errors higher up in the hierarchy. For
example, the Sum aggregator, returns NodeOutput, if the outputs (and crucially their units) can be summed up. It returns
None,
if the units don't fit together.
This function is already called during the initiation of an experiment as part of each scenario validation.

`aggregate_node_result(self, node: BasicTreeNode[ScenarioResultNodeData])`

Aggregate the results of the children of a node.

Static methods:

`def node_indicator() -> str`

The indicator that can be used, to indicate that a node should use this aggregator (alternatively, the name, as given in
name() can also be used).

`name() -> str`

Get the name of the adapter.

`get_config_schemas() -> dict`

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

## Experiment Objects

#### get\_node

```python
def get_node(name: str) -> BasicTreeNode[TechTreeNodeData]
```

Get a node from the hierarchy by its name

**Arguments**:

- `name`: name of the node

**Returns**:

All node-data

#### get\_structural\_node

```python
def get_structural_node(name: str) -> BasicTreeNode[TechTreeNodeData]
```

Get a node by either its name as it is defined in the experiment data.

**Arguments**:

- `name`: Name of the node (as defined in the experiment hierarchy)

**Returns**:

All node-data

#### get\_node\_module

```python
def get_node_module(node: Union[str, BasicTreeNode[TechTreeNodeData]],
                    module_type: Optional[T] = EnbiosNodeModule) -> T
```

Get the module of a node in the experiment hierarchy


#### get\_adapter\_by\_name

```python
def get_adapter_by_name(name: str) -> EnbiosAdapter
```

Get an adapter by its name

**Arguments**:

- `name`: name of the adapter

**Returns**:

The adapter

#### get\_module\_by\_name\_or\_node\_indicator

```python
def get_module_by_name_or_node_indicator(name_or_indicator: str,
                                         module_type: Type[T]) -> T
```

#### get\_scenario

```python
def get_scenario(scenario_name: str) -> Scenario
```

Get a scenario by its name

**Arguments**:

- `scenario_name`: The name of the scenario as defined in the config or (Experiment.DEFAULT_SCENARIO_NAME)

**Returns**:

The scenario object

#### run\_scenario

```python
def run_scenario(
    scenario_name: str,
    results_as_dict: bool = True
) -> Union[BasicTreeNode[ScenarioResultNodeData], dict]
```

Run a specific scenario

**Arguments**:

- `scenario_name`: Name of the scenario to run
- `results_as_dict`: If the result should be returned as a dict instead of a tree object

**Returns**:

The result_tree (eventually converted into a dict)

#### run

```python
def run(
    results_as_dict: bool = True
) -> dict[str, Union[BasicTreeNode[ScenarioResultNodeData], dict]]
```

Run all scenarios. Returns a dict with the scenario name as key and the result_tree as value

**Arguments**:

- `results_as_dict`: If the result should be returned as a dict instead of a tree object

**Returns**:

dictionary scenario-name : result_tree  (eventually converted into a dict)

#### execution\_time

```python
@property
def execution_time() -> str
```

Get the execution time of the experiment (or all its scenarios) in a readable format

**Returns**:

execution time in the format HH:MM:SS

#### results\_to\_csv

```python
def results_to_csv(file_path: PathLike,
                   scenarios: Optional[Union[str, list[str]]] = None,
                   level_names: Optional[list[str]] = None,
                   include_method_units: bool = True,
                   include_output: bool = True,
                   flat_hierarchy: Optional[bool] = False,
                   include_extras: Optional[bool] = True,
                   repeat_parent_name: bool = False,
                   alternative_hierarchy: Optional[dict] = None)
```

Turn the results into a csv file. If no scenario name is given,

it will export all scenarios to the same file,

**Arguments**:

- `file_path`: File path to export to
- `scenarios`: string or list of strings. If no scenario name is given, it will export all scenarios
to the same file,
with an additional column for the scenario alias
- `level_names`: (list of strings) If given, the results will be exported with the given level names.
This is only effective when flat_hierarchy is False.
- `include_method_units`: (Include the units of the methods in the header)
- `include_output`: Include the output of all nodes (default: True)
- `flat_hierarchy`: If instead of representing each level of the hierarchy with its own column,
we just indicate the node levels.
- `include_extras`: Include extras from adapters and aggregators in the results (default: True)
- `repeat_parent_name`: If True, the parent name will be repeated for each child node in the
corresponding level column.  This is only effective when flat_hierarchy is False. (default: False)
- `alternative_hierarchy`: If given, the results will be recalculated using the given alternative hierarchy.
In this alternative hierarchy, tho, already defined nodeds need no config and no adapter/aggregator.


#### results\_to\_dict

```python
def results_to_dict(
        scenarios: Optional[Union[str, list[str]]] = None,
        include_method_units: bool = True,
        include_output: bool = True,
        include_extras: Optional[bool] = True,
        alternative_hierarchy: Optional[dict] = None) -> list[dict[str, Any]]
```

Get the results of all scenarios as a list of dictionaries as dictionaries

**Arguments**:

- `scenarios`: A selection of scenarios to export. If None, all scenarios will be exported.
- `alternative_hierarchy`: If given, the results will be recalculated using the given alternative hierarchy.
In this alternative hierarchy, tho, already defined nodeds need no config and no adapter/aggregator.
- `include_method_units`: Include the units of the methods in the header (default: True)
- `include_output`: Include the output of each node in the tree (default: True)
- `include_extras`: Include extras from adapters and aggregators in the results (default: True)


#### config

```python
@property
def config() -> ExperimentConfig
```

get the config of the experiment


#### structural\_nodes\_names

```python
@property
def structural_nodes_names() -> list[str]
```

Return the names of all structural nodes (bottom)

**Returns**:

names of all structural nodes

#### scenario\_names

```python
@property
def scenario_names() -> list[str]
```

Get all scenario names

**Returns**:

list of strings of the scenario names

#### adapters

```python
@property
def adapters() -> list[EnbiosAdapter]
```

Get all adapters in a list

**Returns**:

A list of all adapters

#### run\_scenario\_config

```python
def run_scenario_config(
    scenario_config: dict,
    result_as_dict: bool = True,
    append_scenario: bool = True
) -> Union[BasicTreeNode[ScenarioResultNodeData], dict]
```

Run a scenario from a config dictionary. Scenario will be validated and run. An

**Arguments**:

- `scenario_config`: The scenario config as a dictionary (as it would be defined in the experiment config)
- `result_as_dict`: If True, the result will be returned as a dictionary. If False, the result will be
returned as a BasicTreeNode.
- `append_scenario`: If True, the scenario will be appended to the experiment. If False, the scenario will
not be appended.

**Returns**:

The scenario result as a dictionary or a BasicTreeNode

#### info

```python
def info() -> str
```

Information about the experiment

**Returns**:

Generated information as a string

#### get\_module\_definition

```python
@staticmethod
def get_module_definition(clazz: Union[Type[EnbiosAdapter], EnbiosAdapter,
                                       Type[EnbiosAggregator],
                                       EnbiosAggregator],
                          details: bool = True) -> dict[str, Any]
```

Get the 'node_indicator' and schema of a module (adapter or aggregator)

**Arguments**:

- `clazz`: The class of the module (adapter or aggregator)
- `details`: If the whole schema should be returned (True) or just the node_indicator (False) (default: True)

**Returns**:

returns a dictionary {node_indicator: <node_indicator>, config: <schema>}

#### get\_builtin\_adapters

```python
@staticmethod
def get_builtin_adapters(details: bool = True) -> dict[str, dict[str, Any]]
```

Get the built-in adapters

**Arguments**:

- `details`: If the schema should be included or not (default: True)

**Returns**:

all built-in adapters as a dictionary name: {node_indicator: <node_indicator>, config: <json-schema>}

#### get\_builtin\_aggregators

```python
@staticmethod
def get_builtin_aggregators(details: bool = True) -> dict[str, dict[str, Any]]
```

Get the built-in aggregators

**Arguments**:

- `details`: If the schema should be included or not (default: True)

**Returns**:

all built-in aggregators as a dictionary name: {node_indicator: <node_indicator>, config: <json-schema>}

#### get\_all\_configs

```python
def get_all_configs(
    include_all_builtin_configs: bool = True
) -> dict[str, dict[str, dict[str, Any]]]
```

Result structure:

```json
    {
        "adapters": { <adapter_name>: <adapter_config>},
        "aggregators": { ... }
    }
    ```

**Arguments**:

- `include_all_builtin_configs`:

**Returns**:

all configs

#### method\_names

```python
@property
def method_names() -> list[str]
```

Names of all methods

**Returns**:

a list of method names

#### hierarchy2mermaid

```python
def hierarchy2mermaid() -> str
```

Convert the hierarchy to a mermaid graph diagram

**Returns**:

a string representing in mermaid syntax

#### get\_simplified\_hierarchy

```python
def get_simplified_hierarchy(
        print_it: bool = False) -> dict[str, Optional[dict[str, Any]]]
```

Get the hierarchy as a dictionary, but in a simplified form, i.e. only the nodes with children are included.

**Arguments**:

- `print_it`: Print it to the console

**Returns**:

A simplified dictionary of the hierarchy

#### delete\_pint\_and\_logging\_file

```python
@staticmethod
def delete_pint_and_logging_file()
```

"

Deletes the pint unit file and the logging config file




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