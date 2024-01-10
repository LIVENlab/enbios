# Fundamentals

This version is based on a very simple assumption. Calculating arbitrary (structural/terminal nodes) and aggregated
values (functional nodes) in
a MuSIASEM hierarchy for any type of scenarios (functional outputs).

An enbios experiment is setup with one configuration dictionary(json object), which is on initiation strictly validated.
Afterwards any scenario or all scenarios, defined in the config object can be executed.

This vesion of enbios, is built with flexibility in mind. That means, the value calculation for structural nodes and
aggregation calculations for functional nodes is done in external python modules. Through this approeach,
users can develop new arbitrary calculation (**Adapter**) and aggregation (**Aggregator**) modules and use them in
Enbios.
There are a some builtin adapters and aggregators:

**Adapters:**

- SimpleAssignmentAdapter: For this Adapter, the outputs and impacts can be defined in the adapter configuration
- BrightwayAdapter: This Adapter, uses brightway2 (https://docs.brightway.dev) in order to calculate impacts,
  based on the outputs of activities (structural nodes)

**Aggregators**

- SumAggregator: This Aggregator simply sums up the impact results of it's children in the hierarchy.

## Structure of an Enbios configuration

Enbios uses pydantic (https://docs.pydantic.dev/latest/), as data validation library
for the structural validation of the configuration file. The complete structural definitions of the data, is also given
as a JSON Schema (https://json-schema.org/)
file https://github.com/LIVENlab/enbios/blob/main/data/schema/experiment.schema.gen.json
Therefor the config data can also be validated with any JSON Schema validator (
e.g. https://www.jsonschemavalidator.net/)

The configuration data for an Enbios experiment has the following structure:

- adapters: a list of adapter configurations, that should be used in this experiment
- aggregators: a list of aggregator configurations, that should be used in this experiment
- hierarchy: a tree-like structure, where each node in the tree needs a name, depending on its position (structural or
  functional) an association with an adapter or aggregator, and some specific configuration for that (e.g how to
  identify the node in the adapter/aggregator, default outputs)
- scenarios: a list of scenario configuration, containing in particular the outputs of the stuctural units
- config: Some generic configurations

## A first simple example

This example uses the brightway adapter and 4 activities of ecoinvent 3.9.1. The hierarchy contains 2 windfarms and 2
solar plants, which are in 2 functional nodes ('wind' and 'solar').
Additionally, the configuration contains 2 scenarios.
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
            "name": "electricity production, wind, >3MW turbine, onshore",
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
            "name": "electricity production, wind, 1-3MW turbine, onshore",
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
            "name": "electricity production, solar tower power plant, 20 MW",
            "adapter": "bw",
            "config": {
              "code": "f2700b2ffcb6b32143a6f95d9cca1721"
            }
          },
          {
            "name": "electricity production, solar thermal parabolic trough, 50 MW",
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
      "activities": {
        "electricity production, wind, >3MW turbine, onshore": [
          "kilowatt_hour",
          1
        ],
        "electricity production, wind, 1-3MW turbine, onshore": [
          "kilowatt_hour",
          1
        ],
        "electricity production, solar tower power plant, 20 MW": [
          "kilowatt_hour",
          1
        ],
        "electricity production, solar thermal parabolic trough, 50 MW": [
          "kilowatt_hour",
          1
        ]
      }
    },
    {
      "name": "randomized outputs",
      "activities": {
        "electricity production, wind, >3MW turbine, onshore": [
          "kilowatt_hour",
          6
        ],
        "electricity production, wind, 1-3MW turbine, onshore": [
          "kilowatt_hour",
          1
        ],
        "electricity production, solar tower power plant, 20 MW": [
          "kilowatt_hour",
          8
        ],
        "electricity production, solar thermal parabolic trough, 50 MW": [
          "kilowatt_hour",
          6
        ]
      }
    }
  ]
}

```

### Configuration details

**adapters:** This list contains only the configuration for brightway-adapter. For each adapter, one of the given fields
must be given:

`module_path`: The absolute path of the module that contains the adapter.

`adapter_name`: The name of the builtin adapter.

Respectively for an aggregator (`module_path`, `aggregator_name`)

Besides the identification the adapter should have the fields `config` and `methods`.
For the `config` of the brightway-adapter it is crucial to include the field `bw_project`, so that enbios know, which
brighway project to use.

For `methods` we need to pass a dictionary, where the keys are arbitrary names that we give to the method and the tuple
of strings, which are the names/identifiers of methods in brightway.

**aggregators**:

Since we only make use of the builtin _sum-aggregator_ which does not require any configuration, we can ommit, this
field in the experiment configuration.

**hierarchy**:

Each node in the hierarchy has the following fields

_name_: An arbitrary name for that node (all names must be unique in the hierarchy)
_

## How to config Adapters and Aggregators

```json
{
  "simple-assignment-adapter": {
    "activity_indicator": "assign",
    "config": {
      "activity": {
        "$defs": {
          "ActivityOutput": {
            "properties": {
              "unit": {
                "title": "Unit",
                "type": "string"
              },
              "magnitude": {
                "default": 1.0,
                "title": "Magnitude",
                "type": "number"
              }
            },
            "required": [
              "unit"
            ],
            "title": "ActivityOutput",
            "type": "object"
          },
          "ResultValue": {
            "additionalProperties": false,
            "properties": {
              "unit": {
                "title": "Unit",
                "type": "string"
              },
              "amount": {
                "anyOf": [
                  {
                    "type": "number"
                  },
                  {
                    "type": "null"
                  }
                ],
                "default": null,
                "title": "Amount"
              },
              "multi_amount": {
                "anyOf": [
                  {
                    "items": {
                      "type": "number"
                    },
                    "type": "array"
                  },
                  {
                    "type": "null"
                  }
                ],
                "title": "Multi Amount"
              }
            },
            "required": [
              "unit"
            ],
            "title": "ResultValue",
            "type": "object"
          }
        },
        "properties": {
          "activity": {
            "title": "Activity",
            "type": "string"
          },
          "output_unit": {
            "title": "Output Unit",
            "type": "string"
          },
          "default_output": {
            "$ref": "#/$defs/ActivityOutput"
          },
          "default_impacts": {
            "anyOf": [
              {
                "additionalProperties": {
                  "$ref": "#/$defs/ResultValue"
                },
                "type": "object"
              },
              {
                "type": "null"
              }
            ],
            "default": null,
            "title": "Default Impacts"
          },
          "scenario_outputs": {
            "anyOf": [
              {
                "additionalProperties": {
                  "$ref": "#/$defs/ActivityOutput"
                },
                "type": "object"
              },
              {
                "type": "null"
              }
            ],
            "default": null,
            "title": "Scenario Outputs"
          },
          "scenario_impacts": {
            "anyOf": [
              {
                "additionalProperties": {
                  "additionalProperties": {
                    "$ref": "#/$defs/ResultValue"
                  },
                  "type": "object"
                },
                "type": "object"
              },
              {
                "type": "null"
              }
            ],
            "default": null,
            "title": "Scenario Impacts"
          }
        },
        "required": [
          "activity",
          "output_unit",
          "default_output"
        ],
        "title": "SimpleAssignment",
        "type": "object"
      }
    }
  },
  "brightway-adapter": {
    "activity_indicator": "bw",
    "config": {
      "adapter": {
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
          "ActivityOutput": {
            "properties": {
              "unit": {
                "title": "Unit",
                "type": "string"
              },
              "magnitude": {
                "default": 1.0,
                "title": "Magnitude",
                "type": "number"
              }
            },
            "required": [
              "unit"
            ],
            "title": "ActivityOutput",
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
          "unit": {
            "default": null,
            "description": "Search: unit filter of results",
            "title": "Unit",
            "type": "string"
          },
          "default_output": {
            "allOf": [
              {
                "$ref": "#/$defs/ActivityOutput"
              }
            ],
            "default": null,
            "description": "Default output of the activity for all scenarios"
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
        "title": "Method defintion",
        "type": "object"
      }
    }
  }
}

```