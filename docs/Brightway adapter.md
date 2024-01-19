# Brigthway adapter

The Enbios [brightway](https://docs.brightway.dev/en/latest/) adapter uses brightway as an adapter for structural nodes
in enbios. Features include

- similar to brightway's MultiLCA class, we can run lca with multi-column demand vectors and multiple methods
- semi-automatic activity search (by name, location, unit)
- running with uncertainties
- run regionalization

## Basics

The adapter has the `name`  __brightway-adapter__ and `node_indicator` __bw__

The adapter configuration has the following schema

```json
{
  "properties": {
    "bw_project": {
      "title": "Bw Project",
      "type": "string"
    },
    "use_k_bw_distributions": {
      "default": 1,
      "description": "Number of samples to use for MonteCarlo",
      "type": "integer"
    },
    "store_raw_results": {
      "default": false,
      "description": "If the numpy matrix of brightway should be stored in the adapter. Will be stored in `raw_results[scenario.name]`",
      "type": "boolean"
    },
    "store_lca_object": {
      "default": false,
      "description": "If the LCA object should be stored. Will be stored in `lca_objects[scenario.name]`",
      "type": "boolean"
    },
    "simple_regionalization": {
      "allOf": [
        {
          "$ref": "#/$defs/RegionalizationConfig"
        }
      ],
      "description": "Generate regionalized LCA"
    }
  },
  "required": [
    "bw_project"
  ],
  "type": "object",
  "$defs": {
    "RegionalizationConfig": {
      "additionalProperties": false,
      "properties": {
        "run_regionalization": {
          "default": false,
          "type": "boolean"
        },
        "select_regions": {
          "default": null,
          "description": "regions to store the results for",
          "items": {},
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
          "type": "object"
        }
      },
      "type": "object"
    }
  }
}
```

`bw_project` is the only required field.

When `store_raw_results` and `store_lca_object` are set to True the
result matrices are stored in the adapter object as `raw_results` and `lca_objects` respectively.

The adapter object can be accessed through the experiment object like this:

`experiment._get_module_by_name_or_node_indicator('brightway-adapter')`

`use_k_bw_distributions` is by default 1, meaning LCA is NOT initiated with uncertainties.
We get uncertainties by setting this value higher.

`simple_regionalization` has its own schema with the fields:

- `run_regionalization`, if regionalization should be executed
- `select_regions`, regions to select for the results (a set of strings)
- `set_node_regions`, a dictionary of activity-codes > string tuples, to assign regionalization locations to activities.

## Activity selection/configuration

BW activities are selected through the node configuration (field `config`) in the experiment hierarchy.
Based on the fields `name`, `code`, `database`, `location` and `unit` the activity search will be able to select the
correct activity. Note that the `database` field is generally not required.

`enb_location` is a specific field for the enbios regionalization.
activities should have a location tuple, with increasing granularity of locations. e.g. `("EU", "ES","cat")`
location levels should be consistent. meaning, since ES is on index 1,
there should be no activity with location `("ES", "ara")` (where ES is at index 0)

Note that `enb_location` can also be added to the bw activities data in any other way.

`default_output` is used in scenarios where no output for that activity is specified. Note that the unit must be
compatible with the unit that brightway specifies for this activity (e.g. when the bw activity unit is kilowatt-hour, it
is possible to put MWh, Wh or GWh...)

```json
{
  "properties": {
    "name": {
      "default": null,
      "description": "Search:Name of the brightway activity",
      "type": "string"
    },
    "database": {
      "default": null,
      "description": "Search:Name of the database to search first",
      "type": "string"
    },
    "code": {
      "default": null,
      "description": "Search:Brightway activity code",
      "type": "string"
    },
    "location": {
      "default": null,
      "description": "Search:Location filter",
      "type": "string"
    },
    "enb_location": {
      "default": null,
      "description": "Location for regionalization",
      "items": {
        "type": "string"
      },
      "type": "array"
    },
    "unit": {
      "default": null,
      "description": "Search: unit filter of results",
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
    }
  },
  "type": "object",
  "$defs": {
    "NodeOutput": {
      "additionalProperties": false,
      "properties": {
        "unit": {
          "type": "string"
        },
        "magnitude": {
          "default": 1.0,
          "type": "number"
        }
      },
      "required": [
        "unit"
      ],
      "type": "object"
    }
  }
}
```

## Methods

Methods are directly specified in the adapter definition in the field `methods` as a dictionary of str to string
tuples (which are the identifiers in brightway).

```json

{
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

```

## Results

Node-data that use the brightway adapter have the following structure to store the results.
`results` in the `node.data` is a dictionary str -> ResultValue
where the keys are `<method_name>` or `<method_name>.<region>`. When brightway is run normally (distribution = 1) the
values are stored in `magnitude` for distributions in `multi_magnitude`.
E.g. a node in the result tree can look like this:

```json
{
  "name": "root",
  "results": {
    "GWP1000": {
      "unit": "kg CO2-Eq",
      "magnitude": 0.13466483029470988
    },
    "FETP": {
      "unit": "kg 1,4-DCB-Eq",
      "magnitude": 0.01735045631653643
    },
    "HTPnc": {
      "unit": "kg 1,4-DCB-Eq",
      "magnitude": 0.1368117616297974
    }
  },
  "output": {
    "unit": "kilowatt_hour",
    "magnitude": 4.0
  },
  "children": []
}

```
