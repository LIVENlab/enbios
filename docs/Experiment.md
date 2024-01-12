# Running Experiments

An experiment configuration requires the following 3 properties:

- bw_project: the brightway project to be used
- activities: The activities from the project to be included
- methods: The assessment methods of the project to be used

Optional properties are:

- bw_default_database: Default database to be used for the project (speeds up search of activities)
- hierarchy: The technology hierarchy to be used for structuring the results
- scenarios: The scenarios to be used for the experiment, with different activity demands

Following we define the most straight forward way to describe all properties. There are alternatives, which are
described in the documentation of the experiment schema.

The example are given in YAML, tho the ExperimentData objects are created from ordinary dictionaries.

## Activities

The activities should be defined in a object, where the keys are the "aliases" (arbitrary names for the activities).
The values are objects as well with one mandatory key `id` and an optional `output' key, which should be a list of 2
items:
The output unit and the output magnitude. Whenever a scenario does not define an output, the default output will be used.

The `id` is an object that takes any combination of the following keys:
code, database, name, location, unit, (alias). Think of them as identifier, that must always allow to uniquely identify
an activity.
When a code is given, uniqueness is guaranteed. If no code is given, the combination of the other keys must be unique (
name, location unit).
Enbios2 will perform database searches and filtering to find the activities.

An example:

```yaml
activities:
  water production CH:
    id:
      code: 00c823e4084eb4ee5c7557613568bfd0
    output: [ kg, 1 ]
```

or

```yaml
activities:
  water production CH:
    id:
      name: water production
      location: CH
      unit: kg
```

## Methods

Methods in brightway are defined as tuples of strings.
In enbios2 the methods can be defined by an object, where the keys are the "aliases" (arbitrary names for the methods)
and the values, the lists/tuples that identify the methods.

```yaml
methods:
  CML no LT: [
    CML v4.8 2016 no LT,
    acidification no LT,
    acidification (incl. fate, average Europe total, A&B) no LT
  ]
```

## Hierarchy:

The hierarchy is options and defined a hierarchy of technologies, which will be used to create tree-like structures in
the results. The bottom nodes of the tree (leaves) are activities and all other nodes scores will be calculated from
by summing up the scores of its children.

The hierarchy is describes in a dictionary/list structure, where all keys are names of nodes. On the last level, there
needs to be a list of strings, which are activity-aliases.

The example is more clear in json format:
```json
{
  "hierarchy": {
    "renewable energy": {
      "solar": [],
      "wind": [
        "onshore",
        "offshore"
      ],
      "hydro": []
    },
    "fossil energy": [
      "coal"
    ]
  }
}
```



All fundamental elements (activities, methods, scenarios) have aliases, which can be part of the definition or will be
calculated in the following way:

- Activities: their name
- Methods: the complete identifier joined by underscores
- Scenarios: Scenario {index}

