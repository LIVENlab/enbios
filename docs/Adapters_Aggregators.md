# Adapters

A class that inherits from  `enbios.base.adapters_aggregators.adapter.EnbiosAdapter` like this:
`class MyAdapter(EnbiosAdapter):`

Adapters are specified like this in the Experiment configuration:

```
module_path : str | Path      : the path to the module that contains the adapter class
config      : Optional[dict]  : an optional configuration for the adapter
```

that should have a constructor `__init__()` (if configuration is needed) and implement the following
methods:


`def validate_config(self, config: dict[str, Any])`

Here the configuration should be validated and stored in the adapter.

`def validate_methods(self)`


```
def validate_activity_output(self, node_name: str, target_output: ActivityOutput):
    pass

def validate_activity(self,
                      node_name: str,
                      activity_id: ExperimentActivityId,
                      output: ActivityOutput,
                      required_output: bool = False):
    pass

def get_activity_output_unit(self, activity_name: str) -> str:
    pass

def get_activity_result_units(self, activity_name: str) -> list[str]:
    pass

def get_default_output_value(self, activity_name: str) -> float:
    pass

def run(self):
    pass

def run_scenario(self, scenario: Scenario) -> dict[str, Any]:
    pass

@property
def activity_indicator(self) -> str:
    pass

@property
def name(self) -> str:
    pass

```