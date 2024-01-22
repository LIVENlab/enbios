#  Experiment Validation

1. Basic Pydantic type validation

2. Validate (and load) adapters and aggregators

3. Tree conversion
    3.1 Validate the tree
    3.2 Validate tree leaves against their adapters (Adapter.validate_activity)
        Might also include Adapter.validate_activity_output (Adapter.get_activity_output_unit)

4. Method validation (also in the adapters)