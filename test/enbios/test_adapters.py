from enbios.base.experiment import Experiment


def test_simple_assignment_adapter():
    data = {
        "adapters": [
            {
                "adapter_name": "simple-assignment-adapter",
                "config": {

                },
                "methods": {
                    "test": "co2"
                }
            }
        ],
        "hierarchy": {
            "name": "root",
            "aggregator": "sum",
            "children": [
                {
                    "name": "test",
                    "adapter": "assign",
                    "config": {
                        "output_unit": "kg",
                        "default_output": {
                            "unit": "kg",
                            "magnitude": 1
                        },
                        "default_impacts": {
                            "test": {
                                "unit": "co2",
                                "amount": 1
                            }
                        }
                    }
                }
            ]
        }
    }

    exp = Experiment(data)
    res = exp.run()
    pass
