from enbios.base.experiment import Experiment


def test_simple_assignment_adapter():
    data = {
        "adapters": [
            {
                "adapter_name": "simple-assignment-adapter",
                "config": {},
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
                            "magnitude": 1.2
                        },
                        "default_impacts": {
                            "test": {
                                "unit": "co2",
                                "magnitude": 31.254
                            }
                        }
                    }
                }
            ]
        }
    }

    exp = Experiment(data)
    res = exp.run()
    rearrange = exp.scenarios[0].result_to_dict(alternative_hierarchy={
        "name": "root",
        "children": [
            {
                "name": "middle",
                "aggregator":"sum",
                "children": [
                    {
                        "name": "test",
                    }]
            }
        ]
    })
    print(rearrange)
    assert rearrange
