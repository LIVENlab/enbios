from enbios.base.experiment import Experiment


def test_simple_assignment_adapter():
    data = {
        "adapters":[],
        "hierarchy": {
            "name": "root",
            "aggregator": "sum",
            "children": [
                {
                    "name": "test",
                    "adapter": "assign",
                    "id": "test",
                    "output": ["kg", 1]
                }
            ]
        }
    }

    Experiment(data)
