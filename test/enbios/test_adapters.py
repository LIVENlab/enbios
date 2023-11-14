from enbios.base.experiment import Experiment
from enbios.models.experiment_models import ExperimentData


a1 = {
    "activities": {
        "single_activity": {
            "id": {
                "name": "heat and power co-generation, wood chips, 6667 kW, state-of-the-art 2014",
                "location": "DK",
                "unit": "kilowatt hour"
            },
            "output": [
                "MWh",
                30
            ]
        }
    },
    "adapters": [{
        "module_path": "/home/ra/projects/enbios/enbios/bw2/birghtway_experiment_adapter.py",
        "config": {
            "bw_project": "ecoinvent_391",
            "methods": [
                {
                    "id": ["EDIP 2003 no LT", "non-renewable resources no LT", "zinc no LT"]
                }
            ],
        }
    }]
}


def test_a():
    data = ExperimentData(**a1)
    Experiment(data)
