from enbios.base.experiment import Experiment
from enbios.base.pydantic_experiment_validation import validate_experiment_data

a1 = {
    "hierarchy": {
        "name": "root",
        "aggregator": "sum",
        "children": [
            {
                "name": "x",
                "id": {
                    "name": "heat and power co-generation, wood chips, 6667 kW, state-of-the-art 2014",
                    "location": "DK",
                    "unit": "kilowatt hour"
                },
                "adapter": "bw"
            }
            # {
            #     "name": "aa",
            #     "id": {
            #         "code": "xxx",
            #     },
            #     "adapter": "bw"
            # }
        ]
    },
    # "activities": {
    #     "single_activity": {
    #         "id": {
    #             "name": "heat and power co-generation, wood chips, 6667 kW, state-of-the-art 2014",
    #             "location": "DK",
    #             "unit": "kilowatt hour"
    #         },
    #         "output": [
    #             "MWh",
    #             30
    #         ],
    #         "adapter": "bw"
    #     }
    #     # ,
    #     # "solar1": {'id': {'name': 'electricity production, wind, >3MW turbine, onshore',
    #     #                   'code': '0d48975a3766c13e68cedeb6c24f6f74'},
    #     #            'output': ['kilowatt_hour', 3]},
    #     # "solar2":
    #     #     {'id': {'name': 'electricity production, wind, 1-3MW turbine, onshore',
    #     #             'code': 'ed3da88fc23311ee183e9ffd376de89b'},
    #     #      'output': ['kilowatt_hour', 4]}
    # },
    "adapters": [{
        "module_path": "/home/ra/projects/enbios/enbios/bw2/brightway_experiment_adapter.py",
        "config": {
            "bw_project": "ecoinvent_391",
            "methods": [
                {
                    "id": ["EDIP 2003 no LT", "non-renewable resources no LT", "zinc no LT"]
                }
                # ,
                # {'id': ('ReCiPe 2016 v1.03, midpoint (H)',
                #         'ozone depletion',
                #         'ozone depletion potential (ODPinfinite)')
                #  }
            ],
        }
    }],
    "aggregators": [{
        "module_path": "/home/ra/projects/enbios/enbios/bw2/brightway_experiment_adapter.py"
    }
    ]
}


def test_a():
    data = validate_experiment_data(a1)
    exp = Experiment(data)
    res = exp.run_scenario(Experiment.DEFAULT_SCENARIO_ALIAS)
    pass


if __name__ == "__main__":
    test_a()
