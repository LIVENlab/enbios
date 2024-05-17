import pickle
from pathlib import Path
from random import randint

from enbios import Experiment


def create_experiment_config(num_scenarios) -> dict:
    bw_adapter_config = {
        "config": {"bw_project": "ecoinvent_391"},
        "methods": {
            "GWP1000": (
                "ReCiPe 2016 v1.03, midpoint (E)",
                "climate change",
                "global warming potential (GWP1000)",
            ),
            "WCP": (
                "ReCiPe 2016 v1.03, midpoint (E)",
                "water use",
                "water consumption potential (WCP)",
            ),
            "LandUse": (
                "ReCiPe 2016 v1.03, midpoint (E)",
                "land use",
                "agricultural land occupation (LOP)",
            ),
        },
        "note": "brightway-adapter",
        "adapter_name": "brightway-adapter",
    }

    hierarchy = {
        "name": "root",
        "aggregator": "sum",
        "children": [
            {
                "name": "wind",
                "aggregator": "sum",
                "children": [
                    {
                        "name": "electricity production, wind, 1-3MW turbine, onshore",
                        "adapter": "brightway-adapter",
                        "config": {"code": "ed3da88fc23311ee183e9ffd376de89b"},
                    },
                    {
                        "name": "electricity production, wind, 1-3MW turbine, offshore",
                        "adapter": "brightway-adapter",
                        "config": {"code": "6ebfe52dc3ef5b4d35bb603b03559023"},
                    },
                ],
            },
            {
                "name": "solar",
                "aggregator": "sum",
                "children": [
                    {
                        "name": "electricity production, solar tower power plant, 20 MW",
                        "adapter": "bw",
                        "config": {"code": "f2700b2ffcb6b32143a6f95d9cca1721"},
                    },
                    {
                        "name": "electricity production, solar thermal parabolic trough, 50 MW",
                        "adapter": "bw",
                        "config": {"code": "19040cdacdbf038e2f6ad59814f7a9ed"},
                    },
                ],
            },
        ],
    }

    if num_scenarios > 2:
        print(
            "note that all experiments beyond the first 2 ones, have randomized outputs"
        )
    default_scenarios = [
        {
            "name": "scenario 1",
            "nodes": {
                "electricity production, wind, 1-3MW turbine, onshore": {
                    "unit": "kilowatt_hour",
                    "magnitude": 4,
                },
                "electricity production, wind, 1-3MW turbine, offshore": {
                    "unit": "kilowatt_hour",
                    "magnitude": 3,
                },
                "electricity production, solar tower power plant, 20 MW": {
                    "unit": "kilowatt_hour",
                    "magnitude": 3,
                },
                "electricity production, solar thermal parabolic trough, 50 MW": {
                    "unit": "kilowatt_hour",
                    "magnitude": 4,
                },
            },
        },
        {
            "name": "scenario 2",
            "nodes": {
                "electricity production, wind, 1-3MW turbine, onshore": {
                    "unit": "kilowatt_hour",
                    "magnitude": 2,
                },
                "electricity production, wind, 1-3MW turbine, offshore": {
                    "unit": "kilowatt_hour",
                    "magnitude": 5,
                },
                "electricity production, solar tower power plant, 20 MW": {
                    "unit": "kilowatt_hour",
                    "magnitude": 4,
                },
                "electricity production, solar thermal parabolic trough, 50 MW": {
                    "unit": "kilowatt_hour",
                    "magnitude": 3,
                },
            },
        },
    ]

    scenarios: list[dict] = []

    node_names = [
        "electricity production, wind, 1-3MW turbine, onshore",
        "electricity production, wind, 1-3MW turbine, offshore",
        "electricity production, solar tower power plant, 20 MW",
        "electricity production, solar thermal parabolic trough, 50 MW",
    ]

    for idx in range(num_scenarios):
        if idx < len(default_scenarios):
            scenarios.append(default_scenarios[idx])
        else:
            scenarios.append(
                {
                    "name": f"scenario {idx + 1}",
                    "nodes": {
                        n: {"unit": "kilowatt_hour", "magnitude": randint(2, 5)}
                        for n in node_names
                    },
                }
            )

    config = {
        "adapters": [bw_adapter_config],
        "hierarchy": hierarchy,
        "scenarios": scenarios,
    }
    return config


def create_experiment(num_scenarios) -> Experiment:
    return Experiment(create_experiment_config(num_scenarios))


def get_demo_experiment(num_scenarios: int = 2) -> Experiment:
    experiment_path = Path(f"data/demo_experiment_{num_scenarios}.pickle")
    # for the adapter loader...
    from enbios.bw2 import brightway_experiment_adapter

    brightway_experiment_adapter.logger.setLevel("INFO")
    try:
        if experiment_path.exists():
            print("loading experiment from pickle file...")
            return pickle.load(experiment_path.open("rb"))
    except Exception as err:
        raise err
    print("running experiment...")
    exp = create_experiment(num_scenarios)
    exp.run()
    experiment_path.parent.mkdir(parents=True, exist_ok=True)
    pickle.dump(exp, experiment_path.open("wb"))
    return exp
