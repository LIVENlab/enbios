from enbios.base.experiment import Experiment
import pickle

demo_config = {
    "adapters": [
        {
            "adapter_name": "brightway-adapter",
            "config": {"bw_project": "ecoinvent_391"},
            "methods": {
                "GWP1000": (
                    "ReCiPe 2016 v1.03, midpoint (H)",
                    "climate change",
                    "global warming potential (GWP1000)",
                ),
                "LOP": (
                    "ReCiPe 2016 v1.03, midpoint (E)",
                    "land use",
                    "agricultural land occupation (LOP)",
                ),
                "WCP": (
                    "ReCiPe 2016 v1.03, midpoint (E)",
                    "water use",
                    "water consumption potential (WCP)",
                ),
            },
        }
    ],
    "hierarchy": {
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
                        "config": {
                            "code": "ed3da88fc23311ee183e9ffd376de89b",
                            "default_output": {"unit": "kilowatt_hour", "magnitude": 3},
                        },
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
    },
    "scenarios": [
        {
            "name": "normal scenario",
            "nodes": {
                "electricity production, wind, 1-3MW turbine, onshore": (
                    "kilowatt_hour",
                    4,
                ),
                "electricity production, wind, 1-3MW turbine, offshore": (
                    "kilowatt_hour",
                    4,
                ),
                "electricity production, solar tower power plant, 20 MW": (
                    "kilowatt_hour",
                    4,
                ),
                "electricity production, solar thermal parabolic trough, 50 MW": (
                    "kilowatt_hour",
                    4,
                ),
            },
        },
        {
            "name": None,
            "nodes": {
                "electricity production, wind, 1-3MW turbine, onshore": (
                    "kilowatt_hour",
                    1,
                ),
                "electricity production, wind, 1-3MW turbine, offshore": (
                    "kilowatt_hour",
                    2,
                ),
                "electricity production, solar tower power plant, 20 MW": (
                    "kilowatt_hour",
                    6,
                ),
                "electricity production, solar thermal parabolic trough, 50 MW": (
                    "kilowatt_hour",
                    7,
                ),
            },
        },
        {
            "name": None,
            "nodes": {
                "electricity production, wind, 1-3MW turbine, onshore": (
                    "kilowatt_hour",
                    4,
                ),
                "electricity production, wind, 1-3MW turbine, offshore": (
                    "kilowatt_hour",
                    0,
                ),
                "electricity production, solar tower power plant, 20 MW": (
                    "kilowatt_hour",
                    3,
                ),
                "electricity production, solar thermal parabolic trough, 50 MW": (
                    "kilowatt_hour",
                    9,
                ),
            },
        },
    ],
}

exp = Experiment(demo_config)
results = exp.run()


pickle.dump(exp, open("exp.pickle", "wb"))
