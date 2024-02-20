import json

import bw2data

from enbios.base.experiment import Experiment

# select the brightway project and database (e.g. some ecoinvent database)
PROJECT_NAME = "ecoinvent_391"
DATABASE = "ecoinvent_391_cutoff"

bw2data.projects.set_current(PROJECT_NAME)
db = bw2data.Database(DATABASE)

wind_turbines_spain = db.search(
    "electricity production, wind, 1-3MW turbine, onshore", filter={"location": "ES"}
)[:2]


# Now we use those, to define 2 leaf-nodes in our hierarchy.
experiment_activities = []

for activity in wind_turbines_spain:
    experiment_activities.append(
        {
            "name": activity["name"],
            "adapter": "brightway-adapter",
            "config": {"code": activity["code"]},
        }
    )

# we can modify the output of the activities, by default it is the reference product (1 of the activity unit)
experiment_activities[0]["config"]["default_output"] = {
    "unit": "kilowatt_hour",
    "magnitude": 3,
}


hierarchy = {
    "name": "root",
    "aggregator": "sum-aggregator",
    "children": experiment_activities,
}

# alternatively, we could just specify two methods
experiment_methods = {
    "GWP1000": (
        "ReCiPe 2016 v1.03, midpoint (H)",
        "climate change",
        "global warming potential (GWP1000)",
    ),
    "FETP": (
        "ReCiPe 2016 v1.03, midpoint (H)",
        "ecotoxicity: freshwater",
        "freshwater ecotoxicity potential (FETP)",
    ),
}

# let's store the raw data, because we want to modify it later
simple_raw_data = {
    "adapters": [
        {
            "adapter_name": "brightway-adapter",
            "config": {"bw_project": PROJECT_NAME},
            "methods": experiment_methods,
        }
    ],
    "hierarchy": hierarchy,
}

# create experiment object. This will validate the activities, their outputs, the methods and the scenarios.
simple_experiment: Experiment = Experiment(simple_raw_data)

results = simple_experiment.run()
print(json.dumps(results, indent=2))
