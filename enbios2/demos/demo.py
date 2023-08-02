from random import choice
from enbios2.base.experiment import Experiment
import bw2data

from enbios2.bw2.util import report
from enbios2.models.experiment_models import ExperimentData


bw2data.projects.set_current("ecoinvent")
database_name = 'cutoff_3.9.1_default'
db = bw2data.Database(database_name)

wind_turbines_spain = db.search("electricity production, wind, 1-3MW turbine, onshore", filter={"location": "ES"})
print(wind_turbines_spain)


experiment_activities = []
for activity in wind_turbines_spain:
    experiment_activities.append(
        {"id":
            {
                "name": activity["name"],
                "location": activity["location"],
                "code": activity["code"]
            }
        }
    )



experiment_activities[0]["output"] = ["kilowatt_hour", 3]
print(experiment_activities)

all_methods = list(bw2data.methods)
methods = [choice(all_methods) for _ in range(2)]
print(methods)


experiment_methods = [
    {
        "id": method
    }
    for method in methods
]


exp_data = ExperimentData(
    bw_project="ecoinvent",
    activities=experiment_activities,
    methods=experiment_methods
)


exp: Experiment = Experiment(exp_data)


# run all scenarios at once
results = exp.run()

exp.results_to_csv("test.csv")
exp.results_to_csv("test2.csv", include_method_units=False)
exp.scenarios[0].result_to_dict()
