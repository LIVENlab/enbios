from enbios2.base.experiment import Experiment
import bw2data

from enbios2.models.experiment_models import ExperimentData


bw2data.projects.set_current("ecoinvent")
database_name = "cutoff_3.9.1_default"
db = bw2data.Database(database_name)

wind_turbines_spain = db.search(
    "electricity production, wind, 1-3MW turbine, onshore", filter={"location": "ES"}
)
print(wind_turbines_spain)


experiment_activities = []
for activity in wind_turbines_spain:
    experiment_activities.append(
        {
            "id": {
                "name": activity["name"],
                "location": activity["location"],
                "code": activity["code"],
            }
        }
    )


experiment_activities[0]["output"] = ["kilowatt_hour", 3]
print(experiment_activities)

# select 2 random methods and convert them into the form for enbios2
methods = [bw2data.methods.random() for _ in range(2)]
experiment_methods = [{"id": method} for method in methods]


# let's store the raw data, because we want to modify it later
raw_data = {
    "bw_project": "ecoinvent",
    "activities": experiment_activities,
    "methods": experiment_methods,
}

# make a first validation of the experiment data
exp_data = ExperimentData(**raw_data)

exp: Experiment = Experiment(exp_data)


# run all scenarios at once
results = exp.run()

exp.results_to_csv("test.csv")
# exp.scenarios[0].result_to_dict()


# solar_spain = db.search("solar", filter={"location": "ES"})[:2]
# raw_data["activities"].extend([
#     {
#         "id": {
#             "name": activity["name"],
#             "code": activity["code"]
#         }
#     }
#     for activity in solar_spain
# ])
#
# #%%
# hierarchy = {
#     "wind": [wind_act["name"] for wind_act in wind_turbines_spain],
#     "solar": [solar_act["name"] for solar_act in solar_spain]
# }
#
# raw_data["hierarchy"] = hierarchy
# hierarchy
#
# exp: Experiment = Experiment(raw_data)
