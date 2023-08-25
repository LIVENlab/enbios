from typing import Optional

from pandas import DataFrame
from sklearn.preprocessing import MinMaxScaler

from enbios2.base.experiment import Experiment

class BaselineScaler:
    def __init__(self, baseline_value=1.0):
        self.baseline_value = baseline_value

    def fit(self, X, y=None):
        return self

    def transform(self, X, y=None):
        return [x / self.baseline_value for x in X]


class DataTransformer:

    def __init__(self, experiment: Experiment,
                 scenarios: Optional[list[str]] = None,
                 methods: Optional[list[str]] = None):
        self.experiment = experiment

        all_aliases = [sc.alias for sc in self.experiment.scenarios]
        if scenarios is not None:
            for scenario in scenarios:
                if scenario not in all_aliases:
                    raise ValueError(f"Scenario {scenario} not found in experiment")
            self.scenarios = scenarios
        else:
            self.scenarios = all_aliases

        if methods is not None:
            for method in methods:
                if method not in self.experiment.methods.keys():
                    raise ValueError(f"Method {method} not found in experiment")
            self.methods = methods
        else:
            self.methods: list[str] = [m for m in self.experiment.methods.keys()]

        self.base_df = self.results_as_df()
        self.normalized_df = self.normalize()

    def results_as_df(self, scenarios: Optional[list[str]] = None,
                      methods: Optional[list[str]] = None) -> DataFrame:

        data = [{"scenario": scenario} |
                {k: v for k, v in self.experiment.get_scenario(scenario).result_tree.data.results.items() if
                 k in self.methods}
                for scenario in self.scenarios]

        return DataFrame(data)

    def normalize(self) -> DataFrame:
        scaler = MinMaxScaler()
        values = self.base_df.columns[1:]
        noramlized_df = DataFrame()
        # copy the scenario column
        noramlized_df['scenario'] = self.base_df['scenario']
        for value in values:
            noramlized_df[value] = scaler.fit_transform(self.base_df[value].to_numpy().reshape(-1, 1))
        return noramlized_df
