from typing import Optional, Union

from numpy import ndarray
from pandas import DataFrame
from sklearn.preprocessing import MinMaxScaler

from enbios2.base.experiment import Experiment


class ResultsSelector:

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
            self.methods: list[str] = [m for m in self.experiment.methods.keys()]  # type: ignore

        self._base_df = None
        self._normalized_df = None

    @staticmethod
    def get_result_selector(result_selector: Union[Experiment, "ResultsSelector"],
                            scenarios: Optional[list[str]] = None,
                            methods: Optional[list[str]] = None) -> "ResultsSelector":
        if isinstance(result_selector, Experiment):
            return ResultsSelector(result_selector, scenarios=scenarios, methods=methods)
        return result_selector

    @property
    def base_df(self) -> DataFrame:
        if self._base_df is None:
            data = [{"scenario": scenario} |
                    {k: v for k, v in self.experiment.get_scenario(scenario).result_tree.data.results.items() if
                     k in self.methods}
                    for scenario in self.scenarios]

            self._base_df = DataFrame(data)
        return self._base_df

    @property
    def normalized_df(self) -> DataFrame:
        if self._normalized_df is None:
            scaler = MinMaxScaler()
            values = self.base_df.columns[1:]
            normalized_df = DataFrame()
            # copy the scenario column
            normalized_df['scenario'] = self.base_df['scenario']
            for value in values:
                normalized_df[value] = scaler.fit_transform(self.base_df[value].to_numpy().reshape(-1, 1))
            self._normalized_df = normalized_df
        return self._normalized_df

    def short_method_names(self) -> list[str]:
        return [self.experiment.methods[l].id[-1] for l in self.methods]

    def compare_to_baseline(self, baseline_data: ndarray):
        assert len(baseline_data) == len(self.methods)
        # Create a copy of the original dataframe, without modifying it in place
        baseline_df = self.base_df.copy()
        # Use loc to select the columns related to methods and divide them by baseline_data
        baseline_df[self.methods] = self.base_df[self.methods].div(baseline_data, axis=1)
        return baseline_df

    def collect_tech_results(self, node_aliases: list[str]):
        df = DataFrame()
        for scenario in self.scenarios:
            scenario_results = self.experiment.get_scenario(scenario).result_tree
            for node_alias in node_aliases:
                node = scenario_results.find_subnode_by_name(node_alias)
                assert node is not None
                # add a row
                df = df._append({
                                    "scenario": scenario,
                                    "tech": node_alias,
                                } | {method: value for method, value in node.data.results.items() if
                                     method in self.methods},
                                ignore_index=True)

        return df
