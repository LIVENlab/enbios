from typing import Optional, Union

from numpy import ndarray
from pandas import DataFrame
from sklearn.preprocessing import MinMaxScaler

from enbios2.base.experiment import Experiment


class ResultsSelector:
    def __init__(
        self,
        experiment: Experiment,
        scenarios: Optional[list[str]] = None,
        methods: Optional[list[str]] = None,
    ):
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
            self.methods: list[str] = [
                m for m in self.experiment.methods.keys()
            ]  # type: ignore

        self._complete_df = None
        self._base_df = None

    @staticmethod
    def get_result_selector(
        result_selector: Union[Experiment, "ResultsSelector"],
        scenarios: Optional[list[str]] = None,
        methods: Optional[list[str]] = None,
    ) -> "ResultsSelector":
        if isinstance(result_selector, Experiment):
            return ResultsSelector(result_selector, scenarios=scenarios, methods=methods)
        return result_selector

    def check_special_df(self, df: DataFrame):
        if not self.base_df.shape == df.shape:
            raise ValueError(
                f"Dataframe shape {df.shape} does not match base dataframe shape "
                f"{self.base_df.shape}"
            )
        if not list(self.base_df.dtypes) == list(df.dtypes):
            raise ValueError(
                f"Dataframe dtypes {df.dtypes} do not match base dataframe dtypes "
                f"{self.base_df.dtypes}"
            )

    @property
    def complete_df(self) -> DataFrame:
        if self._complete_df is None:
            data = [
                {"scenario": scenario}
                | {
                    k: v
                    for k, v in self.experiment.get_scenario(
                        scenario
                    ).result_tree.data.results.items()
                }
                for scenario in self.experiment.scenario_aliases
            ]

            self._complete_df = DataFrame(data)
        return self._complete_df

    @property
    def base_df(self) -> DataFrame:
        if self._base_df is None:
            data = [
                {"scenario": scenario}
                | {
                    k: v
                    for k, v in self.experiment.get_scenario(
                        scenario
                    ).result_tree.data.results.items()
                    if k in self.methods
                }
                for scenario in self.scenarios
            ]

            self._base_df = DataFrame(data)
        return self._base_df

    def normalized_df(self, normalize_with_all_scenarios: bool = True) -> DataFrame:
        used_df = self.complete_df if normalize_with_all_scenarios else self.base_df
        columns = used_df.columns[1:]
        normalized_df = DataFrame()
        normalized_df["scenario"] = used_df["scenario"]
        for column in columns:
            normalized_df[column] = MinMaxScaler().fit_transform(
                used_df[column].to_numpy().reshape(-1, 1)
            )

        if normalize_with_all_scenarios:
            # delete all rows where scenario is not in self.scenarios
            normalized_df = normalized_df[normalized_df["scenario"].isin(self.scenarios)]
        return normalized_df

    def method_label_names(
        self, short: bool = True, include_unit: bool = True
    ) -> list[str]:
        exp_methods = self.experiment.methods
        return [
            (
                (exp_methods[method].id[-1] if short else "\n".join(exp_methods))
                + ("\n" + exp_methods[method].bw_method_unit if include_unit else "")
            )
            for method in self.methods
        ]

    def compare_to_baseline(self, baseline_data: ndarray):
        assert len(baseline_data) == len(self.methods), (
            f"Baseline data must have the same length as the number of methods "
            f"({len(self.methods)})"
        )
        # Create a copy of the original dataframe, without modifying it in place
        baseline_df = self.base_df.copy()
        # Use loc to select the columns related to methods
        # and divide them by baseline_data
        baseline_df[self.methods] = self.base_df[self.methods].div(baseline_data, axis=1)
        # Set the dtypes back to their original types
        original_dtypes = self.base_df[self.methods].dtypes
        for col, dtype in original_dtypes.items():
            # print(col, dtype)
            baseline_df[col] = baseline_df[col].astype(dtype)
        return baseline_df.reset_index(drop=True)

    def collect_tech_results(self, node_aliases: list[str]):
        df = DataFrame()
        for scenario in self.scenarios:
            scenario_results = self.experiment.get_scenario(scenario).result_tree
            for node_alias in node_aliases:
                node = scenario_results.find_subnode_by_name(node_alias)
                assert node is not None
                # add a row
                df = df._append(
                    {
                        "scenario": scenario,
                        "tech": node_alias,
                    }
                    | {
                        method: value
                        for method, value in node.data.results.items()
                        if method in self.methods
                    },
                    ignore_index=True,
                )

        return df
