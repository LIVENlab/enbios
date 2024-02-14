from typing import Optional, Union, Literal

import pandas as pd
from numpy import ndarray
from pandas import DataFrame
from sklearn.preprocessing import MinMaxScaler

from enbios.base.experiment import Experiment
from enbios.generic.enbios2_logging import get_logger

logger = get_logger(__name__)


class ResultsSelector:
    def __init__(
        self,
        experiment: Experiment,
        scenarios: Optional[list[str]] = None,
        methods: Optional[list[str]] = None,
    ):
        """
        Initialize the object with experiment, scenarios, and methods.

        :param experiment: The Experiment object.
        :param scenarios: A list of scenario names. If None, all scenario names from the experiment will be used.
        :param methods: A list of method names. If None, all method names from the experiment will be used.
        """
        self.experiment = experiment

        all_scenarios = [sc.name for sc in self.experiment.scenarios]
        if scenarios is not None:
            for scenario in scenarios:
                if scenario not in all_scenarios:
                    raise ValueError(f"Scenario {scenario} not found in experiment")
            self.scenarios: list[str] = scenarios
        else:
            self.scenarios: list[str] = all_scenarios

        all_methods: list[str] = self.experiment.method_names

        all_method_names: list[str] = self.experiment.method_names
        self.methods: list[str] = []
        self.method_names: list[str] = []
        if methods is not None:
            for idx, method in enumerate(methods):
                method_name = method.split(".")[-1]
                if method_name not in all_method_names:
                    raise ValueError(f"Method {method_name} not found in experiment")
                self.methods.append(method)
                self.method_names.append(method_name)
        else:
            self.methods = all_methods
            self.method_names = all_method_names

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
                    k: v.magnitude
                    for k, v in self.experiment.get_scenario(
                        scenario
                    ).result_tree.data.results.items()
                }
                for scenario in self.experiment.scenario_names
            ]

            self._complete_df = DataFrame(data)
        return self._complete_df

    @property
    def base_df(self) -> DataFrame:
        if self._base_df is None:
            data = [
                {"scenario": scenario}
                | {
                    k: v.magnitude
                    for k, v in self.experiment.get_scenario(
                        scenario
                    ).result_tree.data.results.items()
                    if k in self.method_names
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

    def method_label_names(self, include_unit: bool = True) -> list[str]:
        return [
            (
                method
                + ("\n" + self.experiment.get_method_unit(method) if include_unit else "")
            )
            for method in self.methods
        ]

    def compare_to_baseline(self, baseline_data: ndarray):
        assert len(baseline_data) == len(self.method_names), (
            f"Baseline data must have the same length as the number of methods "
            f"({len(self.method_names)})"
        )
        # Create a copy of the original dataframe, without modifying it in place
        baseline_df = self.base_df.copy()
        # Use loc to select the columns related to methods
        # and divide them by baseline_data
        baseline_df[self.method_names] = self.base_df[self.method_names].div(
            baseline_data, axis=1
        )
        # Set the dtypes back to their original types
        original_dtypes = self.base_df[self.method_names].dtypes
        for col, dtype in original_dtypes.items():
            # print(col, dtype)
            baseline_df[col] = baseline_df[col].astype(dtype)
        return baseline_df.reset_index(drop=True)

    def validate_node_selection(self, level: int, nodes: list[str]) -> list[str]:
        if level >= self.experiment.hierarchy_root.depth:
            logger.warning(
                f"Level {level} is higher or equal (>=) than the depth of the hierarchy "
                f"({self.experiment.hierarchy_root.depth}). "
                f"Limiting to {self.experiment.hierarchy_root.depth - 1}"
            )
            level = self.experiment.hierarchy_root.depth - 1

        if nodes:
            for node_name in nodes:
                node = self.experiment.hierarchy_root.find_subnode_by_name(node_name)
                if not node:
                    raise ValueError(f"Alias {node_name} not found in hierarchy")
        else:
            nodes = [
                n.name
                for n in self.experiment.hierarchy_root.collect_all_nodes_at_level(level)
            ]
        return nodes

    def collect_tech_results(
        self,
        nodes: list[str],
        value_name: Literal["magnitude", "multi_magnitude"] = "magnitude",
    ):
        df = DataFrame()
        for scenario in self.scenarios:
            scenario_results = self.experiment.get_scenario(scenario).result_tree
            for node_name in nodes:
                node = scenario_results.find_subnode_by_name(node_name)
                assert node is not None
                # add a row
                df = pd.concat(
                    [
                        df,
                        DataFrame(
                            [
                                {
                                    "scenario": scenario,
                                    "tech": node_name,
                                }
                                | {
                                    method: getattr(value, value_name)
                                    for method, value in node.data.results.items()
                                    if method in self.method_names
                                }
                            ]
                        ),
                    ],
                    ignore_index=True,
                    copy=False,
                )
        return df
