"""
Plot Enbios results
"""
import json
import logging
from csv import DictReader
from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Union, List, Dict, Tuple, Literal

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from pandas import DataFrame

from .const import scenario_col, processor_col, value_col, level_col, ABSOLUTE, RELATIVE, INDICATOR, PROCESSOR, \
    SCENARIO, \
    indicator_col, indicator_map_file
from experiment_exporter import ExperimentExporter
from .util import generate_indicator_map


@dataclass
class ColumnMapper:
    scenario: str
    processor: str
    indicator: str
    value: str


class Experiment:
    """
    Enbios experiment result-plotter class
    """

    def __init__(self, experiment_file: str,
                 column_mapper: ColumnMapper,
                 force_redo_split: bool = False,
                 auto_indicator_map: bool = True):
        """
        Initiate a ENBIOS experiment result analysis with a result file (duplicates should be removed)
        It will create a folder with the same name as the experiment-result file (and in the same folder)
        Upon first time, it will split the result file into scneario-based files and scenario-indicator based files.
        (which are used for plotting)
        :param experiment_file: (filtered) enbios result file
        :param force_redo_split: remove and redo splitting
        :param auto_indicator_map: try to create indicator map automatically using 
        """
        self.file_path = Path(experiment_file)
        self.column_mapper = column_mapper

        self.experiment_path = self.file_path.parent.joinpath(self.file_path.stem)

        if not self.file_path.exists():
            raise ValueError(f"File {self.file_path} does not exist")

        clean_file = self.file_path.parent.joinpath(self.file_path.stem + "_clean.csv")
        if clean_file.exists():
            self.file_path = clean_file
            self.use_clean = True

        self.complete_df = pd.read_csv(self.file_path, encoding="utf-8")
        self.exporter = ExperimentExporter(self)

        if auto_indicator_map:
            df_indicators: list[str] = list(self.complete_df[self.column_mapper.indicator].unique())
            self.indicator_map = generate_indicator_map(df_indicators)
        else:
            self.indicator_map: dict[str, dict] = {}

        # self.indicator_info = self.create_indicator_map(indicator_map_file)

        self.indicator_info: Dict[str, Dict[str, str]] = json.load(
            self.experiment_path.joinpath(indicator_map_file).open(encoding="utf-8"))

        # self.abbreviationReverseMap = {v["abbre"]: k for k, v in self.indicator_info.items()}

        # if force_redo_split:
        #     shutil.rmtree(self.experiment_path.joinpath("split-scenario"), ignore_errors=True)
        #     shutil.rmtree(self.experiment_path.joinpath("split-scenario-indicator"), ignore_errors=True)
        if self.setup_folder() or force_redo_split:
            self.remove_duplicates()
            # self._split_experiment()

    def col(self, col: Literal["scenario", "processor", "indicator", "value"]) -> str:
        return getattr(self.column_mapper, col)

    def setup_folder(self) -> bool:
        """
        creates all required directory.
        :return: true if the split folders were newly created
        """
        self.experiment_path.mkdir(exist_ok=True)
        self.experiment_path.joinpath("results").mkdir(exist_ok=True)
        newly_created_split_dirs = (not self.experiment_path.joinpath("split-scenario").exists()
                                    or not self.experiment_path.joinpath("split-scenario-indicator"))
        self.experiment_path.joinpath("split-scenario").mkdir(exist_ok=True)
        self.experiment_path.joinpath("split-scenario-indicator").mkdir(exist_ok=True)
        return newly_created_split_dirs

    def remove_duplicates(self):
        """
        Remove Duplicates, identifies by the Column Scenario, Processor, Indicator, Value

        :return:
        """
        print("Removing duplicates")
        df = self.get_complete_df()
        orig_len = len(df)
        df.drop_duplicates(subset=[scenario_col, processor_col, indicator_col, value_col], inplace=True)
        new_len = len(df)
        print(f"Removed {orig_len - new_len} duplicates from {orig_len} rows")
        new_name = self.file_path.stem + "_no_dupl.csv"
        df.to_csv(new_name, index=False)

    def create_indicator_map(self, indicator_map_path: Path) -> List[Dict[str, str]]:
        """
        create the info file, that contains , if possible abbreviation of the indicator
        :param indicator_map_path:
        :return:
        """
        complete_df = self.get_complete_df()
        # select all unique values in the indicator column
        indicators = complete_df[indicator_col].unique()
        result: Dict[str, Dict[str, str]] = {}
        for indicator in indicators:
            parts = indicator.split("_")
            if len(parts) > 1:
                abbre = parts[-1]
            else:
                print("No abbreviation found for indicator", indicator)
            result[indicator] = {"abbre": abbre}
        json.dump(result, indicator_map_path.open("w", encoding="utf-8"), indent=2)
        return result

    def _select_file(self, scenario: str, indicator: Optional[str] = None) -> Path:
        """
        select a Path (scenario) or (scenario-indicator), and throw an error if the file does not exists

        :param scenario: scenario-name
        :param indicator:  if indicator starts with "_" it should be the abbreviation of the indicator
        :return: Path of the file
        """
        if indicator:
            if indicator.startswith("_"):
                indicator_abbreviation = indicator[1:]
            else:
                indicator_abbreviation = self.indicator_info[indicator]["abbre"]
            path = self.experiment_path.joinpath("split-scenario-indicator", f"{scenario}_{indicator_abbreviation}.csv")
        else:
            path = self.experiment_path.joinpath("split-scenario", f"{scenario}.csv")
        if not path.exists():
            raise ValueError(f"File {path} does not exist")

        return path

    def get_data(self, df: DataFrame, filters: dict[str, any]) -> DataFrame:
        """

        :return: a list of dictionaries, with the columns as keys
        """
        if not filters:
            return df
        mask = True
        for column, value in filters.items():
            mask &= (df[column] == value)
        return df[mask]

    def get_complete_df(self):
        """
        get the complete dataframe of the experiment
        :return: pandas dataframe
        """
        if self.complete_df is None:
            data: List[Dict[str, any]] = list(DictReader(self.file_path.open(encoding="utf-8")))
            self.complete_df = pd.DataFrame(data)
        return self.complete_df

    # def _split_experiment(self):
    #     """
    #     Split the experiment into scenario and scenario-indicator files (alls csvs)
    #     within the experiment directory
    #     """
    #     complete_df = self.get_complete_df()
    #
    #     # group by scenario
    #     scenarios = complete_df.groupby(scenario_col)
    #     # get the name of each group
    #     scenarios.groups.keys()
    #     # write each group into its own csv file with the name of the group
    #     for scenario, group in scenarios:
    #         scenario_fp = self.experiment_path.joinpath("split-scenario", f"{scenario}.csv")
    #         group.to_csv(str(scenario_fp), index=False)
    #         # split each group further by the "Indicator" column
    #         indicators = group.groupby("Indicator")
    #         # write each indicator group into a file in "split-scenario-indicator" folder
    #         for indicator_name, indicator_group in indicators:
    #             indicator_abbreviation = self.indicator_info[str(indicator_name)]["abbre"]
    #             sce_ind_fp = self.experiment_path.joinpath("split-scenario-indicator",
    #                                                        f"{scenario}_{indicator_abbreviation}.csv")
    #             indicator_group.to_csv(str(sce_ind_fp), index=False)
    #
    #     def dumpToExp(filename, data_: any):
    #         with self.experiment_path.joinpath(filename).open("w", encoding="utf-8") as fout:
    #             json.dump(data_, fout, indent=2)
    #
    #     dumpToExp("scenarios.json", complete_df[scenario_col].unique().tolist())
    #     dumpToExp("indicators.json", complete_df["Indicator"].unique().tolist())
    #     dumpToExp("processor.json", complete_df[processor_col].unique().tolist())

    def get_scenario_indicator_data(self,
                                    scenario: str,
                                    indicator: str,
                                    processor_name: Union[str, tuple, List]) -> pd.DataFrame:
        """
        Get a pandas dataframe for scenario-indicator-processor_name group.
        It will select all sub-processors and the next lower level.
        the resulting dataframe also includes a "relative" column with the values relative to the total impact
        :param scenario:
        :param indicator:
        :param processor_name: a string (sub-processes seperated by "." or a
            list/tuple of processors within the hierarchy
        :return: a pandas dataframe
        """
        data = self.get_data(scenario, indicator)
        complete_df = pd.DataFrame(data)
        # covert column "Value" to float
        complete_df[value_col] = complete_df[value_col].astype(float)
        # convert column "Dendrogram level" to int
        complete_df[level_col] = complete_df[level_col].astype(int)

        # split the df Processor column by "." and add the result into columns named
        # "level_x" where x is the level starting from 0.
        complete_df = complete_df.join(complete_df[processor_col].str.split(".", expand=True).add_prefix("level_"))
        # split the processor_name into a tuple by "."
        if isinstance(processor_name, str):
            processor_name = tuple(processor_name.split("."))

        # filter the df by the processor_name
        df = complete_df.copy()
        for i, name in enumerate(processor_name):
            df = df[df[f"level_{i}"] == name]

        # get the rows of the next level bytes filtering level_col column
        df = df[df[level_col] == len(processor_name)]
        # sum up the values the same level as the processor_name
        # others = complete_df[complete_df[level_col] == len(processor_name) - 2]
        # we don't need that. just get the total sum at the top level
        total_value = float(complete_df[complete_df[level_col] == 0][value_col])
        # print(total_value)
        # convert values in Value to float
        df[value_col] = df[value_col].astype(float)

        # throw out all columns but "level_x" and value_col
        df = df[[f"level_{len(processor_name)}", value_col]]
        # rename the "level_x" column to "Technology"
        df = df.rename(columns={f"level_{len(processor_name)}": "Technology"})
        # all a column relative to the total value
        df["relative"] = df[value_col] / total_value
        return df

    def plot_impacts(self,
                     scenarios: Union[str, List[str]],
                     indicator: str,
                     processor_name: Union[str, tuple, List],
                     _type: Literal["absolute", "relative"] = ABSOLUTE,
                     *,
                     yscale_max: float = None,
                     indicator_plot: Tuple[plt.Figure, plt.Axes, str] = None,
                     save: bool = True,
                     show: bool = False) -> Tuple[plt.Figure, plt.Axes, str]:
        """
        Create plots for a scenarios, indicator, processor_name group (multiple scnearios can be included)
        Plots absolute and/or relative values.
        Plots can be displayed and/or stored (<experiment_folder>/results/plots)
        :param scenarios: a scenario or a list of scenarios
        :param indicator: an indicator
        :param processor_name: a processor name or a tuple of processor names
        :param _type:
        :param indicator_plot:
        :param yscale_max:
        :param save: save the plots
        :param show: show the plots
        :return: a tuple of tuples of figure and axe
        """
        scenarios = scenarios if isinstance(scenarios, list) else [scenarios]

        scenarios_data = {
            scenario: self.get_scenario_indicator_data(scenario, indicator, processor_name)
            for scenario in scenarios
        }
        if indicator.startswith("_"):
            indicator_abbreviation = indicator[1:]
            indicator = self.abbreviationReverseMap[indicator[1:]]
        else:
            indicator_abbreviation = self.indicator_info[indicator]["abbre"]

        if indicator_plot is not None:
            _, _, passed_indicator_abbre = indicator_plot
            if passed_indicator_abbre != indicator_abbreviation:
                logging.warning("You are comparing different indicators!")

        # put them into one dataframe with scenario as index
        # df = pd.concat(scenarios_data.values(), keys=scenarios_data.keys())

        # create a second index from the Technology column
        # df = df.set_index("Technology", append=True)

        # TABLE VERSION
        # technologies = list(list(scenarios_data.values())[0]["Technology"])

        # get unique technologies
        technologies = scenarios_data[scenarios[0]]["Technology"].unique()

        # each scenario is a dataframe. I want to get the values of each technology
        weights = {
            tech: []
            for tech in technologies
        }
        for tech in technologies:
            # print(tech)
            for scenario, data in scenarios_data.items():
                # print(scenario)
                # print(data)
                selection = data[data["Technology"] == tech]
                if selection.empty:
                    logging.warning(f" No data for '{tech}' found in scenario '{scenario}'")
                    weights[tech].append(0)
                    continue
                if _type == ABSOLUTE:
                    weights[tech].append(selection[value_col].values[0])
                else:
                    weights[tech].append(selection["relative"].values[0])
        # plot them in bar-plots, for each scenario one bar, and for each technology stacked upon:
        width = 0.6

        fig, ax, _ = indicator_plot if indicator_plot else (*plt.subplots(), indicator)

        plt.xticks(rotation=-25)
        bottom = np.zeros(len(scenarios_data.keys()))

        for tech, weight_count in weights.items():
            _ = ax.bar(scenarios_data.keys(), weight_count, width, label=tech, bottom=bottom)
            bottom += weight_count
            if yscale_max:
                ax.set_ylim(0, yscale_max)

        indicator_data = self.indicator_info[indicator]
        ax.set_title(indicator_data["name"])
        ax.legend(loc="upper left", bbox_to_anchor=(1.01, 1))
        if _type == ABSOLUTE:
            indicator_unit = self.indicator_info[indicator]["unit"]
            plt.ylabel(indicator_unit)
        else:
            plt.ylabel("relative to total impact")

        plt.tight_layout()
        # save
        if save:
            self.experiment_path.joinpath("results", "plots").mkdir(exist_ok=True)
            plot_result_folder = self.experiment_path.joinpath("results", "plots")
            scenarios_name = self.exporter.scenario_name_generator(scenarios)
            processor_short_name = self.exporter.processor_name_generator(processor_name)
            base_name = f"{scenarios_name}__{indicator_abbreviation}__{processor_short_name}"
            filename = plot_result_folder.joinpath(f"{base_name}{'_REL' if _type == RELATIVE else ''}.png")
            fig.savefig(filename)

        if show:
            plt.show()

        return fig, ax, self.indicator_info[indicator]["abbre"]
        return fig_abs, fig_rel

    # def prepare_pca(self, processor_name: Union[str, tuple, List], scenarios: Optional[List[str]] = None):
    #     # get all indicators
    #     # load the data from the scenarios
    #     if scenarios is None:
    #         df = self.get_complete_df()
    #     else:
    #         dfs = {scenario: pd.DataFrame(self.get_data(scenario)) for scenario in scenarios}
    #         # filter all dfs in the processor_col column for the processor_name
    #         df = pd.concat(dfs)
    #
    #     df_min = df.drop(['System', "Period", 'Scope', 'Unit'], axis=1)
    #     min_max_df = df_min.groupby('Indicator').agg(Min=('Value', 'min'), Max=('Value', 'max')).reset_index()
    #     merged_df = df_min.merge(min_max_df, on='Indicator', how='left')
    #
    #     # create a df, with all unique indicators as columns, and all scenarios as rows
    #     # df = pd.DataFrame()
    #     indicators = df_min["Indicator"].unique().tolist()
    #     print(indicators)
    #
    #     return merged_df
    #     # processor_name = ".".join(processor_name) if isinstance(processor_name, list) else processor_name
    #     #
    #     # # Pivot the table to have scenarios as columns, keeping only the 'Indicator' and 'Value' columns
    #     # pivot_df = df.pivot_table(index='Indicator', columns='Scenario', values='Value')
    #     #
    #     # # Calculate the 'min' and 'max' values for each indicator across all scenarios
    #     # pivot_df['min'] = pivot_df.min(axis=1)
    #     # pivot_df['max'] = pivot_df.max(axis=1)
    #     #
    #     # # Reset the index to move 'Indicator' back to a column
    #     # final_df = pivot_df.reset_index()
    #     #
    #     # # print(final_df)
    #     # return final_df
    #
    #     # merge
    #     # concat_df = pd.concat(dfs, axis=0)
    #     # min_max_df = concat_df.groupby(indicator_col).agg(MIN=('Value', 'min'), MAX=('Value', 'max')).reset_index()
    #     #
    #     # result_dfs = []
    #     # for scneario, df in dfs.items():
    #     #     merged_df = df.merge(min_max_df, on=indicator_col, how='left')
    #     #     result_dfs.append(merged_df)
    #     #
    #     # final_df = pd.concat(result_dfs, axis=0).drop_duplicates(subset=[indicator_col])
    #     #
    #     # return final_df

    # def prepare_pca_python(self, processor_name: Union[str, tuple, List], scenarios: Optional[List[str]] = None):
    #     if scenarios is None:
    #         df = self.get_complete_df()
    #     else:
    #         dfs = {scenario: pd.DataFrame(self.get_data(scenario)) for scenario in scenarios}
    #         # filter all dfs in the processor_col column for the processor_name
    #         df = pd.concat(dfs)
    #
    #     # drop all rows where column processor_name is not === processor_name
    #     processor_name = ".".join(processor_name) if isinstance(processor_name, list) else processor_name
    #     df = (df[df["Processor"] == processor_name]).drop(['System', "Period", 'Scope', 'Unit'], axis=1)
    #     df['rel'] = np.nan
    #     df["centered"] = np.nan
    #     indicators = df["Indicator"].unique().tolist()
    #
    #     @dataclass
    #     class IndMinMax:
    #         min: float = np.inf
    #         max: float = 0
    #         diff: float = np.inf
    #         values: List[float] = field(default_factory=list)
    #         avg: float = 0
    #
    #     result: Dict[str, Dict[str, Union[float,List[float]]]] = {}
    #     ind_min_max: Dict[str, IndMinMax] = {
    #         i: IndMinMax() for i in indicators
    #     }
    #
    #     for index, row in df.iterrows():
    #         indicator = row["Indicator"]
    #         value = float(row["Value"])
    #         ind_data = ind_min_max[indicator]
    #         ind_data.min = min(ind_min_max[indicator]["min"], value)
    #         ind_data.max = max(ind_min_max[indicator]["max"], value)
    #         ind_data.values.append(value)
    #         result.setdefault(row["Scenario"], {})[indicator] = value
    #
    #     # for all ind_min_max add the diff,which is max - min
    #     for min_max in ind_min_max.values():
    #         min_max["diff"] = min_max["max"] - min_max["min"]
    #         # calculate the average
    #         min_max["avg"] = sum(min_max["values"]) / len(min_max["values"])
    #
    #     for index, row in df.iterrows():
    #         # set the relative value based on the min and max values
    #         indicator = row["Indicator"]
    #         value = float(row["Value"])
    #         # write the relative value to the rel column
    #         df.at[index, "rel"] = (value - ind_min_max[indicator]["min"]) / ind_min_max[indicator]["diff"]
    #         df.at[index, "centered"] = value - ind_min_max[indicator]["avg"]
    #
    #     scenarios_values = {}
    #     for index, row in df.iterrows():
    #         scenario = row["Scenario"]
    #         indicator = row["Indicator"]
    #         centered = row["centered"]
    #         scenarios_values.setdefault(scenario, {})[indicator] = centered
    #
    #     ordered = [[val[i] for i in indicators] for val in scenarios_values.values()]
    #
    #     pca = decomposition.PCA(n_components=2)
    #     transformed = pca.fit_transform(ordered)
    #
    #     return transformed

    def get_all_names(self, type_: Literal[INDICATOR, PROCESSOR, SCENARIO]) -> List[str]:

        def readExpFile(filename: str) -> List[str]:
            return json.load(self.experiment_path.joinpath(filename).open("r", encoding="utf-8"))

        if type_ == INDICATOR:
            return readExpFile("indicators.json")
        elif type_ == PROCESSOR:
            return readExpFile("processors.json")
        elif type_ == SCENARIO:
            return readExpFile("scenarios.json")
