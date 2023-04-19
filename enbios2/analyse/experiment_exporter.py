import json
import re
from csv import DictWriter
from typing import Dict, Union, List

from const import processor_col, value_col


class ExperimentExporter:

    def __init__(self, experiment: "Experiment"):
        self.experiment = experiment

    def build_simple_tree(self, scenario: str, indicator: str, save: bool = True) -> Dict[str, any]:
        """
        builds a simple tree with the structure of each node:
        {name: full-process-name, children: {short-name: <child-node>}
        :param scenario:
        :param indicator:
        :param save:
        :return:
        """
        data = self.experiment.get_data(scenario, indicator)
        tree = {}
        for row in data:
            # get the value in column called processor_col
            proc = row[processor_col]
            proc_tuple = tuple(proc.split("."))
            if len(proc_tuple) == 1:
                tree[proc] = {"name": proc, "children": {}, "value": float(row[value_col])}
            else:
                # find all parent nodes starting from the root
                parent = tree
                for i in range(len(proc_tuple) - 1):
                    parent = parent[proc_tuple[i]]["children"]
                parent[proc_tuple[-1]] = {"name": proc, "children": {}, "value": float(row[value_col])}

        if save:
            self.experiment.experiment_path.joinpath("results", "simple_trees").mkdir(exist_ok=True)
            indicator_name = self.experiment.indicator_info[indicator]["abbre"]
            file_path = self.experiment.experiment_path.joinpath("results",
                                                                 "simple_trees",
                                                                 f"{scenario}_{indicator_name}.json")
            json.dump(tree, file_path.open("w", encoding="utf-8"), indent=2, ensure_ascii=False)
        return tree

    def d3sanki(self, scenario: str, indicator: str, save: bool = True) -> List[Dict[str, Union[str, float]]]:
        """
        creates a csv file with the following columns:
        source	target	value

        :param scenario:
        :param indicator:
        :param save: save to file
        :return: result data
        """
        data = self.experiment.get_data(scenario, indicator)
        # data = [
        #     {"Processor": "A", "Value": 5},
        #     {"Processor": "A.A", "Value": 4},
        #     {"Processor": "A.B", "Value": 1},
        #     {"Processor": "A.A.A", "Value": 2},
        #     {"Processor": "A.A.B", "Value": 1},
        #     {"Processor": "A.A.C", "Value": 1},
        # ]

        # print(data)
        result_data = []
        for row in data:
            proc = row[processor_col]
            proc_tuple = proc.split(".")
            if len(proc_tuple) > 1:
                result_node = {
                    "source": proc_tuple[-2],
                    "target": proc_tuple[-1],
                    "target_level": len(proc_tuple) - 1,
                    "value": float(row[value_col])
                }
                result_data.append(result_node)
        # print(json.dumps(result_data, indent=2))
        if save:
            dir = self.experiment.experiment_path.joinpath("results", "d3sanki")
            dir.mkdir(exist_ok=True)

            indicator_abbre = indicator[1:] if indicator.startswith("_") else  self.experiment.indicator_info[indicator]["abbre"]
            writer = DictWriter(dir.joinpath(f"{scenario}_{indicator_abbre}.csv").open("w", encoding="utf-8"),
                                fieldnames=["source", "target", "value", "target_level"])
            writer.writeheader()
            writer.writerows(result_data)
        return result_data

    def scenario_name_generator(self, scenarios: List[str]):
        """
        This is some very specific code, for scenarios in the decades after 2000
        :param scenarios:
        :return:
        """
        # scenario_splits = [s.split("_") for s in scenarios]
        # scenario_types = set([s[0] for s in scenario_splits])
        sce_grouped = {}
        for sce in scenarios:
            sce_type, sce_year = sce.split("_")
            sce_grouped.setdefault(sce_type, []).append(sce_year[2:])
        # create a string for each scenario type
        sce_strs = []
        for sce_type, sce_years in sce_grouped.items():
            sce_strs.append(f"{sce_type[:5]}_{'_'.join(sce_years)}")
        return "_".join(sce_strs)

    def processor_name_generator(self, processor: str):
        proc_parts = processor.split(".")
        if len(proc_parts) == 1:
            return processor
        return "_".join(proc_parts[1:])
