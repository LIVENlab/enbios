from os import PathLike
from typing import Union, Optional

from flatten_dict import unflatten
from frictionless import Schema
from frictionless.fields import NumberField, StringField

from enbios.generic.files import ReadPath
from enbios.models.experiment_base_models import (
    ExperimentData,
    ExperimentHierarchyNodeData,
    ExperimentScenarioData,
    # ExperimentMethodData,
)

activities_schema = Schema(
    fields=[
        StringField(name="alias"),
        StringField(name="database"),
        StringField(name="code"),
        StringField(name="name"),
        StringField(name="location"),
        StringField(name="unit"),
        NumberField(name="output.value", float_number=True),
        StringField(name="output.unit"),
    ]
)

activities_unflatten_map = {
    "alias": "id.alias",
    "database": "id.database",
    "code": "id.code",
    "name": "id.name",
    "location": "id.location",
    "unit": "id.unit",
    "output.value": "output.value",
    "output.unit": "output.unit",
}

methods_schema = Schema(
    fields=[
        StringField(name="alias"),
        StringField(name="id"),
        StringField(name="id0"),
        StringField(name="id1"),
        StringField(name="id2"),
        StringField(name="id3"),
    ]
)


def unflatten_data(data: dict, structure_map: dict):
    res = {}
    for key, value in data.items():
        if key in structure_map:
            res[structure_map[key]] = value
        else:
            res[key] = value
    print(res)
    return unflatten(res, splitter="dot")


def get_abs_path(path: Union[str, PathLike], base_dir: Optional[str] = None) -> ReadPath:
    if base_dir:
        return ReadPath(base_dir) / path
    else:
        return ReadPath(path)


def resolve_input_files(raw_input: ExperimentData):
    # hierarchy
    if isinstance(raw_input.hierarchy, str) or isinstance(raw_input.hierarchy, PathLike):
        hierarchy_file: ReadPath = get_abs_path(
            raw_input.hierarchy, raw_input.config.base_directory
        )
        if hierarchy_file.suffix == ".json":
            hierarchy_data = hierarchy_file.read_data()
        # elif hierarchy_file.suffix == ".csv":
        #     hierarchy_data = csv_tree2dict(hierarchy_file, False)
        else:
            raise Exception(f"Invalid hierarchy file: {raw_input.hierarchy}")

        raw_input.hierarchy = ExperimentHierarchyNodeData(**hierarchy_data)

    # scenarios
    if isinstance(raw_input.scenarios, str) or isinstance(raw_input.scenarios, PathLike):
        scenario_file: ReadPath = get_abs_path(
            raw_input.scenarios, raw_input.config.base_directory
        )
        if scenario_file.suffix == ".json":
            scenario_data = scenario_file.read_data()
            raw_input.scenarios = [
                ExperimentScenarioData(**scenario) for scenario in scenario_data
            ]
        else:
            raise Exception(f"Invalid scenario file: {raw_input.scenarios}")
