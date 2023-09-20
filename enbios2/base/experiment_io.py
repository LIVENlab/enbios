from csv import DictReader
from os import PathLike
from typing import Union, Optional

from flatten_dict import unflatten
from frictionless import Schema, Resource, validate, system
from frictionless.fields import NumberField, StringField

from enbios2.generic.files import ReadPath
from enbios2.generic.tree.basic_tree import BasicTreeNode
from enbios2.models.experiment_models import (
    ExperimentDataIO,
    ExperimentData,
    ActivitiesDataRows,
    ExperimentMethodData, ActivitiesDataTypes,
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


# methods_fields = ["alias", "id", "id0", "id1", "id2", "id3"]


def unflatten_data(data: dict, structure_map: dict):
    res = {}
    for key, value in data.items():
        if key in structure_map:
            res[structure_map[key]] = value
        else:
            res[key] = value
    print(res)
    return unflatten(res, splitter="dot")


def parse_method_row(row: dict) -> ExperimentMethodData:
    id_: list[str] = []
    if row_id := row.get("id"):
        id_ = row_id.split(",")
    else:
        for i in range(4):
            if row_id := row.get(f"id{i}"):
                id_.append(row_id)
            else:
                break
    return ExperimentMethodData(alias=row.get("alias"), id=tuple(id_))


def get_abs_path(path: Union[str, PathLike], base_dir: Optional[str] = None) -> ReadPath:
    if base_dir:
        return ReadPath(base_dir) / path
    else:
        return ReadPath(path)


def resolve_input_files(raw_input: ExperimentData):
    # activities
    if isinstance(raw_input.activities, str) or isinstance(
        raw_input.activities, PathLike
    ):

        activities_file: ReadPath = get_abs_path(
            raw_input.activities, raw_input.config.base_directory
        )
        if activities_file.suffix == ".json":
            data = activities_file.read_data()
            raw_input.activities = data
        elif activities_file.suffix == ".csv":
            with system.use_context(trusted=True):
                resource: Resource = Resource(
                    path=activities_file.as_posix(), schema=activities_schema
                )
                report = validate(resource)
                if not report.valid:
                    raise Exception(
                        f"Invalid activities file: {raw_input.activities}; "
                        f"errors: {report.task.errors}"
                    )
                raw_input.activities = ActivitiesDataRows(
                    list(
                        (
                            unflatten_data(r, activities_unflatten_map)
                            for r in resource.read_rows()
                        )
                    )
                )

    # methods
    if isinstance(raw_input.methods, str) or isinstance(raw_input.methods, PathLike):
        methods_file: ReadPath = get_abs_path(raw_input.methods)
        if methods_file.suffix == ".json":
            data = methods_file.read_data()
            raw_input.methods = data
        if methods_file.suffix == ".csv":
            with system.use_context(trusted=True):
                resource: Resource = Resource(
                    path=methods_file.as_posix(), schema=methods_schema
                )
                report = validate(resource)
                if not report.valid:
                    raise Exception(
                        f"Invalid methods file: {raw_input.methods}; "
                        f"errors: {report.task.errors}"
                    )
                raw_input.methods = list(
                    (parse_method_row(r) for r in resource.read_rows())
                )

    # hierarchy
    if isinstance(raw_input.hierarchy, str) or isinstance(raw_input.hierarchy, PathLike):
        hierarchy_file: ReadPath = get_abs_path(raw_input.hierarchy)
        if hierarchy_file.suffix == ".json":
            data = hierarchy_file.read_data()
            raw_input.hierarchy = data
        else:
            raise Exception(f"Invalid hierarchy file: {raw_input.hierarchy}")

    # scenarios
    if isinstance(raw_input.scenarios, str) or isinstance(raw_input.scenarios, PathLike):
        scenario_file: ReadPath = get_abs_path(raw_input.scenarios)
        if scenario_file.suffix == ".json":
            data = scenario_file.read_data()
            raw_input.scenario = data
        else:
            raise Exception(f"Invalid scenario file: {raw_input.scenarios}")


def read_experiment_io(data: Union[dict, ExperimentDataIO]) -> "Experiment":
    from base.experiment import Experiment

    if isinstance(data, dict):
        exp_io = ExperimentDataIO(**data)
    else:
        exp_io = data

    raw_experiment_data = {"bw_project": exp_io.bw_project}

    def get_abs_path(path: Union[str, PathLike]) -> ReadPath:
        if exp_io.config.base_directory:
            return ReadPath(exp_io.config.base_directory) / path
        else:
            return ReadPath(path)

    if isinstance(exp_io.activities, str) or isinstance(exp_io.activities, PathLike):
        activities_file: ReadPath = get_abs_path(exp_io.activities)
        if activities_file.suffix == ".json":
            data = activities_file.read_data()
            raw_experiment_data["activities"] = data
        elif activities_file.suffix == ".csv":
            with system.use_context(trusted=True):
                resource: Resource = Resource(
                    path=activities_file.as_posix(), schema=activities_schema
                )
                report = validate(resource)
                if not report.valid:
                    raise Exception(
                        f"Invalid activities file: {exp_io.activities}; "
                        f"errors: {report.task.errors}"
                    )
                raw_experiment_data["activities"] = ActivitiesDataRows(
                    list(
                        (
                            unflatten_data(r, activities_unflatten_map)
                            for r in resource.read_rows()
                        )
                    )
                )
    else:
        raw_experiment_data["activities"] = exp_io.activities

    if isinstance(exp_io.methods, str) or isinstance(exp_io.methods, PathLike):
        methods_file: ReadPath = get_abs_path(exp_io.methods)
        if methods_file.suffix == ".json":
            data = methods_file.read_data()
            raw_experiment_data["methods"] = data
        if methods_file.suffix == ".csv":
            with system.use_context(trusted=True):
                resource: Resource = Resource(
                    path=methods_file.as_posix(), schema=methods_schema
                )
                report = validate(resource)
                if not report.valid:
                    raise Exception(
                        f"Invalid methods file: {exp_io.methods}; "
                        f"errors: {report.task.errors}"
                    )
                raw_experiment_data["methods"] = list(
                    (parse_method_row(r) for r in resource.read_rows())
                )
    else:
        raw_experiment_data["methods"] = exp_io.methods

    if isinstance(exp_io.hierarchy, str) or isinstance(exp_io.hierarchy, PathLike):
        hierarchy_file: ReadPath = get_abs_path(exp_io.hierarchy)
        if hierarchy_file.suffix == ".json":
            data = hierarchy_file.read_data()
            raw_experiment_data["hierarchy"] = data
        else:
            raise Exception(f"Invalid hierarchy file: {exp_io.hierarchy}")

    if isinstance(exp_io.scenarios, str) or isinstance(exp_io.scenarios, PathLike):
        scenario_file: ReadPath = get_abs_path(exp_io.scenarios)
        if scenario_file.suffix == ".json":
            data = scenario_file.read_data()
            raw_experiment_data["scenario"] = data
        else:
            raise Exception(f"Invalid scenario file: {exp_io.scenarios}")

    return Experiment(raw_experiment_data)


if __name__ == "__main__":
    scenario_data = {
        "bw_project": "uab_bw_ei39",
        "activities_config": {"default_database": "ei391"},
        "config": {
            "base_directory": "/mnt/SSD/projects/LIVENLab/enbios2/"
            "data/test_data/experiment_separated/a/"
        },
        "activities": "single_activity.csv",
        "methods": "methods.csv",
        "hierarchy": {"energy": ["single_activity"]},
    }
    exp: ExperimentData = read_experiment_io(scenario_data)

    reader = DictReader(
        open(
            "/mnt/SSD/projects/LIVENLab/enbios2/data/templates/experiment_activity_data.csv"
        )
    )

    node = BasicTreeNode.from_compact_dict(scenario_data["hierarchy"])
