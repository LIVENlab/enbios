from os import PathLike
from typing import Union, Optional

from flatten_dict import unflatten
from frictionless import Schema, Resource, validate, system
from frictionless.fields import NumberField, StringField


from enbios2.generic.files import ReadPath
from enbios2.models.experiment_models import (
    ExperimentData,
    ActivitiesDataRows,
    ExperimentMethodData,
)
from enbios2.generic.tree.csv2dict import csv_tree2dict

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
        methods_file: ReadPath = get_abs_path(
            raw_input.methods, raw_input.config.base_directory
        )
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
        hierarchy_file: ReadPath = get_abs_path(
            raw_input.hierarchy, raw_input.config.base_directory
        )
        if hierarchy_file.suffix == ".json":
            data = hierarchy_file.read_data()
            raw_input.hierarchy = data
        elif hierarchy_file.suffix == ".csv":
            data = csv_tree2dict(hierarchy_file, False)
            raw_input.hierarchy = data
        else:
            raise Exception(f"Invalid hierarchy file: {raw_input.hierarchy}")

    # scenarios
    if isinstance(raw_input.scenarios, str) or isinstance(raw_input.scenarios, PathLike):
        scenario_file: ReadPath = get_abs_path(
            raw_input.scenarios, raw_input.config.base_directory
        )
        if scenario_file.suffix == ".json":
            data = scenario_file.read_data()
            raw_input.scenario = data
        else:
            raise Exception(f"Invalid scenario file: {raw_input.scenarios}")
