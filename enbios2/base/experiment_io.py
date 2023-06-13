from csv import DictReader

from flatten_dict import unflatten
from frictionless import Schema, Resource, validate, system
from frictionless.fields import NumberField, StringField

from enbios2.generic.files import ReadPath
from enbios2.generic.tree.basic_tree import BasicTreeNode
from enbios2.models.experiment_models import (ExperimentDataIO, ExperimentData, ActivitiesDataRows,
                                              ExperimentMethodData, ActivitiesDataTypes, MethodsDataTypes)

activities_schema = Schema(fields=[
    StringField(name="alias"),
    StringField(name="database"),
    StringField(name="code"),
    StringField(name="name"),
    StringField(name="location"),
    StringField(name="unit"),
    NumberField(name="output.value", float_number=True),
    StringField(name="output.unit")
])

activities_unflatten_map = {
    "alias": "id.alias",
    "database": "id.database",
    "code": "id.code",
    "name": "id.name",
    "location": "id.location",
    "unit": "id.unit",
    "output.value": "output.value",
    "output.unit": "output.unit"
}

methods_schema = Schema(fields=[
    StringField(name="alias"),
    StringField(name="id"),
    StringField(name="id0"),
    StringField(name="id1"),
    StringField(name="id2"),
    StringField(name="id3")
])


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
    return ExperimentMethodData(alias=row.get("alias"), id=id_)


def read_experiment_io(data: dict) -> ExperimentData:
    exp_io = ExperimentDataIO(**data)
    experiment_data = ExperimentData(exp_io.bw_project)

    def get_abs_path(path: str) -> ReadPath:
        if exp_io.config.base_directory:
            return ReadPath(exp_io.config.base_directory) / path
        else:
            return ReadPath(path)

    if isinstance(exp_io.activities, str):
        activities_file: ReadPath = get_abs_path(exp_io.activities)
        if activities_file.suffix == ".json":
            data = activities_file.read_data()
            experiment_data.activities = data
        elif activities_file.suffix == ".csv":
            with system.use_context(trusted=True):
                resource: Resource = Resource(path=activities_file.as_posix(), schema=activities_schema)
                report = validate(resource)
                if not report.valid:
                    raise Exception(f"Invalid activities file: {exp_io.activities}; errors: {report.task.errors}")
                experiment_data.activities = ActivitiesDataRows(
                    list((unflatten_data(r, activities_unflatten_map) for r in resource.read_rows())))
    else:
        experiment_data.activities = ActivitiesDataTypes(exp_io.activities)

    if isinstance(exp_io.methods, str):
        methods_file: ReadPath = get_abs_path(exp_io.methods)
        if methods_file.suffix == ".json":
            data = methods_file.read_data()
            experiment_data.methods = data
        if methods_file.suffix == ".csv":
            with system.use_context(trusted=True):
                resource: Resource = Resource(path=methods_file.as_posix(), schema=methods_schema)
                report = validate(resource)
                if not report.valid:
                    raise Exception(f"Invalid methods file: {exp_io.methods}; errors: {report.task.errors}")
                experiment_data.methods = list((parse_method_row(r) for r in resource.read_rows()))
    else:
        experiment_data.methods = MethodsDataTypes(exp_io.methods)

    if isinstance(exp_io.hierarchy, str):
        hierarchy_file: ReadPath = get_abs_path(exp_io.hierarchy)
        if hierarchy_file.suffix == ".json":
            data = hierarchy_file.read_data()
            experiment_data.hierarchy = data
        else:
            raise Exception(f"Invalid hierarchy file: {exp_io.hierarchy}")
    return experiment_data


if __name__ == "__main__":
    scenario_data = {
        "bw_project": "uab_bw_ei39",
        "activities_config": {
            "default_database": "ei391"
        },
        "config": {
            "base_directory": "/mnt/SSD/projects/LIVENLab/enbios2/data/test_data/experiment_separated/a/"
        },
        "activities": "single_activity.csv",
        "methods": "methods.csv",
        "hierarchy": {
            "energy": [
                "single_activity"
            ]
        }
    }
    exp: ExperimentData = read_experiment_io(scenario_data)

    reader = DictReader(open("/mnt/SSD/projects/LIVENLab/enbios2/data/templates/experiment_activity_data.csv"))

    node = BasicTreeNode.from_compact_dict(scenario_data["hierarchy"])
