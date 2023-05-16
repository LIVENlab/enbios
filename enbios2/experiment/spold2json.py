"""
convert spold files to json files and create a schema for them and a set of units.
Do a deepdiff between the schemas
"""

import json
from os import listdir
from pathlib import Path
from typing import Optional

import xmltodict
from deepdiff.diff import DeepDiff
from genson import SchemaBuilder
from tqdm import tqdm

from enbios2.const import BASE_DATA_PATH

ecoinvent_folder = BASE_DATA_PATH / "ecoinvent"
from jsonpath_ng import parse


def get_output_folder_for_spold(spold_folder: Path) -> Path:
    output_folder = spold_folder.relative_to(ecoinvent_folder)
    # should be ecoinventJson/ecoinvent 3.9.1_cutoff_ecoSpold02/datasets
    output_folder = BASE_DATA_PATH / "ecoinventJson" / output_folder
    return output_folder


def convert_folder(spold_folder: Path) -> Path:
    output_folder = get_output_folder_for_spold(spold_folder)
    output_folder.mkdir(parents=True, exist_ok=True)
    for file in tqdm(spold_folder.glob("*.spold")):
        # print(file)
        rel = file.relative_to(spold_folder)
        # print(rel)
        with open(file, "r") as f:
            data: dict = xmltodict.parse(f.read())
            # write to output_folder
            with open(output_folder / rel.with_suffix(".json"), "w", encoding="utf-8") as f2:
                f2.write(json.dumps(data))
    return output_folder


def build_schema_from_spold(data: dict, prev_builder: SchemaBuilder = None) -> SchemaBuilder:
    if prev_builder:
        builder = prev_builder
    else:
        builder = SchemaBuilder()
        builder.add_schema({"type": "object", "properties": {}})

    builder.add_object(data)
    return builder


def read_units(data: dict, prev_matches: set[str] = None) -> set[str]:
    jsonpath_expr = parse(
        '$.ecoSpold.["activityDataset", "childActivityDataset"][*].flowData.intermediateExchange[*].unitName[*]."#text"')
    res = set(match.value for match in jsonpath_expr.find(data))
    if prev_matches:
        prev_matches.update(res)
        return prev_matches
    else:
        return res


def data_iterator_funcs(output_folder: Path):
    print(output_folder)
    files = listdir(output_folder)

    unit_set = set()
    builder: Optional[SchemaBuilder] = None
    for file in tqdm(files):
        with open(output_folder / file, "r", encoding="utf-8") as f:
            data = json.loads(f.read())
            unit_set = read_units(data, unit_set)
            builder = build_schema_from_spold(data, builder)

    (output_folder.parent / "schema.json").write_text(json.dumps(builder.to_schema(), indent=2),
                                                      encoding="utf-8")
    (output_folder.parent / "units.json").write_text(json.dumps(list(unit_set), indent=2),
                                                     encoding="utf-8")


sources = ["ecoinvent 3.9.1_cutoff_ecoSpold02", "ecoinvent 3.9.1_cutoff_lcia_ecoSpold02"]

schema_map = {}
for source in sources:
    # spold_folder = ecoinvent_folder / "ecoinvent 3.9.1_cutoff_ecoSpold02/datasets"
    spold_folder = ecoinvent_folder / f"{source}/datasets"
    output_folder = get_output_folder_for_spold(spold_folder)
    if not output_folder.exists():
        output_folder = convert_folder(spold_folder)

    data_iterator_funcs(output_folder)

    # compare the 2 schemas
    diff = DeepDiff(*schema_map.values(), ignore_order=True)

