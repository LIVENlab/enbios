import json
from copy import deepcopy
from pathlib import Path

import csv
from typing import Optional, Union

from enbios2.const import BASE_DATA_PATH
from deprecated import deprecated


def build_tree_from_csv(csv_file: Path, level_cols: list[str], attr_maps: dict[str, str]) -> dict:
    # Placeholder for root of the tree
    root = None

    # list to store the last node at each level
    last_nodes = []

    with open(csv_file, newline='') as csvfile:
        reader = csv.reader(csvfile)

        # get header
        header = next(reader)

        # ensure all level columns are in the header
        for col in level_cols:
            if col not in header:
                raise ValueError(f"Level column {col} not found in CSV header")

        for row in reader:
            row_dict = dict(zip(header, row))

            for level, level_col in enumerate(level_cols):
                name = row_dict[level_col]

                if name != '':
                    # new node with attributes
                    new_node = {"name": name, "children": {}}
                    attr_map = attr_maps[level] if level < len(attr_maps) else {}
                    for col, attr in attr_map.items():
                        if row_dict[col] != '':
                            new_node[attr] = row_dict[col]

                    # if this is the root node
                    if level == 0:
                        root = new_node

                    # add as child of the last node at the previous level, if there is one
                    if level > 0:
                        last_nodes[level - 1]["children"][name] = new_node

                    # this node is now the last node at this level
                    if level >= len(last_nodes):
                        last_nodes.append(new_node)
                    else:
                        last_nodes[level] = new_node

    return root


import csv


@deprecated(reason="Use BasicTreeNode.to_csv")
def tree_to_csv(root: dict, csv_file: Path, *, include_attrs=None, level_names: list[str] = None,
                merge_first_sub_row: bool = False, repeat_parent_name: bool = False):
    # Calculate max_depth based on root if not provided
    if include_attrs is None:
        include_attrs = []
    _total_level_names = level_names if level_names else []

    def level_name(level: int) -> str:
        if level >= len(_total_level_names):
            _total_level_names.append(f"lvl{level}")
        return _total_level_names[level]

    def rec_add_node_row(node: dict, current_level: int = 0) -> list[dict[str, Union[str, float]]]:
        row = {}
        for attr in include_attrs:
            row[attr] = node.get(attr, "")
        row[level_name(current_level)] = node["name"]
        _sub_rows = []
        for child in node.get("children", []):
            _sub_rows.extend(rec_add_node_row(child, current_level + 1))
        if _sub_rows:
            if merge_first_sub_row:
                row = {**row, **_sub_rows[0]}
                _sub_rows = _sub_rows[1:]
            if repeat_parent_name:
                for sub_row in _sub_rows:
                    sub_row[level_name(current_level)] = node["name"]
        return [row] + _sub_rows

    # Write rows to csv
    with csv_file.open('w', newline='') as csvfile:
        rows = rec_add_node_row(root)
        headers = _total_level_names + include_attrs
        writer = csv.DictWriter(csvfile, headers)
        writer.writeheader()
        writer.writerows(rows)

# json_file = BASE_DATA_PATH / "temp/miquel_upscaling/complete.json"
# print(json_file.exists())
# root = json.load(json_file.open())["impacts"]
# tree_to_csv({
#     "name": "root",
#     "children": {
#         "a": {
#             "name": "a",
#             "children": {
#                 "a1": {
#                     "name": "a1",
#                     "children": {}
#                 },
#                 "a2": {
#                     "name": "a2",
#                     "children": {}
#                 }
#             }
#         },
#         "b": {
#             "name": "b",
#             "children": {}}}},
#     BASE_DATA_PATH / "temp/miquel_upscaling/test.csv", ["value"], repeat_parent_name=False,
#     merge_first_sub_row=True)
