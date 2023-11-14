import csv
from pathlib import Path

from enbios.generic.files import PathLike


def csv_tree2dict(csv_file: PathLike, ignore_header: bool = True) -> dict:
    tree: dict = {}
    reader = csv.reader(Path(csv_file).open())
    current_node = tree
    _path: list[str] = []

    def insert(_cell: str, _current_node: dict):
        _content = _cell.strip()
        _next_node: dict = {}
        _current_node[_content] = _next_node
        _current_node = _next_node
        _path.append(_content)
        return _current_node

    if ignore_header:
        next(reader)

    for row in reader:
        for depth, cell in enumerate(row):
            if not cell:
                continue
            if cell:
                if not _path:
                    content = cell.strip()
                    current_node = {}
                    tree[content] = current_node
                    _path.append(content)
                else:
                    if depth == len(_path):
                        current_node = insert(cell, current_node)
                    elif depth < len(_path):
                        _path = _path[:depth]
                        current_node = tree
                        for p in _path:
                            current_node = current_node[p]
                        current_node = insert(cell, current_node)
                    else:
                        raise IndexError(
                            f"row: {row} has a cell: '{cell}'"
                            f" that is not positioned correctly"
                        )
    return tree
