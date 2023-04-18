from typing import Any, TypeVar, Union

from enbios2.models.multi_scale_bw import BWDatabase

import importlib
import inspect
import bw2io as bi


def list_importers() -> dict[str, Union[dict[str, list[TypeVar]], dict[str, TypeVar]]]:
    """

    :return: format map to list of importers, names to importers map
    """
    package = importlib.import_module("bw2io.importers")
    format2importer: dict[str, list[TypeVar]] = {}
    name2importer: dict[str, TypeVar] = {}
    for name, obj in inspect.getmembers(package):
        if inspect.isclass(obj):
            format_value = getattr(obj, 'format', None)
            if format_value is not None:
                format2importer.setdefault(format_value, []).append(obj)
                name2importer[name] = obj
    return {"formats": format2importer, "importers": name2importer}


def get_bw_importer(bw_database: BWDatabase) -> Any:
    if bw_database.importer:
        return list_importers()["importers"][bw_database.importer]
    else:
        formats = list_importers()["formats"][bw_database.format]
        if len(formats) == 1:
            return formats[0]
        else:
            raise Exception("Multiple importers for format, please specify importer")
