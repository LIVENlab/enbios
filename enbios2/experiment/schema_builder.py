

import json
from os import listdir
from pathlib import Path
from typing import Optional

from deepdiff.diff import DeepDiff
from genson import SchemaBuilder


def iterative_model_build(data, builder: Optional[SchemaBuilder] = None) -> SchemaBuilder:
    """
    Build a schema from a json object. Uses existing builder if provided. Otherwise creates a new one.
    :param data:
    :param builder:
    :return:
    """
    if builder is None:
        builder = SchemaBuilder()
        builder.add_schema({"type": "object", "properties": {}})
    builder.add_object(data)
    return builder

