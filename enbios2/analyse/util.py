import json
from dataclasses import dataclass

from enbios2.analyse.const import indicator_map_file
from enbios2.generic.util import generate_levensthein_dict_map


@dataclass
class Indicator:
    name: str
    description: str
    unit: str
    abbr: str


def generate_indicator_map(indicator_names: list[str]) -> dict[str, dict]:
    indicators: list[dict[str, str]] = json.loads(indicator_map_file.read_text(encoding="utf-8"))
    map = generate_levensthein_dict_map(indicator_names, indicators, "name")
    # now create a map
    print(map)
    return map
