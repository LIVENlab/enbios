import re
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Optional

from enbios2.generic.files import DataPath


def parse_xml(file_path: Path, tag: str, attributes: list[str], value_tag: Optional[list[str]] = None) -> list[
    dict[str, Optional[str]]]:
    def get_namespace(element):
        m = re.match('\{.*\}', element.tag)
        return m.group(0) if m else ''

    text = file_path.read_text(encoding="utf-8")

    root = ET.fromstring(text)
    ns = get_namespace(root)
    result = []

    for elem in root.iterfind(f".//{ns}{tag}"):
        # print(elem)
        data = {attr: elem.attrib.get(attr, None) for attr in attributes}
        # print(elem, elem.attrib, value_tag)
        if value_tag:
            for vt in value_tag:
                value_elem = elem.find(f"{ns}{vt}")
                # print(value_elem)
                if value_elem is not None:
                    data[vt] = value_elem.text
        result.append(data)

    return result


if __name__ == "__main__":
    data_path: DataPath = DataPath("ecoinvent/ecoinvent 3.9.1_cutoff_ecoSpold02/MasterData/Units.xml")
    res = parse_xml(data_path, "unit", [], ["name"])
