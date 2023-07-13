from pint import UnitRegistry

from enbios2.bw2.extract_from_xml import parse_xml
from enbios2.generic.files import DataPath

ureg = UnitRegistry()

ecoinvent_units_file_path: DataPath = DataPath("ecoinvent_pint_unit_match.txt")

if not ecoinvent_units_file_path.exists():
    ecoinvent_units_file_path.touch()

ureg.load_definitions(ecoinvent_units_file_path)

if __name__ == "__main__":
    data_path: DataPath = DataPath("ecoinvent/ecoinvent 3.9.1_cutoff_ecoSpold02/MasterData/Units.xml")
    res = parse_xml(data_path, "unit", [], ["name", "comment"])

    for unit in res:
        u = str(unit["name"])
        try:
            p = ureg.parse_expression(u)
        except Exception as e:
            print(e)
            print(unit["comment"])
            print("******")
            continue
