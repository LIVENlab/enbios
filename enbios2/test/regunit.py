from enbios2.base.unit_registry import ureg
from enbios2.bw2.extract_from_xml import parse_xml
from enbios2.generic.files import DataPath

ureg.parse_expression("kWh")

data_path: DataPath = DataPath("ecoinvent/ecoinvent 3.9.1_cutoff_ecoSpold02/MasterData/Units.xml")
res = parse_xml(data_path, "unit", [], ["name", "comment"])

for unit in res:
    u = str(unit["name"])
    try:
        p = ureg.parse_expression(u)
        print(u, " - ", p)
    except Exception as e:
        print(u, " ? ", unit["comment"])
    finally:
        print("******")