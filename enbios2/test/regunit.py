from enbios2.base.unit_registry import ureg
from enbios2.base.unit_registry import ureg as o_ureg

# ureg.parse_expression("kWh")

# data_path: DataPath =
# DataPath("ecoinvent/ecoinvent 3.9.1_cutoff_ecoSpold02/MasterData/Units.xml")
# res = parse_xml(data_path, "unit", [], ["name", "comment"])
#
# for unit in res:
#     u = str(unit["name"])
#     try:
#         p = ureg.parse_expression(u)
#         print(u, " - ", p)
#     except Exception as e:
#         print(u, " ? ", unit["comment"])
#     finally:
#         print("******")

print(ureg.parse_expression("kilowatt hour"))
print(ureg.parse_expression("kilowatt hour") * 1)
print(ureg.parse_expression("kilowatt_hour") * 1)


print(o_ureg.parse_expression("kilowatt hour"))

# ureg.define('kilowatt_hour = hour * kilowatt')
print(o_ureg.parse_expression("kilowatt hour"))
