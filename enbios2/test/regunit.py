TEST_BW_PROJECT = "ecoinvent_391"
TEST_BW_DATABASE = "ecoinvent_391_cutoff"

data = {
    "bw_project": TEST_BW_PROJECT,
    "bw_default_database": TEST_BW_DATABASE,
    "activities": {
        "single_activity": {
            "id": {
                "name": "concentrated solar power plant construction, "
                "solar tower power plant, 20 MW",
                "code": "19978cf531d88e55aed33574e1087d78",
            },
            "output": ["unit", 1],
        }
    },
    "methods": [
        {"id": ["EDIP 2003 no LT", "non-renewable resources no LT", "zinc no LT"]}
    ],
}

# exp = Experiment(data)
# from enbios2.base.unit_registry import ureg

# from enbios2.base.unit_registry import ureg as o_ureg

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

# print(ureg.parse_expression("kilowatt hour"))
# print(ureg.parse_expression("kilowatt hour") * 1)
# print(ureg.parse_expression("kilowatt_hour") * 1)
#
#
# print(o_ureg.parse_expression("kilowatt hour"))
#
# # ureg.define('kilowatt_hour = hour * kilowatt')
# print(o_ureg.parse_expression("kilowatt hour"))
