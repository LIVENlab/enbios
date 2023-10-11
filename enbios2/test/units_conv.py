from enbios2.base.unit_registry import ureg


# ureg.define('Wh_from_battery = 3 * kg')  # Define a new unit
# # ureg.define('kWh_from_batteries = 3 * kg / kWh')  # Define a new unit
#
# target_quantity = ureg.parse_expression("Wh_from_batteries", case_sensitive=False) * 1
# conv = target_quantity.to("kilogram")
# print(conv)
# bw2data.projects.set_current("ecoinvent")
# db = bw2data.Database("cutoff_3.9.1_default")
# battery = db.get_node("a2634d3b6d27046ccb1d0af9c0384c64")


# conv = target_quantity.to(battery["unit"])

# print(conv)

# target_quantity_Wh = ureg.Quantity(1, 'Wh_from_batteries')
# conv_Wh = target_quantity_Wh.to("kg")
# print(f'1 Wh_from_batteries is {conv_Wh.magnitude} kg')

# target_quantity_kWh = ureg.Quantity(1, 'kWh_from_batteries')
# conv_kWh = target_quantity_kWh.to("kg")
# print(f'1 kWh_from_batteries is {conv_kWh.magnitude} kg')


# wh_w = ureg("kilohour * watt")
# kw_x_h = ureg("kilowatt * hour")
# kw_x_h.to_preferred()
# kw_h = ureg("kilowatt_hour")

us = [
    ureg("kilowatt_hour") * 1,
    ureg("kilowatt_hour") * 2,
    ureg("kilowatt_hour") * 3,
    ureg("kilowatt_hour") * 40000,
    ureg("megawatt_hour") * 3,
]
c = us[3].to_compact()
