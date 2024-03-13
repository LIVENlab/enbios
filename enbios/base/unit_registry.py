from decimal import Decimal
from typing import Union

from pint import UnitRegistry, Quantity

from enbios.generic.files import DataPath

ureg_decimal = UnitRegistry(non_int_type=Decimal)
ureg_float = UnitRegistry()

ecoinvent_units_file_path = DataPath("ecoinvent_pint_unit_match.txt")

if not ecoinvent_units_file_path.exists():
    print(
        f"Creating 'ecoinvent_pint_unit_match' file at: "
        f"{ecoinvent_units_file_path.as_posix()}"
    )
    ecoinvent_units_file_path.touch()
    ecoinvent_units_file_path.write_text("unspecificEcoinventUnit = []\n")

ureg_decimal.load_definitions(ecoinvent_units_file_path)


# print("loaded ecoinvent units")

def flexible_parse(unit: str, quantity: Union[int, float]) -> Quantity:
    try:
        return ureg_decimal.parse_expression(unit) * quantity
    except TypeError as err:
        return ureg_float.parse_expression(unit) * quantity
