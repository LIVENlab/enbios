from pint import UnitRegistry

from enbios.generic.files import DataPath

ureg = UnitRegistry()

ecoinvent_units_file_path = DataPath("ecoinvent_pint_unit_match.txt")

if not ecoinvent_units_file_path.exists():
    print(
        f"Creating 'ecoinvent_pint_unit_match' file at: "
        f"{ecoinvent_units_file_path.as_posix()}"
    )
    ecoinvent_units_file_path.touch()
    ecoinvent_units_file_path.write_text("unspecificEcoinventUnit = []\n")

ureg.load_definitions(ecoinvent_units_file_path)
# print("loaded ecoinvent units")
