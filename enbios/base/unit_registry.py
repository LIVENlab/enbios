from pint import UnitRegistry

from enbios.generic.files import DataPath

ureg = UnitRegistry()
file_loaded = False


def get_pint_units_file_path() -> DataPath:
    return DataPath("ecoinvent_pint_unit_match.txt")


def register_units():
    global file_loaded
    if file_loaded:
        return

    ecoinvent_units_file_path = get_pint_units_file_path()

    if not ecoinvent_units_file_path.exists():
        print(
            f"Creating 'ecoinvent_pint_unit_match' file at: "
            f"{ecoinvent_units_file_path.as_posix()}"
        )
        ecoinvent_units_file_path.touch()
        ecoinvent_units_file_path.write_text("unspecificEcoinventUnit = []\n")

    ureg.load_definitions(ecoinvent_units_file_path)
    file_loaded = True
