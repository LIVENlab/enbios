from pint import UnitRegistry

from enbios2.init_appdir import get_init_appdir

ureg = UnitRegistry()

ecoinvent_units_file_path = get_init_appdir() / "ecoinvent_pint_unit_match.txt"

if not ecoinvent_units_file_path.exists():
    print(f"Creating 'ecoinvent_pint_unit_match' file at: {ecoinvent_units_file_path.as_posix()}")
    ecoinvent_units_file_path.touch()

ureg.load_definitions(ecoinvent_units_file_path)
