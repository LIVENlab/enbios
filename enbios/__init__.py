import importlib.metadata

from pint import UnitRegistry

from enbios.base.unit_registry import ureg

version = importlib.metadata.version('enbios')


def get_enbios_ureg() -> UnitRegistry:
    return ureg
