from pint import Quantity, DimensionalityError, UndefinedUnitError

from enbios.base.unit_registry import ureg
from enbios.models.experiment_base_models import NodeOutput


def compact_all_to(quantities: list[Quantity], use_min: bool = True) -> list[Quantity]:
    """
    Convert all quantities to the same unit, and return the compacted values
    :param quantities:
    :param use_min: use the unit of the smallest quantity (otherwise use the largest)
    :return: a list of compacted quantities with the same unit
    """
    base_func = min if use_min else max
    base_value = base_func([q.to_compact() for q in quantities])
    return [q.to(base_value.units) for q in quantities]


def unit_match(unit1: str, unit2: str) -> bool:
    try:
        # Attempt to convert 1 unit of the first type to the second type
        (1 * ureg(unit1)).to(ureg(unit2))
        return True
    except UndefinedUnitError as err:
        return False
    except DimensionalityError:
        # Conversion failed due to incompatible units
        return False


def get_output_in_unit(output: NodeOutput, target_unit: str) -> float:
    """
    Convert the output to a magnitude the given unit
    :param output:
    :param target_unit:
    :return:
    """
    return (
        (ureg.parse_expression(output.unit) * output.magnitude).to(target_unit).magnitude
    )
