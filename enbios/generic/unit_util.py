from pint import Quantity

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
    return ureg(unit1).is_compatible_with(unit2)


def get_output_in_unit(output: NodeOutput, target_unit: str) -> float:
    """
    Convert the output to a magnitude the given unit
    :param output:
    :param target_unit:
    :return:
    """
    conversion_quant = (ureg.parse_expression(output.unit) * output.magnitude).to(
        target_unit
    )
    # experiment to avoid something like 1ML converted to 1000000.00000001
    if (
        ureg.parse_expression(output.unit) / ureg(target_unit)
    ).to_base_units().magnitude > 1e6:
        return round(conversion_quant.magnitude, 0)
    else:
        return conversion_quant.magnitude
