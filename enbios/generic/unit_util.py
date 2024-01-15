from pint import Quantity

from enbios import ureg
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
