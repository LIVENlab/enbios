from pint import Quantity


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
