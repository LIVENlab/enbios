from typing import Callable

def gwp_cf_double(v: float)-> float:
    return v * 2


def wpg_1000() -> dict[tuple[str, str], Callable[[float], float]]:
    return {("biosphere3",'e259263c-d1f1-449f-bb9b-73c6d0a32a00'): gwp_cf_double}
