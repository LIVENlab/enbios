from typing import Callable


def wpg_1000() -> dict[tuple[str, str], Callable[[float], float]]:
    return {("biosphere3", "16eeda8a-1ea2-408e-ab37-2648495058dd"): lambda v: v * 1}
