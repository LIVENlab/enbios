from typing import Callable

import numpy as np

from enbios.bw2.MultiLCA_util import BaseStackedMultiLCA
from enbios.bw2.bw_models import BWCalculationSetup


class StackedMultiLCA(BaseStackedMultiLCA):
    """Wrapper class for performing LCA calculations with
    many functional units and LCIA methods.

    Needs to be passed a ``calculation_setup`` name.

    This class does not subclass the `LCA` class, and performs all
    calculations upon instantiation.

    Initialization creates `self.results`, which is a NumPy array of LCA scores,
    with rows of functional units and columns of LCIA methods.
    Ordering is the same as in the `calculation_setup`.

    """

    def __init__(
        self,
        calc_setup: BWCalculationSetup,
        use_distributions: bool = False,
        method_activity_func_maps: dict[
            tuple[str, ...], dict[int, Callable[[float], float]]
        ] = None,
    ):
        super().__init__(
            calc_setup,
            np.zeros((len(calc_setup.inv), len(calc_setup.ia))),
            use_distributions,
            method_activity_func_maps,
        )
        self.main_loop()
