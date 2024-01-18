import logging
from dataclasses import dataclass

import bw2data
import numpy as np

from bw2data import get_activity
from bw2data.backends import Activity

from bw2calc.lca import LCA
from bw2calc.utils import wrap_functional_unit

logger = logging.getLogger("bw2calc")


@dataclass
class BWCalculationSetup:
    name: str
    inv: list[dict[Activity, float]]
    ia: list[tuple[str, ...]]

    def register(self):
        bw2data.calculation_setups[self.name] = {"inv": self.inv, "ia": self.ia}


class InventoryMatrices:
    def __init__(self, biosphere_matrix, supply_arrays):
        self.biosphere_matrix = biosphere_matrix
        self.supply_arrays = supply_arrays

    def __getitem__(self, fu_index):
        if fu_index is Ellipsis:
            raise ValueError("Must specify integer indices")

        return self.biosphere_matrix * self.supply_arrays[fu_index]


class StackedMultiLCA:
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
        log_config=None,
    ):
        self.func_units = calc_setup.inv
        self.methods = calc_setup.ia
        self.lca = LCA(
            demand=self.all,
            method=self.methods[0],
            log_config=log_config,
            use_distributions=use_distributions,
        )
        logger.info(
            {
                "message": "Started MultiLCA calculation",
                "methods": list(self.methods),
                "functional units": [wrap_functional_unit(o) for o in self.func_units],
            }
        )
        self.lca.lci()
        self.method_matrices = []
        self.supply_arrays = []
        self.results = np.zeros((len(self.func_units), len(self.methods)))
        for method in self.methods:
            self.lca.switch_method(method)
            self.method_matrices.append(self.lca.characterization_matrix)

        for row, func_unit in enumerate(self.func_units):
            fu_spec, fu_demand = list(func_unit.items())[0]
            if isinstance(fu_spec, int):
                fu = {fu_spec: fu_demand}
            elif isinstance(fu_spec, Activity):
                fu = {fu[0].id: fu[1] for fu in list(func_unit.items())}
            elif isinstance(fu_spec, tuple):
                a = get_activity(fu_spec)
                fu = {a.id: fu[1] for fu in list(func_unit.items())}
            else:
                raise ValueError("Unknown functional unit type")
            self.lca.lci(fu)
            self.supply_arrays.append(self.lca.supply_array)

            for col, cf_matrix in enumerate(self.method_matrices):
                self.lca.characterization_matrix = cf_matrix
                self.lca.lcia_calculation()
                self.results[row, col] = self.lca.score

        self.inventory = InventoryMatrices(self.lca.biosphere_matrix, self.supply_arrays)

    @property
    def all(self):
        """Get all possible databases by merging all functional units"""
        return {key: 1 for func_unit in self.func_units for key in func_unit}
