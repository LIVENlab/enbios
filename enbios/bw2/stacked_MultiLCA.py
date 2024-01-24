import logging
from dataclasses import dataclass
from typing import Optional, Callable

import bw2data
import numpy as np
from bw2calc.lca import LCA
from bw2calc.utils import wrap_functional_unit
from bw2data import get_activity
from bw2data.backends import Activity

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
            method_activity_func_maps: dict[tuple[str,...], dict[int,Callable[[float], float]]] = None,
            logger: Optional[logging.Logger] = None,

    ):
        self.func_units = calc_setup.inv
        self.methods = calc_setup.ia
        has_nonlinear_functions: bool = method_activity_func_maps is not None
        self.lca = LCA(
            demand=self.all,
            method=self.methods[0],
            use_distributions=use_distributions,
        )
        if not logger:
            logger = logging.getLogger(__name__)
        logger.info(
            {
                "message": "Started MultiLCA calculation",
                "methods": list(self.methods),
                "functional units": [wrap_functional_unit(o) for o in self.func_units],
            }
        )
        self.lca.lci()
        non_linear_methods_flags:list[bool] = []
        self.method_matrices = []
        self.supply_arrays = []
        self.results = np.zeros((len(self.func_units), len(self.methods)))
        for method in self.methods:
            self.lca.switch_method(method)
            if has_nonlinear_functions and method in method_activity_func_maps:
                method_characterization = method_activity_func_maps[method]
                self.method_matrices.append(method_characterization)
                non_linear_methods_flags.append(True)
            else:
                self.method_matrices.append(self.lca.characterization_matrix)
                non_linear_methods_flags.append(False)

        for row, func_unit in enumerate(self.func_units):
            logger.debug(f"Demand {row}/{len(self.func_units)}")
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
                # logger.debug(f"Method {col}/{len(self.method_matrices)}")
                self.lca.characterization_matrix = cf_matrix
                self.lcia_calculation(non_linear_methods_flags[col])
                self.results[row, col] = self.lca.score

        self.inventory = InventoryMatrices(self.lca.biosphere_matrix, self.supply_arrays)

    def lcia_calculation(self, non_linear: bool = False) -> None:
        """The actual LCIA calculation.
        Separated from ``lcia`` to be reusable in cases where the matrices are already built, e.g. ``redo_lcia`` and Monte Carlo classes.
        """
        if non_linear:
            summed_inventory = self.lca.inventory.sum(1)
            func_array = [
                lambda v: 0 for _ in range(self.lca.biosphere_matrix.shape[0])
            ]

            for activity_id, matrix_idx in self.lca.dicts.biosphere.items():
                if activity_id in self.lca.characterization_matrix:
                    func_array[self.lca.dicts.biosphere[activity_id]] = self.lca.characterization_matrix[activity_id]
            result = np.array([])
            for summed_row, function in zip(summed_inventory, func_array):
                result = np.append(result, function(summed_row))
            # TODO characterized_inventory should actully be m*n (m:num bioflows, n:processes)
            # here it is (1*n)
            # but we dont calc that, cuz not using the summed_inventory would probably take much longer
            self.lca.characterized_inventory = result
        else:
            self.lca.characterized_inventory = self.lca.characterization_matrix * self.lca.inventory

    @property
    def all(self):
        """Get all possible databases by merging all functional units"""
        return {key: 1 for func_unit in self.func_units for key in func_unit}
