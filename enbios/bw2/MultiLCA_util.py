import logging
from abc import ABC
from typing import Optional, Callable, Any

import numpy as np
from bw2calc import LCA
from bw2calc.multi_lca import InventoryMatrices
from bw2data import get_activity
from bw2data.backends import Activity

from enbios.bw2.bw_models import BWCalculationSetup


class BaseStackedMultiLCA(ABC):
    def __init__(
        self,
        calc_setup: BWCalculationSetup,
        results_structure: np.ndarray,
        use_distributions: bool = False,
        method_activity_func_maps: dict[
            tuple[str, ...], dict[int, Callable[[float], float]]
        ] = None,
    ):
        self.func_units = calc_setup.inv
        self.methods = calc_setup.ia
        self.has_nonlinear_functions: bool = method_activity_func_maps is not None
        self.all = {key: 1 for func_unit in self.func_units for key in func_unit}
        self.lca = LCA(
            demand=self.all,
            method=self.methods[0],
            use_distributions=use_distributions,
        )
        self.logger = logging.getLogger(__name__)
        self.results = results_structure
        self.lca.lci()
        self.non_linear_methods_flags: list[bool] = []
        self.method_matrices = []
        self.supply_arrays = []
        self.inventory = None

        for method in self.methods:
            self.lca.switch_method(method)
            if self.has_nonlinear_functions and method in method_activity_func_maps:
                method_characterization = method_activity_func_maps[method]
                self.method_matrices.append(method_characterization)
                self.non_linear_methods_flags.append(True)
            else:
                self.method_matrices.append(self.lca.characterization_matrix)
                self.non_linear_methods_flags.append(False)

    def main_loop(self):
        for row, func_unit in enumerate(self.func_units):
            self.prep_demand(row, func_unit)

            for col, cf_matrix in enumerate(self.method_matrices):
                # logger.debug(f"Method {col}/{len(self.method_matrices)}")
                self.lca.characterization_matrix = cf_matrix
                self.lcia_calculation(self.non_linear_methods_flags[col])
                self.results[row, col] = self.lca.score

        self.inventory = InventoryMatrices(self.lca.biosphere_matrix, self.supply_arrays)

    def prep_demand(self, row: int, func_unit: dict[Activity, float]):
        self.logger.debug(f"Demand {row}/{len(self.func_units)}")
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

    def lcia_calculation(
        self, non_linear: bool = False, inventory: Optional[Any] = None
    ) -> Any:
        """The actual LCIA calculation.
        Separated from ``lcia`` to be reusable in cases where the matrices are already built, e.g. ``redo_lcia`` and Monte Carlo classes.
        """
        if inventory is None:
            inventory = self.lca.inventory
        if non_linear:
            summed_inventory = inventory.sum(1)
            # initiate cf array with 0 functions
            func_array = [lambda v: 0 for _ in range(self.lca.biosphere_matrix.shape[0])]
            # assign the proper function to the cf array
            for activity_id, matrix_idx in self.lca.dicts.biosphere.items():
                if activity_id in self.lca.characterization_matrix:
                    func_array[
                        self.lca.dicts.biosphere[activity_id]
                    ] = self.lca.characterization_matrix[activity_id]
            result = np.array([])
            for summed_row, function in zip(summed_inventory, func_array):
                result = np.append(result, function(summed_row))
            # TODO characterized_inventory should actually be m*n (m:num bioflows, n:processes)
            # here it is (1*n)
            # but we dont calc that, cuz not using the summed_inventory would probably take much longer
            self.lca.characterized_inventory = result
        else:
            self.lca.characterized_inventory = (
                self.lca.characterization_matrix * inventory
            )
        return self.lca.characterized_inventory
