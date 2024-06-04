import logging
from abc import ABC
from typing import Optional, Callable, Any, Sequence

import numpy as np
from bw2calc import LCA
from bw2calc.multi_lca import InventoryMatrices
from bw2data import get_activity
from bw2data.backends import Activity, ActivityDataset

from enbios.bw2.bw_models import BWCalculationSetup
from enbios.bw2.util import split_inventory


class BaseStackedMultiLCA(ABC):
    def __init__(
            self,
            calc_setup: BWCalculationSetup,
            results_structure: np.ndarray,
            subset_labels: Optional[set[str]] = None,
            activity_label_key: Optional[str] = None,
            use_distributions: bool = False,
            method_activity_func_maps: dict[
                tuple[str, ...], dict[int, Callable[[float], float]]
            ] = {},
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
        self.supply_arrays: list = []
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

        if subset_labels:
            self.subset_labels = subset_labels
            self.calc_subsets_results = True
            assert activity_label_key is not None
            self.subset_label_map: dict[str, list[int]] = self.resolve_subsets(
                activity_label_key
            )
            self.subset_mainloop()
        else:
            self.main_loop()

    def main_loop(self):
        for row, func_unit in enumerate(self.func_units):
            self.prep_demand(row, func_unit)

            for col, cf_matrix in enumerate(self.method_matrices):
                # logger.debug(f"Method {col}/{len(self.method_matrices)}")
                self.lca.characterization_matrix = cf_matrix
                self.lcia_calculation(self.non_linear_methods_flags[col])
                self.results[row, col] = self.lca.score

        self.inventory = InventoryMatrices(self.lca.biosphere_matrix, self.supply_arrays)

    def subset_mainloop(self):
        for row, func_unit in enumerate(self.func_units):
            self.prep_demand(row, func_unit)
            for col, cf_matrix in enumerate(self.method_matrices):
                self.lca.characterization_matrix = cf_matrix

                for loc_idx, subset in enumerate(self.subset_labels):
                    if subset not in self.subset_label_map:
                        from enbios.bw2.brightway_experiment_adapter import BrightwayAdapter
                        BrightwayAdapter.get_logger().error(f"Subset '{subset}' not found! Skipped. Results will be 0")
                        continue
                    activity_ids = self.subset_label_map[subset]
                    # todo, this is a bw_utils method split_inventory
                    regional_characterized_inventory = self.lcia_calculation(
                        self.non_linear_methods_flags[col],
                        split_inventory(self.lca, activity_ids),
                    )
                    self.results[
                        row, col, loc_idx
                    ] = regional_characterized_inventory.sum()
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

    def resolve_subsets(self, activity_label_key: str):
        # final division -> id
        base_div_map: dict[str, list[int]] = {}
        # all other indices to last group
        div_tree: list[dict] = []
        for a in ActivityDataset.select(ActivityDataset).where(
                ActivityDataset.type == "process"
        ):
            # if a.type == "process":
            loc: Sequence[str] = a.data.get(activity_label_key)
            if not isinstance(loc, tuple) and not isinstance(loc, list):
                continue
            final_subgroups = loc[-1]
            base_div_map.setdefault(final_subgroups, []).append(a.id)
            # make tree list at least as long as length
            for idx, rest in enumerate(loc[:-1]):
                if len(div_tree) <= idx:
                    div_tree.append({})
                # set location default and add location
                div_tree[idx].setdefault(rest, set()).add(loc[idx + 1])
        div_tree.reverse()
        for level in div_tree:
            for group_, sub_group in level.items():
                base_div_map.setdefault(group_, [])
                for sub_loc in sub_group:
                    base_div_map[group_].extend(base_div_map[sub_loc])

        return base_div_map
