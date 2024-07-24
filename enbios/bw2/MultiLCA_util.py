import logging
from typing import Optional, Callable, Any, Sequence

import bw2data
import numpy as np
from bw2calc import LCA, MultiLCA
# from bw2calc.multi_lca import InventoryMatrices
from bw2data import get_activity
from bw2data.backends import Activity, ActivityDataset
from matrix_utils import SparseMatrixDict
from scipy import sparse
from scipy.sparse import csr_matrix

from enbios.bw2.bw_models import BWCalculationSetup
from enbios.bw2.util import split_inventory


def get_lca_method_matrices(calc_setup: BWCalculationSetup):
    func_units = calc_setup.inv
    methods = calc_setup.ia

    all = {key: 1 for func_unit in func_units for key in func_unit}
    lca = LCA(demand=all, method=methods[0], log_config=None)
    lca.lci()
    method_matrices = []
    supply_arrays = []
    results = np.zeros((len(func_units), len(methods)))
    for m_idx, method in enumerate(methods):
        lca.switch_method(method)
        method_matrices.append(lca.characterization_matrix)

    for row, func_unit in enumerate(func_units):
        fu_spec, fu_demand = list(func_unit.items())[0]
        if isinstance(fu_spec, int):
            fu = {fu_spec: fu_demand}
        elif isinstance(fu_spec, Activity):
            fu = {fu[0].id: fu[1] for fu in list(func_unit.items())}
        elif isinstance(fu_spec, tuple):
            a = get_activity(fu_spec)
            fu = {a.id: fu[1] for fu in list(func_unit.items())}
        lca.lci(fu)
        supply_arrays.append(lca.supply_array)

        for col, cf_matrix in enumerate(method_matrices):
            lca.characterization_matrix = cf_matrix
            lca.lcia_calculation()
            results[row, col] = lca.score


class BaseStackedMultiLCA():
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

        self.func_units = {f"e_{idx}": {act.id: demand for act, demand in acts.items()} for idx, acts in
                           enumerate(calc_setup.inv)}
        if len(self.func_units) == 1:
            if "xxx" in self.func_units:
                raise ValueError("'xxx' is RESERVED")
            self.func_units["xxx"] = self.func_units["e_0"]
        self.methods_config = {"impact_categories": calc_setup.ia}
        data_objs = bw2data.get_multilca_data_objs(functional_units=self.func_units, method_config=self.methods_config)

        self.methods = calc_setup.ia
        self.has_nonlinear_functions: bool = method_activity_func_maps is not None
        self.all = {key: 1 for func_unit in calc_setup.inv for key in func_unit}

        for method in self.methods_config["impact_categories"]:
            bw2data.Method(method).process()

        self.lca = MultiLCA(demands=self.func_units, method_config=self.methods_config, data_objs=data_objs, use_distributions=use_distributions)

        self.logger = logging.getLogger(__name__)
        self.results = results_structure
        self.lca.lci()
        self.non_linear_methods_flags: list[bool] = []
        self.method_matrices = []

        for m_idx, method in enumerate(self.methods):
            if self.has_nonlinear_functions and method in method_activity_func_maps:
                method_characterization = method_activity_func_maps[method]
                self.method_matrices.append(method_characterization)
                self.non_linear_methods_flags.append(True)
            else:
                self.method_matrices.append(None)
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
        self.lca.lci()
        self.lca.lcia()
        rows = list(self.func_units.keys())
        if rows[-1] == "xxx":
            rows.pop()
        num_cols = len(self.methods)
        ci = 0
        ri = 0
        for f_m, res in self.lca.scores.items():
            if f_m[1] == "xxx":
                continue
            self.results[ri, ci] = res
            if self.non_linear_methods_flags[ri]:
                self.results[ri, ci] = self.method_matrices
            ci += 1
            if ci == num_cols:
                ri += 1
                ci = 0

    def subset_mainloop(self):
        self.lca.lci()
        self.lca.lcia()

        for idx, subset in enumerate(self.subset_labels):
            if subset not in self.subset_label_map:
                from enbios.bw2.brightway_experiment_adapter import (
                    BrightwayAdapter,
                )

                BrightwayAdapter.get_logger().error(
                    f"Subset '{subset}' not found! Skipped. Results will be 0"
                )
                continue

            activity_ids = self.subset_label_map[subset]
            ri = 0
            ci = 0
            num_cols = len(self.methods)
            for x_x, inv in self.lca.inventories.items():
                if x_x == "xxx":
                    continue
                ixes = [self.lca.dicts.activity[i] for i in activity_ids]
                some_calc:SparseMatrixDict = self.lca.characterization_matrices @ inv[:, ixes]
                for k,v in some_calc.items():
                    self.results[ri, ci, idx] = v.sum()
                    if self.non_linear_methods_flags[ri]:
                        self.results[ri, ci] = self.method_matrices
                    ci += 1
                    if ci == num_cols:
                        ri += 1
                        ci = 0

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
                result = np.append(result, function(summed_row.item()))
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
            sub: Sequence[str] = a.data.get(activity_label_key)
            if not isinstance(sub, tuple) and not isinstance(sub, list):
                continue
            final_subgroups = sub[-1]
            base_div_map.setdefault(final_subgroups, []).append(a.id)
            # make tree list at least as long as length
            for idx, rest in enumerate(sub[:-1]):
                if len(div_tree) <= idx:
                    div_tree.append({})
                # set group default and add sub_groups
                div_tree[idx].setdefault(rest, set()).add(sub[idx + 1])
        div_tree.reverse()
        for level in div_tree:
            for group_, sub_group in level.items():
                base_div_map.setdefault(group_, [])
                for sub_loc in sub_group:
                    base_div_map[group_].extend(base_div_map[sub_loc])

        return base_div_map
