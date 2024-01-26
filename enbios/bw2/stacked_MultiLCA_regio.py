from typing import Callable

import numpy as np
from bw2calc.multi_lca import InventoryMatrices
from bw2data.backends import ActivityDataset

from enbios.bw2.MultiLCA_util import BaseStackedMultiLCA
from enbios.bw2.bw_models import BWCalculationSetup


class RegioStackedMultiLCA(BaseStackedMultiLCA):
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
            select_locations: set[str],
            use_distributions: bool = False,
            location_key: str = "enb_location",
            method_activity_func_maps: dict[tuple[str, ...], dict[int, Callable[[float], float]]] = None,
    ):
        super().__init__(calc_setup,
                         np.zeros(
                             (len(self.func_units), len(self.methods), len(select_locations))
                         ),
                         use_distributions)

        self.locations_base_map: dict[str, list[int]] = {}
        self.loc_tree: list[dict[str, set[str]]] = []
        self.resolve_loc_basemap(location_key)

        for row, func_unit in enumerate(self.func_units):
            self.prep_demand(row, func_unit)

            res_map = {}

            for col, cf_matrix in enumerate(self.method_matrices):
                self.lca.characterization_matrix = cf_matrix
                for loc, idxs in self.locations_base_map.items():
                    regional_characterized_inventory = self.lcia_calculation(self.non_linear_methods_flags[col], self.lca.inventory[
                              :, [self.lca.dicts.activity[c] for c in idxs]
                              ])
                    res_map[loc] = regional_characterized_inventory.sum()

                # sum up location results, per level...
                for lvl in self.loc_tree:
                    for loc, sub_loc in lvl.items():
                        # print(loc, contained)
                        res_map[loc] = 0
                        for cloc in sub_loc:
                            res_map[loc] += res_map[cloc]

                for loc, loc_name in enumerate(select_locations):
                    self.results[row, col, loc] = res_map[loc_name]

        self.inventory = InventoryMatrices(self.lca.biosphere_matrix, self.supply_arrays)

    def resolve_loc_basemap(self, location_key: str = "enb_location"):
        # final location -> id
        # base_loc_map = {}
        # all other indices to last locs
        # loc_tree = []
        for a in ActivityDataset.select(ActivityDataset).where(
                ActivityDataset.type == "process"
        ):
            # if a.type == "process":
            loc = a.data.get(location_key)
            if not isinstance(loc, tuple):
                continue
            final_loc = loc[-1]
            self.locations_base_map.setdefault(final_loc, []).append(a.id)
            for idx, rest in enumerate(loc[:-1]):
                if len(self.loc_tree) <= idx:
                    self.loc_tree.append({})
                self.loc_tree[idx].setdefault(rest, set()).add(loc[idx + 1])

        self.loc_tree.reverse()

    @property
    def all(self):
        """Get all possible databases by merging all functional units"""
        return {key: 1 for func_unit in self.func_units for key in func_unit}
