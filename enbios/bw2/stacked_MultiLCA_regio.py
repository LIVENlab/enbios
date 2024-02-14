from typing import Callable

import numpy as np
from bw2calc.multi_lca import InventoryMatrices
from bw2data.backends import ActivityDataset

from enbios.bw2.MultiLCA_util import BaseStackedMultiLCA
from enbios.bw2.bw_models import BWCalculationSetup
from enbios.bw2.util import split_inventory


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
        method_activity_func_maps: dict[
            tuple[str, ...], dict[int, Callable[[float], float]]
        ] = None,
    ):
        super().__init__(
            calc_setup,
            np.zeros((len(calc_setup.inv), len(calc_setup.ia), len(select_locations))),
            use_distributions,
            method_activity_func_maps,
        )

        self.locations_base_map: dict[str, list[int]] = self.resolve_loc_basemap(
            location_key
        )

        for row, func_unit in enumerate(self.func_units):
            self.prep_demand(row, func_unit)
            for col, cf_matrix in enumerate(self.method_matrices):
                self.lca.characterization_matrix = cf_matrix

                for loc_idx, loc in enumerate(select_locations):
                    activity_ids = self.locations_base_map[loc]
                    # todo, this is a bw_utils method split_inventory
                    regional_characterized_inventory = self.lcia_calculation(
                        self.non_linear_methods_flags[col],
                        split_inventory(self.lca, activity_ids),
                    )
                    self.results[
                        row, col, loc_idx
                    ] = regional_characterized_inventory.sum()
        self.inventory = InventoryMatrices(self.lca.biosphere_matrix, self.supply_arrays)

    def resolve_loc_basemap(self, location_key: str = "enb_location"):
        # final location -> id
        base_loc_map = {}
        # all other indices to last locs
        loc_tree = []
        for a in ActivityDataset.select(ActivityDataset).where(
            ActivityDataset.type == "process"
        ):
            # if a.type == "process":
            loc = a.data.get(location_key)
            if not isinstance(loc, tuple) and not isinstance(loc, list):
                continue
            final_loc = loc[-1]
            base_loc_map.setdefault(final_loc, []).append(a.id)
            # make tree list at least as long as length
            for idx, rest in enumerate(loc[:-1]):
                if len(loc_tree) <= idx:
                    loc_tree.append({})
                # set location default and add location
                loc_tree[idx].setdefault(rest, set()).add(loc[idx + 1])
        loc_tree.reverse()
        for level in loc_tree:
            for loc, sub_locs in level.items():
                base_loc_map.setdefault(loc, [])
                for sub_loc in sub_locs:
                    base_loc_map[loc].extend(base_loc_map[sub_loc])

        return base_loc_map
