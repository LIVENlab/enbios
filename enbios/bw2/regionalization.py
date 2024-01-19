from bw2calc import LCA
from bw2data.backends import ActivityDataset

"""
Go through your activity dataset and assign location tuples to data["enb_locations"]
activities should have a location tuple, with increasing granularity of locations. e.g. ("EU", "ES","cat")
location levels should be consistent. meaning, since ES is on index 1, 
there should be no activity with location ("ES", "ara") (where ES is at index 0)
"""


def regionalization(lca: LCA, location_key: str = "enb_location") -> dict[str, float]:
    """
    :param lca: bw LCA object
    :param location_key:
    :return: dictionary, location > impact score
    """
    if not hasattr(lca, "inventory"):
        print("calculating inventory")
        lca.lci()

    if not hasattr(lca, "characterization_matrix"):
        lca.load_lcia_data()

    # final location -> id
    base_loc_map = {}
    # all other indices to last locs
    loc_tree = []
    for a in ActivityDataset.select(ActivityDataset).where(
        ActivityDataset.type == "process"
    ):
        # if a.type == "process":
        loc = a.data.get(location_key)
        if not isinstance(loc, tuple):
            continue
        final_loc = loc[-1]
        base_loc_map.setdefault(final_loc, []).append(a.id)
        for idx, rest in enumerate(loc[:-1]):
            if len(loc_tree) <= idx:
                loc_tree.append({})
            loc_tree[idx].setdefault(rest, set()).add(loc[idx + 1])

    loc_tree.reverse()

    res_map = {}
    # do matrix multiplication for each final location
    for loc, idxs in base_loc_map.items():
        res_map[loc] = (
            lca.characterization_matrix
            * lca.inventory[:, [lca.dicts.activity[c] for c in idxs]]
        ).sum()

    # sum up location results, per level...
    for lvl in loc_tree:
        for loc, sub_loc in lvl.items():
            # print(loc, contained)
            res_map[loc] = 0
            for cloc in sub_loc:
                res_map[loc] += res_map[cloc]

    return res_map
