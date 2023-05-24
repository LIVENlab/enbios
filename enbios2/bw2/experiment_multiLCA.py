from random import choice

import pandas as pd
from bw2calc import MultiLCA
from bw2data import calculation_setups

import bw2data as bd
from bw2data.backends import Activity


def run_multi_lca(name: str, functional_units: dict[Activity: float], impact_methods: list[str]):
    """
    Perform MultiLCA calculations with many functional units and LCIA methods.

    """
    if len(functional_units) > 0 and len(impact_methods) > 0:
        calculation_setups[name] = {'inv': functional_units, 'ia': impact_methods}
        multi_lca = MultiLCA(name)
        index = [str(x) for x in list(multi_lca.all.keys())]
        columns = [str(x) for x in impact_methods]
        results = pd.DataFrame(multi_lca.results,
                               columns=columns,
                               index=index)
        return results
    else:
        raise ValueError('Check the in inputs')


bd.projects.set_current("uab_bw_ei39")

db = bd.Database("ei391")


rand_acts = [{db.random(): 1} for _ in range(2)]
all_methods = list(bd.methods)
methods = list(set(choice(all_methods) for _ in range(1)))

print("activities", rand_acts)
print("methods", methods)

res = run_multi_lca('test', rand_acts, methods)

print(res)

"""
Also see:
https://stackoverflow.com/questions/42984831/create-a-dataframe-from-multilca-results-in-brightway2
A demand can already include multiple activities!
https://oie-mines-paristech.github.io/lca_algebraic/example-notebook.html
"""