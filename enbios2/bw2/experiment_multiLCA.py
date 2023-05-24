from random import choice

import pandas as pd
from bw2calc import LCA, MultiLCA
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

# one demand with 2 funcitonal unit ... short, bad looking code for {key1: value1, key2: value2}
# lca = LCA(demand={list(a.keys())[0]: list(a.values())[0] for a in rand_acts}, method=methods[0], use_distributions=True)
# lca.lci()
# lca.lcia()
#
# # df = pd.DataFrame([{'score': lca.score} for _ in zip(lca, range(10))])
# # print(df)
# print(lca.score)
"""
Also see:
https://stackoverflow.com/questions/42984831/create-a-dataframe-from-multilca-results-in-brightway2
A demand can already include multiple activities!

also read...
https://oie-mines-paristech.github.io/lca_algebraic/example-notebook.html
https://github.com/brightway-lca/from-the-ground-up/blob/main/basic%20tasks/Searching.ipynb
"""