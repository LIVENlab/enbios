import bw2data
from bw2calc import MultiLCA
from bw2data import calculation_setups
from bw2data.backends import Activity

bw2data.projects.set_current("ecoinvent")
db = bw2data.Database("cutoff_3.9.1_default")

if __name__ == "__main__":
    for i in range(10):
        act: Activity = db.random()
        method = list(bw2data.methods)[0]
        single_result = act.lca(method).score
        print(f"{act} - {method}")

        calculation_setups["multiLCATest"] = {
            "inv": [{act: 1}],
            "ia": [method]
        }

        calculation_setups.flush()
        multi_result = MultiLCA("multiLCATest").results[0][0]

        print(single_result)
        print(multi_result)
        print(abs(single_result - multi_result))