import json
from pathlib import Path
base = Path(r'C:\Users\Administrator\PycharmProjects\enbios2\projects\seed\MixUpdater\results\subregion')
# grab all the result files, and get the top level results
total_res = []
for index in range(len(list(base.glob("*.json")))):
    print(index)
    file = base / f"{index}.json"
    total_res.append(json.load(file.open())[index]["results"])
# store to results.json file
with open("results_grouped.json", "w") as f:
    json.dump(total_res, f)