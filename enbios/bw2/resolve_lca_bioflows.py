from csv import DictWriter

from bw2calc import LCA
from bw2data.backends import ActivityDataset


def solve_bioflows(lca: LCA) -> list[tuple[ActivityDataset, float]]:
    assert hasattr(lca, "characterized_inventory"), "Must do lcia first"
    # characterized_inventory:matrix biosphere (rows) x technosphere (cols), sum each row
    # solved_bioflows = lca.characterized_inventory.sum(1)
    # turn the result into a simple list
    # flat_solved_bioflows = solved_bioflows.flatten().tolist()[0]
    flat_solved_bioflows = lca.characterized_inventory.sum(1).flatten().tolist()[0]

    # collect all Activities from the database that are in the biosphere
    # biosphere_activities_ids = list(lca.dicts.biosphere.keys())
    # biosphere_activities = list(ActivityDataset.select().where(ActivityDataset.id.in_(biosphere_activities_ids))
    biosphere_activities = list(
        ActivityDataset.select().where(
            ActivityDataset.id.in_(list(lca.dicts.biosphere.keys()))
        )
    )
    # Sorting according to the index in lca.dicts.biosphere
    biosphere_activities.sort(key=lambda a: lca.dicts.biosphere.get(a.id))
    return list(zip(biosphere_activities, flat_solved_bioflows))


def solved_bioflow2csv(
    solved_bioflow: list[tuple[ActivityDataset, float]], file_path: str
):
    header = ["exchange name", "compartment", "amount", "unit"]
    with open(file_path, "w") as fout:
        writer = DictWriter(fout, fieldnames=header)
        writer.writeheader()
        for activity, output in solved_bioflow:
            data = activity.data
            writer.writerow(
                {
                    "exchange name": data["name"],
                    "compartment": ", ".join(data["categories"]),
                    "amount": output,
                    "unit": data["unit"],
                }
            )


# some_lca = LCA({some_activity: 1})
# some_lca.lci()
# some_lca.lcia()
# solved_bioflow = solve_bioflows(some_lca)
# solved_bioflow2csv(solved_bioflow, "some_activity_bioflows.csv")
