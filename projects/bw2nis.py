import json
from collections import Counter
from typing import Optional

import bw2data
import openpyxl
from bw2data.backends import ActivityDataset, Activity

from enbios2.bw2.project_index import get_bw_database, set_bw_current_project
from enbios2.const import BASE_DATA_PATH
from enbios2.enbios_extend.bw_lci2nis import read_exising_nis_file, export_solved_inventory, insert_activity_processor
from input.data_preparation.lci_to_nis import spold2nis
from processing.main import Enviro

base_folder = BASE_DATA_PATH / "nis_tests"

unit_lookup_data = {}

# 1 create an index of version 3.9.1, lci
# analyze_directory(Path("/mnt/hd2/projects/icta/livenlab/ecoinvent-datasets"))


# 2 get the default db to find an activity
# db = get_bw_database("cutoff", "3.9.1")
# res = db.search("solar pv", filter={"location": "PT"})
# if res:
#     res = res[0]
# print(res["name"])
# # electricity production, photovoltaic, 3kWp slanted-roof installation, single-Si, panel, mounted
# print(res._document.code)
# # 08a684cd60d03f17de397396af5289ce


# 3 get the data path and copy the file to our special spold folder
# ds = get_ecoinvent_dataset_index(version="3.9.1", system_model="cutoff", type="lci", xlsx=False)
# print(ds[0].dataset_path)
# filename = res["filename"]
# print(filename)
# file_path = ds[0].dataset_path / filename
# print(file_path.exists())
# shutil.copyfile(file_path, base_folder / "spold" / filename)

# 4 run spold2nis, which will create "output.xlsx"
# output_file = (base_folder / "output.xlsx")
# output_file.parent.mkdir(exist_ok=True, parents=True)
#
# spold_files_folder = (base_folder / "spold").as_posix()
# nis_base_path = (base_folder / "BASELINE_UPDATE_APOS.xlsx").as_posix()
# correspondence_path = ""
# #
# spold2nis("generic_energy_production",
#           spold_files_folder,
#           correspondence_path,
#           nis_base_path,
#           None,
#           output_file.as_posix())

# 5 lets run our bw-activity to nis and see if we get the same table:
# set_bw_current_project(version="3.9.1", system_model="cutoff")

# generate = False
# if generate:
#     bi.bw2setup()
#     if "ei39" not in bd.databases:
#         im = SingleOutputEcospold2Importer(
#             (BASE_DATA_PATH / f"ecoinvent/ecoinvent 3.9_cutoff_ecoSpold02/datasets").as_posix(),
#             "ei39")
#     im.apply_strategies()
#     if im.statistics()[2] == 0:
#         im.write_database()
#     else:
#         print("unlinked exchanges")
# #
# activities = bd.Database("cutoff391",).search("heat and power co-generation, oil", filter={"location": "PT"})
# activity = activities[0]
# print(activity._document.code)

# 6 generate new output file
db = get_bw_database("cutoff", "3.9.1")

activity_ds = ActivityDataset.select().where(ActivityDataset.code == "08a684cd60d03f17de397396af5289ce")
activity = Activity(activity_ds[0])

nis_file = base_folder / "output-e.xlsx"
#

lci_result = export_solved_inventory(activity,
                                     ('CML v4.8 2016', 'acidification',
                                      'acidification (incl. fate, average Europe total, A&B)'),
                                     base_folder / "resolved_lci_product.xlsx",
                                     "product")

lci_result = export_solved_inventory(activity,
                                     ('CML v4.8 2016', 'acidification',
                                      'acidification (incl. fate, average Europe total, A&B)'),
                                     base_folder / "resolved_lci.xlsx")

lci_result = export_solved_inventory(activity,
                                     ('CML v4.8 2016', 'acidification',
                                      'acidification (incl. fate, average Europe total, A&B)'),
                                     base_folder / "resolved_lci_activity.xlsx",
                                     "activity")


dataframes = read_exising_nis_file(nis_file)

# insert_activity_processor(activity, dataframes, lci_result, base_folder / "output2.xlsx")

# lci_result is a pandas dataframe
# lci_result.to_csv(base_folder / "output2_raw.csv")
# # sort by the column "name"
# lci_result = lci_result.sort_values(by=["name"])
# # filter out, where the amount smaller than 0
# lci_result = lci_result[lci_result["amount"] < 0]
# lci_result.to_csv(base_folder / "output2_sorted_non0.csv")
# # count each value in "name" column and turn it into a dict
# name_counts = lci_result["name"].value_counts().to_dict()
# # open the excel file "output.xlsx" sheet: "Interfaces"
# openpyxl_file = openpyxl.load_workbook(nis_file,  data_only=True)
# interfaces = openpyxl_file["Interfaces"]
# # take the values in column "B"
# interface_names = [cell.value for cell in interfaces["B"]]
# # Counter for all names:
# o_counter = dict(Counter(interface_names))
# unames = set(name_counts.keys())
# # turn all chars which are not a-z or A-Z or 0-9 into "_"
# unames = set(["".join([c if c.isalnum() else "_" for c in name]) for name in unames])
# onames = set(o_counter.keys())
# print("in output but not our export", len(onames - unames), onames - unames)
# print("in our export but not in output", len(unames - onames), unames - onames)


# 7 run enbios
# right one...
# cfg_file_path = (base_folder / "base.yaml").as_posix()
# t = Enviro()
# t.set_cfg_file_path(cfg_file_path)
#
# t.compute_indicators_from_base_and_simulation()

# recreated...

# cfg_file_path = (base_folder / "base2.yaml").as_posix()
# t = Enviro()
# t.set_cfg_file_path(cfg_file_path)
#
# t.compute_indicators_from_base_and_simulation()
