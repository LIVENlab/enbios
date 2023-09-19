import bw2data
import bw2io
from bw2io import ExcelImporter

from enbios2.bw2.util import report
from enbios2.generic.files import DataPath

report()
project_name = "excel_import"

reset = False

if reset:
    bw2data.projects.delete_project(project_name, True)
    bw2data.projects.purge_deleted_directories()

if project_name not in bw2data.projects:
    bw2data.projects.create_project(project_name)
    bw2data.projects.set_current(project_name)
    bw2io.bw2setup()
else:
    bw2data.projects.set_current(project_name)

file_path = DataPath("test_data/bw2/csv_excel/a.xlsx")
imported = ExcelImporter(file_path)
imported.apply_strategies()
if imported.all_linked:
    imported.write_database()

print(len(bw2data.Database("csv_db")))
act = list(bw2data.Database("csv_db"))[0]
print(act)
print(act.temp_data)