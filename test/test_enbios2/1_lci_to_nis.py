from pathlib import Path

from enbios.input.data_preparation.lci_to_nis import spold2nis2

base_folder = Path("/home/ra/PycharmProjects/enbios2/data/enbios2/_1_")
output_file = (base_folder / "output/output.xlsx").as_posix()

spold_files_folder = (base_folder / "SPOLDS").as_posix()
nis_base_path = (base_folder / "BASELINE_UPDATE_APOS.xlsx").as_posix()
correspondence_path = ""

if __name__ == "__main__":
    spold2nis2("generic_energy_production",
              spold_files_folder,
              correspondence_path,
              nis_base_path,
              None,
              output_file, True)
