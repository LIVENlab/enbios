from pathlib import Path

from enbios.input.data_preparation.lci_to_nis import spold2nis

base_folder = Path("/home/ra/PycharmProjects/enbios2/data/enbios/_1_")
base_in_folder = base_folder / "input"
output_file = (base_folder / "output/output.xlsx").as_posix()

spold_files_folder = (base_in_folder / "SPOLDS").as_posix()
nis_base_path = (base_in_folder / "BASELINE_UPDATE_APOS.xlsx").as_posix()
correspondence_path = ""

if __name__ == "__main__":
    spold2nis("generic_energy_production",
              spold_files_folder,
              correspondence_path,
              nis_base_path,
              None,
              output_file)
