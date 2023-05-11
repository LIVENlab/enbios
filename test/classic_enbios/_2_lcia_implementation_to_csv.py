from pathlib import Path
from typing import List

from enbios.input.data_preparation.lcia_implementation_to_nis import convert_lcia_implementation_to_nis

base_folder = Path("/home/ra/PycharmProjects/enbios2/data/enbios/_2_")
lcia_implementation_file: str = (base_folder / "LCIA_Implementation_v3_8.xlsx").as_posix()
lcia_file: str = (base_folder / "output/lcia_method_out.csv").as_posix()

method_like: str = ""
method_is: List[str] = None
include_obsolete: bool = False
use_nis_name_syntax: bool = True

if isinstance(method_is, str):
    method_is = [method_is]
convert_lcia_implementation_to_nis(lcia_implementation_file, lcia_file,
                                   method_like, method_is,
                                   include_obsolete, use_nis_name_syntax)
