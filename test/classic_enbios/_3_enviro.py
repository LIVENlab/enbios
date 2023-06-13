from typing import Optional

from enbios.processing.main import Enviro
from enbios2.const import BASE_DATA_PATH

cfg_file_path = (BASE_DATA_PATH / "AlexEnbios1/base.yaml").as_posix()

t = Enviro()
t.set_cfg_file_path(cfg_file_path)

t.compute_indicators_from_base_and_simulation()