from pathlib import Path

import appdirs


def get_init_appdir() -> Path:
    enbios_appdir = Path(appdirs.user_data_dir("enbios2"))
    if not enbios_appdir.exists():
        enbios_appdir.mkdir(parents=True, exist_ok=True)
        print(f"Creating 'enbios2' appdir at: {enbios_appdir.as_posix()}")
    return enbios_appdir

