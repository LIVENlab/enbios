import contextlib
import inspect
import io
import re
import sys
from enum import Enum
from importlib import import_module
from pathlib import Path
from time import time
from types import ModuleType
from typing import Union, Type, Any, Callable

from enbios.const import BASE_DATA_PATH
from enbios.generic.enbios2_logging import get_logger

logger = get_logger(__name__)


def generate_levensthein_name_map(
    names_a: list[str], names_b: list[str]
) -> dict[str, str]:
    try:
        from Levenshtein import ratio
    except ImportError:
        raise ImportError(
            "Levensthein module not found. Install with `pip install Levensthein`"
        )

    names_map: dict[str, str] = {}
    remaning_names = names_b.copy()
    for term in names_a:
        if term not in remaning_names:
            # find closest match
            closest_match = max(remaning_names, key=lambda x: ratio(term, x))
            names_map[term] = closest_match
            remaning_names.remove(closest_match)
    return names_map


def generate_levensthein_dict_map(
    names_a: list[str], dicts: list[dict], dict_key: str
) -> dict[str, dict]:
    try:
        from Levenshtein import ratio
    except ImportError:
        raise ImportError(
            "Levensthein module not found. Install with `pip install Levensthein`"
        )

    names_map: dict[str, dict] = {}
    remaning_dicts = dicts.copy()
    for term in names_a:
        # find closest match
        closest_match = max(remaning_dicts, key=lambda x: ratio(term, x[dict_key]))
        names_map[term] = closest_match
        remaning_dicts.remove(closest_match)
    return names_map


def get_data_file_path(path: Union[str, Path]) -> Path:
    """
    Returns a Path object from the data directory
    :param path:
    :return:
    """
    path = Path(path)
    if path.is_absolute():
        return path
    else:
        return BASE_DATA_PATH / path


def safe_name(text: str, replacer_char: str = "_") -> str:
    """
    Replace characters that are not allowed in file names with a replacer character.
    :param text: text to convert
    :param replacer_char: character to replace with
    :return: sane file name
    """
    if len(replacer_char) > 1:
        logger.warning("replacer_char should be a single character")
        replacer_char = replacer_char[0]
    return re.sub(r"[/\\?%*:|\"<>\x7F\x00-\x1F]", replacer_char, text)


def simple_name(text, replacer_char: str = "_") -> str:
    """
    Replace characters that are not  alphanumeric of _ with a replacer character.
    :param text:
    :param replacer_char: should be just a single character
    :return:
    """
    if len(replacer_char) > 1:
        logger.warning("replacer_char should be a single character")
        replacer_char = replacer_char[0]
    return re.sub(f"[^0-9a-zA-Z{replacer_char}]", replacer_char, text)


def get_enum_by_value(enum_type: Type[Enum], value):
    for enum_member in enum_type.__members__.values():
        if enum_member.value == value:
            return enum_member
    return None


def call_muted(func, *args, **kwargs) -> Any:
    """
    Call a function
    :param func: function to call
    :param args: function args
    :param kwargs: function kwargs
    :return: function return value
    """
    with contextlib.redirect_stdout(io.StringIO()):
        return func(*args, **kwargs)


@contextlib.contextmanager
def muted():
    with contextlib.redirect_stdout(io.StringIO()):
        yield


@contextlib.contextmanager
def timed():
    start = time()
    try:
        yield
    finally:
        print(f"({time() - start:.2f}s)")


def load_module(module_path: str) -> ModuleType:
    _path = Path(module_path)
    if not _path.exists():
        raise ValueError(f"Module path '{module_path}' does not exist")
    sys.path.insert(0, _path.parent.as_posix())
    return import_module(_path.stem)


def get_module_functions(module: ModuleType) -> dict[str, Callable]:
    return {
        name: func
        for name, func in inspect.getmembers(module)
        if inspect.isfunction(func)
    }
