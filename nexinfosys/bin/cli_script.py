import copy
import json
import logging
import os
import re
import tempfile
import urllib
from io import BytesIO, StringIO
from typing import List, Union, BinaryIO, Tuple
from zipfile import ZipFile
import fire
import platform
import sys

from nexinfosys.command_generators import Issue, IType
from nexinfosys.common.helper import download_file, generate_json
from nexinfosys.embedded_nis import NIS
from nexinfosys.model_services import State
from nexinfosys.serialization import deserialize_state


def set_zip_timestamp(in_zip: Union[os.PathLike, str, BinaryIO], timestamp=(2020, 1, 1, 0, 0, 0)) -> BinaryIO:
    """
    Modify the timestamp of all files, to stabilize the hash

    :param in_zip: Zip file whose files timestamp will be modified
    :param timestamp: Tuple with the timestamp to set for all the files
    :return A BytesIO with the resulting Zip
    """
    out_zip = BytesIO()
    with ZipFile(in_zip, mode="r") as zin:
        with ZipFile(out_zip, mode="w") as zout:
            for zinfo in zin.infolist():
                data = zin.read(zinfo.filename)
                zinfo_new = copy.copy(zinfo)
                zinfo_new.date_time = timestamp
                zout.writestr(zinfo_new, data)
    return out_zip


def hash_array(f: Union[str, bytes]):
    import hashlib
    m = hashlib.md5()
    if isinstance(f, str):
        m.update(f.encode("utf-8"))
    else:
        m.update(f)
    return m.digest()


def get_valid_name(original_name):
    prefix = original_name[0] if original_name[0].isalpha() else "_"
    remainder = original_name[1:] if original_name[0].isalpha() else original_name
    return prefix + re.sub("[^0-9a-zA-Z_]+", "", remainder)


def get_file_url(fn):
    return "file:" + urllib.request.pathname2url(fn)


def print_sys_info(show_sys_info):
    if show_sys_info:
        print(f"Executable: {sys.executable}; V: {sys.version}; {platform.python_build()}; "
              f"CONDA_PREFIX: {os.getenv('CONDA_PREFIX')}")


def read_submit_solve_nis_file(nis_file_url: str, state: State, solve=False) -> Tuple[NIS, List[Issue]]:
    nis = NIS()
    nis.open_session(True, state)
    nis.reset_commands()
    # Load file and execute it
    nis.load_workbook(nis_file_url)
    if solve:
        issues = nis.submit_and_solve()
    else:
        issues = nis.submit()
    # tmp = nis.query_available_datasets()
    # print(tmp)
    # d = nis.get_results([(tmp[0]["type"], tmp[0]["name"])])
    return nis, issues


def issue_str(issue: Issue):
    # description, ctype, location
    return str(issue)


def prepare_base_state(base_url: str, solve: bool, directory: str = None, force_refresh: bool = False):
    from nexinfosys import initialize_configuration
    initialize_configuration()  # Needed to make download and NIS later work properly
    if base_url.startswith(os.sep) or base_url[1] == ":":
        base_url = get_file_url(base_url)
    logging.debug(f"File to be downloaded: {base_url}")
    tmp_io = download_file(base_url)
    bytes_io = set_zip_timestamp(tmp_io)
    hash_ = hash_array(bytes_io.getvalue())
    val_name = get_valid_name(base_url)
    if directory is None:
        directory = tempfile.gettempdir()
    hash_file = f"{directory}{os.sep}base.hash.{val_name}"
    state_file = f"{directory}{os.sep}base.state.{val_name}"
    if force_refresh:
        update = True
    else:
        if os.path.isfile(hash_file) and os.path.isfile(state_file):
            with open(hash_file, "rb") as f:
                cached_hash = f.read()
            update = cached_hash != hash_
        else:
            update = True

    if update:
        temp_name = tempfile.NamedTemporaryFile(dir=directory, delete=False)
        temp_name = temp_name.name
        with open(temp_name, "wb") as f:
            f.write(bytes_io.getvalue())
        f_name = get_file_url(temp_name)
        logging.debug(f"Temporary being loaded: {f_name}")
        nis, issues = read_submit_solve_nis_file(f_name, None, solve=solve)
        os.remove(temp_name)
        any_error = False
        for issue in issues:
            if issue.itype == IType.ERROR:
                any_error = True
        # Write if there are no errors
        if not any_error:
            from nexinfosys.serialization import serialize_state
            state = nis.get_state()
            serial_state = serialize_state(state)
            nis.close_session()
            with open(hash_file, "wb") as f:
                f.write(hash_)
            with open(state_file, "wb") as f:
                f.write(serial_state)
        else:
            # raise Exception(f'There were errors with the NIS base file {base_url}.')
            # print_issues("NIS base preparation", f_name, issues, "NIS base preparation failed, check errors")
            state = None
            serial_state = None
    else:
        issues = None  # Cached, no issues
        with open(state_file, "rb") as f:
            serial_state = f.read()
        state = deserialize_state(serial_state)

    return state, serial_state, issues


class PrintColors:
    PURPLE = '\033[95m'
    CYAN = '\033[96m'
    DARKCYAN = '\033[36m'
    BLUE = '\033[94m'
    GREEN = '\033[92m'
    YELLOW = '\033[93m'
    RED = '\033[91m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'
    END = '\033[0m'


def print_issues(operation, file, issues: List[Issue], msg_in_case_of_error: str = None):
    if issues is not None:
        # First print all the issues
        w = len(str(len(issues)))
        i = 0
        max_of_a_type = 1000
        for issue_type in [IType.ERROR, IType.WARNING]:
            j = 0
            for issue in issues:
                if issue.itype == issue_type:
                    i += 1
                    j += 1
                    s = f'{issue.itype.name:7}: {issue.description} ({issue.ctype}, {issue.location})'  # f"{issue.itype.name:7}: {issue.description}"
                    print(f"{i:{w}}: {s}")
                    if j > max_of_a_type:
                        print(f"{PrintColors.YELLOW}There are more than {max_of_a_type} issues of type {issue_type.name} than allowed.{PrintColors.END}")
                        break
        # Finally, summary: # errors and warnings
        errors = 0
        warnings = 0
        for issue in issues:
            if issue.itype == IType.ERROR:
                errors += 1
            elif issue.itype == IType.WARNING:
                warnings += 1

        if warnings or errors:
            print("-------------------------------------------------------")

        if warnings > 0:
            print(f"{PrintColors.YELLOW}{warnings} warning{'s' if warnings != 1 else ''} found{PrintColors.END}")
        if errors > 0:
            if msg_in_case_of_error:
                print(msg_in_case_of_error)
            print(f"{PrintColors.RED}{errors} error{'s' if errors != 1 else ''} found{PrintColors.END}")

        if errors == 0:
            if operation:
                print(f"{PrintColors.GREEN}{operation} successful{PrintColors.END} (no errors detected)")
            else:
                print(f"{PrintColors.GREEN}Operation finished successfully{PrintColors.END} (no errors detected)")
    else:
        print(
            f"File '{file}' was already cached, meaning no change and no errors detected. "
            f"Please, add --force-refresh to force reprocessing.")


def write_results(state_or_nis: Union[State, NIS], output_dir: str, datasets: List[str]):
    if isinstance(state_or_nis, State):
        nis = NIS()
        nis.open_session(state=state_or_nis)
    else:  # NIS
        nis = state_or_nis

    # Convert to required format
    if datasets:
        datasets = datasets.split(",") if datasets.strip() != "" else []
        _ = []
        for ds in datasets:
            tmp = ds.strip().split(".")
            if len(tmp) == 3:
                _.append([tmp[0], tmp[1]+"."+tmp[2]])
            elif len(tmp) == 2:
                _.append([None, tmp[0] + "." + tmp[1]])
            else:
                print(f"Could not parse dataset request entry: {ds}")
        tmp = nis.get_results(_)
        any_error = False
        for i, ds in enumerate(tmp):
            name = datasets[i].strip()
            if ds[2]:
                if isinstance(ds[0], StringIO):
                    val = ds[0].getvalue()
                else:
                    val = ds[0]
                flag = "b" if not isinstance(ds[0], StringIO) and not isinstance(ds[0], str) else "t"
                print(f"Writing {name} to: {output_dir}{os.sep}{name}")
                with open(f"{output_dir}{os.sep}{name}", f"w{flag}") as f:
                    f.write(val)
            else:
                any_error = True
                print(f"Could not retrieve dataset: {name}")
    else:
        any_error = True
        tmp = []
    if any_error or len(tmp) == 0:
        print("Reference of available datasets: --------")
        lst = nis.query_available_datasets()
        for ds in lst:
            fmts = '|'.join([fmt['format'] for fmt in ds['formats']])
            print(f"[{ds['type']}.]{ds['name']}.({fmts}); ({ds['description']})")


def set_log_level_from_cli_param(log_param: str):
    """
    Set global logging level to one of: Off, Info, Warn, Debug

    :param log_param:
    :return:
    """
    if log_param is None or log_param == "":
        log_param = ""

    level = log_param.lower()
    if level in ("debug", "d"):
        level = logging.DEBUG
    elif level == "off":
        level = logging.NOTSET
    elif level in ("error", "err", "e"):
        level = logging.DEBUG
    elif level in ("info", "i"):
        level = logging.INFO
    elif level in ("warn", "warning", "w"):
        level = logging.WARNING
    elif level in ("critical", "fatal"):
        level = logging.CRITICAL
    else:
        if level != "":
            print(f"Log level not set, '{log_param}' not recognized. Valid options: Error (E, Err), Debug (D), "
                  f"Warning (W, Warn), Info (I), Off, Critical (Fatal)")
        level = None

    if level is not None:
        logging.basicConfig(level=level)


class Nexinfosys:
    """
nexinfosys parse https://docs.google.com/spreadsheets/d/1BlvHI56kP2kzogKbAa8BHr2GYMfJIZTDkBaPYIfiPMQ/edit#gid=0 /home/rnebot/tmp/example1

nexinfosys solve https://docs.google.com/spreadsheets/d/1C5xNGvdORWqrWL6Ux2nBXMdX51ktEDJCmFdDrbFrhX4/edit#gid=0 /home/rnebot/tmp/example1withValues

nexinfosys solve https://docs.google.com/spreadsheets/d/1C5xNGvdORWqrWL6Ux2nBXMdX51ktEDJCmFdDrbFrhX4/edit#gid=0 /home/rnebot/tmp/example1withValues --datasets="flow_graph_solution.csv"

    """
    def parse(self, file: str, work_path: str, datasets: str = "", force_refresh: bool = False, log: str = None, sys_info: bool = False):
        """
        Parse and retrieve datasets

        :param file: Input file
        :param work_path: Where to place cache and result files
        :param datasets: Comma separated list of datasets to export
        :param force_refresh: True to force parsing even if the main file has not changed
        :param log: Set log level to one of: Error (E, Err), Debug (D), Warning (W, Warn), Info (I), Off, Critical (Fatal)
        :param sys_info: Print system information
        :return:
        """
        print_sys_info(sys_info)
        set_log_level_from_cli_param(log)
        os.makedirs(work_path, exist_ok=True)
        state, _, issues = prepare_base_state(file, False, work_path, force_refresh)
        print_issues("Parsing", file, issues)
        write_results(state, work_path, datasets)

    def solve(self, file: str, work_path: str, datasets: str = "", force_refresh: bool = False, log: str = None, sys_info: bool = False):
        """
        Parse, solve and retrieve datasets

        :param file: Input file
        :param work_path: Where to place cache and result files
        :param datasets: Comma separated list of datasets to export
        :param force_refresh: True to force parsing and solving even if the main file has not changed
        :param log: Set log level to one of: Error (E, Err), Debug (D), Warning (W, Warn), Info (I), Off, Critical (Fatal)
        :param sys_info: Print system information
        :return:
        """
        print_sys_info(sys_info)
        set_log_level_from_cli_param(log)
        os.makedirs(work_path, exist_ok=True)
        state, _, issues = prepare_base_state(file, True, work_path, force_refresh)
        print_issues("Solving", file, issues)
        write_results(state, work_path, datasets)

    def solve_dynamic_scenario(self, file: str, work_path: str, params_file_path: str = "",
                               datasets: str = "", force_refresh: bool = False,
                               log: str = None, sys_info: bool = False):
        """
        Run (solve) dynamic scenario and get output datasets

        :param file: Input file
        :param work_path: Where to place cache and result files
        :param params_file_path: Path of the JSON file where parameters are specified. If empty, a sample params file is output to screen
        :param datasets: Comma separated list of datasets to export
        :param force_refresh: True to force parsing and solving even if the main file has not changed
        :param log: Set log level to one of: Error (E, Err), Debug (D), Warning (W, Warn), Info (I), Off, Critical (Fatal)
        :param sys_info: Print system information
        :return:
        """
        print_sys_info(sys_info)
        set_log_level_from_cli_param(log)
        os.makedirs(work_path, exist_ok=True)
        state, _, issues = prepare_base_state(file, True, work_path, force_refresh)
        print_issues("Dynamic scenario", file, issues)
        nis = NIS()
        nis.open_session(state=state)
        if params_file_path is None or params_file_path.strip() == "":
            # Print sample parameters file
            params = nis.query_parameters()
            d = {}
            for param in params:
                d[param["name"]] = f'{param["type"]}{": " if param["range"].strip() != "" else ""}{param["range"]}'
            print(generate_json(d))

        else:
            # Read file
            with open(params_file_path, "rt") as f:
                params = json.loads(f.read())
                issues = nis.recalculate(params)
                write_results(nis, work_path, datasets)

    def backend(self):
        from nexinfosys.restful_service import app
        import nexinfosys.restful_service.service_main
        app.run(host='0.0.0.0',
                debug=True,
                use_reloader=False,  # Avoid loading twice the application
                processes=1,
                threaded=False)  # Default port, 5000


def main():
    os.environ["PAGER"] = "cat" if platform.system().lower() != "windows" else "-"
    fire.Fire(Nexinfosys)


if __name__ == '__main__':
    main()
