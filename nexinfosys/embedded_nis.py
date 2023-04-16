import io
import logging
import traceback
from typing import List, Tuple, Dict

import pandas as pd
import xlrd

import nexinfosys
from nexinfosys.command_generators import IType, Issue
from nexinfosys.common.helper import any_error_issue, download_file
from nexinfosys.model_services import get_case_study_registry_objects, State
from nexinfosys.model_services.workspace import InteractiveSession, prepare_and_solve_model
from nexinfosys.models.musiasem_methodology_support import DBSession
from nexinfosys.initialization import initialize_databases, get_parameters_in_state, get_scenarios_in_state, \
    register_external_datasources, get_graph_from_state, get_dataset_from_state, get_model, get_geolayer, get_ontology, \
    validate_command, command_field_help, comm_help
from nexinfosys import initialize_configuration
from nexinfosys.serialization import serialize_state, deserialize_state


class NIS:
    def __init__(self):
        logging.basicConfig(level=logging.DEBUG)
        self._isession = None
        self._rsession = None
        self._dataframe_names = []  # type: List[str]
        self._dataframes = []  # type: List[pd.DataFrame]
        self._issues = None
        self._state = None
        initialize_configuration()
        # Disable optimizations
        nexinfosys.set_global_configuration_variable("ENABLE_CYTHON_OPTIMIZATIONS", False)
        initialize_databases()
        nexinfosys.data_source_manager = register_external_datasources()

    @property
    def dataframes(self):
        return self._dataframes

    @property
    def dataframe_names(self):
        return self._dataframe_names

    # --------------- SESSION ---------------

    def open_session(self, reset_commands=False, state: State=None):
        self._isession = InteractiveSession(DBSession)
        if state:
            self._isession.state = state
        self._isession.identify({"user": "test_user", "password": None}, testing=True)
        self._rsession = self._isession.open_reproducible_session(case_study_version_uuid=None,
                                                                  recover_previous_state=False,
                                                                  cr_new=True,
                                                                  allow_saving=False)

        if reset_commands:
            # Clear dataframes
            self.reset_commands()

        return self._isession

    def close_session(self):
        # Close reproducible session
        uuid_, v_uuid, cs_uuid = self._isession.close_reproducible_session(issues=None, output=None, save=False, from_web_service=False, cs_uuid=None, cs_name=None)
        self._isession.quit()
        self._isession = None
        self._rsession = None

    # --------------- SUBMISSION PREPARATION ---------------

    def reset_commands(self):
        self._dataframe_names.clear()
        self._dataframes.clear()

    def load_workbook(self, fname, wv_user=None, wv_password=None, wv_host_name=None):
        """

        :param fname:
        :param wv_user: In case of use of WebDav server, user name
        :param wv_password: In case of use of WebDav server, password
        :param wv_host_name: In case of use of WebDav server, host name
        :return: Number of added DataFrames
        """
        # Load a XLSX workbook into memory, as dataframes
        bytes_io = download_file(fname, wv_user, wv_password, wv_host_name)
        xl = pd.ExcelFile(bytes_io, engine='openpyxl')
        # xl = pd.ExcelFile(xlrd.open_workbook(file_contents=bytes_io.getvalue()), engine="xlrd")
        cont = 0
        for sheet_name in xl.sheet_names:
            df = xl.parse(sheet_name, header=0)
            # Manage columns
            cols = []
            for col in df.columns:
                col_parts = col.split(".")
                if col.lower().startswith("unnamed"):
                    cols.append("")
                elif len(col_parts) > 1:
                    try:
                        int(col_parts[1])  # This is the case of "col.1"
                        cols.append(col_parts[0])
                    except:  # This is the case of "col_part.col_part" (second part is string)
                        cols.append(col)
                else:
                    cols.append(col)

            df.columns = cols
            self._dataframes.append(df)
            self._dataframe_names.append(sheet_name)
            cont += 1

        return cont

    def _generate_in_memory_excel(self):
        output = io.BytesIO()
        writer = pd.ExcelWriter(output, engine="xlsxwriter")
        for idx, sheet_name in enumerate(self._dataframe_names):
            header = sheet_name.lower() != "metadata"
            self._dataframes[idx].to_excel(writer, sheet_name=sheet_name, index=False)
        writer.save()
        return output.getvalue()

    def append_command(self, name: str, df: pd.DataFrame):
        for n in self._dataframe_names:
            if n.lower() == name.lower():
                raise Exception("Duplicated name '"+name+"' for a command.")

        if not isinstance(df, pd.DataFrame):
            raise Exception("'df' parameter must be a Pandas DataFrame")

        self._dataframes.append(df)
        self._dataframe_names.append(name)

    def get_external_datasets(self) -> Dict[str, Tuple[str, str]]:
        """
        Obtain a list of datasets usable in DatasetQry command

        :return: Dict(dataset_code, (source, dataset_description))
        """
        pass

    def get_external_dataset_structure(self, dataset_code) -> Tuple[Dict[str, List[str]], List[str]]:
        """
        Obtain the dimensions

        :param dataset_code:
        :return: Tuple with: Dimensions in dictionary (dim_name, list of codes), List of measure names
        """
        pass

    def get_datasetqry_dataframe(self, dataset_code, filter, out_dimensions, out_measures: List[Tuple[str, str, str]], out_dataset_name) -> pd.DataFrame:
        """
        Obtain a pd.DataFrame with a DatasetQry command

        :param dataset_code: code of the external dataset to use as input
        :param filter: dictionary with dimension name and list of codes passing the filter
        :param out_dimensions: List of dimensions in the output
        :param out_measures: List of measures in the output dataset. Each measure: aggregation function, measure to aggregate, result measure name
        :param out_dataset_name: Name of the output dataset
        :return: pd.DataFrame compatible with NIS format (append_command method can be invoked immediately, then submit method and the "after submission" methods to obtain the resulting dataset)
        """
        pass

    # --------------- SUBMISSION ---------------

    def submit(self, ignore_imports=False) -> List:
        if len(self._dataframes) == 0:
            raise Exception("The list of dataframes is empty. Cannot submit.")

        # A reproducible session must be open
        if self._isession:
            # Add system-level entities from JSON definition in "default_cmds"
            self._isession.register_andor_execute_command_generator("json", "application/json", nexinfosys.default_cmds, False, True)

            # PARSE AND BUILD!!!
            generator_type = "spreadsheet"
            content_type = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            buffer = self._generate_in_memory_excel()
            logging.debug(len(buffer))
            execute = True
            register = False

            try:
                ret = self._isession.register_andor_execute_command_generator(generator_type, content_type, buffer, register, execute, ignore_imports)
                if isinstance(ret, tuple):
                    issues = ret[0]
                else:
                    issues = []

                # TODO CHECK SEMANTIC INCONSISTENCIES. Referred to values in Interfaces use either Parameters

            except Exception as e:
                traceback.print_exc()  # Print the Exception to std output
                # Obtain trace as string; split lines in string; take the last three lines
                tmp = traceback.format_exc().splitlines()
                for i in range(len(tmp) - 3, 0, -2):
                    if tmp[i].find("nexinfosys") != -1:
                        tmp = [tmp[-1], tmp[i], tmp[i + 1]]
                        break
                else:
                    tmp = [tmp[-1], "Nexinfosys module not found", "Line not found"]
                exc_info = ' :: '.join([s.strip() for s in tmp])
                # Error Issue with the extracted Exception text
                issues = [Issue(itype=IType.ERROR,
                                description=f"UNCONTROLLED CONDITION: {exc_info}. Please, contact the development team.",
                                location=None)]

            # STORE the issues
            self._issues = issues
            self._state = self._isession.state
            return self._issues
        else:
            raise Exception("Call 'open_session' before submitting")

    def solve(self) -> List:
        # A reproducible session must be open
        if self._isession:
            try:
                # SOLVE !!!!!
                if not any_error_issue(self._issues):
                    issues2 = prepare_and_solve_model(self._isession.state)
                    self._issues.extend(issues2)
            except Exception as e:
                traceback.print_exc()  # Print the Exception to std output
                # Obtain trace as string; split lines in string; take the last three lines
                tmp = traceback.format_exc().splitlines()
                for i in range(len(tmp) - 3, 0, -2):
                    if tmp[i].find("nexinfosys") != -1:
                        tmp = [tmp[-1], tmp[i], tmp[i + 1]]
                        break
                else:
                    tmp = [tmp[-1], "Nexinfosys module not found", "Line not found"]
                exc_info = ' :: '.join([s.strip() for s in tmp])
                # Error Issue with the extracted Exception text
                self._issues.append(
                    Issue(itype=IType.ERROR,
                          description=f"UNCONTROLLED CONDITION: {exc_info}. Please, contact the development team.",
                          location=None))

            self._state = self._isession.state

            return self._issues
        else:
            raise Exception("Call 'open_session' before submitting")

    def submit_and_solve(self):
        self.submit()
        return self.solve()

    def get_state(self):
        return self._state

    # --------------- AFTER SUBMISSION ---------------

    def query_parameters(self):
        """
        Return the parameters.
        For each parameter return: name, type (Number, Code, Boolean, String) and the domain (range for Number, list of codes for Code)

        :return:
        """
        if self._isession:
            r = get_parameters_in_state(self._isession.state)
            return r
        else:
            raise Exception("Call 'open_session' before querying for available parameters")

    def query_scenarios(self):
        """
        Return the list of scenarios and their parameters

        :return:
        """
        if self._isession:
            r = get_scenarios_in_state(self._isession.state)
            return r
        else:
            raise Exception("Call 'open_session' before querying for available scenarios")

    def recalculate(self, parameters: dict):
        """
        Once a case study has been submitted, recalculate (RESUBMIT?) with new parameter values

        :param parameters:
        :return:
        """
        if self._isession:
            # Pass the dictionary of parameters
            issues2 = prepare_and_solve_model(self._isession.state, parameters)
            return issues2
        else:
            raise Exception("Call 'open_session' before resubmitting a new scenario")

    def query_available_datasets(self):
        def get_results_in_session(isess: InteractiveSession):
            dataset_formats = ["CSV", "XLSX", "SDMX.json"]  # , "XLSXwithPivotTable", "NISembedded", "NISdetached"]
            graph_formats = ["VisJS", "html", "GML"]  # , "GraphML"]
            ontology_formats = ["OWL"]
            geo_formats = ["GeoJSON"]
            # A reproducible session must be open, signal about it if not
            if isess.reproducible_session_opened():
                if isess.state:
                    glb_idx, p_sets, hh, datasets, mappings = get_case_study_registry_objects(isess.state)
                    r = [dict(name=k,
                              type="dataset",
                              description=F"{datasets[k].description} [{datasets[k].data.shape[0]} rows, {datasets[k].data.size} cells, "
                                          F"{datasets[k].data.memory_usage(True).sum()} bytes]",
                              formats=[dict(format=f,
                                            name=f"{k}.{f.lower()}")
                                       for f in dataset_formats],

                              ) for k in datasets
                         ] + \
                        [
                            dict(name="interfaces_graph",
                                 type="graph",
                                 description="Graph of Interfaces, Quantities; Scales and Exchanges",
                                 formats=[dict(format=f,
                                               name=F"interfaces_graph.{f.lower()}")
                                          for f in graph_formats]),
                        ] + \
                        [dict(name="processors_graph",
                              type="graph",
                              description="Processors and exchanges graph",
                              formats=[dict(format=f,
                                            url=F"processors_graph.{f.lower()}")
                                       for f in graph_formats])
                         ] + \
                        [dict(name="processors_geolayer",
                              type="geolayer",
                              description="Processors, located in a geographic layer",
                              formats=[
                                  dict(format=f, url=F"geolayer.{f.lower()}")
                                  for f in geo_formats]),
                         ] + \
                        [dict(name="model",
                              type="model",
                              description="Model",
                              formats=[
                                  dict(format=f, url=F"model.{f.lower()}")
                                  for f in ["JSON", "XLSX", "XML"]]),
                         ]
                        # [dict(name="Ontology",
                        #       type="ontology",
                        #       description="OWL ontology",
                        #       formats=[
                        #           dict(format=f, url=F"ontology.{f.lower()}")
                        #           for f in ontology_formats]),
                        #  ]

            return r

        if self._isession:
            # Obtain a list of datasets and the type
            r = get_results_in_session(self._isession)
            if r is not None:
                return r
            else:
                raise Exception("Could not retrieve the available datasets")
        else:
            raise Exception("Call 'open_session' before querying for available datasets")

    def get_results(self, datasets: List[Tuple[str, str, str]]):
        """
        Get one or more results, in the specified format

        List of Tuple: ("type of structure", "dataset name", "output format")

        Example:
          nis.get_results(["dataset", "ds1"])
          nis.get_results([None, "ds1"])  # Guesses the dataset type

        :param datasets:
        :return:
        """
        if self._isession:
            struc_types = {ds["name"]: ds["type"] for ds in self.query_available_datasets()}

            res = []

            for t in datasets:
                struc_type = t[0]
                ds_name = t[1]
                if len(t) == 3:
                    out_format = t[2]
                else:
                    out_format = "bytearray"

                if "." in ds_name:
                    pos = ds_name.find(".")
                    extension = ds_name[pos + 1:]
                    ds_name = ds_name[:pos]
                else:
                    extension = None

                if struc_type is None:
                    struc_type = struc_types.get(ds_name, "")

                if struc_type == "dataset":
                    ds, ctype, ok = get_dataset_from_state(self._isession.state, ds_name, extension, labels_enabled=True)
                elif struc_type == "graph":
                    ds, ctype, ok = get_graph_from_state(self._isession.state, ds_name+"."+extension)
                elif struc_type == "geolayer":
                    ds, ctype, ok = get_geolayer(self._isession.state, extension)
                elif struc_type == "model":
                    ds, ctype, ok = get_model(self._isession.state, extension)
                elif struc_type == "ontology":
                    ds, ctype, ok = get_ontology(self._isession.state, extension)
                else:
                    ds, ctype, ok = None, None, False

                res.append((ds, ctype, ok))
            return res
        else:
            raise Exception("Call 'open_session' before obtaining available datasets")

    # --------------- SYNTAX FUNCTIONS ---------------

    @staticmethod
    def validate_cell(command_type: str, column_name: str, value: str):
        """
        Validate syntax of cell, given the command type and column under which the value is specified

        :param command_type: Command type
        :param column_name: Column in command type
        :param value: Specific cell value
        :return:
        """
        return validate_command(dict(command=command_type, fields={column_name: value}))

    def list_of_available_command_types(self):
        """
        List of commands
        :return:
        """

    def fields_for_command_type(self, command_type: str):
        """
        Fields expected by command type
        :param command_type: Name of t
        :return:
        """

    def examples_for_command_type(self, command_type: str):
        """
        Examples of command type. Returns a list of CSVs or URLs (pointing to case studies using this command type)

        :param command_type: Command type
        :return:
        """

    def description_for_command_type(self, command_type: str):
        """
        A text describing the purpose and semantics of the command type

        :param command_type: Command type
        :return:
        """

    @staticmethod
    def command_fields_help(command_type: str, column_name: str):
        return command_field_help(dict(command=command_type, fields=[column_name]))

    @staticmethod
    def command_help(command_type: str):
        return comm_help(dict(command=command_type))


if __name__ == '__main__':
    nis = NIS()
    nis.open_session(True)
    nis.reset_commands()
    # Load file and execute it
    nis.load_workbook("https://docs.google.com/spreadsheets/d/15NNoP8VjC2jlhktT0A8Y0ljqOoTzgar8l42E5-IRD90/edit#gid=2056162596")
    issues = nis.submit()

    tmp = nis.query_available_datasets()
    print("-------------------")
    print(tmp)
    d = nis.get_results([(tmp[0]["type"], tmp[0]["name"])])
    print(d)

    serial_state = serialize_state(nis.get_state())
    nis.close_session()
    # -------------------
    for i in range(10):
        nis = NIS()
        nis.open_session(True, deserialize_state(serial_state))
        nis.load_workbook("https://docs.google.com/spreadsheets/d/1z8QKYkvlUPLAwo6NYM8rRLMj7_QjPRYOgAsTRI2HwWE/edit#gid=1467907392")
        nis.submit(ignore_imports=True)

        tmp = nis.query_available_datasets()
        print("-------------------")
        print(tmp)
        d = nis.get_results([(tmp[0]["type"], tmp[0]["name"])])
        print(d)

        nis.close_session()

    examples = [
        ("https://docs.google.com/spreadsheets/d/1z8QKYkvlUPLAwo6NYM8rRLMj7_QjPRYOgAsTRI2HwWE/edit#gid=1467907392", False),
        ("https://docs.google.com/spreadsheets/d/12AlJ0tdu2b-cfalNzLqFYfiC-hdDlIv1M1pTE3AfSWY/edit#gid=84311637", False),
        ("/home/rnebot/GoogleDrive/AA_PROPUESTAS/Sentinel/Enviro/examples/01_eurostat_datasets.xlsx", False),
        ("/home/rnebot/GoogleDrive/AA_PROPUESTAS/Sentinel/Enviro/examples/howTo Examples SC-Observers.xlsx", True),
        # ("/home/rnebot/Dropbox/nis-backend/backend_tests/z_input_files/v2/Netherlandsv1ToNISHierarchy2.xlsx", True),
        # ("/home/rnebot/Dropbox/nis-backend/backend_tests/z_input_files/v2/Biofuel_NIS.xlsx", False),
                ]
    nis = NIS()
    for example in examples:
        nis.open_session()
        nis.reset_commands()
        nis.load_workbook(example[0])
        issues = nis.submit()
        if example[1]:
            issues2 = nis.solve()
        tmp = nis.query_available_datasets()
        print(tmp)
        d = nis.get_results([(tmp[0]["type"], tmp[0]["name"])])
        print(d)

