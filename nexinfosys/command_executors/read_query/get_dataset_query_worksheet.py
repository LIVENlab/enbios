import json

from nexinfosys.model_services import IExecutableCommand
from nexinfosys import data_source_manager as dsm


class GetDatasetQueyWorksheetCommand(IExecutableCommand):
    """
    Obtain a
    """
    def __init__(self, name: str):
        self._name = name
        self._content = None

    def execute(self, state: "State"):
        """

        """
        # The data source manager is "dsm"


        state.set(self._var_name, self._description)
        return None, None

    def estimate_execution_time(self):
        return 0

    def json_serialize(self):
        # Directly return the metadata dictionary
        return self._content

    def json_deserialize(self, json_input):
        # TODO Read and check keys validity
        issues = []
        if isinstance(json_input, dict):
            self._content = json_input
        else:
            self._content = json.loads(json_input)
        return issues

