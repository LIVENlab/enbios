import json
from nexinfosys.model_services import IExecutableCommand, get_case_study_registry_objects
from nexinfosys.solving.solver_one import solver_one


class SolverOne(IExecutableCommand):
    def __init__(self, name: str):
        self._name = name
        self._content = None

    def execute(self, state: "State"):
        some_error = False
        issues = []

        # TODO Read command parameters

        # Invoke solver one. Results stored directly in state. Issues returned
        issues = solver_one(state)

        return issues, None

    def estimate_execution_time(self):
        return 0

    def json_serialize(self):
        # Directly return the metadata dictionary
        return self._content

    def json_deserialize(self, json_input):
        # TODO Check validity
        issues = []
        if isinstance(json_input, dict):
            self._content = json_input
        else:
            self._content = json.loads(json_input)

        if "description" in json_input:
            self._description = json_input["description"]
        return issues