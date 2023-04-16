import json

from nexinfosys.command_generators import Issue, IType
from nexinfosys.command_generators.spreadsheet_command_parsers_v2 import IssueLocation
from nexinfosys.models.musiasem_concepts import Parameter
from nexinfosys.model_services import IExecutableCommand, get_case_study_registry_objects


# TODO Created for V1 commands, it passes directly to V2 because it has been sparsely used
class ParametersCommand(IExecutableCommand):
    def __init__(self, name: str):
        self._name = name
        self._content = None

    def execute(self, state: "State"):
        issues = []
        sheet_name = self._content["command_name"]
        # Obtain global variables in state
        glb_idx, p_sets, hh, datasets, mappings = get_case_study_registry_objects(state)

        for r, param in enumerate(self._content["items"]):
            name = param["name"]
            p = glb_idx.get(Parameter.partial_key(name))
            if len(p) > 0:
                issues.append(Issue(itype=IType.WARNING,
                                    description="The parameter '" + name + "' has been declared previously. Skipped.",
                                    location=IssueLocation(sheet_name=sheet_name, row=r, column=None)))
                continue
            p = Parameter(name)
            p._default_value = p._current_value = param.get("value")
            p._type = param.get("type")
            p._range = param.get("domain")
            p._description = param.get("description")
            p._group = param.get("group")
            glb_idx.put(p.key(), p)
        return issues, None

    def estimate_execution_time(self):
        return 0

    def json_serialize(self):
        # Directly return the metadata dictionary
        return self._content

    def json_deserialize(self, json_input):
        # TODO Check validity
        issues = []
        if isinstance(json_input, (dict, list)):
            self._content = json_input
        else:
            self._content = json.loads(json_input)
        return issues
