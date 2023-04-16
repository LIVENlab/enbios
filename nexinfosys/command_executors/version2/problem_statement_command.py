import json

from nexinfosys.command_generators import Issue, IType
from nexinfosys.command_generators.parser_ast_evaluators import check_parameter_value
from nexinfosys.command_generators.spreadsheet_command_parsers_v2 import IssueLocation
from nexinfosys.common.helper import create_dictionary, strcmp
from nexinfosys.model_services import IExecutableCommand, get_case_study_registry_objects, State
from nexinfosys.models.musiasem_concepts import Parameter, ProblemStatement, Hierarchy


class ProblemStatementCommand(IExecutableCommand):
    def __init__(self, name: str):
        self._name = name
        self._content = None

    def execute(self, state: "State"):
        any_error = False
        issues = []
        sheet_name = self._content["command_name"]
        # Obtain global variables in state
        glb_idx, p_sets, hh, datasets, mappings = get_case_study_registry_objects(state)

        scenarios = create_dictionary()

        for r, param in enumerate(self._content["items"]):
            parameter = param["parameter"]
            scenario = param.get("scenario_name")
            p = glb_idx.get(Parameter.partial_key(parameter))
            if len(p) == 0:
                issues.append(Issue(itype=IType.ERROR,
                                    description="The parameter '" + parameter + "' has not been declared previously.",
                                    location=IssueLocation(sheet_name=sheet_name, row=r, column=None)))
                any_error = True
                continue

            p = p[0]
            name = parameter

            value = param.get("parameter_value")

            check_parameter_value(glb_idx, p, value, issues, sheet_name, r)

            description = param.get("description")  # For readability of the workbook. Not used for solving
            if scenario:
                if scenario in scenarios:
                    sp = scenarios[scenario]
                else:
                    sp = create_dictionary()
                    scenarios[scenario] = sp
                sp[name] = value
            else:
                p.current_value = value
                p.default_value = value

        if not any_error:
            solver_parameters = {}  # {p.name: p.current_value for p in glb_idx.get(Parameter.partial_key()) if p.group and strcmp(p.group, "NISSolverParameters")}
            if len(scenarios) == 0:
                scenarios["default"] = create_dictionary()
            ps = ProblemStatement(solver_parameters, scenarios)
            glb_idx.put(ps.key(), ps)

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
