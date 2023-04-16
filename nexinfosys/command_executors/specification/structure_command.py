import json
import traceback

from nexinfosys.command_generators import parser_field_parsers
from nexinfosys.command_generators.parser_ast_evaluators import ast_to_string
from nexinfosys.models.musiasem_concepts_helper import create_relation_observations
from nexinfosys.model_services import IExecutableCommand, State, get_case_study_registry_objects


class StructureCommand(IExecutableCommand):
    """
    It serves to specify the structure connecting processors or flows/funds

    """
    def __init__(self, name: str):
        self._name = name
        self._content = None

    def execute(self, state: "State"):
        """
            Process each of the specified relations, creating the endpoints if they do not exist already
            {"name": <processor or factor>,
             "attributes": {"<attr>": "value"},
             "type": <default relation type>,
             "dests": [
                {"name": <processor or factor>,
                 ["type": <relation type>,]
                 "weight": <expression resulting in a numeric value>
                }
             }
        """
        some_error = False
        issues = []
        glb_idx, _, _, _, _ = get_case_study_registry_objects(state)

        # Process each record
        for i, o in enumerate(self._content["structure"]):
            # origin processor[+factor] -> relation (weight) -> destination processor[+factor]
            origin_name = o["origin"]
            if "source" in o:
                source = o["source"]
            else:
                source = None
            if "default_relation" in o:
                default_relation = o["default_relation"]
            else:
                default_relation = None

            destinations = []
            for r in o["destinations"]:
                try:
                    result = parser_field_parsers.string_to_ast(parser_field_parsers.factor_name, r)
                except:
                    try:
                        result = parser_field_parsers.string_to_ast(parser_field_parsers.relation_expression, r)
                    except:
                        traceback.print_exc()
                        some_error = True
                        issues.append((3,
                                       "The specification of destination, '" + r + "', is not valid, in element " + str(
                                           r) + ". It is a sequence of weight (optional) relation (optional) destination element (mandatory)"))

                if result:
                    if result["type"] == "pf_name":
                        base = result
                    else:
                        base = result["name"]
                    tmp = base["processor"]
                    destination_name = ((tmp["ns"] + "::") if "ns" in tmp and tmp["ns"] else '') + '.'.join(tmp["parts"])
                    if "factor" in base and base["factor"]:
                        tmp = base["factor"]
                        destination_name += ':' + ((tmp["ns"] + "::") if "ns" in tmp and tmp["ns"] else '') + '.'.join(tmp["parts"])
                    if "relation_type" in result and result["relation_type"]:
                        rel_type = result["relation_type"]
                    else:
                        rel_type = None
                    if "weight" in result and result["weight"]:
                        weight = ast_to_string(result["weight"])  # For flow relations
                    else:
                        weight = None
                    if rel_type and weight:
                        t = (destination_name, rel_type, weight)
                    elif rel_type and not weight:
                        t = (destination_name, rel_type)
                    elif not rel_type and not weight:
                        t = tuple([destination_name])  # Force it to be a tuple (create_relation_observations expects that)

                    destinations.append(t)

            rels = create_relation_observations(glb_idx, origin_name, destinations, default_relation, source)

        return issues, None

    def estimate_execution_time(self):
        return 0

    def json_serialize(self):
        # Directly return the content
        return self._content

    def json_deserialize(self, json_input):
        # TODO Check validity
        issues = []
        if isinstance(json_input, dict):
            self._content = json_input
        else:
            self._content = json.loads(json_input)
        return issues
