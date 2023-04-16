"""
Declaration of Observables and relations between Processors and/or Factors

"""
import collections
import traceback

from openpyxl.worksheet.worksheet import Worksheet

from nexinfosys import IssuesLabelContentTripleType, AreaTupleType
from nexinfosys.command_generators import parser_field_parsers


def parse_structure_command(sh: Worksheet, area: AreaTupleType, name: str = None) -> IssuesLabelContentTripleType:
    """
    Analyze the input to produce a JSON object with a list of Observables and relations to other Observables

    Result:[
            {"origin": <processor or factor>,
             "description": <label describing origin>,
             "attributes": {"<attr>": "value"},
             "default_relation": <default relation type>,
             "dests": [
                {"name": <processor or factor>,
                 Optional("relation": <relation type>,)
                 "weight": <expression resulting in a numeric value>
                }
             }
            ]
    :param sh: Input worksheet
    :param area: Tuple (top, bottom, left, right) representing the rectangular area of the input worksheet where the
    command is present
    :return: list of issues (issue_type, message), command label, command content
    """
    some_error = False
    issues = []

    # Scan the sheet, the first column must be one of the keys of "k_list", following
    # columns can contain repeating values
    col_names = {("origin", "name"): "origin",
                 ("relation", "default relation"): "default_relation",
                 ("destination", "destinations"): "destinations",
                 ("origin label", "label"): "description"
                 }
    # Check columns
    col_map = collections.OrderedDict()
    for c in range(area[2], area[3]):
        col_name = sh.cell(row=area[0], column=c).value
        if not col_name:
            continue

        for k in col_names:
            if col_name.lower() in k:
                col_map[c] = col_names[k]
                break

    # Map key to a list of values
    content = []  # Dictionary of lists, one per metadata key
    for r in range(area[0]+1, area[1]):
        item = {}
        for c in col_map:
            value = sh.cell(row=r, column=c).value
            if not value:
                continue

            k = col_map[c]
            if k == "origin":  # Mandatory
                # Check syntax
                try:
                    parser_field_parsers.string_to_ast(parser_field_parsers.factor_name, value)
                    item[k] = value
                except:
                    some_error = True
                    issues.append((3, "The name specified for the origin element, '" + value + "', is not valid, in row " + str(r) + ". It must be either a processor or a factor name."))
            elif k == "default_relation":  # Optional (if not specified, all destinations must specify it)
                # Check syntax
                allowed_relations = ('|', '>', '<', '<>', '><', '||')
                if value in allowed_relations:
                    item[k] = value
                else:
                    some_error = True
                    issues.append((3, "The Default relation type specified for the origin element, '" + value + "', is not valid, in row " + str(r) + ". It must be one of " + ', '.join(allowed_relations) + "."))
            elif k == "destinations":  # Mandatory
                # Because the expression (weight relation p_f_name) and the simple p_f_name can collide syntactically,
                # first try the simpler expression then the complex one
                try:
                    dummy = parser_field_parsers.string_to_ast(parser_field_parsers.factor_name, value)
                except:
                    try:
                        dummy = parser_field_parsers.string_to_ast(parser_field_parsers.relation_expression, value)
                    except:
                        traceback.print_exc()
                        some_error = True
                        issues.append((3,
                                       "The specification of destination, '" + value + "', is not valid, in row " + str(
                                           r) + ". It is a sequence of weight (optional) relation (optional) destination element (mandatory)"))

                    # Check syntax. It can contain: a weight, a relation type, a processor or factor name.

                if dummy:
                    if k not in item:
                        lst = []
                        item[k] = lst
                    else:
                        lst = item[k]
                    lst.append(value)
            elif k == "description":  # Optional
                item[k] = value

        # Check parameter completeness before adding it to the list of parameters
        if "origin" not in item:
            issues.append((3, "The element must have an Origin, row "+str(r)))
            continue
        if "destinations" not in item:
            issues.append((3, "The element must have at least one Destination, row "+str(r)))
            continue

        content.append(item)

    return issues, None, dict(structure=content)


