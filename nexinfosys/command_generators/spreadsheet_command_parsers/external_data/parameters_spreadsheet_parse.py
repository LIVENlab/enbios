"""
List of parameters. Name, initial value (optional), domain (range), description

"""
from nexinfosys.command_generators import parser_field_parsers
from nexinfosys.common.helper import strcmp, create_dictionary


def parse_parameters_command(sh, area):
    """
    Analyze the input to produce a JSON object with a list of parameters

    :param sh: Input worksheet
    :param area: Tuple (top, bottom, left, right) representing the rectangular area of the input worksheet where the
    command is present
    :return: list of issues (issue_type, message), command label, command content
    """
    some_error = False
    issues = []

    # Scan the sheet, the first column must be one of the keys of "k_list", following
    # columns can contain repeating values
    col_names = {("name",): "name",
                 ("value",): "value",
                 ("type",): "type",  # category, integer or number
                 ("range", "domain", "interval"): "range",
                 ("group",): "group",  # To group (or filter) parameters
                 ("description", "label", "desc"): "description"
                 }
    # Check columns
    col_map = {}
    for c in range(area[2], area[3]):
        col_name = sh.cell(row=area[0], column=c).value
        for k in col_names:
            if col_name.lower() in k:
                col_map[col_names[k]] = c
                break

    # Map key to a list of values
    content = []  # Dictionary of lists, one per metadata key
    for r in range(area[0]+1, area[1]):
        param = {}
        for k in col_names.values():
            if k not in col_map:
                continue

            value = sh.cell(row=r, column=col_map[k]).value

            if not value:
                continue

            if k == "name":  # Mandatory
                # Check syntax
                try:
                    parser_field_parsers.string_to_ast(parser_field_parsers.simple_ident, value)
                    param[k] = value
                except:
                    some_error = True
                    issues.append((3, "The name specified for the parameter, '" + value + "', is not valid, in row " + str(r) + ". It must be a simple identifier."))
            elif k == "value":  # Optional
                # Check syntax
                try:
                    # TODO Define the parser
                    # basic_elements_parser.string_to_ast(basic_elements_parser.simple_ident, value)
                    param[k] = value
                except:
                    some_error = True
                    issues.append((3, "The Value specified for the parameter, '" + value + "', is not valid, in row " + str(r) + ". It must be an expression."))
            elif k == "type":  # Mandatory
                # Check syntax
                if value.lower().strip() in ("category", "integer", "number", "float"):
                    if value.lower().strip() in ("number", "float"):
                        value = "number"  # "float" --> "number"
                    param[k] = value
                else:
                    some_error = True
                    issues.append((3, "The Type specified for the parameter, '" + value + "', is not valid, in row " + str(r) + ". It must be one of 'category', 'integer', 'number'."))
            elif k == "range":  # Optional
                try:
                    # TODO Define the parser. An interval or a list of possibilities
                    # basic_elements_parser.string_to_ast(basic_elements_parser.simple_ident, value)
                    param[k] = value
                except:
                    some_error = True
                    issues.append((3, "The Range specified for the parameter, '" + value + "', is not valid, in row " + str(r) + ". It must be an interval or a list of categories, depending on the type."))
            elif k == "group":  # Optional
                # Check syntax. A way to group parameters
                try:
                    parser_field_parsers.string_to_ast(parser_field_parsers.simple_ident, value)
                    param[k] = value
                except:
                    some_error = True
                    issues.append((3, "The Group specified for the parameter, '" + value + "', is not valid, in row " + str(r) + ". It must be a simple identifier."))
                param[k] = value
            elif k == "description":  # Optional
                param[k] = value

        # Check parameter completeness before adding it to the list of parameters
        if "name" not in param:
            issues.append((3, "The parameter must have a Name, row "+str(r)))
            continue
        if "type" not in param:
            issues.append((3, "The parameter must have a Type, row " + str(r)))
            continue

        content.append(param)

    return issues, None, content


