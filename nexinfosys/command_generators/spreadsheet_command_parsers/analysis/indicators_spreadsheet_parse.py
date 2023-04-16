from nexinfosys.command_generators import parser_field_parsers


def parse_indicators_command(sh, area):
    """

    :param sh:
    :param area:
    :return:
    """

    some_error = False
    issues = []
    """
        self._name = name
        self._formula = formula
        self._from_indicator = from_indicator
        self._benchmark = benchmark
        self._indicator_category = indicator_category
    
    """
    # Scan the sheet, the first column must be one of the keys
    col_names = {("name",): "name", # Name of the indicator
                 ("formula", "expression",): "formula", # Expression to compute the indicator
                 ("benchmark",): "benchmark",  # Once calculated, a frame to qualify the goodness of the indicator
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
        indicator = {}
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
                    indicator[k] = value
                except:
                    some_error = True
                    issues.append((3, "The name specified for the indicator, '" + value + "', is not valid, in row " + str(r) + ". It must be a simple identifier."))
            elif k == "formula":  # Mandatory
                # Check syntax
                try:
                    parser_field_parsers.string_to_ast(parser_field_parsers.indicator_expression, value)
                    indicator[k] = value
                except:
                    some_error = True
                    issues.append((3, "The Formula specified for the indicator, '" + value + "', is not valid, in row " + str(r) + "."))
            elif k == "benchmark":  # Optional
                # This column can appear multiple times.
                # Check syntax
                if value.lower().strip() in ():
                    if value.lower().strip() in ("number", "float"):
                        value = "number"  # "float" --> "number"
                    indicator[k] = value
                else:
                    some_error = True
                    issues.append((3, "The Type specified for the parameter, '" + value + "', is not valid, in row " + str(r) + ". It must be one of 'category', 'integer', 'number'."))
            elif k == "description":  # Optional
                indicator[k] = value

        # Check indicator completeness before adding it to the list of indicators
        if "name" not in indicator:
            issues.append((3, "The indicator must have a Name, row "+str(r)))
            continue
        if "formula" not in indicator:
            issues.append((3, "The indicator must have a Formula, row "+str(r)))
            continue

        content.append(indicator)

    return issues, None, content
