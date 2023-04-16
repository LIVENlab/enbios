from nexinfosys.command_generators import parser_field_parsers
from nexinfosys.common.helper import create_dictionary


def parse_line(item, fields):
    """
    Convert fields from a line to AST

    :param item:
    :param fields:
    :return:
    """
    asts = {}
    for f, v in item.items():
        if not f.startswith("_"):
            field = fields[f]
            # Parse (success is guaranteed because of the first pass dedicated to parsing)
            asts[f] = parser_field_parsers.string_to_ast(field.parser, v)
    return asts


def classify_variables(asts, datasets, hierarchies, parameters):
    """
    Iterate through the variables in a AST, determining
    for each if it is a:
    * dataset
    * hierarchy
    * parameter
    * or unknown

    :param asts: The AST
    :param datasets: The collection of Datasets
    :param hierarchies: The collection of Category Hierarchies
    :param parameters: The collection of Parameters
    :return: A dictionary with the classification,
    """
    ds = set()
    ds_concepts = set()
    hh = set()
    params = set()
    not_classif = set()
    for ast in asts:
        for var in ast["variables"] if "variables" in ast else []:
            parts = var.split(".")
            first_part = parts[0]
            if first_part in datasets:
                ds.add(datasets[first_part])
                ds_concepts.add(parts[1])  # Complete concept name: dataset"."concept
            elif first_part in hierarchies:
                hh.add(hierarchies[first_part])
            elif first_part in parameters:
                params.add(first_part)
            else:
                not_classif.add(first_part)

    return dict(datasets=ds, ds_concepts=ds_concepts, hierarchies=hh, parameters=params, not_classif=not_classif)


def classify_variables2(h_names, datasets, hierarchies, parameters):
    """
    Iterate through the hierarchical names in "h_names", determining for each if it is a:
        * dataset,
        * hierarchy,
        * parameter, or
        * unknown

    :param h_names: List of hierarchical names
    :param datasets: The collection of Datasets
    :param hierarchies: The collection of Category Hierarchies
    :param parameters: The collection of Parameters
    :return: A dictionary with the classification,
    """
    ds = set()
    ds_concepts = set()
    hh = set()
    params = set()
    not_classif = set()
    for var in h_names:
        parts = var.split(".")
        first_part = parts[0]
        if first_part in datasets:
            ds.add(datasets[first_part])
            ds_concepts.add(parts[1])  # Complete concept name: dataset"."concept
        elif first_part in hierarchies:
            hh.add(hierarchies[first_part])
        elif first_part in parameters:
            params.add(first_part)
        else:
            not_classif.add(first_part)

    return dict(datasets=list(ds), ds_concepts=list(ds_concepts), hierarchies=list(hh), parameters=list(params), not_classif=list(not_classif))


def obtain_dictionary_with_literal_fields(item, asts):
    d = create_dictionary()
    for f in item:
        if not f.startswith("_"):
            ast = asts[f]
            if "complex" not in ast or ("complex" in ast and not ast["complex"]):
                d[f] = item[f]
    return d
