from openpyxl.worksheet.worksheet import Worksheet

from nexinfosys import IssuesLabelContentTripleType, AreaTupleType
from nexinfosys.command_generators.spreadsheet_command_parsers.specification import profile_field_name_sets, ref_prof


def find_profile(col_names):
    # Convert in set and remove "ref_id"
    cn = set(col_names)
    cn.remove("ref_id")
    for prof_name, fields in profile_field_name_sets().items():
        if fields.issuperset(cn):
            for prof in ref_prof:
                if prof["type"] == prof_name:
                    return prof
            raise Exception("Should not be here!?")
    return None  # Free profile (or misspelled field name)


def validate(v, field):
    return True


def parse_references_command(sh: Worksheet, area: AreaTupleType, name: str = None) -> IssuesLabelContentTripleType:
    """
    Elaborate a list of dictionaries {key: value} which can be reused by other objects, referring them by a unique ref_id field

    For the definition of keys:values, two options:
     * If the column has a header, that would be the key
     * If the column does not have a header, both key and value can be specified in the cell, separated by "->" or ":" (which one?)

    :param sh: Input worksheet
    :param area: Tuple (top, bottom, left, right) representing the rectangular area of the input worksheet where the
    command is present
    :return: list of issues (issue_type, message), command label, command content
    """

    some_error = False
    issues = []

    references = []

    column_names = []
    for c in range(area[2], area[3]):
        value = sh.cell(row=area[0], column=c).value
        column_names.append(value.lower())
    if "ref_id" not in column_names:
        issues.append((3, "'ref_id' column is mandatory"))

    if some_error:
        return issues, None, []

    # Determine the type of reference contained in the worksheet
    profile = find_profile(column_names)
    if profile:
        type_ = profile["type"]
        col2field = {}
        for col in column_names:
            for f in profile["fields"]:
                if col == f.name:
                    col2field[col] = f
                    break
    else:
        type_ = "free_form"

    # Read each row
    for r in range(area[0]+1, area[1]):
        ref = dict(type=type_)  # Result dictionary
        for c in range(area[2], area[3]):
            value = sh.cell(row=area[0], column=c).value
            col = column_names[c-area[2]]
            if col == "ref_id":
                # Validate "ref_id" ¿syntax rules?
                ref["ref_id"] = value
            else:
                ref[col] = value
                field = col2field[col]
                if not validate(value, field):
                    issues.append((3, "Problem validating field '"+field.name+"' with value: "+value))

        references.append(ref)

    content = dict(references=references)

    return issues, None, content
