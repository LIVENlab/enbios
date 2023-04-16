from openpyxl.worksheet.worksheet import Worksheet

from nexinfosys import metadata_fields, AreaTupleType, IssuesLabelContentTripleType
from nexinfosys.common.helper import create_dictionary


def parse_metadata_command(sh: Worksheet, area: AreaTupleType, name: str = None) -> IssuesLabelContentTripleType:
    """
    Most "parse" methods are mostly syntactic (as opposed to semantic). They do not check existence of names.
    But in this case, the valid field names are fixed beforehand, so they are checked at this time.
    Some of the fields will be controlled also, according to some

    :param sh: Input worksheet
    :param area: Tuple (top, bottom, left, right) representing the rectangular area of the input worksheet where the
    command is present
    :return: list of issues (issue_type, message), command label, command content
    """
    some_error = False
    issues = []
    controlled = create_dictionary()
    mandatory = create_dictionary()
    keys = create_dictionary()
    for t in metadata_fields:
        controlled[t[4]] = t[3]
        mandatory[t[4]] = t[2]
        keys[t[0]] = t[4]

    # Scan the sheet, the first column must be one of the keys of "k_list", following
    # columns can contain repeating values

    # Map key to a list of values
    content = {}  # Dictionary of lists, one per metadata key
    for r in range(area[0], area[1]):
        label = sh.cell(row=r, column=area[2]).value
        if label in keys:
            key = keys[label]
            for c in range(area[2]+1, area[3]):
                value = sh.cell(row=r, column=c).value
                if value:
                    value = str(value).strip()
                    if controlled[key]:
                        # Control "value" if the field is controllable
                        cl = {"dimensions": ["water", "energy", "food", "land", "climate"],
                              "subject_topic_keywords": None,
                              "geographical_level": ["local", "regional", "region", "country", "europe", "global", "sectoral", "sector"],
                              "geographical_situation": None,  # TODO Read the list of all geographical regions (A long list!!)
                              "restriction_level": ["internal", "confidential", "public"],
                              "language": None,  # TODO Read the list of ALL languages (or just "English"??)
                              }
                        if cl[key] and value.lower() not in cl[key]:
                            issues.append((3, "The key '"+key+"' should be one of: "+",".join(cl[key])))

                    if key not in content:
                        content[key] = []
                    content[key].append(value)
        else:
            issues.append((2, "Row "+str(r)+": unknown metadata label '"+label+"'"))

    for key in keys.values():
        if mandatory[key] and key not in content:
            some_error = True
            issues.append((3, "The value '"+key+"' is mandatory in the definition of the metadata"))

    return issues, None, content
    # else:
    #     if not some_error:
    #         cmd = MetadataCommand(None)
    #         cmd.json_deserialize(content)
    #     else:
    #         cmd = None
    #     return cmd, issues


