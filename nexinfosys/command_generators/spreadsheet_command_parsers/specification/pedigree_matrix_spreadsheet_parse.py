from openpyxl.worksheet.worksheet import Worksheet

from nexinfosys import IssuesLabelContentTripleType, AreaTupleType
from nexinfosys.command_generators import parser_field_parsers


def parse_pedigree_matrix_command(sh: Worksheet, area: AreaTupleType, name: str) -> IssuesLabelContentTripleType:
    """
    A pedigree matrix is formed by several columns, with a header naming a phase, and below a list of modes, normally in
    ascending qualitative order.

    Modes can be referred later by the order number specified in the "Code" column (mandatory). The order of the columns
    serves also to sequence the codes of the matrix, from left to right.

    Columns can be accompanied by a description column, to the right

    :param sh: Input worksheet
    :param area: Tuple (top, bottom, left, right) representing the rectangular area of the input worksheet where the
    command is present
    :param name: Name of the Pedigree Matrix
    :return: list of issues (issue_type, message), command label, command content
    """

    issues = []

    # Analyze columns
    phases = []  # A phase per column
    codes = None  # Column with codes
    max_len = 0  # Column with max length
    for c in range(area[2], area[3]):
        phase_modes = []
        current_phase = None
        for r in range(area[0], area[1]):
            value = sh.cell(row=r, column=c).value
            # First row has to be defined. If not, skip to the next column
            if r == area[0] and not value:
                break
            if value is None:
                continue

            if r == area[0]:
                current_phase = value

            try:
                if current_phase.lower() != "code":
                    parser_field_parsers.string_to_ast(parser_field_parsers.simple_ident, value)
                else:
                    if r != area[0]:
                        # An Integer
                        try:
                            int(value)
                        except:
                            issues.append((3, "The code must be an integer"))
            except:
                if r == area[0]:
                    issues.append((3, "Phase '"+value+"' of the Pedigree Matrix must be a simple identity (alphabet letter followed by either alphabet letters or numbers"))
                else:
                    issues.append((3, "A mode ("+value+") in phase '"+current_phase+"' of the Pedigree Matrix must be a simple identity (alphabet letter followed by either alphabet letters or numbers"))

            # Append mode to the current phase
            phase_modes.append(dict(mode=value, description=""))

        # Check: at least one element
        if len(phase_modes) < 2:
            issues.append((3, "Phase '"+current_phase+"' should have at least one mode"))

        # Check: no repetitions
        if len(phase_modes) != len(set([mode["mode"] for mode in phase_modes])):
            if current_phase.lower() != "code":
                issues.append((3, "There is at least a repeated mode in phase '"+current_phase+"'"))
            else:
                issues.append((3, "There is at least a repeated code in the list of codes"))

        # Update max column length
        if len(phase_modes) > max_len:
            max_len = len(phase_modes)

        if current_phase.lower() != "code":
            phases.append(phase_modes)
        else:
            codes = phase_modes[1:]

    # If not codes
    if not codes:
        codes = [str(i) for i in range(max_len-2, -1, -1)]

    return issues, None, dict(name=name, codes=codes, phases=phases)


