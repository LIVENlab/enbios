import json
import logging
import re
from typing import Optional, List, Dict, Any, Union, Tuple, Set

from nexinfosys import ExecutableCommandIssuesPairType, Command, CommandField, IssuesOutputPairType, case_sensitive
from nexinfosys.command_definitions import commands
from nexinfosys.command_executors.execution_helpers import classify_variables2
from nexinfosys.command_generators import IType, IssueLocation, Issue, parser_field_parsers
from nexinfosys.command_generators.parser_ast_evaluators import dictionary_from_key_value_list, ast_evaluator
from nexinfosys.command_generators.parser_field_parsers import arith_boolean_expression
from nexinfosys.common.helper import first, PartialRetrievalDictionary, head, strcmp, create_dictionary
from nexinfosys.model_services import IExecutableCommand, get_case_study_registry_objects, State
from nexinfosys.models.musiasem_concepts import Processor, Factor, FactorType, Parameter, Hierarchy, \
    FactorTypesRelationUnidirectionalLinearTransformObservation
from nexinfosys.models.musiasem_concepts_helper import find_processor_by_name, find_factor_types_transform_relation


class CommandExecutionError(Exception):
    pass


def subrow_issue_message(subrow=None):
    return f" (expanded row: {subrow})" if subrow else ""


class BasicCommand(IExecutableCommand):
    def __init__(self, name: str, command_fields: List[CommandField]):
        self._name = name
        self._content: Dict = {}
        self._command_name = ""
        self._command_fields = command_fields

        # Convenience
        self._glb_idx: Optional[PartialRetrievalDictionary] = None
        self._hierarchies = None
        self._datasets = None
        self._mappings = None
        self._parameters: Optional[List[Parameter]] = None
        self._state: Optional[State] = None
        # Execution state per command
        self._issues: List[Issue] = []
        # Execution state per row
        self._current_row_number: Optional[int] = None
        self._fields: Dict[str, Any] = {}

    def _init_execution_state(self, state: Optional["State"] = None) -> None:
        self._issues = []
        self._current_row_number = None
        self._glb_idx = None
        self._hierarchies = None
        self._p_sets = None
        self._datasets = None
        self._mappings = None
        self._parameters = None
        self._fields = {}
        self._state = None

        if state:
            self._glb_idx, self._p_sets, self._hierarchies, self._datasets, self._mappings = get_case_study_registry_objects(state)
            self._parameters = [p.name for p in self._glb_idx.get(Parameter.partial_key())]
            self._state = state

    def _process_row(self, fields: Dict[str, Any], subrow=None) -> None:
        """This is the only method subclasses need to define. See current subclasses as examples"""
        pass

    def _get_command_fields_values(self, row: Dict[str, Any]) -> Dict[str, Any]:
        return {f.name: row.get(f.name, f.default_value) for f in self._command_fields}

    def _check_all_mandatory_fields_have_values(self) -> None:
        empty_fields: List[str] = [f.name
                                   for f in self._command_fields
                                   if f.mandatory and self._fields[f.name] is None]

        if len(empty_fields) > 0:
            raise CommandExecutionError(f"Mandatory field/s '{', '.join(empty_fields)}' is/are empty.")

    def _add_issue(self, itype: IType, description: str) -> None:
        self._issues.append(
            Issue(itype=itype,
                  description=description,
                  location=IssueLocation(sheet_name=self._command_name,
                                         row=self._current_row_number, column=None))
        )
        return

    def _init_and_process_row(self, row: Dict[str, Any]) -> None:
        def obtain_dictionary_with_not_expandable_fields(d):
            output = {}
            for k, v in d.items():
                if v is None or "{" not in v:
                    output[k] = v
            return output

        self._current_row_number = row["_row"]
        self._fields = self._get_command_fields_values(row)
        tmp_fields = self._fields
        self._check_all_mandatory_fields_have_values()
        # If expandable, do it now
        expandable = row["_expandable"]
        if expandable:
            # Extract variables
            state = State()
            issues = []
            asts = {}
            referenced_variables = create_dictionary()
            for e in expandable:
                ast = parser_field_parsers.string_to_ast(arith_boolean_expression, e)
                c_name = f"{{{e}}}"
                asts[c_name] = ast
                res, vars = ast_evaluator(ast, state, None, issues, atomic_h_names=True)
                for v in vars:
                    referenced_variables[v] = None

            res = classify_variables2(referenced_variables.keys(), self._datasets, self._hierarchies, self._parameters)
            ds_list = res["datasets"]
            ds_concepts = res["ds_concepts"]
            h_list = res["hierarchies"]
            if len(ds_list) >= 1 and len(h_list) >= 1:
                self._add_issue(itype=IType.ERROR,
                                description="Dataset(s): " + ", ".join(
                                        [d.name for d in ds_list]) + ", and hierarchy(ies): " + ", ".join([h.name for h in
                                                                                                           h_list]) + ", have been specified. Only a single dataset is supported.")
                return
            elif len(ds_list) > 1:
                self._add_issue(itype=IType.ERROR,
                                description="More than one dataset has been specified: " + ", ".join(
                                        [d.name for d in ds_list]) + ", just one dataset is supported.")
                return
            elif len(h_list) > 0:
                self._add_issue(itype=IType.ERROR,
                                description="One or more hierarchies have been specified: " + ", ".join(
                                        [h.name for h in h_list])
                                )
                return
            if len(ds_list) == 1:  # Expand dataset
                ds = ds_list[0]
                measure_requested = False
                all_dimensions = set([c.code for c in ds.dimensions if not c.is_measure])
                requested_dimensions = set()
                requested_measures = set()
                for con in ds_concepts:
                    found = False
                    for c in ds.dimensions:
                        if strcmp(c.code, con):
                            found = True
                            if c.is_measure:
                                measure_requested = True
                                requested_measures.add(c.code)
                            else:  # Dimension
                                all_dimensions.remove(c.code)
                                requested_dimensions.add(c.code)
                    if not found:
                        self._add_issue(
                            itype=IType.ERROR,
                            description=f"The concept '{{{ds.code}.{con}}}' is not in the dataset '{ds.code}'"
                        )
                        return
                ds_concepts = list(requested_measures)
                ds_concepts.extend(list(requested_dimensions))
                all_dimensions_requested = len(all_dimensions) == 0

                if measure_requested and not all_dimensions_requested:
                    self._add_issue(IType.ERROR,
                                    f"It is not possible to use a measure ({', '.join(requested_measures)}), if not all dimensions are used "
                                    f"(cannot assume implicit aggregation). Dimensions not used: {', '.join(all_dimensions)}")
                    return
                elif not measure_requested and not all_dimensions_requested:
                    # Reduce the Dataframe to unique tuples of the specified dimensions
                    # TODO Consider the current case -sensitive or not-sensitive-
                    data = ds.data[list(requested_dimensions)].drop_duplicates()
                else:  # Take the dataset as-is
                    data = ds.data

                # Remove Index, and do it NOT-INPLACE
                data = data.reset_index()

                # Drop rows with empty dimension value
                # import numpy as np
                # data = data.replace(r'^\s*$', np.NaN, regex=True)
                # data.dropna(subset=requested_dimensions, inplace=True)

                const_dict = obtain_dictionary_with_not_expandable_fields(self._fields)  # row?
                var_dict = set([f for f in self._fields.keys() if f not in const_dict])

                re_concepts = {}
                for c in ds_concepts:
                    c_name = f"{{{ds.code}.{c}}}"
                    if case_sensitive:
                        re_concepts[c_name] = re.compile(c_name)
                    else:
                        re_concepts[c_name] = re.compile(c_name, re.IGNORECASE)

                location = IssueLocation(sheet_name=self._command_name, row=self._current_row_number, column=None)
                already_parsed_fields = set(const_dict.keys())
                for ds_row, row2 in enumerate(data.iterrows()):  # Each row in the dataset
                    # Initialize constant values (those with no "{..}" expressions)
                    row3 = const_dict.copy()
                    # Prepare state to evaluate functions
                    state = State()
                    for c in ds_concepts:
                        state.set(f"{ds.code}.{c}", str(row2[1][c]))
                    state.set("_glb_idx", self._glb_idx)  # Pass PartialRetrievalDictionary to the evaluator. For functions needing it

                    # Evaluate all functions
                    expressions = {}
                    for e, ast in asts.items():
                        res, vars = ast_evaluator(ast, state, None, issues, atomic_h_names=True)
                        expressions[e] = res
                    # Expansion into var_dict
                    for f in var_dict:
                        v = self._fields[f]  # Initial value
                        for item in sorted(expressions.keys(), key=len, reverse=True):
                            v = v.replace(item, expressions[item])
                        row3[f] = v

                    # # Concepts change dictionary
                    # concepts = {}
                    # for c in ds_concepts:
                    #     concepts[f"{{{ds.code}.{c}}}"] = str(row2[1][c])
                    # # Expansion into var_dict
                    # for f in var_dict:
                    #     v = self._fields[f]  # Initial value
                    #     for item in sorted(concepts.keys(), key=len, reverse=True):
                    #         v = re_concepts[item].sub(concepts[item], v)
                    #     row3[f] = v

                    # Syntactic verification of the resulting expansion
                    processable, tmp_issues = parse_cmd_row_dict(self._serialization_type,
                                                                 row3,
                                                                 already_parsed_fields,
                                                                 location)
                    if len(tmp_issues) > 0:
                        self._issues.extend(tmp_issues)
                    # Process row
                    if processable:
                        self._fields = row3
                        self._process_row(row3, ds_row)
                        self._fields = tmp_fields
            elif len(h_list) == 1:  # Expand hierarchy
                pass
        else:
            self._process_row(self._fields)  # Process row

    def execute(self, state: "State") -> IssuesOutputPairType:
        """Main entry point"""
        self._init_execution_state(state)

        for row in self._content["items"]:
            try:
                self._init_and_process_row(row)
            except CommandExecutionError as e:
                self._add_issue(IType.ERROR, str(e))

        return self._issues, None

    def estimate_execution_time(self) -> int:
        return 0

    def json_serialize(self) -> Dict:
        """Directly return the metadata dictionary"""
        return self._content

    def json_deserialize(self, json_input: Union[dict, str, bytes, bytearray]) -> List[Issue]:
        # TODO Check validity
        if isinstance(json_input, dict):
            self._content = json_input
        else:
            self._content = json.loads(json_input)

        self._command_name = self._content["command_name"]

        return []

    def _get_processor_from_field(self, field_name: str) -> Processor:
        processor_name = self._fields[field_name]
        try:
            processor = find_processor_by_name(state=self._glb_idx, processor_name=processor_name)
        except Exception as e:
            raise CommandExecutionError(e)

        if not processor:
            raise CommandExecutionError(f"The processor '{processor_name}' defined in field '{field_name}' "
                                        f"has not been previously declared.")
        return processor

    def _get_interface_from_field(self, field_name: str, processor: Processor) -> Factor:
        interface_name = self._fields[field_name]

        if interface_name is None:
            raise CommandExecutionError(f"No interface has been defined for field '{field_name}'.")

        interface = processor.factors_find(interface_name)
        if not interface:
            raise CommandExecutionError(f"The interface '{interface_name}' has not been found in "
                                        f"processor '{processor.name}'.")

        return interface

    def _get_factor_type_from_field(self, hierarchy_field_name: str, interface_type_field_name: str) -> FactorType:
        interface_type_name = self._fields[interface_type_field_name]
        if not interface_type_name:
            raise CommandExecutionError(f"The field '{interface_type_field_name}' has not been specified")

        # Check if FactorType exists
        interface_types = self._glb_idx.get(FactorType.partial_key(interface_type_name))

        if len(interface_types) == 1:
            return interface_types[0]
        elif len(interface_types) == 0:
            raise CommandExecutionError(f"The interface type '{interface_type_name}' has not been found")
        else:
            hierarchy_name = self._fields[hierarchy_field_name]
            if not hierarchy_name:
                raise CommandExecutionError(f"The field '{hierarchy_field_name}' has not been specified and "
                                            f"the interface type '{interface_type_name}' is not unique")

            interface_type = first(interface_types, lambda t: strcmp(t.hierarchy.name, hierarchy_name))
            if not interface_type:
                raise CommandExecutionError(f"The interface type '{interface_type_name}' has not been found in "
                                            f"hierarchy '{hierarchy_name}'")

            return interface_type

    def _get_factor_types_from_field(self, hierarchy_field_name: str, interface_type_field_name: str) -> List[FactorType]:
        """ Possibly obtain not only one but many InterfaceTypes """

        hierarchy_name = self._fields[hierarchy_field_name]
        interface_type_name = self._fields[interface_type_field_name]

        if not interface_type_name and not hierarchy_name:
            raise CommandExecutionError(f"No hierarchy nor interface type have been specified. At least specify one of them.")
        elif interface_type_name and hierarchy_name:
            interface_types = self._glb_idx.get(FactorType.partial_key(interface_type_name))
            if len(interface_types) == 1:
                return [interface_types[0]]
            elif len(interface_types) == 0:
                raise CommandExecutionError(f"The interface type '{interface_type_name}' has not been found")
            else:
                hierarchy_name = self._fields[hierarchy_field_name]
                if not hierarchy_name:
                    raise CommandExecutionError(f"The field '{hierarchy_field_name}' has not been specified and "
                                                f"the interface type '{interface_type_name}' is not unique")

                interface_type = first(interface_types, lambda t: strcmp(t.hierarchy.name, hierarchy_name))
                if not interface_type:
                    raise CommandExecutionError(f"The interface type '{interface_type_name}' has not been found in "
                                                f"hierarchy '{hierarchy_name}'")

                return [interface_type]
        elif interface_type_name and not hierarchy_name:
            interface_types = self._glb_idx.get(FactorType.partial_key(interface_type_name))
            if len(interface_types) == 1:
                return [interface_types[0]]
            elif len(interface_types) == 0:
                raise CommandExecutionError(f"The interface type '{interface_type_name}' has not been found")
            else:
                raise CommandExecutionError(f"The field '{hierarchy_field_name}' has not been specified and "
                                            f"the interface type '{interface_type_name}' is not unique")
        elif not interface_type_name and hierarchy_name:
            hie = self._glb_idx.get(Hierarchy.partial_key(hierarchy_name))
            if len(hie) == 1:
                # All children of "hierarchy_name"
                return [v for v in hie[0].codes.values()]
            elif len(hie) == 0:
                raise CommandExecutionError(f"The InterfaceTypes hierarchy '{hierarchy_name}' has not been found")
            else:
                raise CommandExecutionError(f"The InterfaceTypes hierarchy '{hierarchy_name}' has been found multiple times!!")

        # Check if FactorType exists
        interface_types = self._glb_idx.get(FactorType.partial_key(interface_type_name))

        if len(interface_types) == 1:
            return interface_types[0]
        elif len(interface_types) == 0:
            raise CommandExecutionError(f"The interface type '{interface_type_name}' has not been found")
        else:
            hierarchy_name = self._fields[hierarchy_field_name]
            if not hierarchy_name:
                raise CommandExecutionError(f"The field '{hierarchy_field_name}' has not been specified and "
                                            f"the interface type '{interface_type_name}' is not unique")

            interface_type = first(interface_types, lambda t: strcmp(t.hierarchy.name, hierarchy_name))
            if not interface_type:
                raise CommandExecutionError(f"The interface type '{interface_type_name}' has not been found in "
                                            f"hierarchy '{hierarchy_name}'")

            return interface_type

    def _transform_text_attributes_into_dictionary(self, text_attributes: str, subrow=None) -> Dict:
        dictionary_attributes = {}
        if text_attributes:
            try:
                dictionary_attributes = dictionary_from_key_value_list(text_attributes, self._glb_idx)
            except Exception as e:
                raise CommandExecutionError(str(e) + subrow_issue_message(subrow))

        return dictionary_attributes

    def _get_attributes_from_field(self, field_name: str) -> Dict:
        return self._transform_text_attributes_into_dictionary(self._fields[field_name])

    def _get_interface_types_transform(self,
                                       source_interface_type: FactorType, source_processor: Processor,
                                       target_interface_type: FactorType, target_processor: Processor,
                                       subrow=None) \
            -> FactorTypesRelationUnidirectionalLinearTransformObservation:
        """Check if a transformation between interfaces has been specified"""

        interface_types_transforms = find_factor_types_transform_relation(
            self._glb_idx, source_interface_type, target_interface_type, source_processor, target_processor)

        if len(interface_types_transforms) == 0:
            raise CommandExecutionError(f"Interface types are not the same (and transformation from one "
                                        f"to the other cannot be performed). Origin: "
                                        f"{source_interface_type.name}; Target: {target_interface_type.name}"+subrow_issue_message(subrow))
        elif len(interface_types_transforms) > 1:
            raise CommandExecutionError(
                f"Multiple transformations can be applied between interfaces. Origin: "
                f"{source_interface_type.name}; Target: {target_interface_type.name}"+subrow_issue_message(subrow))

        return interface_types_transforms[0]


class ParseException(Exception):
    pass


def parse_cmd_row_dict(cmd_name: str, row: Dict[str, str], already_parsed_fields: Set[str], location: IssueLocation) -> Tuple[bool, List[Issue]]:
    """
    Parse a row (as a dictionary) from a command
    It is used after expansion of "macros"

    :param cmd_name: Name of command
    :param row: A dictionary containing the values to parse syntactically. Keys are field names, Values are field values
    :param already_parsed_fields: Set of fields already known to be syntactically valid
    :param location: IssueLocation object to use when creating Issues
    :return: A tuple: a boolean (True if the row can be used, otherwise False) and a list of Issues
    """

    issues: List[Issue] = []

    from nexinfosys.command_field_definitions import command_fields
    field_defs_dict = {f.name: f for f in command_fields[cmd_name]}
    mandatory_not_found = set([c.name for c in command_fields[cmd_name] if c.mandatory and isinstance(c.mandatory, bool)])
    # logging.debug(mandatory_not_found)
    complex_mandatory_cols = [c for c in command_fields[cmd_name] if isinstance(c.mandatory, str)]
    may_append = True
    complex_row = False
    for field_name, field_value in row.items():
        field_def = field_defs_dict.get(field_name)
        if not field_def:
            return ParseException(f"Field {field_name} not found for command {cmd_name}")

        if field_value is not None:
            if not isinstance(field_value, str):
                field_value = str(field_value)
            field_value = field_value.strip()
        else:
            continue

        # Parse the field
        if field_def.allowed_values:
            if field_value.lower() not in [v.lower() for v in field_def.allowed_values]:  # TODO Case insensitive CI
                issues.append(
                    Issue(itype=IType.ERROR,
                          description=f"Field '{field_name}' of command '{cmd_name}' has invalid value '{field_value}'."
                                      f" Allowed values are: {', '.join(field_def.allowed_values)}.",
                          location=location))
                may_append = False
            else:
                pass  # OK
        else:  # Instead of a list of values, check if a syntactic rule is met by the value
            if field_def.parser:  # Parse, just check syntax (do not store the AST)
                try:
                    if field_name not in already_parsed_fields:
                        ast = parser_field_parsers.string_to_ast(field_def.parser, field_value)
                        # Rules are in charge of informing if the result is expandable and if it complex
                        if "expandable" in ast and ast["expandable"]:
                            issues.append(
                                Issue(itype=IType.ERROR,
                                      description=f"Field '{field_name}' of command '{cmd_name}' cannot be expandable again.",
                                      location=location)
                            )
                            may_append = False
                        if "complex" in ast and ast["complex"]:
                            complex_row = True
                except:
                    issues.append(Issue(itype=IType.ERROR,
                                        description=f"The value in field '{field_name}' of command '{cmd_name}' "
                                        f"is not syntactically correct. Entered: {field_value}",
                                        location=location))
                    may_append = False
            else:
                pass  # Valid

        if field_def.name in mandatory_not_found:
            mandatory_not_found.discard(field_def.name)

    # MODIFY INPUT Dictionary with this new Key
    if complex_row:
        row["_complex"] = complex_row

    # Append if all mandatory fields have been filled
    if len(mandatory_not_found) > 0:
        issues.append(Issue(itype=IType.ERROR,
                            description=f"Mandatory column{'s' if len(mandatory_not_found) > 1 else ''}: "
                                        f"{', '.join(mandatory_not_found)} "
                                        f"{'have' if len(mandatory_not_found) > 1 else 'has'} not been specified",
                            location=location))
        may_append = False

    # Check varying mandatory fields (fields depending on the value of other fields)
    for c in complex_mandatory_cols:
        field_def = c.name  # next(c2 for c2 in col_map if strcmp(c.name, c2.name))
        if isinstance(c.mandatory, str):
            # Evaluate
            mandatory = eval(c.mandatory, None, row)
            may_append = (mandatory and field_def in row) or (not mandatory)
            if mandatory and field_def not in row:
                issues.append(Issue(itype=IType.ERROR,
                                    description="Mandatory column: " + field_def + " has not been specified",
                                    location=location))
                may_append = False

    return may_append, issues


def create_command(cmd_type, name, json_input, source_block=None) -> ExecutableCommandIssuesPairType:
    """
    Factory creating and initializing a command from its type, optional name and parameters

    :param cmd_type: String describing the type of command, as found in the interchange JSON format
    :param name: An optional name (label) for the command
    :param json_input: Parameters specific of the command type (each command knows how to interpret,
                       validate and integrate them)
    :param source_block: String defining the name of the origin block (in a spreadsheet is the worksheet name, but other block types could appear)
    :return: The instance of the command and the issues creating it
    :raise
    """
    cmd: Optional[Command] = first(commands, condition=lambda c: c.name == cmd_type)

    if cmd:
        if cmd.execution_class:
            exec_cmd: "IExecutableCommand" = cmd.execution_class(name)  # Reflective call
            exec_cmd._serialization_type = cmd_type  # Injected attribute. Used later for serialization
            exec_cmd._serialization_label = name  # Injected attribute. Used later for serialization
            exec_cmd._source_block_name = source_block
            if isinstance(json_input, (str, dict, list)):
                if json_input != {}:
                    issues = exec_cmd.json_deserialize(json_input)
                else:
                    issues = []
            else:
                # NO SPECIFICATION
                raise Exception("The command '" + cmd_type + " " + name if name else "<unnamed>" + " does not have a specification.")
            return exec_cmd, issues  # Return the command and the issues found during the deserialization
        else:
            return None, []  # No execution class. Currently "import_commands" and "list_of_commands"
    else:
        # UNKNOWN COMMAND
        raise Exception("Unknown command type: " + cmd_type)
