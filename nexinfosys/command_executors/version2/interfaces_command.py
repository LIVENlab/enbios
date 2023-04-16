import logging
from typing import Dict, Any, Optional, Sequence, List, NoReturn

from pint import DimensionalityError

from nexinfosys.command_executors import BasicCommand, subrow_issue_message, CommandExecutionError
from nexinfosys.command_field_definitions import get_command_fields_from_class
from nexinfosys.command_generators import parser_field_parsers, IType
from nexinfosys.command_generators.parser_ast_evaluators import ast_to_string
from nexinfosys.common.decorators import memoized_method
from nexinfosys.common.helper import strcmp, first, ifnull, UnitConversion, head
from nexinfosys.models.musiasem_concepts import PedigreeMatrix, FactorType, \
    Factor, FactorInProcessorType, Observer, GeographicReference, ProvenanceReference, \
    BibliographicReference, FactorTypesRelationUnidirectionalLinearTransformObservation, Processor
from nexinfosys.models.musiasem_concepts_helper import _create_or_append_quantitative_observation, \
    find_processors_matching_name, find_factor_types_transform_relation


class InterfacesAndQualifiedQuantitiesCommand(BasicCommand):
    def __init__(self, name: str):
        BasicCommand.__init__(self, name, get_command_fields_from_class(self.__class__))

    invert = {"local": "external",
              "environment": "externalenvironment",
              "external": "local",
              "externalenvironment": "environment"}

    @memoized_method(maxsize=None)
    def _get_relative_to_interface(self, p: Processor, relative_to: str):
        try:
            ast = parser_field_parsers.string_to_ast(parser_field_parsers.factor_unit, relative_to)
        except:
            raise CommandExecutionError(
                f"Could not parse the RelativeTo column, value {str(relative_to)}. " + subrow_issue_message(self._subrow))

        relative_to_interface_name = ast_to_string(ast["factor"])

        # rel_unit_name = ast["unparsed_unit"]
        # try:
        #     f_unit = str((ureg(f_unit) / ureg(rel_unit_name)).units)
        # except (UndefinedUnitError, AttributeError) as ex:
        #     raise CommandExecutionError(f"The final unit could not be computed, interface '{f_unit}' / "
        #                                  f"relative_to '{rel_unit_name}': {str(ex)}"+subrow_issue_message(subrow))

        # relative_to_interface = first(p.factors,
        #                               lambda ifc: strcmp(ifc.name, relative_to_interface_name))
        relative_to_interface = p.factors_find(relative_to_interface_name)

        if not relative_to_interface:
            raise CommandExecutionError(f"Interface specified in 'relative_to' column "
                                        f"'{relative_to_interface_name}' has not been found." + subrow_issue_message(
                self._subrow))

        return relative_to_interface

    def _process_row(self, field_values: Dict[str, Any], subrow=None) -> None:
        """
        Process a dictionary representing a row of the Interfaces command. The dictionary can come directly from
        the worksheet or from a dataset.

        :param field_values: dictionary
        """
        # f_processor_name -> p
        # f_interface_type_name -> it
        # f_interface_name -> i
        #
        # IF NOT i AND it AND p => i_name = it.name => get or create "i"
        # IF i AND it AND p => get or create "i", IF "i" exists, i.it MUST BE equal to "it" (IF NOT, error)
        # IF i AND p AND NOT it => get "i" (MUST EXIST)
        f_interface_type_name = field_values.get("interface_type")
        f_interface_name = field_values.get("interface")

        if not f_interface_name:
            if not f_interface_type_name:
                raise CommandExecutionError("At least one of InterfaceType or Interface must be defined"+subrow_issue_message(subrow))

            f_interface_name = f_interface_type_name

        processor = self.find_processor(field_values.get("processor"), subrow)

        # Try to find Interface
        f_orientation = field_values.get("orientation")
        interface_type: Optional[FactorType] = None
        interface: Optional[Factor] = None
        interfaces: Sequence[Factor] = self._glb_idx.get(Factor.partial_key(processor=processor, name=f_interface_name))
        if len(interfaces) == 1:
            interface = interfaces[0]
            logging.debug(f"Interface '{interface.name}' found")
            interface_type = interface.taxon
            if f_interface_type_name and not strcmp(interface_type.name, f_interface_type_name):
                self._add_issue(IType.WARNING, f"The existing Interface '{interface.name}' has the InterfaceType "
                                               f"'{interface_type.name}' which is different from the specified "
                                               f"InterfaceType '{f_interface_type_name}'. Record skipped." +
                                               subrow_issue_message(subrow))
                return
        elif len(interfaces) > 1:
            raise CommandExecutionError(f"Interface '{f_interface_name}' found {str(len(interfaces))} times. "
                                        f"It must be uniquely identified."+subrow_issue_message(subrow))
        elif len(interfaces) == 0:
            # The interface does not exist, create it below
            if not f_orientation:
                raise CommandExecutionError(f"Orientation must be defined for new Interfaces."+subrow_issue_message(subrow))

        # InterfaceType still not found
        if not interface_type:
            interface_type_name = ifnull(f_interface_type_name, f_interface_name)

            # Find FactorType
            # TODO Allow creating a basic FactorType if it is not found?
            interface_types: Sequence[FactorType] = self._glb_idx.get(FactorType.partial_key(interface_type_name))
            if len(interface_types) == 0:
                raise CommandExecutionError(f"InterfaceType '{interface_type_name}' not declared previously"+subrow_issue_message(subrow))
            elif len(interface_types) > 1:
                raise CommandExecutionError(f"InterfaceType '{interface_type_name}' found {str(len(interface_types))} times. "
                                            f"It must be uniquely identified."+subrow_issue_message(subrow))
            else:
                interface_type = interface_types[0]

        # Get attributes default values taken from Interface Type or Processor attributes
        # Rows   : value of (source) "processor.subsystem_type"
        # Columns: value of (target) "interface_type.opposite_processor_type"
        # Cells  : CORRECTED value of "opposite_processor_type"
        # +--------+-------+--------+-------+---------+
        # |        | Local | Env    | Ext   | ExtEnv  |
        # +--------+-------+--------+-------+---------+
        # | Local  | Local | Env    | Ext   | ExtEnv  |
        # | Env    | Local | Env    | Ext   | ExtEnv? |
        # | Ext    | Ext   | ExtEnv | Local | Env     |
        # | ExtEnv | Ext   | ExtEnv | Local | Env?    |
        # +--------+-------+--------+-------+---------+
        if interface_type.opposite_processor_type:
            tmp = interface_type.opposite_processor_type.lower()
            if processor.subsystem_type.lower() in ["local", "environment"]:  # First two rows
                opposite_processor_type = tmp
            else:
                opposite_processor_type = InterfacesAndQualifiedQuantitiesCommand.invert[tmp]
            # TODO in doubt. Maybe these are undefined (values with question mark in the table)
            #  if tmp == "externalenvironment" and processor.subsystem_type.lower() in ["environment", "externalenvironment"]:
            #      pass
        else:
            opposite_processor_type = None

        interface_type_values = {
            "sphere": interface_type.sphere,
            "roegen_type": interface_type.roegen_type,
            "opposite_processor_type": opposite_processor_type
        }

        # Get internal and user-defined attributes in one dictionary
        # Use: value specified in Interfaces ELSE value specified in InterfaceTypes ELSE first value of allowed values
        attributes = {c.name: ifnull(field_values[c.name], ifnull(interface_type_values.get(c.name), head(c.allowed_values)))
                      for c in self._command_fields if c.attribute_of == Factor}

        if not interface:
            # f_list: Sequence[Factor] = self._glb_idx.get(
            #     Factor.partial_key(processor=p, factor_type=ft, orientation=f_orientation))
            #
            # if len(f_list) > 0:
            #     raise CommandExecutionError(f"An interface called '{f_list[0].name}' for Processor '{f_processor_name}'"
            #                                  f" with InterfaceType '{f_interface_type_name}' and orientation "
            #                                  f"'{f_orientation}' already exists"+subrow_issue_message(subrow))

            # Transform text of "interface_attributes" into a dictionary
            interface_attributes = self._transform_text_attributes_into_dictionary(
                field_values.get("interface_attributes"), subrow)
            attributes.update(interface_attributes)

            location = self.get_location(field_values.get("location"), subrow)

            interface = Factor.create_and_append(f_interface_name,
                                                 processor,
                                                 in_processor_type=FactorInProcessorType(
                                                     external=False,
                                                     incoming=False
                                                 ),
                                                 taxon=interface_type,
                                                 geolocation=location,
                                                 tags=None,
                                                 attributes=attributes)
            self._glb_idx.put(interface.key(), interface)
            logging.debug(f"Interface '{interface.name}' created")
        elif not interface.compare_attributes(attributes):
            initial = ', '.join([f"{k}: {interface.get_attribute(k)}" for k in attributes])
            new = ', '.join([f"{k}: {attributes[k]}" for k in attributes])
            name = interface.processor.full_hierarchy_names(self._glb_idx)[0] + ":" + interface.name
            raise CommandExecutionError(f"The same interface '{name}', is being redeclared with different properties. "
                                        f"INITIAL: {initial}; NEW: {new}."+subrow_issue_message(subrow))

        f_unit = field_values.get("unit")
        if not f_unit:
            f_unit = interface_type.unit

        # Unify unit (it must be done before considering RelativeTo -below-, because it adds a transformation to "f_unit")
        f_value = field_values.get("value")
        if f_value is not None and f_unit != interface_type.unit:
            try:
                f_value = UnitConversion.convert(f_value, f_unit, interface_type.unit)
            except DimensionalityError:
                raise CommandExecutionError(
                    f"Dimensions of units in InterfaceType ({interface_type.unit}) and specified ({f_unit}) are not convertible" + subrow_issue_message(
                        subrow))

            f_unit = interface_type.unit

        # Search for a relative_to interface
        f_relative_to = field_values.get("relative_to")
        relative_to_interface: Optional[Factor] = None
        if f_relative_to:
            self._subrow = subrow
            relative_to_interface = self._get_relative_to_interface(interface.processor, f_relative_to)

        if f_value is None and relative_to_interface is not None:
            # Search for a Interface Type Conversion defined in the ScaleChangeMap command
            interface_types_transforms: List[FactorTypesRelationUnidirectionalLinearTransformObservation] = \
                find_factor_types_transform_relation(self._glb_idx, relative_to_interface.taxon, interface.taxon, processor, processor)

            # Overwrite any specified unit, it doesn't make sense without a value, i.e. it cannot be used for conversion
            f_unit = interface.taxon.unit
            if len(interface_types_transforms) == 1:
                f_value = interface_types_transforms[0].scaled_weight
            else:
                interface_types_transforms_message = "an interface type conversion doesn't exist" \
                    if (len(interface_types_transforms) == 0) \
                    else f"{len(interface_types_transforms)} interface type conversions exist"

                f_value = "0"
                self._add_issue(IType.WARNING, f"Field 'value' should be defined for interfaces having a "
                                               f"'RelativeTo' interface, and {interface_types_transforms_message}. "
                                               f"Using value '0'."+subrow_issue_message(subrow))

        # Create quantitative observation
        if f_value is not None:
            f_uncertainty = field_values.get("uncertainty")
            f_assessment = field_values.get("assessment")
            f_pedigree_matrix = field_values.get("pedigree_matrix")
            f_pedigree = field_values.get("pedigree")
            f_time = field_values.get("time")
            f_comments = field_values.get("comments")

            f_source = field_values.get("qq_source")
            # TODO: source is not being used
            source = self.get_source(f_source, subrow)

            # Find Observer
            observer: Optional[Observer] = None
            if f_source:
                observer = self._glb_idx.get_one(Observer.partial_key(f_source))
                if not observer:
                    self._add_issue(IType.WARNING,
                                    f"Observer '{f_source}' has not been found." + subrow_issue_message(subrow))

            # If an observation exists then "time" is mandatory
            if not f_time:
                raise CommandExecutionError(f"Field 'time' needs to be specified for the given observation."+subrow_issue_message(subrow))

            # An interface can have multiple observations if each of them have a different [time, observer] combination
            for observation in interface.quantitative_observations:
                observer_name = observation.observer.name if observation.observer else None
                if strcmp(observation.attributes["time"], f_time) and strcmp(observer_name, f_source):
                    raise CommandExecutionError(
                        f"The interface '{interface.name}' in processor '{interface.processor.name}' already has an "
                        f"observation with time '{f_time}' and source '{f_source}'.")

            self.check_existence_of_pedigree_matrix(f_pedigree_matrix, f_pedigree, subrow)

            # Transform text of "number_attributes" into a dictionary
            number_attributes = self._transform_text_attributes_into_dictionary(field_values.get("number_attributes"),
                                                                               subrow)

            o = _create_or_append_quantitative_observation(interface,
                                                           f_value, f_unit, f_uncertainty, f_assessment,
                                                           f_pedigree, f_pedigree_matrix,
                                                           observer,
                                                           relative_to_interface,
                                                           f_time,
                                                           None,
                                                           f_comments,
                                                           None, number_attributes
                                                           )

            # TODO Register? Disable for now. Observation can be obtained from a pass over all Interfaces
            # glb_idx.put(o.key(), o)

    def find_processor(self, processor_name, subrow) -> Processor:
        # Find Processor
        # TODO Allow creating a basic Processor if it is not found?
        processors = find_processors_matching_name(processor_name, self._glb_idx)
        # p = find_observable_by_name(processor_name, self._glb_idx)
        # p = self._glb_idx.get(Processor.partial_key(processor_name))
        if len(processors) == 0:
            raise CommandExecutionError(
                "Processor '" + processor_name + "' not declared previously" + subrow_issue_message(subrow))
        elif len(processors) > 1:
            raise CommandExecutionError(
                f"Processor '{processor_name}' declared previously {len(processors)} times" + subrow_issue_message(subrow))

        return processors[0]

    def get_location(self, reference_name, subrow) -> Any:
        reference = None

        if reference_name:
            try:
                # TODO Change to parser for Location (includes references, but also Codes)
                ast = parser_field_parsers.string_to_ast(parser_field_parsers.reference, reference_name)
                ref_id = ast["ref_id"]
                references = self._glb_idx.get(GeographicReference.partial_key(ref_id))
                if len(references) == 1:
                    reference = references[0]
                else:
                    raise CommandExecutionError(f"Reference '{reference_name}' not found" + subrow_issue_message(subrow))
            except:
                reference = reference_name

        return reference

    @memoized_method(maxsize=None)
    def _get_reference(self, ref_name: str):
        try:
            ast = parser_field_parsers.string_to_ast(parser_field_parsers.reference, ref_name)
            ref_id = ast["ref_id"]
            references = self._glb_idx.get(ProvenanceReference.partial_key(ref_id))
            if len(references) == 1:
                reference = references[0]
            else:
                references = self._glb_idx.get(BibliographicReference.partial_key(ref_id))
                if len(references) == 1:
                    reference = references[0]
                else:
                    raise CommandExecutionError(
                        f"Reference '{ref_name}' not found" + subrow_issue_message(self._subrow))
        except:
            # TODO Change when Ref* are implemented
            reference = ref_name + " (not found)"

        return reference

    def get_source(self, reference_name, subrow) -> Any:
        reference = None

        if reference_name:
            self._subrow = subrow
            reference = self._get_reference(reference_name)

        return reference

    def check_existence_of_pedigree_matrix(self, pedigree_matrix: str, pedigree: str, subrow=None) -> NoReturn:
        # Check existence of PedigreeMatrix, if used
        if pedigree_matrix and pedigree:
            pm = self._glb_idx.get(PedigreeMatrix.partial_key(name=pedigree_matrix))
            if len(pm) == 0:
                raise CommandExecutionError("Could not find Pedigree Matrix '" + pedigree_matrix + "'" +
                                            subrow_issue_message(subrow))
            else:
                try:
                    lst = pm[0].get_modes_for_code(pedigree)
                except:
                    raise CommandExecutionError("Could not decode Pedigree '" + pedigree + "' for Pedigree Matrix '"
                                                + pedigree_matrix + "'" + subrow_issue_message(subrow))
        elif pedigree and not pedigree_matrix:
            raise CommandExecutionError("Pedigree specified without accompanying Pedigree Matrix" +
                                        subrow_issue_message(subrow))
