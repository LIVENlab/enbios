from typing import List, Dict, Union, Any, Tuple

from nexinfosys.command_executors import BasicCommand, CommandExecutionError, subrow_issue_message
from nexinfosys.command_field_definitions import get_command_fields_from_class
from nexinfosys.command_generators import IType
from nexinfosys.command_generators.parser_ast_evaluators import ast_evaluator
from nexinfosys.command_generators.parser_field_parsers import string_to_ast, expression_with_parameters
from nexinfosys.common.helper import strcmp, first, FloatOrString
from nexinfosys.model_services import State
from nexinfosys.models.musiasem_concepts import Processor, ProcessorsRelationPartOfObservation, \
    FactorsRelationScaleObservation, ProcessorsSet


class ProcessorScalingsCommand(BasicCommand):
    def __init__(self, name: str):
        BasicCommand.__init__(self, name, get_command_fields_from_class(self.__class__))

    def _process_row(self, fields: Dict[str, Any], subrow=None) -> None:
        scaling_type = fields["scaling_type"]
        scale: str = fields["scale"]

        # Find processors
        invoking_processor = self._get_processor_from_field("invoking_processor")
        requested_processor = self._get_processor_from_field("requested_processor")

        if invoking_processor == requested_processor:
            raise CommandExecutionError(f"Invoking and Requested processors cannot be the same '{invoking_processor.name}'. "
                                        f"Use the 'relative_to' attribute in 'Interfaces' command instead.")

        invoking_interface_name: str = fields["invoking_interface"]
        requested_interface_name: str = fields["requested_interface"]

        requested_new_processor_name: str = fields["new_processor_name"]

        ##
        # Transform text of "attributes" into a dictionary
        fields["attributes"] = self._transform_text_attributes_into_dictionary(fields["attributes"], subrow)

        # Process specific fields

        # Obtain the parent: it must exist. It could be created dynamically but it's important to specify attributes
        if fields.get("parent_processor"):
            try:
                parent_processor = self._get_processor_from_field("parent_processor")
            except CommandExecutionError:
                self._add_issue(IType.ERROR, f"Specified parent processor, '{fields.get('parent_processor')}', does not exist"+subrow_issue_message(subrow))
                return
        else:
            parent_processor = None

        # Get internal and user-defined attributes in one dictionary
        attributes = {c.name: fields[c.name] for c in self._command_fields if c.attribute_of == Processor and fields[c.name] is not None}

        # print(f"Invoking: {invoking_processor.name}:{invoking_interface_name}, Requested: {requested_processor.name}:{requested_interface_name}")

        requested_processor_clone = None
        if strcmp(scaling_type, "CloneAndScale") or strcmp(scaling_type, "Clone"):
            # TODO: check “RequestedProcessor” must be an archetype
            # 1. Clones “RequestedProcessor” as a child of “InvokingProcessor”
            requested_processor_clone = self._clone_processor_as_child(processor=requested_processor,
                                                                       parent_processor=invoking_processor if not parent_processor else parent_processor,
                                                                       name=requested_new_processor_name,
                                                                       other_attributes=attributes)

            if strcmp(scaling_type, "CloneAndScale"):
                # 2. Constrains the value of “RequestedInterface” to the value of “InvokingInterface”, scaled by “Scale”
                try:
                    self._constrains_interface(scale=scale,
                                               invoking_interface_name=invoking_interface_name,
                                               requested_interface_name=requested_interface_name,
                                               parent_processor=invoking_processor,
                                               child_processor=requested_processor_clone)
                except Exception as e:
                    self._add_issue(IType.ERROR, str(e)+subrow_issue_message(subrow))
                    return
        elif strcmp(scaling_type, "Scale"):
            # Processors must be of same type (archetype or instance)
            if not strcmp(invoking_processor.instance_or_archetype, requested_processor.instance_or_archetype):
                raise CommandExecutionError("Requested and invoking processors should be of the same type "
                                            "(both instance or_archetype)")

            # 1. Constrains the value of “RequestedInterface” to the value of “InvokingInterface”, scaled by “Scale”
            try:
                self._constrains_interface(scale=scale,
                                           invoking_interface_name=invoking_interface_name,
                                           requested_interface_name=requested_interface_name,
                                           parent_processor=invoking_processor,
                                           child_processor=requested_processor)
            except Exception as e:
                self._add_issue(IType.ERROR, str(e)+subrow_issue_message(subrow))
                return

        elif strcmp(scaling_type, "CloneScaled"):
            # “RequestedProcessor” must be an archetype
            # if not strcmp(requested_processor.instance_or_archetype, "archetype"):
            #     raise CommandExecutionError(f"Requested processor '{requested_processor.name}' should be of type 'archetype'")

            # “InvokingProcessor” must be an instance
            # if not strcmp(invoking_processor.instance_or_archetype, "instance"):
            #     raise CommandExecutionError(f"Invoking processor '{invoking_processor.name}' should be of type 'instance'")

            # 1. Clones “RequestedProcessor” as a child of “InvokingProcessor”
            # 2. Scales the new processor using “Scale” as the value of “RequestedInterface”
            requested_processor_clone = self._clone_processor_as_child(processor=requested_processor,
                                                                       parent_processor=invoking_processor if not parent_processor else parent_processor,
                                                                       other_attributes=attributes)

            # Value Scale, which can be an expression, should be evaluated (ast) because we need a final float number
            scale_value = self._get_scale_value(scale)

            # In the cloned processor search in all interfaces if there are Observations relative_to RequestedInterface
            # and multiply the observation by the computed scale.
            self._scale_observations_relative_to_interface(processor=requested_processor_clone,
                                                           interface_name=requested_interface_name,
                                                           scale=scale_value)

        if requested_processor_clone:
            # Find or create processor and REGISTER it in "glb_idx"
            # Add to ProcessorsGroup, if specified
            field_val = fields.get("processor_group")
            if field_val:
                p_set = self._p_sets.get(field_val, ProcessorsSet(field_val))
                self._p_sets[field_val] = p_set
                if p_set.append(requested_processor_clone,
                                self._glb_idx):  # Appends codes to the pset if the processor was not member of the pset
                    p_set.append_attributes_codes(fields["attributes"])

    def _clone_processor_as_child(self, processor: Processor, parent_processor: Processor, name: str = None,
                                  other_attributes: Dict = {}) -> Processor:
            # Clone inherits some attributes from parent
            inherited_attributes = dict(
                subsystem_type=parent_processor.subsystem_type,
                processor_system=parent_processor.processor_system,
                instance_or_archetype=parent_processor.instance_or_archetype
            )

            processor_clone, processor_clone_children = processor.clone(state=self._glb_idx, name=name,
                                                                        inherited_attributes={**inherited_attributes,
                                                                                              **other_attributes}
                                                                        )

            # Create PART-OF relation
            relationship = ProcessorsRelationPartOfObservation.create_and_append(parent=parent_processor,
                                                                                 child=processor_clone)
            self._glb_idx.put(relationship.key(), relationship)

            # Add cloned processor hierarchical names to global index
            Processor.register([processor_clone] + list(processor_clone_children), self._glb_idx)

            return processor_clone

    def _constrains_interface(self,
                              scale: str,
                              invoking_interface_name: str,
                              requested_interface_name: str,
                              parent_processor: Processor,
                              child_processor: Processor):

        origin_factor = first(parent_processor.factors, lambda i: strcmp(i.name, invoking_interface_name))

        if not origin_factor:
            raise Exception("Invoking interface name '"+invoking_interface_name+"' not found for processor '"+parent_processor.name+"'")

        destination_factor = first(child_processor.factors, lambda i: strcmp(i.name, requested_interface_name))

        if not destination_factor:
            raise Exception("Requested interface name '"+invoking_interface_name+"' not found for processor '"+parent_processor.name+"'")

        if origin_factor.taxon != destination_factor.taxon:
            # Search for an Interface Type Conversion defined in the ScaleChangeMap command
            interface_types_transform = self._get_interface_types_transform(origin_factor.taxon, parent_processor,
                                                                            destination_factor.taxon, child_processor)
            scale = FloatOrString.multiply(scale, interface_types_transform.scaled_weight)

        relationship = FactorsRelationScaleObservation.create_and_append(origin=origin_factor,
                                                                         destination=destination_factor,
                                                                         observer=None,
                                                                         quantity=scale)

        # relationship = ProcessorsRelationUpscaleObservation.create_and_append(parent=parent_processor,
        #                                                                       child=child_processor,
        #                                                                       observer=None,
        #                                                                       factor_name=interface_name,
        #                                                                       quantity=scale)

        self._glb_idx.put(relationship.key(), relationship)

    def _get_scale_value(self, scale: str):
        try:
            value = float(scale)
        except ValueError:
            ast = string_to_ast(expression_with_parameters, scale)

            evaluation_issues: List[Tuple[int, str]] = []
            s = State()
            value, unresolved_vars = ast_evaluator(exp=ast, state=s, obj=None, issue_lst=evaluation_issues)

            if len(evaluation_issues) > 0:
                evaluation_issues_str = [i[1] for i in evaluation_issues]
                raise CommandExecutionError(f"Problems evaluating scale expression '{scale}': "
                                            f"{', '.join(evaluation_issues_str)}")
            elif len(unresolved_vars) > 0:
                raise CommandExecutionError(f"Unresolved variables evaluating the scale expression '{scale}':"
                                            f" {', '.join(unresolved_vars)}")

            elif not value:
                raise CommandExecutionError(f"The scale expression '{scale}' could not be evaluated.")

        return value

    def _scale_observations_relative_to_interface(self, processor: Processor, interface_name: str,
                                                  scale: Union[int, float]):
        for factor in processor.factors:
            for observation in factor.quantitative_observations:
                relative_to_interface = observation.attributes.get("relative_to", None)
                if relative_to_interface and strcmp(relative_to_interface.name, interface_name):
                    observation.value = float(observation.value) * scale
                    observation.attributes["relative_to"] = None

