from typing import Dict, Any

from nexinfosys.command_executors import BasicCommand, subrow_issue_message
from nexinfosys.command_field_definitions import get_command_fields_from_class
from nexinfosys.command_generators import IType
from nexinfosys.common.helper import strcmp
from nexinfosys.models.musiasem_concepts import Hierarchy, FactorType, FlowFundRoegenType


class InterfaceTypesCommand(BasicCommand):
    def __init__(self, name: str):
        BasicCommand.__init__(self, name, get_command_fields_from_class(self.__class__))

    def _process_row(self, field_values: Dict[str, Any], subrow=None) -> None:
        """
        Process a dictionary representing a row of the InterfaceTypes command. The dictionary can come directly from
        the worksheet or from a dataset.

        :param field_values: dictionary
        """

        # Read variables
        ft_h_name = field_values.get("interface_type_hierarchy", "_default")  # "_default" InterfaceType Hierarchy NAME <<<<<<
        ft_name = field_values.get("interface_type")
        ft_sphere = field_values.get("sphere")
        ft_roegen_type = field_values.get("roegen_type")
        ft_parent = field_values.get("parent_interface_type")
        ft_formula = field_values.get("formula")
        ft_description = field_values.get("description")
        ft_unit = field_values.get("unit")
        ft_opposite_processor_type = field_values.get("opposite_processor_type")
        ft_level = field_values.get("level")
        ft_attributes = self._transform_text_attributes_into_dictionary(field_values.get("attributes", ""))

        # Process
        # Mandatory fields
        if not ft_h_name:
            self._add_issue(IType.WARNING, "Empty interface type hierarchy name. It is recommended to specify one, assuming '_default'."+subrow_issue_message(subrow))
            ft_h_name = "_default"

        if not ft_name:
            self._add_issue(IType.ERROR, "Empty interface type name. Skipped."+subrow_issue_message(subrow))
            return

        # Check if a hierarchy of interface types by the name <ft_h_name> exists, if not, create it and register it
        hie = self._glb_idx.get(Hierarchy.partial_key(name=ft_h_name))
        if not hie:
            hie = Hierarchy(name=ft_h_name, type_name="interfacetype")
            self._glb_idx.put(hie.key(), hie)
        else:
            hie = hie[0]

        # If parent defined, check if it exists
        # (it must be registered both in the global registry AND in the hierarchy)
        if ft_parent:
            parent = self._glb_idx.get(FactorType.partial_key(ft_parent))
            if len(parent) > 0:
                for p in parent:
                    if p.hierarchy == hie:
                        parent = p
                        break
                if not isinstance(parent, FactorType):
                    self._add_issue(IType.ERROR, f"Parent interface type name '{ft_parent}' not found in hierarchy '{ft_h_name}"+subrow_issue_message(subrow))
                    return
            else:
                self._add_issue(IType.ERROR, f"Parent interface type name '{ft_parent}' not found"+subrow_issue_message(subrow))
                return
            # Double check, it must be defined in "hie"
            if ft_parent not in hie.codes:
                self._add_issue(IType.ERROR, f"Parent interface type name '{ft_parent}' not registered in the hierarchy '{ft_h_name}'"+subrow_issue_message(subrow))
                return
        else:
            parent = None

        # Check if FactorType exists
        ft = self._glb_idx.get(FactorType.partial_key(ft_name))
        if len(ft) == 0:
            # TODO Compile and CONSIDER attributes (on the FactorType side)
            roegen_type = None
            if ft_roegen_type:
                roegen_type = FlowFundRoegenType.flow if strcmp(ft_roegen_type, "flow") else FlowFundRoegenType.fund

            ft = FactorType(ft_name,
                            parent=parent, hierarchy=hie,
                            roegen_type=roegen_type,
                            tags=None,  # No tags
                            attributes=dict(unit=ft_unit, description=ft_description, level=ft_level, **ft_attributes),
                            expression=ft_formula,
                            sphere=ft_sphere,
                            opposite_processor_type=ft_opposite_processor_type
                            )
            # Simple name
            self._glb_idx.put(FactorType.partial_key(ft_name, ft.ident), ft)
            if not strcmp(ft_name, ft.full_hierarchy_name()):
                self._glb_idx.put(FactorType.partial_key(ft.full_hierarchy_name(), ft.ident), ft)
        else:
            self._add_issue(IType.WARNING, f"Interface type name '{ft_name}' already registered"+subrow_issue_message(subrow))
            return
