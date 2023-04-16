import json
from typing import Optional, Dict, Any

from nexinfosys.command_generators import Issue, IssueLocation, IType
from nexinfosys.common.helper import strcmp, first, PartialRetrievalDictionary
from nexinfosys.model_services import IExecutableCommand, get_case_study_registry_objects
from nexinfosys.models.musiasem_concepts import Observer, FactorTypesRelationUnidirectionalLinearTransformObservation, \
    FactorType, Processor, Indicator, IndicatorCategories, Hierarchy
from nexinfosys.command_executors import BasicCommand, CommandExecutionError, subrow_issue_message
from nexinfosys.command_field_definitions import get_command_fields_from_class
from nexinfosys.models.musiasem_concepts_helper import find_or_create_observer, find_processor_by_name


class LCIAMethodsCommand(BasicCommand):
    def __init__(self, name: str):
        BasicCommand.__init__(self, name, get_command_fields_from_class(self.__class__))

    def _process_row(self, fields: Dict[str, Any], subrow=None) -> None:
        """
        :param fields:
        :param subrow:
        :return:
        """
        # InterfaceType must exist
        interface_type = self._glb_idx.get(FactorType.partial_key(fields["interface"]))
        if len(interface_type) == 1:
            pass
        elif len(interface_type) == 0:
            ft_name = fields["interface"]
            ft_h_name = "LCIA"
            hie = self._glb_idx.get(Hierarchy.partial_key(name=ft_h_name))
            if not hie:
                hie = Hierarchy(name=ft_h_name, type_name="interfacetype")
                self._glb_idx.put(hie.key(), hie)
            else:
                hie = hie[0]

            # Create it and warn
            ft = FactorType(ft_name,
                            parent=None, hierarchy=hie,
                            roegen_type=None,
                            tags=None,  # No tags
                            attributes=dict(unit=fields["interface_unit"], description=ft_name, level=None),
                            expression=None,
                            sphere=None,
                            opposite_processor_type=None
                            )
            # Simple name
            self._glb_idx.put(FactorType.partial_key(ft_name, ft.ident), ft)
            self._add_issue(IType.WARNING, f"InterfaceType with name '{ft_name}' not found, created" + subrow_issue_message(subrow))
        else:
            self._add_issue(IType.ERROR,
                            f"InterfaceType with name '{fields['interface']}' found {len(interface_type)} times" + subrow_issue_message(subrow))
            return

        # Store LCIA Methods as a new variable.
        lcia_methods = self._state.get("_lcia_methods")
        if not lcia_methods:
            lcia_methods = {}
            self._state.set("_lcia_methods", lcia_methods)
        horizon = fields.get("lcia_horizon", "")
        if horizon is None or horizon in ("_", "-"):
            horizon = ""
        compartment = fields.get("compartment", "")
        if compartment is None:
            compartment = ""
        subcompartment = fields.get("subcompartment", "")
        if subcompartment is None:
            subcompartment = ""
        lcia_methods[(fields["lcia_method"],
                      fields.get("lcia_category", "total"),
                      fields["lcia_indicator"],
                      horizon,
                      fields["interface"],
                      compartment,
                      subcompartment)] = (fields["interface_unit"],
                                          fields["lcia_coefficient"],
                                          fields.get("lcia_interface_unit"))

