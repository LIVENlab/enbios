"""
Export processors to XML
Inputs are both the registry and the output dataframe
The registry serves to prepare the structure of the file

"""
import textwrap
from typing import Dict, Tuple

from nexinfosys import case_sensitive
from nexinfosys.common.helper import strcmp, PartialRetrievalDictionary
from nexinfosys.model_services import State, get_case_study_registry_objects
from nexinfosys.models.musiasem_concepts import ProcessorsRelationPartOfObservation, Processor, Factor


def export_model_to_xml(registry: PartialRetrievalDictionary) -> Tuple[str, Dict[str, Processor]]:
    """
    Elaborate an XML string containing the nested processors and their attributes.
    Also the interfaces inside processors
    <processors>
      <root_p1 fullname="" level="" system="" subsystem="" functional="true|false">
        <interfaces>
          <i1 type="" sphere="" roegen_type="" orientation="" opposite_processor_type="" />
          ...
        </interfaces>
        <child_p2>
          ...
        </child_p2>
      </root_p1>
      ...
    </processors>

    Example (abstract):

    '/processors//[level="n"]'

    :param registry:
    :return:
    """

    def xml_processor(p: Processor, registry: PartialRetrievalDictionary, p_map: Dict[str, Processor], level=0):
        """
        Return the XML of a processor
        Recursive into children

        :param p:
        :return:
        """

        def xml_interface(iface: Factor):
            """

            :param level:
            :param iface:
            :return:
            """
            s = f'<interface name="{iface.name}" type="{iface.taxon.name}" sphere="{iface.sphere}" ' \
                f'roegen_type="{iface.roegen_type}" orientation="{iface.orientation}" ' \
                f'opposite_processor_type="{iface.opposite_processor_type}" />'
            if case_sensitive:
                return s
            else:
                return s.lower()

        children = p.children(registry)
        full_name = p.full_hierarchy_names(registry)[0]
        if case_sensitive:
            p_map[full_name] = p
        else:
            p_map[full_name.lower()] = p

        s = f"""
<processor name="{p.name}" fullname="{full_name}" level="{p.level}" system="{p.processor_system}" subsystem="{p.subsystem_type}" functional="{"true" if strcmp(p.functional_or_structural, "Functional") else "false"}" >
  <interfaces>
{textwrap.indent(chr(10).join([xml_interface(f) for f in p.factors]), "    ")}
  </interfaces>
  {chr(10).join([xml_processor(c, registry, p_map, level + 1) for c in children])}    
</processor>"""
        s = textwrap.indent(s, "  "*(level+1))
        if case_sensitive:
            return s
        else:
            return s.lower()

    # Part of relationships
    por = registry.get(ProcessorsRelationPartOfObservation.partial_key())

    # Set of all instance processors NOT touched by part-of relationships
    unaffected_procs = set([p for p in registry.get(Processor.partial_key()) if strcmp(p.instance_or_archetype, "Instance")])
    for po in por:
        try:
            unaffected_procs.remove(po.parent_processor)
        except KeyError:
            pass
        try:
            unaffected_procs.remove(po.child_processor)
        except KeyError:
            pass

    # Keep those affecting Instance processors
    por = [po for po in por if strcmp(po.parent_processor.instance_or_archetype, "Instance")]

    # Get root processors (set of processors not appearing as child_processor)
    parents = set([po.parent_processor for po in por])
    children = set([po.child_processor for po in por])
    roots = parents.difference(children).union(unaffected_procs)
    # leaves = children.difference(parents)
    result = '<processors>'  # <?xml version="1.0" encoding="utf-8"?>\n
    p_map = {}
    for p in roots:
        result += xml_processor(p, registry, p_map)
    result += "\n</processors>"

    return result, p_map
