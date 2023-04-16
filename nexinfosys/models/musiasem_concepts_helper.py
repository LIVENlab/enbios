from typing import Union, List, Tuple, Optional, Any, Dict, Set

import jsonpickle

from nexinfosys import ureg
from nexinfosys.command_generators import parser_field_parsers
from nexinfosys.command_generators.parser_ast_evaluators import ast_to_string
from nexinfosys.common.helper import PartialRetrievalDictionary, create_dictionary, strcmp, FloatOrString
from nexinfosys.model_services import State, get_case_study_registry_objects
from nexinfosys.models.musiasem_concepts import \
    FlowFundRoegenType, FactorInProcessorType, RelationClassType, \
    Processor, FactorType, Observer, Factor, \
    ProcessorsRelationPartOfObservation, ProcessorsRelationUndirectedFlowObservation, \
    ProcessorsRelationUpscaleObservation, \
    FactorsRelationDirectedFlowObservation, Hierarchy, Taxon, \
    FactorQuantitativeObservation, HierarchyLevel, Geolocation, FactorsRelationScaleObservation, \
    FactorTypesRelationUnidirectionalLinearTransformObservation
from nexinfosys.models.statistical_datasets import CodeList, CodeListLevel, Code


def serialize_hierarchy(h: Hierarchy) -> str:
    return jsonpickle.encode(h)


def deserialize_hierarchy(s: str) -> Hierarchy:
    return jsonpickle.decode(s)


def convert_code_list_to_hierarchy(cl: CodeList) -> Hierarchy:
    """
    It receives a CodeList and elaborates an equivalent Hierarchy, returning it

    :param cl: The input CodeList
    :return: The equivalent Hierarchy
    """
    h = Hierarchy(name=cl.code)
    h._description = cl.description
    # CodeList is organized in levels. Create all Levels and all Nodes (do not interlink Nodes)
    levels_dict = create_dictionary()
    code_node_dict = {}  # Maps a Code to the corresponding HierarchyNode
    for cll in cl.levels:
        hl = HierarchyLevel(cll.code, h)
        h.level_names.append(cll.code)
        h.levels.append(hl)
        levels_dict[cll.code] = hl
        for ct in cll.codes:
            hn = Taxon(ct.code, hierarchy=h, label=ct.description, description=ct.description)
            h.codes[ct.code] = hn
            code_node_dict[ct] = hn
            hn.level = levels_dict.get(ct.level.code, None)  # Point to the containing HierarchyLevel
            if hn.level:
                hn.level.codes.add(hn)

    # Set children & parents
    for ct in code_node_dict:
        p = code_node_dict[ct]
        for ch in ct.children:
            c = code_node_dict[ch]
            p._children.add(c)
            c._parents.append(p)
            c._parents_weights.append(1.0)

    # Finally, set "roots" to HierarchyNodes without "parents"
    tmp = []
    for c in h.codes.values():
        if len(c._parents) == 0:
            tmp.append(c)
    h.roots_append(tmp)

    return h


def convert_hierarchy_to_code_list(h: Hierarchy) -> CodeList:
    """
    It receives a Hierarchy and elaborates an equivalent CodeList, returning it

    NOTE: It does not support references to other Codes. Which means Codes member of multiple hierarchies.
          It does not support Codes with multiple parents and different weights.
          It does not support code lists with no levels. At least a level "" has to be defined (but this means it is a pure CodeList)

    :param h: The input Hierarchy
    :return: The equivalent CodeList
    """
    cl = CodeList()
    cl.code = h.name
    cl.description = h._description
    levels_map = {}
    # Levels
    for hl in h.levels:
        cll = CodeListLevel()
        cll.code_list = cl
        cll.code = hl.name
        levels_map[hl] = cll

    dummy_level = None

    # Codes
    codes_map = {}
    for hn in h.codes.values():
        c = Code()
        c.code = hn.name
        c.description = hn.description
        if hn.level:
            c.level = levels_map[hn.level]
        else:  # No level defined, assign a "dummy_level"
            if not dummy_level:
                dummy_level = CodeListLevel()
                dummy_level.code_list = cl
                dummy_level.code = ""
            c.level = dummy_level
        codes_map[hn] = c

    # Links between nodes
    for hn in h.codes.values():
        for ch in hn._children:
            p = codes_map[hn]
            c = codes_map[ch]
            p.children.append(c)  # Append a child
            c.parents.append(p)  # Add a parent

    return cl


def hierarchical_name_variants(h_name: str):
    """
    Given a hierarchical name, obtain variants

    :param h_name:
    :return: An ordered list of names that may refer to the same object
    """
    parts = h_name.split(".")
    if len(parts) > 1:
        return [h_name, parts[-1]]
    else:
        return [h_name]  # A simple name


def find_quantitative_observations(idx: PartialRetrievalDictionary, processor_instances_only=False) -> List[FactorQuantitativeObservation]:
    """ Find all available quantitative observations """
    interfaces: List[Factor] = idx.get(Factor.partial_key())

    return [o for i in interfaces
                    if not processor_instances_only or i.processor.instance_or_archetype == "Instance"
              for o in i.quantitative_observations
           ]


def find_observable_by_name(name: str, idx: PartialRetrievalDictionary, processor: Processor = None,
                            factor_type: FactorType = None) -> Union[Factor, Processor, FactorType]:
    """
    From a full Factor name "processor:factortype", obtain the corresponding Factor, searching in the INDEX of objects
    It supports also finding a Processor: "processor" (no ":factortype" part)
    It supports also finding a FactorType: ":factortype" (no "processor" part)
    It considers the fact that both Processors and FactorTypes can have different names
    (and consequently that Factors can have also multiple names)

    :param name: ":" separated processor name and factor type name. "p:ft" returns a Factor.
                 "p" or "p:" returns a Processor. ":ft" returns a FactorType
    :param idx: The PartialRetrievalDictionary where the objects have been previously indexed
    :param processor: Already resolved Processor. If ":ft" is specified, it will use this parameter to return a Factor
                 (not a FactorType)
    :param factor_type: Already resolved FactorType. If "p:" is specified (note the ":") , it will use this parameter
                 to return a Factor (not a Processor)
    :return: Processor or FactorType or Factor
    """
    res = None
    if isinstance(name, str):
        s = name.split(":")
        if len(s) == 2:  # There is a ":", so either FactorType or Factor (FactorType if there is no Processor)
            p_name = s[0]
            f_name = s[1]
            if not p_name:  # Processor can be blank
                p_name = None
            if not f_name:  # Factor type can be blank
                f_name = None
        elif len(s) == 1:  # If no ":", go just for the processor
            p_name = s[0]
            f_name = None
        # Retrieve the processor
        if p_name:
            for alt_name in hierarchical_name_variants(p_name):
                p = idx.get(Processor.partial_key(name=alt_name))
                if len(p) == 1:
                    p = p[0]
                    break
                elif len(p) > 1:
                    raise Exception(f"{alt_name} appears more than one time.")
        elif processor:
            p = processor
        else:
            p = None

        # Retrieve the FactorType
        if f_name:
            for alt_name in hierarchical_name_variants(f_name):
                ft = idx.get(FactorType.partial_key(name=alt_name))
                if ft:
                    ft = ft[0]
                    break
        elif factor_type:
            ft = factor_type
        else:
            res = p
            ft = None

        # Retrieve the Factor
        if not p_name and not p:  # If no Processor available at this point, FactorType is being requested, return it
            res = ft
        elif not res and p and ft:
            f = idx.get(Factor.partial_key(processor=p, factor_type=ft))
            if f:
                res = f[0]
    else:
        res = name

    return res


def find_processors_matching_name(processor_name, registry):
    parts, _ = obtain_name_parts(processor_name)
    ps = registry.get(Processor.partial_key(name=parts[0]))
    i = 1
    while len(ps) > 0 and i < len(parts):
        ps_tmp = []
        for p in ps:
            for partof_rel in registry.get(ProcessorsRelationPartOfObservation.partial_key(parent=p)):
                if strcmp(partof_rel.child_processor.name, parts[i]):
                    ps_tmp.append(partof_rel.child_processor)
                    break
        ps = ps_tmp
        i += 1
    return ps


def find_processor_by_name(state: Union[State, PartialRetrievalDictionary], processor_name: str) -> Optional[Processor]:
    """
    Find a processor by its name

    The name can be:
      * simple
      * partial hierarchical
      * absolute hierarchical

    The result can be:
      * None: no Processor,
      * a single Processor or
      * Exception if more than one Processor matches the name

    :param state:
    :param processor_name:
    :exception If more than one Processor can be found
    :return:
    """

    # Decompose the name
    p_names, _ = obtain_name_parts(processor_name)

    # Get registry object
    if isinstance(state, PartialRetrievalDictionary):
        glb_idx = state
    else:
        glb_idx, _, _, _, _ = get_case_study_registry_objects(state)

    if processor_name:
        ps = find_processors_matching_name(processor_name, glb_idx)
        if len(ps) == 1:
            return ps[0]  # One found!
        else:
            raise Exception(f"{len(ps)} processors matched '{processor_name}': {', '.join([p.full_hierarchy_names(glb_idx)[0] for p in ps])}")
    else:
        raise Exception("No processor name specified: '"+processor_name+"'")


def find_factortype_by_name(state: Union[State, PartialRetrievalDictionary], factortype_name: str):
    """
    Find FactorType by its name

    The name can be:
      * simple
      * absolute hierarchical

    The result can be:
      * None: no Factortype,
      * a single FactorType or
      * Exception if more than one FactorType matches the name

    :param state:
    :param factortype_name:
    :exception If more than one FactorType can be found
    :return:
    """

    # Decompose the name
    p_names, _ = obtain_name_parts(factortype_name)

    # Get registry object
    if isinstance(state, PartialRetrievalDictionary):
        glb_idx = state
    else:
        glb_idx, _, _, _, _ = get_case_study_registry_objects(state)

    if len(p_names) > 0:
        # Directly accessible
        ft = glb_idx.get(FactorType.partial_key(p_names[0]))
        if len(ft) == 1:
            ft = ft[0]
            if len(p_names) == 1:
                return ft[0]  # Found!
            else:
                # Look for children of "p" matching each piece
                for partial_name in p_names[1:]:
                    if len(ft.get_children()) == 0:
                        return None
                    else:
                        # Find child Processors matching the name
                        matches = []
                        for ftc in ft.get_children():
                            if strcmp(partial_name, ftc.name):
                                matches.append(ftc)
                        if len(matches) == 0:
                            return None
                        elif len(matches) == 1:
                            ft = matches[0]
                        else:
                            raise Exception(str(len(matches))+" InterfaceTypes matched '"+partial_name+"' in '"+factortype_name+"'")
        else:  # The number of matching top level FactorType is different from ONE
            if len(ft) == 0:
                return None
            else:
                raise Exception(str(len(ft)+" InterfaceTypes found matching '"+factortype_name+"'"))
    else:
        raise Exception("No InterfaceType name specified: '"+factortype_name+"'")


def find_or_create_observable(state: Union[State, PartialRetrievalDictionary],
                              name: str, source: Union[str, Observer]=Observer.no_observer_specified,
                              aliases: str=None,  # "name" (processor part) is an alias of "aliases" Processor
                              proc_attributes: Dict[str, Any] = None, proc_location: Geolocation = None,
                              fact_roegen_type: FlowFundRoegenType=None, fact_attributes: dict=None,
                              fact_incoming: bool=None, fact_external: bool=None, fact_location=None):
    """
    Find or create Observables: Processors, Factor and FactorType objects
    It can also create an Alias for a Processor if the name of the aliased Processor is passed (parameter "aliases")

    "name" is parsed, which can specify a processor AND a factor, both hierarchical ('.'), separated by ":"

    :param state:
    :param name: Full name of processor, processor':'factor or ':'factor
    :param source: Name of the observer or Observer itself (used only when creating nested Processors, because it implies part-of relations)
    :param aliases: Full name of an existing processor to be aliased by the processor part in "name"
    :param proc_attributes: Dictionary with attributes to be added to the processor if it is created
    :param proc_location: Specification of where the processor is physically located, if it applies
    :param fact_roegen_type: Flow or Fund
    :param fact_attributes: Dictionary with attributes to be added to the Factor if it is created
    :param fact_incoming: True if the Factor is incoming regarding the processor; False if it is outgoing
    :param fact_external: True if the Factor comes from Environment
    :param fact_location: Specification of where the processor is physically located, if it applies
    :return: Processor, FactorType, Factor
    """

    # Decompose the name
    p_names, f_names = obtain_name_parts(name)

    # Get objects from state
    if isinstance(state, PartialRetrievalDictionary):
        glb_idx = state
    else:
        glb_idx, p_sets, hh, datasets, mappings = get_case_study_registry_objects(state)

    # Get the Observer for the relations (PART-OF for now)
    if source:
        if isinstance(source, Observer):
            oer = source
        else:
            oer = glb_idx.get(Observer.partial_key(name=source))
            if not oer:
                oer = Observer(source)
                glb_idx.put(oer.key(), oer)
            else:
                oer = oer[0]

    p = None  # Processor to which the Factor is connected
    ft = None  # FactorType
    f = None  # Factor

    if p_names and aliases:
        # Create an alias for the Processor
        if isinstance(aliases, str):
            p = glb_idx.get(Processor.partial_key(aliases))
        elif isinstance(aliases, Processor):
            p = aliases
        if p:
            full_name = ".".join(p_names)
            # Look for a processor named <full_name>, it will be an AMBIGUITY TO BE AVOIDED
            p1, k1 = glb_idx.get(Processor.partial_key(full_name), True)
            if p1:
                # If it is an ALIAS, void the already existing because there would be no way to decide
                # which of the two (or more) do we need
                if Processor.is_alias_key(k1[0]):
                    # Assign NONE to the existing Alias
                    glb_idx.put(k1[0], None)
            else:
                # Create the ALIAS
                k_ = Processor.alias_key(full_name, p)
                glb_idx.put(k_, p)  # An alternative Key pointing to the same processor
    else:
        # Find or create the "lineage" of Processors, using part-of relation ("matryoshka" or recursive containment)
        parent = None
        acum_name = ""
        for i, p_name in enumerate(p_names):
            last = i == (len(p_names)-1)

            # CREATE processor(s) (if it does not exist). The processor is an Observable
            acum_name += ("." if acum_name != "" else "") + p_name
            p = glb_idx.get(Processor.partial_key(name=acum_name))
            if not p or strcmp(acum_name, p_name):
                attrs = proc_attributes if last else None
                location = proc_location if last else None
                # Create processor
                p = Processor(p_name,  # acum_name,
                              geolocation=location,
                              tags=None,
                              attributes=attrs
                              )
                # Index it, with its multiple names, adding the attributes only if it is the processor in play
                # for alt_name in hierarchical_name_variants(acum_name):
                p_key = Processor.partial_key(acum_name, p.ident)
                if last and proc_attributes:
                    p_key.update({k: ("" if v is None else v) for k, v in proc_attributes.items()})
                glb_idx.put(p_key, p)
            else:
                p = p[0]

            if parent:
                # Create PART-OF relation
                o1 = glb_idx.get(ProcessorsRelationPartOfObservation.partial_key(parent=parent, child=p))
                if not o1:
                    o1 = ProcessorsRelationPartOfObservation.create_and_append(parent, p, oer)  # Part-of
                    glb_idx.put(o1.key(), o1)
                    p_key = Processor.partial_key(f"{parent.name}.{acum_name}", p.ident)
                    if proc_attributes:
                        p_key.update({k: ("" if v is None else v) for k, v in proc_attributes.items()})
                    glb_idx.put(p_key, p)

            parent = p

    # Find or create the lineage of FactorTypes and for the last FactorType, find or create Factor
    parent = None
    acum_name = ""
    for i, ft_name in enumerate(f_names):
        last = i == len(f_names)-1

        # CREATE factor type(s) (if it does not exist). The Factor Type is a Class of Observables
        # (it is NOT observable: neither quantities nor relations)
        acum_name += ("." if acum_name != "" else "") + ft_name
        ft = glb_idx.get(FactorType.partial_key(name=acum_name))
        if not ft:
            attrs = fact_attributes if last else None
            ft = FactorType(acum_name,  #
                            parent=parent, hierarchy=None,
                            roegen_type=fact_roegen_type,
                            tags=None,  # No tags
                            attributes=attrs,
                            expression=None  # No expression
                            )
            for alt_name in hierarchical_name_variants(acum_name):
                ft_key = FactorType.partial_key(alt_name, ft.ident)
                if last and fact_attributes:
                    ft_key.update(fact_attributes)
                glb_idx.put(ft_key, ft)
        else:
            ft = ft[0]

        if last and p:  # The Processor must exist. If not, nothing is created or obtained
            # CREATE Factor (if it does not exist). An Observable
            f = glb_idx.get(Factor.partial_key(processor=p, factor_type=ft))
            if not f:
                f = Factor.create_and_append(acum_name,
                                             p,
                                             in_processor_type=FactorInProcessorType(external=fact_external, incoming=fact_incoming),
                                             taxon=ft,
                                             geolocation=fact_location,
                                             tags=None,
                                             attributes=fact_attributes)
                glb_idx.put(f.key(), f)
            else:
                f = f[0]

        parent = ft

    return p, ft, f  # Return all the observables (some may be None)


def find_or_create_factor(state: Union[State, PartialRetrievalDictionary],
                          p: Processor, ft: FactorType,
                          fact_external: bool=None, fact_incoming: bool=None,
                          fact_location=None, fact_attributes: dict=None):
    if isinstance(state, PartialRetrievalDictionary):
        glb_idx = state
    else:
        glb_idx, p_sets, hh, datasets, mappings = get_case_study_registry_objects(state)

    f = glb_idx.get(Factor.partial_key(processor=p, factor_type=ft))
    if not f:
        f = Factor.create_and_append(ft.name,
                                     p,
                                     in_processor_type=FactorInProcessorType(external=fact_external,
                                                                             incoming=fact_incoming),
                                     taxon=ft,
                                     geolocation=fact_location,
                                     tags=None,
                                     attributes=fact_attributes)
        glb_idx.put(f.key(), f)
    else:
        f = f[0]
    return f


def find_or_create_factor_type(state: Union[State, PartialRetrievalDictionary],
                               name: str,
                               fact_roegen_type: FlowFundRoegenType = None, fact_attributes: dict = None):
    """
    "name" has to be a FactorType name, ie, ":<hierarchical_name>"

    :param state:
    :param name:
    :param fact_roegen_type:
    :param fact_attributes:
    :return:
    """
    p_names, f_names = obtain_name_parts(name)
    if not p_names:
        _, ft, _ = find_or_create_observable(state, name, fact_roegen_type=fact_roegen_type,
                                             fact_attributes=fact_attributes)
        return ft
    else:
        raise Exception("It has to be a FactorType name, received '" + name + "'")


def find_or_create_processor(state: Union[State, PartialRetrievalDictionary],
                             name: str,
                             proc_attributes: dict = None,
                             proc_location=None):
    """
    "name" has to be a Processor name

    :param state:
    :param name:
    :param proc_attributes:
    :param proc_location:
    :return:
    """
    p_names, f_names = obtain_name_parts(name)
    if not f_names:
        p, _, _ = find_or_create_observable(state, name,
                                            proc_attributes=proc_attributes,
                                            proc_location=proc_location)
        return p
    else:
        raise Exception("It has to be a Processor name, received '" + name + "'")


def create_or_append_quantitative_observation(state: Union[State, PartialRetrievalDictionary],
                                              factor: Union[str, Factor],
                                              value: str, unit: str,
                                              observer: Union[str, Observer]=Observer.no_observer_specified,
                                              spread: str=None, assessment: str=None, pedigree: str=None, pedigree_template: str=None,
                                              relative_to: Union[str, Factor]=None,
                                              time: str=None,
                                              geolocation: str=None,
                                              comments: str=None,
                                              tags=None, other_attributes=None,
                                              proc_aliases: str=None,
                                              proc_external: bool=None, proc_attributes: dict=None, proc_location=None,
                                              ftype_roegen_type: FlowFundRoegenType=None, ftype_attributes: dict=None,
                                              fact_incoming: bool=None, fact_external: bool=None, fact_location=None):
    """
    Creates an Observation of a Factor
    If the containing Processor does not exist, it is created
    If the FactorType does not exist, it is created
    If the Factor does not exist, it is created
    Finally, if no "value" is passed, only the Factor is created

    :param state: A State or PartialRetrievalDictionary instance
    :param factor: string processor:factor_type or Factor. If str, the Factor (and Processor and FactorType) can be created
    :param value: expression with the value
    :param unit: metric unit
    :param observer: string with the name of the observer or Observer
    :param spread: expression defining uncertainty of :param value
    :param assessment:
    :param pedigree: encoded assessment of the quality of the science/technique of the observation
    :param pedigree_template: reference pedigree matrix used to encode the pedigree
    :param relative_to: Factor Type in the same Processor to which the value is relative
    :param time: time extent in which the value is valid
    :param geolocation: where the observation is
    :param comments: open comments about the observation
    :param tags: list of tags added to the observation
    :param other_attributes: dictionary added to the observation
    :param proc_aliases: name of aliased processor (optional). Used only if the Processor does not exist
    :param proc_external: True if the processor is outside the case study borders, False if it is inside. Used only if the Processor does not exist
    :param proc_attributes: Dictionary with attributes added to the Processor. Used only if the Processor does not exist
    :param proc_location: Reference specifying the location of the Processor. Used only if the Processor does not exist
    :param ftype_roegen_type: Either FUND or FLOW (applied to FactorType). Used only if the FactorType does not exist
    :param ftype_attributes: Dictionary with attributes added to the FactorType. Used only if the FactorType does not exist
    :param fact_incoming: Specifies if the Factor goes into or out the Processor. Used if the Factor (not FactorType) does not exist
    :param fact_external: Specifies if the Factor is injected from an external Processor. Used if the Factor (not FactorType) does not exist
    :param fact_location: Reference specifying the location of the Factor. Used if the Factor does not exist

    :return:
    """
    # Get objects from state
    if isinstance(state, State):
        glb_idx, p_sets, hh, datasets, mappings = get_case_study_registry_objects(state)
    elif isinstance(state, PartialRetrievalDictionary):
        glb_idx = state

    # Obtain factor
    p, ft = None, None
    if not isinstance(factor, Factor):
        p, ft, factor_ = find_or_create_observable(glb_idx,
                                                   factor,
                                                   # source=None,
                                                   aliases=proc_aliases,
                                                   proc_attributes=proc_attributes,
                                                   proc_location=proc_location,
                                                   fact_roegen_type=ftype_roegen_type,
                                                   fact_attributes=ftype_attributes,
                                                   fact_incoming=fact_incoming,
                                                   fact_external=fact_external,
                                                   fact_location=fact_location
                                                   )
        if not isinstance(factor_, Factor):
            raise Exception("The name specified for the factor ('"+factor+"') did not result in the obtention of a Factor")
        else:
            factor = factor_

    # If a value is defined...
    if value:
        # Get the Observer for the relations (PART-OF for now)
        if isinstance(observer, Observer):
            oer = observer
        else:
            if not observer:
                observer = Observer.no_observer_specified
            oer = glb_idx.get(Observer.partial_key(name=observer))
            if not oer:
                oer = Observer(observer)
                glb_idx.put(oer.key(), oer)
            else:
                oer = oer[0]

        # If "relative_to" is specified, maybe a FactorType needs to be created
        if relative_to:
            ast = parser_field_parsers.string_to_ast(parser_field_parsers.factor_unit, relative_to)
            factor_type = ast_to_string(ast["factor"])

            unit_name = ast["unparsed_unit"]
            unit = str((ureg(unit) / ureg(unit_name)).units)

            ft = find_or_create_factor_type(glb_idx, ":"+factor_type, fact_roegen_type=None, fact_attributes=None)
            relative_to = find_or_create_factor(glb_idx, p, ft, fact_external=None, fact_incoming=None,
                                                fact_location=None, fact_attributes=None)

        # Create the observation (and append it to the Interface)
        o = _create_or_append_quantitative_observation(factor,
                                                       value, unit, spread, assessment, pedigree, pedigree_template,
                                                       oer,
                                                       relative_to,
                                                       time,
                                                       geolocation,
                                                       comments,
                                                       tags, other_attributes
                                                       )
        # Register
        # glb_idx.put(o.key(), o)

        # Return the observation
        return p, ft, factor, o
    else:
        # Return the Factor
        return p, ft, factor, None


def create_relation_observations(state: Union[State, PartialRetrievalDictionary],
                                 origin: Union[str, Processor, Factor],
                                 destinations: List[Tuple[Union[str, Processor, Factor], Optional[Tuple[Union[RelationClassType, str], Optional[str]]]]],
                                 relation_class: Union[str, RelationClassType]=None,
                                 oer: Union[str, Observer]=Observer.no_observer_specified,
                                 attributes=None) -> List:
    """
    Create and register one or more relations from a single origin to one or more destinations.
    Relation parameters (type and weight) can be specified for each destination, or a default relation class parameter is used
    Relation are assigned to the observer "oer"

    :param state: Registry of all objects
    :param origin: Origin of the relation as string, Processor or Factor
    :param destinations: List of tuples, where each tuple can be of a single element, the string, Processor or Factor, or can be accompanied by the relation parameters, the relation type, and the string specifying the weight
    :param relation_class: Default relation class
    :param oer: str or Observer for the Observer to which relation observations are accounted
    :param attributes: Attributes attached to the new Relationship
    :return: The list of relations
    """
    def get_requested_object(p_, ft_, f_):
        return f_ if f_ else (p_ if p_ else ft_)

    def get_all_objects(input: Union[str, Processor, Factor]) -> Tuple[Processor, Optional[FactorType], Optional[Factor]]:
        if isinstance(input, str):
            return find_or_create_observable(glb_idx, input)
        elif isinstance(input, Processor):
            return input, None, None
        elif isinstance(input, Factor):
            return input.processor, input.taxon, input
        else:
            raise Exception(f"Input parameter must be String, Processor or Interface: type={type(input)}, value={input}")

    if isinstance(state, PartialRetrievalDictionary):
        glb_idx = state
    else:
        glb_idx, _, _, _, _ = get_case_study_registry_objects(state)

    # Origin
    p, ft, f = get_all_objects(origin)

    origin_obj = get_requested_object(p, ft, f)

    rels = []

    # Default Observer
    if not oer:
        oer = Observer.no_observer_specified

    # Create Observer
    if isinstance(oer, str):
        oer_ = glb_idx.get(Observer.partial_key(name=oer))
        if not oer_:
            oer = Observer(oer)
            glb_idx.put(oer.key(), oer)
        else:
            oer = oer_[0]
    elif not isinstance(oer, Observer):
        raise Exception("'oer' parameter must be a string or an Observer instance")

    if not isinstance(destinations, list):
        destinations = [destinations]

    for dst in destinations:
        if not isinstance(dst, tuple):
            dst = tuple([dst])
        # Destination
        dst_obj = None
        # PART-OF
        if isinstance(origin_obj, Processor) and relation_class == RelationClassType.pp_part_of:
            # Find dst[0]. If it does not exist, create dest UNDER (hierarchically) origin
            dst_obj = find_observable_by_name(dst[0], glb_idx)
            if not dst_obj:
                name = origin_obj.full_hierarchy_names(glb_idx)[0] + "." + dst[0]
                p, ft, f = find_or_create_observable(glb_idx, name, source=oer)
                dst_obj = get_requested_object(p, ft, f)
            rel = glb_idx.get(ProcessorsRelationPartOfObservation.partial_key(parent=origin_obj, child=dst_obj))  # , observer=oer
            if len(rel) == 0:
                rel = _find_or_create_relation(origin_obj, dst_obj, rel_type, oer, "", glb_idx, attributes=attributes)
            rels.append(rel[0])
            continue  # Skip the rest of the loop

        # not PART-OF relationship types
        if not dst_obj:
            p, ft, f = get_all_objects(dst[0])

            dst_obj = get_requested_object(p, ft, f)
            # If origin is Processor and destination is Factor, create Factor in origin (if it does not exist). Or viceversa
            if isinstance(origin_obj, Processor) and isinstance(dst_obj, Factor):
                # Obtain full origin processor name
                names = origin_obj.full_hierarchy_names(glb_idx)
                p, ft, f = find_or_create_observable(glb_idx, names[0] + ":" + dst_obj.taxon.name)
                origin_obj = get_requested_object(p, ft, f)
            elif isinstance(origin_obj, Factor) and isinstance(dst_obj, Processor):
                names = dst_obj.full_hierarchy_names(glb_idx)
                p, ft, f = find_or_create_observable(glb_idx, names[0] + ":" + origin_obj.taxon.name)
                dst_obj = get_requested_object(p, ft, f)
            # Relation class
            if len(dst) > 1:
                rel_type = dst[1]
            else:
                if not relation_class:
                    if isinstance(origin_obj, Processor) and isinstance(dst_obj, Processor):
                        relation_class = RelationClassType.pp_undirected_flow
                    else:
                        relation_class = RelationClassType.ff_directed_flow
                rel_type = relation_class
            if len(dst) > 2:
                weight = dst[2]
            else:
                weight = ""  # No weight, it only can be used to aggregate
            rel = _find_or_create_relation(origin_obj, dst_obj, rel_type, oer, weight, glb_idx, attributes=attributes)
        rels.append(rel)

    return rels

# ########################################################################################
# Auxiliary functions
# ########################################################################################


def obtain_name_parts(n):
    """
    Parse the name. List of processor names + list of factor names
    :param n:
    :return:
    """
    r = n.split(":")
    if len(r) > 1:
        full_p_name = r[0]
        full_f_name = r[1]
    else:
        full_p_name = r[0]
        full_f_name = ""
    p_ = full_p_name.split(".")
    f_ = full_f_name.split(".")
    if len(p_) == 1 and not p_[0]:
        p_ = []
    if len(f_) == 1 and not f_[0]:
        f_ = []
    return p_, f_


def get_factor_id(f_: Union[Factor, Processor], ft: FactorType=None):
    if isinstance(f_, Factor):
        return (f_.processor.name + ":" + f_.taxon.name).lower()
    elif isinstance(f_, Processor) and isinstance(ft, FactorType):
        return (f_.name + ":" + ft.name).lower()


def _create_or_append_quantitative_observation(factor: Factor,
                                               value: str, unit: str,
                                               spread: str, assessment: str, pedigree: str, pedigree_template: str,
                                               observer: Observer,
                                               relative_to: Optional[Factor],
                                               time: str,
                                               geolocation: Optional[str],
                                               comments: str,
                                               tags, other_attributes):
    f_name = get_factor_id(factor)
    # print(f_name)
    if other_attributes:
        attrs = other_attributes.copy()
    else:
        attrs = {}

    attrs.update({"relative_to": relative_to,
                  "time": time,
                  "geolocation": geolocation,
                  "unit": unit,
                  "spread": spread,
                  "assessment": assessment,
                  "pedigree": pedigree,
                  "pedigree_template": pedigree_template,
                  "comments": comments
                  }
                 )

    fo = FactorQuantitativeObservation.create_and_append(v=value,
                                                         factor=factor,
                                                         observer=observer,
                                                         tags=tags,
                                                         attributes=attrs
                                                         )
    return fo


def _get_observer(observer: Union[str, Observer], registry: PartialRetrievalDictionary) -> Optional[Observer]:
    res = None
    if isinstance(observer, Observer):
        res = observer
    else:
        oer = registry.get(Observer.partial_key(name=observer))
        if oer:
            res = oer[0]
    return res


def find_or_create_observer(observer: str, registry: PartialRetrievalDictionary) -> Observer:
    # Find
    obs = _get_observer(observer, registry)

    if not obs:
        # Create
        obs = Observer(observer)
        registry.put(obs.key(), obs)

    return obs


def find_factor_types_transform_relation(registry: PartialRetrievalDictionary,
                                         origin_interface_type: FactorType, destination_interface_type: FactorType,
                                         origin_context: Processor, destination_context: Processor):
    """We try to get the best match from the existing factor types scale changes"""

    def get_relations_from_contexts(orig=None, dest=None) -> List[FactorTypesRelationUnidirectionalLinearTransformObservation]:
        return registry.get(FactorTypesRelationUnidirectionalLinearTransformObservation.partial_key(
            origin=origin_interface_type, destination=destination_interface_type,
            origin_context=orig, destination_context=dest))

    # 1. Do we have a relation with same origin and destination context?
    if origin_context and destination_context:
        relations = get_relations_from_contexts(orig=origin_context, dest=destination_context)
        if len(relations) > 0:
            return relations

    # 2. Do we have a relation with same origin context?
    if origin_context:
        relations = get_relations_from_contexts(orig=origin_context)
        if len(relations) > 0:
            return relations

    # 3. Do we have a relation with same destination context?
    if destination_context:
        relations = get_relations_from_contexts(dest=destination_context)
        if len(relations) > 0:
            return relations

    # 4. Do we have a relation without contexts?
    return get_relations_from_contexts()


def _find_or_create_relation(origin, destination, rel_type: Union[str, RelationClassType], oer: Union[Observer, str], weight: str, state: Union[State, PartialRetrievalDictionary], attributes=None):
    """
    Construct and register a relation between origin and destination

    :param origin: Either processor or factor
    :param destination: Either processor or factor
    :param rel_type: Relation type. Either a string or a member of RelationClassType enumeration
    :param oer: Observer, as object or string
    :param weight: For flow relations
    :param state: State or PartialRetrievalDictionary
    :param attributes: Attributes of the relationship
    :return: The relation observation
    """
    # Get objects from state
    if isinstance(state, State):
        glb_idx, _, _, _, _ = get_case_study_registry_objects(state)
    elif isinstance(state, PartialRetrievalDictionary):
        glb_idx = state

    # CREATE the Observer for the relation
    if oer and isinstance(oer, str):
        oer_ = glb_idx.get(Observer.partial_key(name=oer))
        if not oer_:
            oer = Observer(oer)
            glb_idx.put(oer.key(), oer)
        else:
            oer = oer_[0]

    if isinstance(rel_type, str):
        rel_type = RelationClassType.from_str(rel_type)

    r = None
    if rel_type.is_between_processors and isinstance(origin, Processor) and isinstance(destination, Processor):
        if rel_type == RelationClassType.pp_part_of:
            # Find or Create the relation
            r = glb_idx.get(ProcessorsRelationPartOfObservation.partial_key(parent=origin, child=destination))
            if not r:
                r = ProcessorsRelationPartOfObservation.create_and_append(origin, destination, oer, attributes=attributes)  # Part-of
                glb_idx.put(r.key(), r)
            else:
                r = r[0]
            # Add destination to the index with an alternative name
            # TODO Do the same with all part-of children of destination, recursively
            # TODO "full_hierarchy_names" makes use of
            d_name = destination.simple_name()
            for h_name in origin.full_hierarchy_names(glb_idx):
                full_name = h_name+"."+d_name
                p = glb_idx.get(Processor.partial_key(name=full_name))
                if not p:
                    glb_idx.put(Processor.partial_key(name=full_name, ident=destination.ident), destination)
                else:
                    if p[0].ident != destination.ident:
                        raise Exception("Two Processors under name '"+full_name+"' have been found: ID1: "+p[0].ident+"; ID2: "+destination.ident)
        elif rel_type == RelationClassType.pp_undirected_flow:
            # Find or Create the relation
            r = glb_idx.get(ProcessorsRelationUndirectedFlowObservation.partial_key(source=origin, target=destination))
            if not r:
                r = ProcessorsRelationUndirectedFlowObservation.create_and_append(origin, destination, oer, attributes=attributes)  # Undirected flow
                glb_idx.put(r.key(), r)
            else:
                r = r[0]
        elif rel_type == RelationClassType.pp_upscale:
            # Find or Create the relation
            r = glb_idx.get(ProcessorsRelationUpscaleObservation.partial_key(parent=origin, child=destination))
            if not r:
                r = ProcessorsRelationUpscaleObservation.create_and_append(origin, destination, oer, weight, attributes=attributes)  # Upscale
                glb_idx.put(r.key(), r)
            else:
                r = r[0]
                r._quantity = weight
                r._observer = oer
                r._attributes = attributes
    elif rel_type.is_between_interfaces and isinstance(origin, Factor) and isinstance(destination, Factor):

        if rel_type in (RelationClassType.ff_directed_flow, RelationClassType.ff_reverse_directed_flow,
                        RelationClassType.ff_directed_flow_back):
            if rel_type == RelationClassType.ff_reverse_directed_flow:
                origin, destination = destination, origin

                if weight:
                    weight = f"1/({weight})"

                scale_change_weight = attributes.get("scale_change_weight") if attributes else None
                if scale_change_weight:
                    attributes["scale_change_weight"] = f"1/({scale_change_weight})"

            back_interface: Optional[Factor] = attributes.pop("back_interface", None) if attributes else None

            # Find or Create the relation
            r = glb_idx.get(FactorsRelationDirectedFlowObservation.partial_key(source=origin,
                                                                               target=destination,
                                                                               back=back_interface))
            if not r:
                r = FactorsRelationDirectedFlowObservation.create_and_append(origin, destination, oer, weight,
                                                                             attributes=attributes,
                                                                             back=back_interface)
                glb_idx.put(r.key(), r)
            else:
                r = r[0]
                r._weight = weight
                r._observer = oer
                r._attributes = attributes

        elif rel_type in (RelationClassType.ff_scale, RelationClassType.ff_scale_change):
            scale_change_weight = attributes.pop("scale_change_weight", None) if attributes else None
            if scale_change_weight is not None:
                weight = FloatOrString.multiply(weight, scale_change_weight)

            # Find or Create the relation
            r = glb_idx.get(FactorsRelationScaleObservation.partial_key(origin=origin, destination=destination))
            if not r:
                r = FactorsRelationScaleObservation.create_and_append(origin, destination, oer, weight,
                                                                      attributes=attributes)
                glb_idx.put(r.key(), r)
            else:
                r = r[0]
                r._quantity = weight
                r._observer = oer
                r._attributes = attributes

    return r


def build_hierarchy(name, type_name, registry: PartialRetrievalDictionary, h: dict, oer: Observer=None, level_names=None):
    """
    Take the result of parsing a hierarchy and elaborate either an Hierarchy (for Categories and FactorType)
    or a set of nested Processors

    Shortcut function

    :param name:
    :param type_name:
    :param registry:
    :param h:
    :param oer: An Observer
    :param level_names:
    :return: If type_name is Processor -> "None". If other, return the Hierarchy object
    """
    if type_name.lower() in ["p"]:
        type_name = "processor"
    elif type_name.lower() in ["i", "f"]:
        type_name = "factortype"
    elif type_name.lower() in ["c", "t"]:
        type_name = "taxon"

    return _build_hierarchy(name, type_name, registry, h, oer, level_names, acum_name="", parent=None)


def _build_hierarchy(name, type_name, registry: PartialRetrievalDictionary, h: dict, oer=None, level_names=None, acum_name="", parent=None) -> Optional[Hierarchy]:
    """
    Take the result of parsing a hierarchy and elaborate either an Hierarchy (for Categories and FactorType)
    or a set of nested Processors

    :param name: Name of the hierarchy
    :param type_name: Processor, Taxonomy, FactorType
    :param registry: The state, space of variables where the nodes and the hierarchy itself are stored
    :param h: The list of nodes, which can be recursive
    :param oer: The observer of the hierarchy. It is treated differently for Processors, Categories and FactorTypes
    :param level_names:
    :param acum_name: (Internal - do not use) Will contain the acumulated name in hierarchical form
    :param parent: Parent to be used to define the relations in the current level of the hierarchy
    :return:
    """
    # Get or create hierarchy or observer
    if oer:
        hie = oer
    elif name:
        if type_name.lower() == "processor":
            if registry:
                oer = registry.get(Observer.partial_key(name=name))
                if oer:
                    hie = oer[0]
                else:
                    hie = Observer(name)
                    registry.put(hie.key(), hie)
            else:
                hie = Observer(name)
        else:
            if registry:
                hie = registry.get(Hierarchy.partial_key(name=name))
                if not hie:
                    hie = Hierarchy(name, type_name=type_name)
                    registry.put(hie.key(), hie)
                else:
                    hie = hie[0]
            else:
                hie = Hierarchy(name, type_name=type_name)

    for s in h:
        # Create node
        n_name = s["code"]
        if "description" in s:
            desc = s["description"]
        else:
            desc = None
        if "expression" in s:
            exp = s["expression"]
        else:
            exp = None
        if "children" in s:
            children = s["children"]
        else:
            children = []
        # Accumulated name
        acum_name2 = acum_name + ("." if acum_name != "" else "") + n_name

        if type_name.lower() == "processor":
            # Check if the Processor exists
            # If not, create it
            n = registry.get(Processor.partial_key(name=acum_name2))
            if not n:
                attrs = None
                location = None
                proc_external = None
                n = Processor(acum_name2,
                              geolocation=location,
                              tags=None,
                              attributes=attrs
                              )
                registry.put(n.key(), n)
            else:
                n = n[0]
            if parent:
                # Create "part-of" relation
                rel = _find_or_create_relation(parent, n, RelationClassType.pp_part_of, hie, "", registry)
        elif type_name.lower() == "factortype":
            # Check if the FactorType exists
            # If not, create it
            n = registry.get(FactorType.partial_key(name=acum_name2))
            if not n:
                attrs = None
                n = FactorType(acum_name2,  #
                               parent=parent, hierarchy=hie,
                               roegen_type=FlowFundRoegenType.flow,
                               tags=None,  # No tags
                               attributes=attrs,
                               expression=exp
                               )
                for alt_name in hierarchical_name_variants(acum_name2):
                    ft_key = FactorType.partial_key(alt_name, n.ident)
                    registry.put(ft_key, n)
            else:
                n = n[0]
        elif type_name.lower() == "taxon":
            # Check if the Taxon exists
            # If not, create it
            n = registry.get(Taxon.partial_key(name=acum_name2))
            if not n:
                n = Taxon(acum_name2, parent=parent, hierarchy=hie, expression=exp, description=desc)
                for alt_name in hierarchical_name_variants(acum_name2):
                    t_key = Taxon.partial_key(alt_name, n.ident)
                    registry.put(t_key, n)
            else:
                n = n[0]

        if children:
            _build_hierarchy(name, type_name, registry, children, hie, level_names, acum_name2, parent=n)

        # Add node, only for the first level, if it is a hierarchy
        if not parent and isinstance(hie, Hierarchy):
            hie.roots_append(n)

    # Set level names and return the hierarchy, only for the first level, if it is a hierarchy
    if not parent and isinstance(hie, Hierarchy):
        if level_names:
            hie.level_names = level_names

        return hie
    else:
        return None


def get_processors(registry: PartialRetrievalDictionary):
    # Just remove duplicates (processors can be registered multiple times under different names)
    return set(registry.get(Processor.partial_key()))
