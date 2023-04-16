import io

import networkx as nx

from nexinfosys.models.musiasem_concepts import FactorsRelationScaleObservation
from nexinfosys.solving import *
from nexinfosys import ureg
from nexinfosys.command_generators.parser_ast_evaluators import ast_to_string
from nexinfosys.models.musiasem_concepts_helper import find_or_create_observable, find_quantitative_observations
from nexinfosys.serialization import deserialize_state
from nexinfosys.command_generators import parser_field_parsers

"""
IN DEVELOPMENT. NOT READY FOR INTEGRATION INTO THE DOCKER CONTAINER

This module will contain functions to compose graphs for different purposes

* For Internal/External Processes capable of exploiting the graph topology and associated attributes

* For deduction of Factors:
  - Nodes: Factors
  - Edges: flow relations, relations between FactorTypes (hierarchy, expressions in hierarchies, linear transforms)


* For summary visualization of flow relation between Processors 
  - Nodes: Processors
  - Edges: Flows between factors, several flows between the same pair of processor are summarized into one

* Visualization of graph with hierarchy of processors 
  - Nodes: Processors
  - Edges: part-of relations
* Visualization of Processors and their factors
  - Nodes: Processors and Factors
  - Edges: Processors to Factors, flows Factor to Factor

Allow filtering:
  - By Observer
  - By other criteria 


>> Si hay expresiones no lineales, np hay una manera directa de expresar en el grafo <<

Mapa

Parámetros - Variables SymPy
Factores - Variables SymPy

ECUACIONES DIRECTAMENTE

Factores -> Variables
Parámetros -> Inputs

Observaciones extensivas -> Valores para las variables
Observaciones intensivas -> ECUACIÓN: F2 = (V)*F1
Flujo entre factores: padre <- hijos: F = sum(Fhijos)
Flujo entre factores: SPLIT: F1 = W1*F, F2 = W2*F ó F = F1/W1 + F2/W2 + ...
Flujo entre factores: JOIN : F  = W1*F1 + W2*F2 + ...
Ambos son redundantes

F1  G1
F2  G2
G1 = W1*F1 + W2*F2
G2 = (1-W1)*F1 + (1-W2)*F2  
"""


def construct_flow_graph_2(state: State, query: IQueryObjects, filt: Union[str, dict], format: str="visjs"):
    """
    Prepare a graph from which conclusions about factors can be extracted

    Example:
        1) Obtain "s", the serialized state from Redis or from a test file
        2) state = deserialize_state(s)
        3) query = BasicQuery(state) # Create a Query and execute a query
        4) construct_solve_graph(state, query, None)

    :param state: State
    :param query: A IQueryObjects instance (which has been already injected the state)
    :param filt: A filter to be passed to the query instance
    :param format: VisJS, GML, ...
    :return:
    """
    include_processors = False  # For completeness (not clarity...), include processors nodes, as a way to visualize grouped factors
    will_write = True  # For debugging purposes, affects how the properties attached to nodes and edges are elaborated
    expand_factors_graph = False  # Expand transformation between FactorTypes into instances of Factors

    # Format for different node types
    stated_factor_no_observation = dict(graphics={'fill': "#999900"})  # Golden
    stated_factor_some_observation = dict(graphics={'fill': "#ffff00"})  # Yellow
    qq_attached_to_factor = dict(graphics={'fill': "#eeee00", "type": "ellipse"})  # Less bright Yellow
    non_stated_factor = dict(graphics={'fill': "#999999"})
    a_processor = dict(graphics={"type": "hexagon", "color": "#aa2211"})

    # Format for different edge types
    edge_from_factor_type = dict(graphics={"fill": "#ff0000", "width": 1, "targetArrow": "standard"})
    edge_processor_to_factor = dict(graphics={"fill": "#ff00ff", "width": 3, "targetArrow": "standard"})
    edge_factors_flow = dict(graphics={"fill": "#000000", "width": 5, "targetArrow": "standard"})
    edge_factors_scale = dict(graphics={"fill": "#333333", "width": 3, "targetArrow": "standard"})
    edge_factors_relative_to = dict(graphics={"fill": "#00ffff", "width": 3, "targetArrow": "standard"})
    edge_factor_value = dict(graphics={"fill": "#aaaaaa", "width": 1, "targetArrow": "standard"})

    glb_idx, p_sets, hh, datasets, mappings = get_case_study_registry_objects(state)

    # Obtain the information needed to elaborate the graph
    objs = query.execute([Processor, Factor, FactorType,
                          FactorTypesRelationUnidirectionalLinearTransformObservation,
                          FactorsRelationScaleObservation,
                          FactorsRelationDirectedFlowObservation
                          ],
                         filt
                         )

    # 1) Graphical Representation: BOX -- BOX
    #
    # 2) Internal (not for end-users), pseudo-code:
    #
    # Processor1 <- Factor1 -> FactorType0
    # Processor2 <- Factor2 -> FactorType0
    # Processor3 <- Factor3 -> FactorType1
    # Processor3 <- Factor4 -> FactorType0
    # Factor1 <- FactorsRelationDirectedFlowObservation(0.4) -> Factor2
    # Factor1 <- FactorsRelationDirectedFlowObservation(0.6) -> Factor4
    # Factor1 <- FactorQuantitativeObservation(5.3 m²)
    # FactorType0 -> FactorTypesRelationUnidirectionalLinearTransformObservation(ctx) -> FactorType1
    # Factor4 -> w1 -> Factor3
    # Factor5 -> w2 -> Factor3
    #

    # Index quantitative observations.
    # Also, mark Factors having QQs (later this will serve to color differently these nodes)
    qqs = {}
    qq_cont = 0
    factors_with_some_observation = set()
    for o in find_quantitative_observations(glb_idx):
        # Index quantitative observations.
        if "relative_to" in o.attributes and o.attributes["relative_to"]:
            continue  # Do not index intensive quantities, because they are translated as edges in the graph
        if o.factor in qqs:
            lst = qqs[o.factor]
        else:
            lst = []
            qqs[o.factor] = lst
        lst.append(o)
        # Mark Factors having QQs (later this will serve to color differently these nodes)
        factors_with_some_observation.add(o.factor)

    # ---- MAIN GRAPH: Factors and relations between them --------------------------------------------------------------
    the_node_names_set = set()

    # --   Nodes: "Factor"s passing the filter, and QQs associated to some of the Factors
    n = []
    e = []
    f_types = {}  # Contains a list of Factors for each FactorType
    p_factors = {}  # Contains a list of Factors per Processor7
    rel_to_observations = set()  # Set of FactorObservation having "relative_to" property defined
    factors = create_dictionary()  # Factor_ID -> Factor
    for f in objs[Factor]:
        f_id = get_factor_id(f, prd=glb_idx)
        factors[f_id] = f  # Dictionary Factor_ID -> Factor
        # f_types
        if f.taxon in f_types:
            lst = f_types[f.taxon]
        else:
            lst = []
            f_types[f.taxon] = lst
        lst.append(f)
        # p_factors
        if f.processor in p_factors:
            lst = p_factors[f.processor]
        else:
            lst = []
            p_factors[f.processor] = lst
        lst.append(f)

        # Add Node to graph
        the_node_names_set.add(f_id)
        if will_write:
            n.append((f_id, stated_factor_some_observation if f in factors_with_some_observation else stated_factor_no_observation))
            if f in qqs:
                for qq in qqs[f]:
                    if not ("relative_to" in qq.attributes and qq.attributes["relative_to"]):
                        # value = str(qq.value)  # str(qq_cont) + ": " + str(qq.value)
                        value_node_name = f_id + " " + str(qq.value)
                        n.append((value_node_name, qq_attached_to_factor))
                        e.append((value_node_name, f_id, {"w": "", "label": "", **edge_factor_value}))
                        qq_cont += 1
                    else:
                        rel_to_observations.add(qq)
        else:
            qqs2 = [qq for qq in qqs if not ("relative_to" in qq.attributes and qq.attributes["relative_to"])]
            d = dict(factor=factor_to_dict(f), observations=qqs[f_id] if f_id in qqs2 else [])
            n.append((f_id, d))

    # --   Edges
    # "Relative to" relation (internal to the Processor) -> Intensive to Extensive
    for o in rel_to_observations:
        if "relative_to" in o.attributes and o.attributes["relative_to"]:
            # Parse "defining_factor", it can be composed of the factor name AND the unit
            defining_factor = o.attributes["relative_to"]
            ast = parser_field_parsers.string_to_ast(parser_field_parsers.factor_unit, defining_factor)
            factor_type = ast_to_string(ast["factor"])
            unit_name = ast["unparsed_unit"]
            ureg(unit_name)
            f_id = get_factor_id(o.factor, prd=glb_idx)
            # Check that "f_id" exists in the nodes list (using "factors")
            factors[f_id]
            # If "defining_factor" exists in the processor, ok. If not, create it.
            # Find factor_type in the processor
            factor_name = get_processor_id(o.factor.processor) + ":" + factor_type
            factors[factor_name]
            e.append((factor_name, f_id, {"w": o.value.expression, "label": o.value.expression, **edge_factors_relative_to}))

    # Directed Flows between Factors
    for df in objs[FactorsRelationDirectedFlowObservation]:
        sf = get_factor_id(df.source_factor, prd=glb_idx)
        tf = get_factor_id(df.target_factor, prd=glb_idx)
        # Check that both "sf" and "tf" exist in the nodes list (using "factors")
        factors[sf]
        factors[tf]
        weight = df.weight if df.weight else "1"
        e.append((sf, tf, {"w": weight, "label": weight, **edge_factors_flow}))

    # Scale Flows between Factors
    for df in objs[FactorsRelationScaleObservation]:
        sf = get_factor_id(df.origin, prd=glb_idx)
        tf = get_factor_id(df.destination, prd=glb_idx)
        # Check that both "sf" and "tf" exist in the nodes list (using "factors")
        factors[sf]
        factors[tf]
        weight = str(df.quantity) if df.quantity else "1"
        e.append((sf, tf, {"w": weight, "label": weight, **edge_factors_scale}))

    # TODO Consider Upscale relations
    # e.append((..., ..., {"w": upscale_weight, "label": upscale_weight, **edge_factors_upscale}))

    # -- Create the graph
    factors_graph = nx.DiGraph()
    factors_graph.add_nodes_from(n)
    factors_graph.add_edges_from(e)

    # nx.write_gml(factors_graph, "/home/rnebot/IntermediateGraph.gml")

    # ---- AUXILIARY GRAPH: FACTOR TYPES AND THEIR INTERRELATIONS ----
    n = []
    e = []
    # --   Nodes: "FactorType"s passing the filter
    for ft in objs[FactorType]:
        n.append((get_factor_type_id(ft), dict(factor_type=ft)))

    # --   Edges
    # Hierarchy and expressions stated in the hierarchy
    ft_in = {}  # Because FactorTypes cannot be both in hierarchy AND expression, marks if it has been specified one was, to raise an error if it is specified also the other way
    for ft in objs[FactorType]:
        ft_id = get_factor_type_id(ft)
        if ft.expression:
            if ft not in ft_in:
                # TODO Create one or more relations, from other FactorTypes (same Hierarchy) to this one
                # TODO The expression can only be a sum of FactorTypes (same Hierarchy)
                ft_in[ft] = "expression"
                # TODO Check that both "ft-id" and "..." exist in the nodes list (keep a temporary set)
                # weight = ...
                # e.append((ft_id, ..., {"w": weight, "label": weight, "origin": ft, "destination": ...}))

        if ft.parent:
            if ft.parent not in ft_in or (ft.parent in ft_in and ft_in[ft.parent] == "hierarchy"):
                # Create an edge from this FactorType
                ft_in[ft.parent] = "hierarchy"
                parent_ft_id = get_factor_type_id(ft.parent)
                # TODO Check that both "ft-id" and "parent_ft_id" exist in the nodes list (keep a temporary set)
                # Add the edge
                e.append((ft_id, parent_ft_id, {"w": "1", "origin": ft, "destination": ft.parent}))
            else:
                raise Exception("The FactorType '"+ft_id+"' has been specified by an expression, it cannot be parent.")
    # Linear transformations
    for f_rel in objs[FactorTypesRelationUnidirectionalLinearTransformObservation]:
        origin = get_factor_type_id(f_rel.origin)
        destination = get_factor_type_id(f_rel.destination)
        e.append((origin, destination, {"w": f_rel.weight, "label": f_rel.weight, "origin": f_rel.origin, "destination": f_rel.destination}))

    # ---- Create FACTOR TYPES graph ----

    factor_types_graph = nx.DiGraph()
    factor_types_graph.add_nodes_from(n)
    factor_types_graph.add_edges_from(e)

    # ---- EXPAND "FACTORS GRAPH" with "FACTOR TYPE" RELATIONS ----
    sg_list = []  # List of modified (augmented) subgraphs
    if expand_factors_graph:
        # The idea is: clone a FactorTypes subgraph if a Factor instances some of its member nodes
        # This cloning process can imply creating NEW Factors

        the_new_node_names_set = set()

        # Obtain weak components of the main graph. Each can be considered separately

        # for sg in nx.weakly_connected_component_subgraphs(factors_graph):  # For each subgraph
        #     print("--------------------------------")
        #     for n in sg.nodes():
        #         print(n)

        # ---- Weakly connected components of "factor_types_graph" ----
        factor_types_subgraphs = list(nx.weakly_connected_component_subgraphs(factor_types_graph))

        for sg in nx.weakly_connected_component_subgraphs(factors_graph):  # For each subgraph
            sg_list.append(sg)
            # Consider each Factor of the subgraph
            unprocessed_factors = set(sg.nodes())
            while unprocessed_factors:  # For each UNPROCESSED Factor
                tmp = unprocessed_factors.pop()  # Get next unprocessed "factor name"
                if tmp not in factors:  # QQ Observations are in the graph and not in "factors". The same with Processors
                    continue
                f_ = factors[tmp]  # Obtain Factor from "factor name"
                ft_id = get_factor_type_id(f_)  # Obtain FactorType name from Factor
                # Iterate through FactorTypes and check if the Factor appears
                for sg2 in factor_types_subgraphs:  # Each FactorTypes subgraph
                    if ft_id in sg2:  # If the current Factor is in the subgraph
                        if len(sg2.nodes()) > 1:  # If the FactorType subgraph has at least two nodes
                            # CLONE FACTOR TYPES SUBGRAPH
                            # Nodes. Create if not present already
                            n = []
                            e = []
                            for n2, attrs in sg2.nodes().items():  # Each node in the FactorTypes subgraph
                                ft_ = attrs["factor_type"]
                                f_id = get_factor_id(f_.processor, ft_, prd=glb_idx)
                                if f_id not in sg:  # If the FactorType is not
                                    # Create Factor, from processor and ft_ -> f_new
                                    _, _, f_new = find_or_create_observable(state, name=f_id, source="solver")
                                    factors[f_id] = f_new
                                    if f_id not in the_node_names_set:
                                        if will_write:
                                            n.append((f_id, non_stated_factor))
                                        else:
                                            d = dict(factor=factor_to_dict(f_new), observations=[])
                                            n.append((f_id, d))
                                    if f_id not in the_node_names_set:
                                        the_new_node_names_set.add(f_id)
                                    the_node_names_set.add(f_id)
                                else:
                                    unprocessed_factors.discard(f_id)
                            # Edges. Create relations between factors
                            for r2, w_ in sg2.edges().items():
                                # Find origin and destination nodes. Copy weight. Adapt weight? If it refers to a FactorType, instance it?
                                origin = get_factor_id(f_.processor, w_["origin"], prd=glb_idx)
                                destination = get_factor_id(f_.processor, w_["destination"], prd=glb_idx)
                                if origin in the_new_node_names_set or destination in the_new_node_names_set:
                                    graphics = edge_from_factor_type
                                else:
                                    graphics = {}
                                e.append((origin, destination, {"w": w_["w"], "label": w_["w"], **graphics}))
                            sg.add_nodes_from(n)
                            sg.add_edges_from(e)
                            break

        # for sg in sg_list:
        #     print("--------------------------------")
        #     for n in sg.nodes():
        #         print(n)

    # Recompose the original graph
    if sg_list:
        factors_graph = nx.compose_all(sg_list)
    else:
        pass
        ##factors_graph = nx.DiGraph()

    # ----
    # Add "Processor"s just as a way to visualize grouping of factors (they do not influence relations between factors)
    # -
    if include_processors:
        n = []
        e = []
        for p in objs[Processor]:
            p_id = get_processor_id(p)
            if will_write:
                n.append((p_id, a_processor))
            else:
                n.append((p_id, processor_to_dict(p)))
            # Edges between Processors and Factors
            for f in p_factors[p]:
                f_id = get_factor_id(f, prd=glb_idx)
                e.append((p_id, f_id, edge_processor_to_factor))
        factors_graph.add_nodes_from(n)
        factors_graph.add_edges_from(e)

    #
    # for ft in objs[FactorType]:
    #     if ft.parent:
    #         # Check which Factors are instances of this FactorType
    #         if ft in f_types:
    #             for f in f_types[ft]:
    #                 # Check if the processor contains the parent Factor
    #                 processor_factors = p_factors[f.processor]
    #                 if ft.parent not in processor_factors:
    #                     factor_data = (f.processor, ft)
    #                 else:
    #                     factor_data = None
    #                 create_factor = f in qqs  # If there is some Observation
    #                 create_factor = True # Force creation
    #
    #
    #         # Consider the creation of a relation
    #         # Consider also the creation of a new Factor (a new Node for now): if the child has some observation for sure (maybe a child of the child had an observation, so it is the same)
    #         ft_id =
    #     ft_id =

    # Plot graph to file
    # import matplotlib.pyplot as plt
    # ax = plt.subplot(111)
    # ax.set_title('Soslaires Graph', fontsize=10)
    # nx.draw(factors_graph, with_labels=True)
    # plt.savefig("/home/rnebot/Graph.png", format="PNG")

    # GML File
    # nx.write_gml(factors_graph, "/home/rnebot/Graph.gml")

    ret = None
    if format == "visjs":
        # Assign IDs to nodes. Change edges "from" and "to" accordingly
        ids_map = create_dictionary()
        id_count = 0
        for node in factors_graph.nodes(data=True):
            sid = str(id_count)
            node[1]["id"] = sid
            ids_map[node[0]] = sid
            id_count += 1

        vis_nodes = []
        vis_edges = []
        for node in factors_graph.nodes(data=True):
            d = dict(id=node[1]["id"], label=node[0])
            if "shape" in node[1]:
                # circle, ellipse, database, box, diamond, dot, square, triangle, triangleDown, text, star
                d["shape"] = node[1]["shape"]
            else:
                d["shape"] = "box"
            if "color" in node[1]:
                d["color"] = node[1]["color"]
            vis_nodes.append(d)
        for edge in factors_graph.edges(data=True):
            f = ids_map[edge[0]]
            t = ids_map[edge[1]]
            d = {"from": f, "to": t, "arrows": "to"}
            data = edge[2]
            if "w" in data:
                d["label"] = data["w"]
                d["font"] = {"align": "horizontal"}

            vis_edges.append(d)
        ret = {"nodes": vis_nodes, "edges": vis_edges}
    elif format == "gml":
        ret1 = io.BytesIO()
        nx.write_gml(factors_graph, ret1)
        ret = ret1.getvalue()
        ret1.close()

    return ret

# #########################################################################3

    # GEXF File
    # nx.write_gexf(factors_graph, "/home/rnebot/Graph.gexf")

    # Legend graph
    n = []
    e = []
    n.append(("Factor with Observation", stated_factor_some_observation))
    n.append(("Factor with No Observation", stated_factor_no_observation))
    if include_processors:
        n.append(("Processor", a_processor))
    n.append(("Factor from FactorType", non_stated_factor))
    n.append(("QQ Observation", qq_attached_to_factor))
    n.append(("QQ Intensive Observation", qq_attached_to_factor))

    e.append(("A Factor", "Another Factor", {"label": "Flow between Factors, attaching the weight", **edge_factors_flow}))
    e.append(("Factor #1", "Factor #2", {"label": "Relation from a FactorType", **edge_from_factor_type}))
    if include_processors:
        e.append(("Processor", "A Factor", {"label": "Link from Processor to Factor", **edge_processor_to_factor}))
    e.append(("A Factor", "Same Factor in another processor", {"label": "Upscale a Factor in two processors", **edge_factors_upscale}))
    e.append(("Factor with Observation", "QQ Intensive Observation", {"label": "Observation proportional to extensive value of factor same processor", **edge_factors_relative_to}))
    e.append(("QQ Observation", "A Factor", {"label": "A QQ Observation", **edge_factor_value}))
    factors_graph = nx.DiGraph()
    factors_graph.add_nodes_from(n)
    factors_graph.add_edges_from(e)
    # nx.write_gml(factors_graph, "/home/rnebot/LegendGraph.gml")


def construct_flow_graph(state: State, query: IQueryObjects, filt: Union[str, dict], format: str="visjs"):
    """
    Prepare a graph from which conclusions about factors can be extracted

    Example:
        1) Obtain "s", the serialized state from Redis or from a test file
        2) state = deserialize_state(s)
        3) query = BasicQuery(state) # Create a Query and execute a query
        4) construct_solve_graph(state, query, None)

    :param state: State
    :param query: A IQueryObjects instance (which has been already injected the state)
    :param filt: A filter to be passed to the query instance
    :return:
    """
    include_processors = False  # For clarity, include processors nodes, as a way to visualize grouped factors
    will_write = True  # For debugging purposes, affects how the properties attached to nodes and edges are elaborated
    # Format
    stated_factor_no_observation = dict(graphics={'fill': "#999900"})  # Golden
    stated_factor_some_observation = dict(graphics={'fill': "#ffff00"})  # Yellow
    qq_attached_to_factor = dict(graphics={'fill': "#eeee00", "type": "ellipse"})  # Less bright Yellow
    non_stated_factor = dict(graphics={'fill': "#999999"})
    a_processor = dict(graphics={"type": "hexagon", "color": "#aa2211"})

    edge_from_factor_type = dict(graphics={"fill": "#ff0000", "width": 1, "targetArrow": "standard"})
    edge_processor_to_factor = dict(graphics={"fill": "#ff00ff", "width": 3, "targetArrow": "standard"})
    edge_factors_flow = dict(graphics={"fill": "#000000", "width": 5, "targetArrow": "standard"})
    edge_factors_upscale = dict(graphics={"fill": "#333333", "width": 3, "targetArrow": "standard"})
    edge_factors_relative_to = dict(graphics={"fill": "#00ffff", "width": 3, "targetArrow": "standard"})
    edge_factor_value = dict(graphics={"fill": "#aaaaaa", "width": 1, "targetArrow": "standard"})

    glb_idx, p_sets, hh, datasets, mappings = get_case_study_registry_objects(state)

    # Obtain the information needed to elaborate the graph
    objs = query.execute([Processor, Factor, FactorType,
                          FactorTypesRelationUnidirectionalLinearTransformObservation,
                          ProcessorsRelationPartOfObservation, ProcessorsRelationUpscaleObservation,
                          ProcessorsRelationUndirectedFlowObservation,
                          FactorsRelationDirectedFlowObservation
                          ],
                         filt
                         )

    # Processor <- ProcessorsRelationPartOfObservation -> Processor

    # 1) Graphical Representation: BOX -- BOX
    #
    # 2) Internal (not for end-users), pseudo-code:
    #
    # Processor1 <- Factor1 -> FactorType0
    # Processor2 <- Factor2 -> FactorType0
    # Processor3 <- Factor3 -> FactorType1
    # Processor3 <- Factor4 -> FactorType0
    # Factor1 <- FactorsRelationDirectedFlowObservation(0.4) -> Factor2
    # Factor1 <- FactorsRelationDirectedFlowObservation(0.6) -> Factor4
    # Factor1 <- FactorQuantitativeObservation(5.3 m²)
    # FactorType0 -> FactorTypesRelationUnidirectionalLinearTransformObservation(ctx) -> FactorType1
    # Factor4 -> w1 -> Factor3
    # Factor5 -> w2 -> Factor3
    #

    # EIM:
    # LETs "eco"
    # for interface in eco.interfaces:
    #     if
    #
    # EUM:

    # 3) Spreadsheet representation
    # ....

    # Index quantitative observations.
    # Also, mark Factors having QQs (later this will serve to color differently these nodes)
    qqs = {}
    qq_cont = 0
    factors_with_some_observation = set()
    for o in find_quantitative_observations(glb_idx):
        # Index quantitative observations.
        if "relative_to" in o.attributes and o.attributes["relative_to"]:
            continue  # Do not index intensive quantities, because they are translated as edges in the graph
        if o.factor in qqs:
            lst = qqs[o.factor]
        else:
            lst = []
            qqs[o.factor] = lst
        lst.append(o)
        # Mark Factors having QQs (later this will serve to color differently these nodes)
        factors_with_some_observation.add(o.factor)

    # ---- MAIN GRAPH: Factors and relations between them --------------------------------------------------------------
    the_node_names_set = set()

    # --   Nodes: "Factor"s passing the filter, and QQs associated to some of the Factors
    n = []
    e = []
    f_types = {}  # Contains a list of Factors for each FactorType
    p_factors = {}  # Contains a list of Factors per Processor
    factors = create_dictionary() # Factor_ID -> Factor
    for f in objs[Factor]:
        f_id = get_factor_id(f)
        factors[f_id] = f  # Dictionary Factor_ID -> Factor
        # f_types
        if f.taxon in f_types:
            lst = f_types[f.taxon]
        else:
            lst = []
            f_types[f.taxon] = lst
        lst.append(f)
        # p_factors
        if f.processor in p_factors:
            lst = p_factors[f.processor]
        else:
            lst = []
            p_factors[f.processor] = lst
        lst.append(f)

        # Node
        the_node_names_set.add(f_id)
        if will_write:
            n.append((f_id, stated_factor_some_observation if f in factors_with_some_observation else stated_factor_no_observation))
            if f in qqs:
                for qq in qqs[f]:
                    if not ("relative_to" in qq.attributes and qq.attributes["relative_to"]):
                        value = str(qq.value)  # str(qq_cont) + ": " + str(qq.value)
                        n.append((value, qq_attached_to_factor))
                        e.append((value, f_id, {"w": "", "label": "", **edge_factor_value}))
                        qq_cont += 1
        else:
            qqs2 = [qq for qq in qqs if not ("relative_to" in qq.attributes and qq.attributes["relative_to"])]
            d = dict(factor=factor_to_dict(f), observations=qqs[f_id] if f_id in qqs2 else [])
            n.append((f_id, d))

    # --   Edges
    # "Relative to" relation (internal to the Processor) -> Intensive to Extensive
    for o in objs[FactorQuantitativeObservation]:
        if "relative_to" in o.attributes and o.attributes["relative_to"]:
            # Parse "defining_factor", it can be composed of the factor name AND the unit
            defining_factor = o.attributes["relative_to"]
            ast = parser_field_parsers.string_to_ast(parser_field_parsers.factor_unit, defining_factor)
            factor_type = ast_to_string(ast["factor"])
            unit_name = ast["unparsed_unit"]
            ureg(unit_name)
            f_id = get_factor_id(o.factor)
            # Check that "f_id" exists in the nodes list (using "factors")
            factors[f_id]
            # If "defining_factor" exists in the processor, ok. If not, create it.
            # Find factor_type in the processor
            factor_name = get_processor_id(o.factor.processor) + ":" + factor_type
            factors[factor_name]
            e.append((factor_name, f_id, {"w": o.value.expression, "label": o.value.expression, **edge_factors_relative_to}))

    # Directed Flows between Factors
    for df in objs[FactorsRelationDirectedFlowObservation]:
        sf = get_factor_id(df.source_factor)
        tf = get_factor_id(df.target_factor)
        # Check that both "sf" and "tf" exist in the nodes list (using "factors")
        factors[sf]
        factors[tf]
        weight = df.weight if df.weight else "1"
        e.append((sf, tf, {"w": weight, "label": weight, **edge_factors_flow}))

    # TODO Consider Upscale relations
    # e.append((..., ..., {"w": upscale_weight, "label": upscale_weight, **edge_factors_upscale}))

    # -- Create the graph
    factors_graph = nx.DiGraph()
    factors_graph.add_nodes_from(n)
    factors_graph.add_edges_from(e)

    # nx.write_gml(factors_graph, "/home/rnebot/IntermediateGraph.gml")

    # ---- AUXILIARY GRAPH: FACTOR TYPES AND THEIR INTERRELATIONS ----
    n = []
    e = []
    # --   Nodes: "FactorType"s passing the filter
    for ft in objs[FactorType]:
        n.append((get_factor_type_id(ft), dict(factor_type=ft)))

    # --   Edges
    # Hierarchy and expressions stated in the hierarchy
    ft_in = {}  # Because FactorTypes cannot be both in hierarchy AND expression, marks if it has been specified one was, to raise an error if it is specified also the other way
    for ft in objs[FactorType]:
        ft_id = get_factor_type_id(ft)
        if ft.expression:
            if ft not in ft_in:
                # TODO Create one or more relations, from other FactorTypes (same Hierarchy) to this one
                # TODO The expression can only be a sum of FactorTypes (same Hierarchy)
                ft_in[ft] = "expression"
                # TODO Check that both "ft-id" and "..." exist in the nodes list (keep a temporary set)
                # weight = ...
                # e.append((ft_id, ..., {"w": weight, "label": weight, "origin": ft, "destination": ...}))

        if ft.parent:
            if ft.parent not in ft_in or (ft.parent in ft_in and ft_in[ft.parent] == "hierarchy"):
                # Create an edge from this FactorType
                ft_in[ft.parent] = "hierarchy"
                parent_ft_id = get_factor_type_id(ft.parent)
                # TODO Check that both "ft-id" and "parent_ft_id" exist in the nodes list (keep a temporary set)
                # Add the edge
                e.append((ft_id, parent_ft_id, {"w": "1", "origin": ft, "destination": ft.parent}))
            else:
                raise Exception("The FactorType '"+ft_id+"' has been specified by an expression, it cannot be parent.")
    # Linear transformations
    for f_rel in objs[FactorTypesRelationUnidirectionalLinearTransformObservation]:
        origin = get_factor_type_id(f_rel.origin)
        destination = get_factor_type_id(f_rel.destination)
        e.append((origin, destination, {"w": f_rel.weight, "label": f_rel.weight, "origin": f_rel.origin, "destination": f_rel.destination}))

    # --   Create FACTOR TYPES graph
    factor_types_graph = nx.DiGraph()
    factor_types_graph.add_nodes_from(n)
    factor_types_graph.add_edges_from(e)

    # ---- Obtain weakly connected components of factor_types_graph ----
    factor_types_subgraphs = list(nx.weakly_connected_component_subgraphs(factor_types_graph))

    # ---- EXPAND FACTORS GRAPH with FACTOR TYPES RELATIONS ----
    # The idea is: clone a FactorTypes subgraph if a Factor instances some of its member nodes
    # This cloning process can imply creating NEW Factors

    the_new_node_names_set = set()

    # Obtain weak components of the main graph. Each can be considered separately

    # for sg in nx.weakly_connected_component_subgraphs(factors_graph):  # For each subgraph
    #     print("--------------------------------")
    #     for n in sg.nodes():
    #         print(n)

    sg_list = []  # List of modified (augmented) subgraphs
    for sg in nx.weakly_connected_component_subgraphs(factors_graph):  # For each subgraph
        sg_list.append(sg)
        # Consider each Factor of the subgraph
        unprocessed_factors = set(sg.nodes())
        while unprocessed_factors:  # For each UNPROCESSED Factor
            tmp = unprocessed_factors.pop()  # Get next unprocessed "factor name"
            if tmp not in factors:  # QQ Observations are in the graph and not in "factors". The same with Processors
                continue
            f_ = factors[tmp]  # Obtain Factor from "factor name"
            ft_id = get_factor_type_id(f_)  # Obtain FactorType name from Factor
            # Iterate through FactorTypes and check if the Factor appears
            for sg2 in factor_types_subgraphs:  # Each FactorTypes subgraph
                if ft_id in sg2:  # If the current Factor is in the subgraph
                    if len(sg2.nodes()) > 1:  # If the FactorType subgraph has at least two nodes
                        # CLONE FACTOR TYPES SUBGRAPH
                        # Nodes. Create if not present already
                        n = []
                        e = []
                        for n2, attrs in sg2.nodes().items():  # Each node in the FactorTypes subgraph
                            ft_ = attrs["factor_type"]
                            f_id = get_factor_id(f_.processor, ft_)
                            if f_id not in sg:  # If the FactorType is not
                                # Create Factor, from processor and ft_ -> f_new
                                _, _, f_new = find_or_create_observable(state, name=f_id, source="solver")
                                factors[f_id] = f_new
                                if f_id not in the_node_names_set:
                                    if will_write:
                                        n.append((f_id, non_stated_factor))
                                    else:
                                        d = dict(factor=factor_to_dict(f_new), observations=[])
                                        n.append((f_id, d))
                                if f_id not in the_node_names_set:
                                    the_new_node_names_set.add(f_id)
                                the_node_names_set.add(f_id)
                            else:
                                unprocessed_factors.discard(f_id)
                        # Edges. Create relations between factors
                        for r2, w_ in sg2.edges().items():
                            # Find origin and destination nodes. Copy weight. Adapt weight? If it refers to a FactorType, instance it?
                            origin = get_factor_id(f_.processor, w_["origin"])
                            destination = get_factor_id(f_.processor, w_["destination"])
                            if origin in the_new_node_names_set or destination in the_new_node_names_set:
                                graphics = edge_from_factor_type
                            else:
                                graphics = {}
                            e.append((origin, destination, {"w": w_["w"], "label": w_["w"], **graphics}))
                        sg.add_nodes_from(n)
                        sg.add_edges_from(e)
                        break

    # for sg in sg_list:
    #     print("--------------------------------")
    #     for n in sg.nodes():
    #         print(n)

    # Recompose the original graph
    if sg_list:
        factors_graph = nx.compose_all(sg_list)
    else:
        factors_graph = nx.DiGraph()

    # ----
    # Add "Processor"s just as a way to visualize grouping of factors (they do not influence relations between factors)
    # -
    if include_processors:
        n = []
        e = []
        for p in objs[Processor]:
            p_id = get_processor_id(p)
            if will_write:
                n.append((p_id, a_processor))
            else:
                n.append((p_id, processor_to_dict(p)))
            # Edges between Processors and Factors
            for f in p_factors[p]:
                f_id = get_factor_id(f)
                e.append((p_id, f_id, edge_processor_to_factor))
        factors_graph.add_nodes_from(n)
        factors_graph.add_edges_from(e)

    #
    # for ft in objs[FactorType]:
    #     if ft.parent:
    #         # Check which Factors are instances of this FactorType
    #         if ft in f_types:
    #             for f in f_types[ft]:
    #                 # Check if the processor contains the parent Factor
    #                 processor_factors = p_factors[f.processor]
    #                 if ft.parent not in processor_factors:
    #                     factor_data = (f.processor, ft)
    #                 else:
    #                     factor_data = None
    #                 create_factor = f in qqs  # If there is some Observation
    #                 create_factor = True # Force creation
    #
    #
    #         # Consider the creation of a relation
    #         # Consider also the creation of a new Factor (a new Node for now): if the child has some observation for sure (maybe a child of the child had an observation, so it is the same)
    #         ft_id =
    #     ft_id =

    # Plot graph to file
    # import matplotlib.pyplot as plt
    # ax = plt.subplot(111)
    # ax.set_title('Soslaires Graph', fontsize=10)
    # nx.draw(factors_graph, with_labels=True)
    # plt.savefig("/home/rnebot/Graph.png", format="PNG")

    # GML File
    # nx.write_gml(factors_graph, "/home/rnebot/Graph.gml")

    # VISJS JSON

    # Assign IDs to nodes. Change edges "from" and "to" accordingly
    ids_map = create_dictionary()
    id_count = 0
    for node in factors_graph.nodes(data=True):
        sid = str(id_count)
        node[1]["id"] = sid
        ids_map[node[0]] = sid
        id_count += 1

    vis_nodes = []
    vis_edges = []
    for node in factors_graph.nodes(data=True):
        d = dict(id=node[1]["id"], label=node[0])
        if "shape" in node[1]:
            # circle, ellipse, database, box, diamond, dot, square, triangle, triangleDown, text, star
            d["shape"] = node[1]["shape"]
        else:
            d["shape"] = "box"
        if "color" in node[1]:
            d["color"] = node[1]["color"]
        vis_nodes.append(d)
    for edge in factors_graph.edges(data=True):
        f = ids_map[edge[0]]
        t = ids_map[edge[1]]
        d = {"from": f, "to": t, "arrows": "to"}
        data = edge[2]
        if "w" in data:
            d["label"] = data["w"]
            d["font"] = {"align": "horizontal"}

        vis_edges.append(d)
    visjs = {"nodes": vis_nodes, "edges": vis_edges}
    return visjs

# #########################################################################3

    # GEXF File
    # nx.write_gexf(factors_graph, "/home/rnebot/Graph.gexf")

    # Legend graph
    n = []
    e = []
    n.append(("Factor with Observation", stated_factor_some_observation))
    n.append(("Factor with No Observation", stated_factor_no_observation))
    if include_processors:
        n.append(("Processor", a_processor))
    n.append(("Factor from FactorType", non_stated_factor))
    n.append(("QQ Observation", qq_attached_to_factor))
    n.append(("QQ Intensive Observation", qq_attached_to_factor))

    e.append(("A Factor", "Another Factor", {"label": "Flow between Factors, attaching the weight", **edge_factors_flow}))
    e.append(("Factor #1", "Factor #2", {"label": "Relation from a FactorType", **edge_from_factor_type}))
    if include_processors:
        e.append(("Processor", "A Factor", {"label": "Link from Processor to Factor", **edge_processor_to_factor}))
    e.append(("A Factor", "Same Factor in another processor", {"label": "Upscale a Factor in two processors", **edge_factors_upscale}))
    e.append(("Factor with Observation", "QQ Intensive Observation", {"label": "Observation proportional to extensive value of factor same processor", **edge_factors_relative_to}))
    e.append(("QQ Observation", "A Factor", {"label": "A QQ Observation", **edge_factor_value}))
    factors_graph = nx.DiGraph()
    factors_graph.add_nodes_from(n)
    factors_graph.add_edges_from(e)
    # nx.write_gml(factors_graph, "/home/rnebot/LegendGraph.gml")


if __name__ == '__main__':
    # Deserialize previously recorded Soslaires State (WARNING! Execute unit tests to generated the ".serialized" file)
    with open("/home/rnebot/GoogleDrive/AA_MAGIC/Soslaires.serialized", "r") as file:
        s = file.read()
    state = deserialize_state(s)
    # Create a Query and execute a query
    query = BasicQuery(state)
    construct_flow_graph(state, query, None)


# NUTS files
# Each file is a Code List
# Levels. Each level has a name
# Entry.
#   Each code has a shape (reproject (unify) to geographic coordinates (WKT)).
#   Each code can have a parent.
#   Each code is in a level.
#   An entry can have several codes and descriptions

#
# import shapefile
# dbf = "NUTS_RG_BN_60M_2013"
# # "NUTS_AT_2013",
# lst = ["NUTS_LB_2013",
#        "NUTS_SEPA_LI_2013",
#        "NUTS_RG_60M_2013",
#        "NUTS_JOIN_LI_2013",
#        "NUTS_BN_60M_2013"
#        ]
# base_dir = "/home/rnebot/Downloads/borrame/NUTS_2013_60M_SH/data/"
# for f in lst:
#     sh = shapefile.Reader(base_dir + f)
#     n_shapes = len(sh.shapes())
#     print("-----------------------------------------")
#     print(f + " #" + str(n_shapes))
#     shapes = sh.shapes()
#     fields = sh.fields
#     records = sh.records()
#     print("-----------------------------------------")
#     for i in range(10):
#         s_type = sh.shape(i).shapeType
#         if s_type == 1:
#             bbox = [0, 0, 0, 0]
#         else:
#             bbox = sh.shape(i).bbox
#         n_points = len(sh.shape(i).points)
#         print(str(s_type)+"; # points:" + str(n_points) + "; " + str(['%.3f' % coord for coord in bbox]))
