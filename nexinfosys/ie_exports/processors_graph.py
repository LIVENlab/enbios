import io
import logging

import networkx as nx

from nexinfosys.solving import *


def construct_processors_graph_2(state: State, query: IQueryObjects, filt: Union[str, dict], part_of: bool, scale: bool, flow: bool, format: str):
    """

    :param state:
    :param query:
    :param filt:
    :param part_of: show part_of relationships
    :param scale: show scale relationships (between processors)
    :param flow: show flow relationships
    :return:
    """

    def append_processor(proc, nodes, p_dict):
        p_id = get_processor_ident(proc)
        d = processor_to_dict(proc, reg)
        d.update(dict(graphics={}))
        # Instance / archetype
        if proc.instance_or_archetype.lower() == "instance":
            color = "#aa2211"
        else:
            color = "#552211"
        # Biosphere / technosphere
        if proc.subsystem_type.lower() in ["environment", "localenvironment", "externalenvironment"]:
            shape = "ellipse"
        else:
            shape = "hexagon"
        # Internal / external
        if proc.subsystem_type.lower() in ["external", "externalenvironment"]:  # Internal / external
            d["shape_properties"] = dict(borderDashes="[5,5]")
        d["color"] = color
        d["type"] = shape
        p_dict[p_id] = d
        # print(F"{p_id} -> {d}")
        nodes.append((p_id, d))

    # Format
    edge_flow = dict(graphics={"fill": "#ff00ff", "width": 3, "targetArrow": "standard"})
    edge_part_of = dict(graphics={"fill": "#00ffff", "width": 3, "targetArrow": "standard"})
    edge_scale = dict(graphics={"fill": "#00ffff", "width": 2, "targetArrow": "standard"})

    # Obtain the information needed to elaborate the graph
    objs = query.execute([Processor, Factor,
                          ProcessorsRelationPartOfObservation,
                          FactorsRelationScaleObservation,
                          FactorsRelationDirectedFlowObservation
                          ],
                         filt
                         )

    # PartialRetrievalDictionary
    reg = state.get("_glb_idx")

    processors = {}
    n = []
    e = []
    # Processors
    for p in objs[Processor]:
        # nam = " ,".join(p.full_hierarchy_names(reg))
        # print(nam)
        append_processor(p, n, processors)

    if flow:
        # Flow Links
        for flow in objs[FactorsRelationDirectedFlowObservation]:
            sf = get_factor_type_id(flow.source_factor)
            tf = get_factor_type_id(flow.target_factor)
            weight = flow.weight if flow.weight else ""
            weight = ("\n" + str(weight)) if weight != "" else ""
            sp_id = get_processor_ident(flow.source_factor.processor)
            dp_id = get_processor_ident(flow.target_factor.processor)
            if sf == tf:
                edge = dict(etype="flow", font={"size": 7}, label=sf + weight, w=weight)
            else:
                edge = dict(etype="flow", font={"size": 7}, label=sf + weight + "\n" + tf, w=weight)
            edge.update(edge_flow)
            e.append((sp_id, dp_id, edge))

    if part_of:
        # Part-of Relations
        for po in objs[ProcessorsRelationPartOfObservation]:
            # Add edge between two Processors
            pp = get_processor_ident(po.parent_processor)
            cp = get_processor_ident(po.child_processor)
            if pp not in processors:
                append_processor(po.parent_processor, n, processors)
            if cp not in processors:
                append_processor(po.child_processor, n, processors)
            edge = dict(etype="part-of", font={"size": 7})
            edge.update(edge_part_of)
            # print(F"{cp} -> {pp} [{edge}]")
            e.append((cp, pp, edge))

    if scale:
        # Scale Relations
        for scale in objs[FactorsRelationScaleObservation]:
            # Add edge between two Processors
            pp = get_processor_ident(scale.origin.processor)
            cp = get_processor_ident(scale.destination.processor)
            if pp not in processors:
                print("Not registered invoker")
            if cp not in processors:
                print("Not registered invoked")
            weight = scale.quantity if scale.quantity else ""
            weight = str(weight) if weight != "" else ""
            edge = dict(etype="scale", font={"size": 7}, dashes="true", label=scale.origin.name + " x " + weight +" > " + scale.destination.name, w=weight)
            edge.update(edge_scale)
            # print(F"{pp} -> {cp} [{edge}]")
            e.append((pp, cp, edge))

    # NetworkX
    # -- Create the graph
    processors_graph = nx.MultiDiGraph()
    processors_graph.add_nodes_from(n)
    processors_graph.add_edges_from(e)

    # Convert to VisJS
    ids_map = create_dictionary()
    id_count = 0
    for node in processors_graph.nodes(data=True):
        sid = str(id_count)
        node[1]["id"] = sid
        ids_map[node[0]] = sid
        id_count += 1

    ret = None
    if format == "visjs":
        vis_nodes = []
        vis_edges = []
        for node in processors_graph.nodes(data=True):
            d2 = node[1]
            logging.debug(node)
            if "uname" not in d2:
                continue
            d = dict(id=node[1]["id"], label=node[1]["uname"])
            if "shape" in node[1]:
                # circle, ellipse, database, box, diamond, dot, square, triangle, triangleDown, text, star
                d["shape"] = node[1]["shape"]
            else:
                d["shape"] = "hexagon"
            if "color" in node[1]:
                d["color"] = node[1]["color"]
            vis_nodes.append(d)
        for edge in processors_graph.edges(data=True):
            f = ids_map[edge[0]]
            t = ids_map[edge[1]]
            d = {"from": f, "to": t, "arrows": "to"}
            data = edge[2]
            if "label" in data:
                d["font"] = {"align": "middle"}
            d.update(data)

            vis_edges.append(d)
        ret = {"nodes": vis_nodes, "edges": vis_edges}
    elif format == "gml":
        ret1 = io.BytesIO()
        nx.write_gml(processors_graph, ret1)
        ret = ret1.getvalue()
        ret1.close()

    return ret

def construct_processors_graph(state: State, query: IQueryObjects, filt: Union[str, dict]):
    """
    Prepare a graph representing processors and their interconnections
    Optionally represent the relation "part-of" between Processors

    :param state: State
    :param query: A IQueryObjects instance (which has been already injected the state)
    :param filt: A filter to be passed to the query instance
    :return:
    """
    # Format
    a_processor = dict(graphics={"type": "hexagon", "color": "#aa2211"})
    edge_flow = dict(relation="flow", graphics={"fill": "#ff00ff", "width": 3, "targetArrow": "standard"})
    edge_part_of = dict(relation="part_of", graphics={"fill": "#00ffff", "width": 3, "targetArrow": "standard"})

    # Obtain the information needed to elaborate the graph
    objs = query.execute([Processor, Factor,
                          ProcessorsRelationPartOfObservation,
                          FactorsRelationDirectedFlowObservation
                          ],
                         filt
                         )

    # PartialRetrievalDictionary
    reg = state.get("_glb_idx")

    n = []
    e = []
    # Processors
    for p in objs[Processor]:
        # nam = " ,".join(p.full_hierarchy_names(reg))
        # print(nam)

        p_id = get_processor_ident(p)
        n.append((p_id, processor_to_dict(p, reg)))

    # Flow Links
    for flow in objs[FactorsRelationDirectedFlowObservation]:
        sf = get_factor_type_id(flow.source_factor)
        tf = get_factor_type_id(flow.target_factor)
        weight = flow.weight if flow.weight else ""
        weight = ("\n" + str(weight)) if weight != "" else ""
        sp_id = get_processor_ident(flow.source_factor.processor)
        dp_id = get_processor_ident(flow.target_factor.processor)
        if sf == tf:
            edge = dict(font={"size": 7}, label=sf + weight, w=str(weight))
        else:
            edge = dict(font={"size": 7}, label=sf + weight + "\n" + tf, w=str(weight))
        edge.update(edge_flow)
        e.append((sp_id, dp_id, edge))

    # Part-of Relations
    for po in objs[ProcessorsRelationPartOfObservation]:
        # TODO add edge between two Processors
        pp = get_processor_ident(po.parent_processor)
        cp = get_processor_ident(po.child_processor)
        edge = dict(font={"size": 7})
        edge.update(edge_part_of)
        e.append((cp, pp, edge))

    # NetworkX
    # -- Create the graph
    processors_graph = nx.MultiDiGraph()
    processors_graph.add_nodes_from(n)
    processors_graph.add_edges_from(e)

    # Convert to VisJS
    ids_map = create_dictionary()
    id_count = 0
    for node in processors_graph.nodes(data=True):
        sid = str(id_count)
        node[1]["id"] = sid
        ids_map[node[0]] = sid
        id_count += 1

    vis_nodes = []
    vis_edges = []
    for node in processors_graph.nodes(data=True):
        d = dict(id=node[1]["id"], label=node[1]["uname"])
        if "shape" in node[1]:
            # circle, ellipse, database, box, diamond, dot, square, triangle, triangleDown, text, star
            d["shape"] = node[1]["shape"]
        else:
            d["shape"] = "hexagon"
        if "color" in node[1]:
            d["color"] = node[1]["color"]
        vis_nodes.append(d)
    for edge in processors_graph.edges(data=True):
        f = ids_map[edge[0]]
        t = ids_map[edge[1]]
        d = {"from": f, "to": t, "arrows": "to"}
        data = edge[2]
        if "label" in data:
            d["label"] = data["label"]
            d["font"] = {"align": "middle"}
            if "font" in data:
                d["font"].update(data["font"])

        vis_edges.append(d)
    visjs = {"nodes": vis_nodes, "edges": vis_edges}
    # print(visjs)
    return visjs


if __name__ == '__main__':
    from nexinfosys.serialization import deserialize_state

    # Deserialize previously recorded Soslaires State (WARNING! Execute unit tests to generated the ".serialized" file)
    fname = "/home/rnebot/GoogleDrive/AA_MAGIC/Soslaires.serialized"
    fname = "/home/rnebot/GoogleDrive/AA_MAGIC/MiniAlmeria.serialized"
    with open(fname, "r") as file:
        s = file.read()
    state = deserialize_state(s)
    # Create a Query and execute a query
    query = BasicQuery(state)
    logging.debug(construct_processors_graph(state, query, None))
