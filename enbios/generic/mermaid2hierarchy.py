import json
from pathlib import Path
from typing import Optional, Any

from pydantic import BaseModel, Field
from pyparsing import ParserElement, Word, alphas, Or, Literal, Keyword, \
    one_of, DelimitedList, Opt, srange, Group, alphanums, ParseException

from enbios.generic.files import PathLike


def _create_init_def() -> ParserElement:
    init: ParserElement = Keyword("graph") + Literal(" ") + one_of("TB TD BT RL LR")
    init = init.leave_whitespace()
    return init


def _create_node_def(group_name: Optional[str] = None) -> ParserElement:
    # Define the base components of a node
    node_name = Word(alphas, alphanums).set_results_name("name")
    attribute_expression = Group(
        Word(alphas).set_results_name("key", list_all_matches=True) + Literal(":") + Word(
            srange("[a-zA-Z_0-9]")).set_results_name("value", list_all_matches=True)).set_results_name("conf")

    NODE_SHAPES = {
        "normal": ("[", "]"),
        "round-edge": ("(", ")"),
        "stadium-shape": ("([", "])"),
        "subroutine-shape": ("[[", "]]"),
        "cylindrical": ("[(", ")]"),
        "circle": ("((", "))"),
        "label-shape": (">", "]"),
        "rhombus": ("{", ")"),
        "hexagon": ("{{", ")}"),
        "parallelogram": ("[/", "/]"),
        "parallelogram-alt": ("[\\", "\\]"),
        "trapezoid": ("[/", "\\]"),
        "trapezoid-alt": ("[\\", "/]"),
        "double-circle": ("(((", ")))"),
    }
    # Define all possible shapes for a node
    all_expressions = [node_name]
    for shape in NODE_SHAPES.values():
        node_display_name = Word(alphas)  # Assuming this definition is correct
        all_expressions.append(node_name + Literal(shape[0]) + node_display_name + Literal(shape[1]))

    # Combine the base node definition with optional process type and unit expression
    node_def_base = Or(all_expressions)
    ada_agg_expression = Word(alphas).set_results_name("module_name")

    node_def = Or([
        node_def_base,
        node_def_base + Literal(":::") + ada_agg_expression + Literal(",") + Opt(DelimitedList(attribute_expression))
    ])

    if group_name:
        return Group(node_def).set_results_name(group_name)
    return node_def


def _create_link_def() -> ParserElement:
    simple_link_strings = ["-->"]
    simple_arrow = Or([Literal(s) for s in simple_link_strings])
    # text_arrow = (Literal("--") + pp.pyparsing_common.real.set_results_name("value") + Literal("-->"))
    # arrow = Or((simple_arrow, text_arrow))
    return _create_node_def("l") + simple_arrow + _create_node_def("r")


def _read_file(file_path: Path) -> list[dict]:
    init_def = _create_init_def()
    node_def = _create_node_def()
    link_def = _create_link_def()
    after_init = False
    results = []
    with open(file_path, encoding="utf-8") as file:
        for line in file:
            line = line.strip()
            if not line:
                continue
            if not after_init:
                init_def.parse_string(line)
                after_init = True
                continue
            combined = Or((Group(node_def).set_results_name("node"), Group(link_def).set_results_name("link")))
            try:
                res = combined.parse_string(line)
            except ParseException as err:
                print(f"PARSER ERROR ON LINE: {line}")
                print(err)
            # print(line)
            print(res.as_dict())
            results.append(res.as_dict())
            after_init = True
        return results


class _Node(BaseModel):
    name: str
    module_name: Optional[str] = None
    config: Optional[dict[str, Any]] = None
    parent: Optional["_Node"] = None
    children: list["_Node"] = Field(default_factory=list)

    def __repr__(self):
        return self.name


class _Link(BaseModel):
    left_node: _Node = Field(validation_alias="l")
    right_node: _Node = Field(validation_alias="r")
    # value: float = 1


def _process_result(results: list[dict],
                    strict_node_def: bool = True) -> _Node:
    all_nodes: list[_Node] = []

    # all_links: list[Link] = []

    # [x] check all nodes have only one def
    # [x] check all nodes are somehow connected (one parent check)
    # [x] check hierarchy is ok (one parent check)
    # [x] check all node set its module, config (param)

    def add_node(node: _Node) -> _Node:
        node_names = [n.name for n in all_nodes]
        if node.name not in node_names:
            all_nodes.append(node)
            return node
        else:
            node_index = node_names.index(node.name)
            existing: _Node = all_nodes[node_index]
            if node.module_name:
                # check: no duplicate definition
                if existing.module_name:
                    raise ValueError(f"error. just one node definition. {node.name}")
                else:
                    all_nodes[node_index] = node
                    return node
            return existing

    for res in results:
        if "node" in res:
            add_node(_Node.model_validate(res["node"]))
        elif "link" in res:
            link = _Link.model_validate(res["link"])
            link.left_node = add_node(link.left_node)
            link.right_node = add_node(link.right_node)
            if link.right_node.parent:
                raise ValueError(f"Node '{link.right_node}' can only have one parent")
            link.right_node.parent = link.left_node
            link.left_node.children.append(link.right_node)
            # all_links.append(link)

    root = None
    for node in all_nodes:
        # check all have definition
        if strict_node_def:
            if not node.module_name or not node.config:
                raise Exception(f"error. missing module_name or config. node: '{node}'")
        if not node.parent:
            if not root:
                root = node
            else:
                raise ValueError(f"There can only be one root, which is '{root}. '{node}' has no parent")
        # cycle check
        else:
            next_parent = node.parent
            chain = [node]
            while next_parent:
                chain.append(next_parent)
                if next_parent.name == node.name:
                    raise Exception(f"Looks like a cycle: {chain}")
                next_parent = next_parent.parent
    if not root:
        raise Exception("Tree has no root")
    return root


def _nodes2enbios_hierarchy(root_node: _Node):
    def convert_node(node: _Node) -> dict:
        module_type = "aggregator" if node.children else "adapter"
        node_dict = {
            "name": node.name,
            module_type: node.module_name,
            "config": node.config,

        }
        if node.children:
            node_dict["children"] = [convert_node(kid) for kid in node.children]

        return node_dict

    return convert_node(root_node)


def _dump_hierarchy(hierarchy: dict, file_path: Path):
    json.dump(hierarchy, file_path.open("w", encoding="utf-8"), indent=4)


def convert_file(file_path: PathLike, destination_path: PathLike):
    p = _read_file(Path(file_path))
    res = _process_result(p, False)
    hierarchy = _nodes2enbios_hierarchy(res)
    _dump_hierarchy(hierarchy, Path(destination_path))