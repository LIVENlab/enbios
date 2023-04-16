import json
import logging

from anytree import Node

from nexinfosys.model_services import IExecutableCommand, get_case_study_registry_objects
from nexinfosys.common.helper import obtain_dataset_metadata, strcmp, create_dictionary
from nexinfosys.models.musiasem_concepts import Mapping


def convert_code_list_to_hierarchy(cl, as_list=False):
    """
    Receives a list of codes. Codes are sorted lexicographically (to include numbers).

    Two types of coding schemes are supported by assuming that trailing zeros can be ignored to match parent -> child
    relations. The first is uniformly sized codes (those with trailing zeros). The second is growing length codes.

    Those with length less than others but common prefix are parents

    :param cl:
    :param as_list: if True, return a flat tree (all nodes are siblings, descending from a single root)
    :return:
    """

    def can_be_child(parent_candidate, child_candidate):
        # Strip zeros to the right, from parent_candidate, and
        # check if the child starts with the resulting substring
        return child_candidate.startswith(parent_candidate.rstrip("0"))

    root = Node("")
    path = [root]
    code_to_node = create_dictionary()
    for c in sorted(cl):
        if as_list:
            n = Node(c, path[-1])
        else:
            found = False
            while len(path) > 0 and not found:
                if can_be_child(path[-1].name, c):
                    found = True
                else:
                    path.pop()
            if c.rstrip("0") == path[-1].name:
                # Just modify (it may enter here only in the root node)
                path[-1].name = c
                n = path[-1]
            else:
                # Create node and append it to the active path
                n = Node(c, path[-1])
                path.append(n)
        code_to_node[c] = n  # Map the code to the node

    return root, code_to_node


def map_codelists(src, dst, corresp, dst_tree=False) -> (list, set):
    """
    Obtain map of two code lists
    If the source is a tree, children of a mapped node are assigned to the same mapped node
    The same source may be mapped more than once, to different nodes
    The codes from the source not mapped, are stored in "unmapped"

    :param src: source full code list
    :param dst: destination full code list
    :param corresp: list of tuples with the correspondence
    :param dst_tree: Is the dst code list a tree?
    :return: List of tuples (source code, target code), set of unmapped codes
    """

    def assign(n: str, v: str):
        """
        Assign a destination code name to a source code name
        If the source has children, assign the same destination to children, recursively

        :param n: Source code name
        :param v: Destination code name
        :return:
        """
        mapped.add(n, v)
        if n in unmapped:
            unmapped.remove(n)
        for c in cn_src[n].children:
            assign(c.name, v)

    unmapped = set(src)
    r_src, cn_src = convert_code_list_to_hierarchy(src, as_list=True)
    if dst_tree:
        r_dst, cn_dst = convert_code_list_to_hierarchy(dst)
    else:
        cn_dst = create_dictionary()
        for i in dst:
            cn_dst[i] = None  # Simply create the entry
    mapped = create_dictionary(multi_dict=True)  # MANY TO MANY
    for t in corresp:
        if t[0] in cn_src and t[1] in cn_dst:
            # Check that t[1] is a leaf node. If not, ERROR
            if isinstance(cn_dst[t[1]], Node) and len(cn_dst[t[1]].children) > 0:
                # TODO ERROR: the target destination code is not a leaf node
                pass
            else:
                # Node and its children (recursively) correspond to t[1]
                assign(t[0], t[1])

    for k in sorted(unmapped):
        logging.debug("Unmapped: " + k)
    # for k in sorted(r):
    #     print(k+" -> "+r[k])

    # Convert mapped to a list of tuples
    # Upper case
    mapped_lst = []
    for k in mapped:
        for i in mapped.getall(k):
            mapped_lst.append((k, i))

    return mapped_lst, unmapped


# Combinatory of lists (for many-to-many mappings)
#
# import itertools
# a = [[(0.5, "a"),(4, "b"),(7, "c")],[(1, "b")],[(0.2, "z"),(0.1, "y"),(0.7, "x")]]
# list(itertools.product(*a))
#

def fill_map_with_all_origin_categories(dim, map):
    # Check all codes exist
    mapped_codes = set([d["o"] for d in map])
    all_codes = set([c for c in dim.code_list])
    for c in all_codes - mapped_codes:  # Loop over "unmapped" origin codes
        # This sentence MODIFIES map, so it is not necessary to return it
        map.append({"o": c, "to": [{"d": None, "w": 1.0}]})  # Map to placeholder, with weight 1

    return map


class MappingCommand(IExecutableCommand):
    def __init__(self, name: str):
        self._name = name
        self._content = None

    def execute(self, state: "State"):
        some_error = False
        issues = []
        # Read mapping parameters
        origin_dataset = self._content["origin_dataset"]
        origin_dimension = self._content["origin_dimension"]
        destination = self._content["destination"]
        # [{"o": "", "to": [{"d": "", "w": ""}]}]
        # [ {o: origin category, to: [{d: destination category, w: weight assigned to destination category}] } ]
        map = self._content["map"]
        # Obtain the origin dataset Metadata, obtain the code list
        dims, attrs, meas = obtain_dataset_metadata(origin_dataset)
        if origin_dimension not in dims:
            some_error = True
            issues.append((3, "The origin dimension '"+origin_dimension+"' does not exist in dataset '"+origin_dataset+"'"))
        else:
            dim = dims[origin_dimension]
            map = fill_map_with_all_origin_categories(dim, map)
            # # Check all codes exist
            # src_code_list = [c for c in dim.code_list]
            # dst_code_set = set()
            # many_to_one_list = []
            # for i in map:
            #     o = i["o"]
            #     for j in i["to"]:
            #         d = j["d"]
            #         dst_code_set.add(d)
            #         many_to_one_list.append((o, d))
            # hierarchical_code = True
            # if hierarchical_code:
            #     mapped, unmapped = map_codelists(src_code_list, list(dst_code_set), many_to_one_list)
            # else:
            #     # Literal. All codes on the left MUST exist
            #     mapped = many_to_one_list
            #     for i in mapped:
            #         o = i["o"]
            #         if o not in dim.code_list:
            #             some_error = True
            #             issues.append((3, "The origin category '" + o + "' does not exist in dataset dimension '" + origin_dataset + "." +origin_dimension + "'"))

        if some_error:  # Issues at this point are errors, return if there are any
            return issues, None

        # Create and store the mapping
        glb_idx, p_sets, hh, datasets, mappings = get_case_study_registry_objects(state)

        from nexinfosys.ie_imports.data_source_manager import DataSourceManager
        source = DataSourceManager.obtain_dataset_source(origin_dataset, datasets)

        mappings[self._name] = Mapping(self._name, source, origin_dataset, origin_dimension, destination, map)

        # TODO If the categories to the left are not totally covered, what to do?
        # TODO - If a non-listed category appears, remove the line
        # TODO - If a non-listed category appears, leave the target column NA

        # TODO - If there are datasets matching the origin, JOIN

        return None, None

    def estimate_execution_time(self):
        return 0

    def json_serialize(self):
        # Directly return the metadata dictionary
        return self._content

    def json_deserialize(self, json_input):
        # TODO Check validity
        issues = []
        if isinstance(json_input, dict):
            self._content = json_input
        else:
            self._content = json.loads(json_input)

        if "description" in json_input:
            self._description = json_input["description"]
        return issues