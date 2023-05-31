import csv
from base64 import b64encode
from copy import deepcopy, copy
from pathlib import Path
from typing import Optional, Any, Literal, Union, Generator, TypeVar, Generic, Callable
from uuid import uuid4

from enbios2.experiment.tree_transformer import tree_to_csv

T = TypeVar("T")


class BasicTreeNode(Generic[T]):
    """
    The `BasicTreeNode` class represents a node in a tree hierarchy. Each node has a name,
    can have zero or more children.

    Attributes:
    ----------
    name : str
        The name of the node.

    children : list["HierarchyNode"]
        The child nodes of this node.

    parent : HierarchyNode, optional
        The parent node of this node. None if this node is the root.
    """

    def __init__(self,
                 name: str,
                 children: list["BasicTreeNode"] = (),
                 data: Optional[T] = None):
        """
        Initialize the HierarchyNode.

        :param name: The name of the node.
        :param children: A list of child nodes (default is an empty list).
        """
        self._name: str = name
        self.children: list[BasicTreeNode[T]] = []
        for child in children:
            self.add_child(child)
        self.parent: Optional[BasicTreeNode] = None
        self.data: Optional[T] = data
        self._data: dict[str, Any] = {}  # this is used for temporary storage of data
        self._id: bytes = self.generate_id()

    def generate_id(self) -> bytes:
        self._id = b64encode(uuid4().bytes)
        return self._id

    @property
    def is_leaf(self):
        """
        Check whether this node is a leaf node (i.e., it has no children).

        :return: True if this node is a leaf node, False otherwise.
        """
        return not self.children

    @property
    def level(self) -> int:
        """
        Get the level of this node in the tree. The root node has level 0.
        :return: level of this node
        """
        return len(self.location()) - 1

    @property
    def name(self) -> str:
        """
        Get the name of this node.

        :return: The name of this node.
        """
        return self._name

    @name.setter
    def name(self, name: str):
        """
        Set the name of this node.

        :param name: The new name of this node.
        """
        if self.parent:
            if name in self.parent:
                raise ValueError(f"Node {name} already exists in {self.parent}")
        self._name = name

    def add_child(self, node: "BasicTreeNode") -> "BasicTreeNode":
        """
        Add a child node to this node.

        :param node: The node to be added as a child.
        :return: The node that was added.
        """
        if node is self or node in self:
            raise ValueError(f"Node {node} is already a child of {self}")
        self.children.append(node)
        node.parent = self
        return node

    def add_children(self, nodes: list["BasicTreeNode"]):
        """
        Add multiple child nodes to this node.
        :param nodes:
        :return:
        """
        for node in nodes:
            self.add_child(node)

    def remove_child(self, node: Union["BasicTreeNode", int]) -> "BasicTreeNode":
        """
        Remove a child node from this node.

        :param node: The node to be removed.
        :return: The node that was removed.
        """
        if isinstance(node, int):
            node = self.children[node]
        self.children.remove(node)
        node.parent = None
        return node

    def as_dict(self) -> dict[str, Any]:
        """
        Convert the hierarchy from this node down into a dictionary.

        :return: The dictionary representing the hierarchy.
        """
        return {
            "name": self.name,
            "children": {
                child.name: child.as_dict() for child in self.children
            }
        }

    def location(self) -> list["BasicTreeNode"]:
        """
        Get the path from the root to this node.

        :return: List of nodes representing the path from the root to this node.
        """
        nodes: list[BasicTreeNode] = []
        current = self
        while current is not None:
            nodes.append(current)
            current = current.parent
        return list(reversed(nodes))

    def location_names(self) -> list[str]:
        """
        Get the names of the nodes in the path from the root to this node.

        :return: List of names representing the path from the root to this node.
        """
        nodes: list[str] = []
        current = self
        while current:
            nodes.append(current.name)
            current = current.parent
        return list(reversed(nodes))

    def location_id(self) -> str:
        """
        Get the ids of the nodes in the path from the root to this node.

        :return: List of ids representing the path from the root to this node.
        """
        return "_".join(self.location_names())

    def assert_all_names_unique(self):
        """
        Assert that all names in the tree are unique. Throws an exception
        if any name is found more than once.
        """
        all_names: set = set()

        def rec_assert_unique_name(node: BasicTreeNode):
            if node.name in all_names:
                print(node.name)
            assert node.name not in all_names, f"{node.name} seems to appear twice"
            all_names.add(node.name)
            for child in node.children:
                rec_assert_unique_name(child)

        rec_assert_unique_name(self)

    def make_names_unique(self, strategy: Literal["parent_name"] = "parent_name"):
        """
        Make all names in the tree unique. The strategy parameter determines how this is done.
        Currently, there is just one strategy, which is to prefix the name with the name of the parent
        :param strategy:
        """

        if strategy == "parent_name":
            name_map: dict[str, list[BasicTreeNode]] = {}
            for node in self.iter_all_nodes():
                node._data["orig_name"] = node.name
                name_map.setdefault(node.name, []).append(node)

            for name, nodes in name_map.items():
                if len(nodes) == 1:
                    continue
                parent_level: int = 1

                while len(set((n.name for n in nodes))) != len(nodes):
                    for node in nodes:
                        # in theory this should not happen
                        # if parent_level > node.level:
                        #     raise ValueError(
                        #  f"Cannot make names unique for {name} because it is not unique at level {parent_level}")
                        new_name = node.name
                        p = node
                        for i in range(parent_level):
                            p = p.parent
                            new_name = f"{p._data['orig_name']}_{new_name}"
                        node.name = new_name
                    parent_level += 1

    def join_tree(self, node: "BasicTreeNode") -> "BasicTreeNode":
        """
        Add the children of the given node to this  node.

        :param node: The node whose children are to be added.
        :return: This node.
        """
        for child in node.children:
            node.remove_child(child)
            self.add_child(child)
        return self

    def find_child_by_name(self, name: str, recursive: bool = True) -> Optional["BasicTreeNode"]:
        """
        Find a child node by its name.

        :param name: The name of the node to be found.
        :param recursive: Whether to search recursively into the children's children
                          and so on (default is True).
        :return: The node if found, None otherwise.
        """

        def rec_find_child(node: BasicTreeNode) -> Optional["BasicTreeNode"]:
            if node.name == name:
                return node
            for child in node.children:
                found_node = rec_find_child(child)
                if found_node:
                    return found_node

        if not recursive:
            for node in [self] + self.children:
                if node.name == name:
                    return node
            return None

        return rec_find_child(self)

    def get_leaves(self) -> Generator["BasicTreeNode[T]", None, None]:
        """
        Get all leaf nodes of this node.

        :return: List of all leaf nodes.
        """

        def rec_get_leaves(node: BasicTreeNode) -> Generator["BasicTreeNode", None, None]:
            if not node.children:
                yield node
            else:
                for _child in node.children:
                    yield from rec_get_leaves(_child)

        for child in self.children:
            yield from rec_get_leaves(child)

    def depth(self) -> int:
        """
        Get the depth of this node in the tree. Go down the tree until the deepest leaf is reached.
        :return: The depth of this node.
        """

        def calc_max_depth(node):
            if not node.children:
                return 1
            else:
                return 1 + max((calc_max_depth(child) for child in node.children), default=0)

        return calc_max_depth(self)

    def to_csv(self, file_path: Path, **kwargs):
        """
        Write the hierarchy to a csv file.

        :param file_path: The path to the csv file.
        """
        tree_to_csv(self.as_dict(), file_path, **kwargs)

    def to_sanky_tree(self, file_path: Path, value_key: str = "value"):
        """
        Write the hierarchy to a csv file.
        :param file_path: The path to the csv file.
        :param value_key: The key to use for the value of the links.
        """

        def rec_add_link_row(node, writer: csv.DictWriter, level: int = 0):
            for child in node.children:
                writer.writerow({"source": node.name,
                                 "target": child.name,
                                 "value": getattr(child, value_key, 0),
                                 "target_level": level + 1})
                rec_add_link_row(child, writer, level + 1)

        with file_path.open("w", encoding="utf-8") as fout:
            writer = csv.DictWriter(fout,
                                    fieldnames=["source", "target", "value", "target_level"])
            writer.writeheader()
            rec_add_link_row(self, writer)

    def get_child_names(self) -> list[str]:
        """
        Get the names of all children of this node.
        :return: The names of all children of this node.
        """
        return [child.name for child in self.children]

    def get_num_children(self) -> int:
        """
        Get the number of children of this node.
        :return: The number of children of this node.  == len(self)
        """
        return len(self)

    def collect_all_nodes_at_level(self, level: int) -> list["BasicTreeNode"]:
        """
        Collect all nodes at a given level.

        :param level: The level of the nodes to be collected.
        :return: List of nodes at the given level.
        """
        current = [self]
        for _ in range(level):
            current = [child for node in current for child in node.children]
        return current

    def get_sub_tree(self, max_level: int) -> "BasicTreeNode":
        """
        Get a subtree of the current tree.
        Creates a copy of the tree.

        :param max_level: The maximum level of the subtree.
        :return: The subtree.
        """

        def rec_get_sub_tree(node: BasicTreeNode, max_level: int, **kwargs) -> BasicTreeNode:

            if max_level == 0:
                return BasicTreeNode(node.name, **{k: getattr(node, k) for k in kwargs})
            else:
                sub_tree = BasicTreeNode(node.name)
                for child in node.children:
                    sub_tree.add_child(child.get_sub_tree(max_level - 1))
                return sub_tree

        return rec_get_sub_tree(self, max_level)

    def iter_all_nodes(self) -> Generator["BasicTreeNode", None, None]:
        """
        iterates all nodes of the tree. (Depth-First Search)
        :return: Generator of all nodes.
        """
        for _child in self.children:
            yield from _child.iter_all_nodes()

        yield self

    def level_up(self, levels: int) -> "BasicTreeNode":
        """
        Level up the tree by a given number of levels.
        :param levels: The number of levels to level up.
        :return: Returns the node at the new level.
        """
        current = self
        levels_orig = levels
        while levels > 0:
            if not current.parent:
                raise ValueError(
                    f"Cannot level up from {self.name}/level: {self.level}. Requested level: {levels_orig}")
            current = current.parent
            levels -= 1
        return current

    def copy(self, new_name: Optional[str] = None) -> "BasicTreeNode":
        """
        Clone this node (and all children).
        :return: The cloned node.
        """

        _copy = copy(self)
        if new_name:
            _copy.name = new_name
        return _copy

    def __copy__(self) -> "BasicTreeNode":
        """
        Copy the node.
        :return:
        """

        def reset_parents(node: BasicTreeNode):
            node.generate_id()
            for child in node.children:
                child.parent = node
                reset_parents(child)

        node = deepcopy(self)
        reset_parents(node)
        return node

    def copy_an_merge(self, child_names: list[str], parent_name: Optional[str] = None) -> "BasicTreeNode":
        """
        Copy this node (and all children) for any child_name in the list and make them children of one node.
        :param child_names: The names of the children to be copied.
        :param parent_name: The name of the new root node. Default: self.name
        :return: node that contains all "copies" as children.
        """
        _root = BasicTreeNode(parent_name if parent_name else self.name)
        for child_name in child_names:
            _root.add_child(self.copy(child_name))
        return _root

    @staticmethod
    def from_dict(data: dict):
        """
        Parse a dict and create a tree from it.
        :param data:
        :return:
        """
        children = {}
        if "children" in data:
            children = data["children"]
            del data["children"]
        node = BasicTreeNode(**data)
        for child in children:
            node.add_child(BasicTreeNode.from_dict(child))
        return node

    @staticmethod
    def from_csv(csv_file: Path,
                 node_columns: list[str] = None,
                 merged_first_sub_row: bool = True) -> "BasicTreeNode":
        reader: csv.DictReader = csv.DictReader(csv_file.open("r", encoding="utf-8"))
        if not node_columns:
            node_columns = reader.fieldnames

        root: Optional["BasicTreeNode"] = None
        current_node: Optional["BasicTreeNode"] = None
        for row in reader:
            for index, lvl_name in enumerate(node_columns):
                if row[lvl_name].strip() == "":
                    continue
                if index == 0 and not root:
                    root = BasicTreeNode(row[reader.fieldnames[0]])
                    current_node = root
                    continue
                if current_node.level >= index:
                    current_node = current_node.level_up(current_node.level - index + 1)
                current_node = current_node.add_child(BasicTreeNode(row[lvl_name]))

        return root

    def recursive_apply(self, func: Callable[["BasicTreeNode", ...], Any], *args, **kwargs):
        func(self, *args, **kwargs)

        for child in self.children:
            child.recursive_apply(func, *args, **kwargs)

    # def recursive_apply(self, func: Callable[["BasicTreeNode"], None]):
    #     """
    #     Apply a function recursively to all nodes in the tree.
    #
    #     :param func: The function to be applied.
    #     """
    #     func(self)
    #     for child in self.children:
    #         child.recursive_apply(func)
    def get_child(self, child_index_name: Union[int, str]) -> "BasicTreeNode":
        """
        Get a child node by its index or name.
        Throws KeyError or IndexError if the child is not found.
        :param child_index_name: int index or name of the child node.
        :return: Child node.
        """
        return self[child_index_name]

    def contains(self, item: Union[str, "BasicTreeNode"]) -> bool:
        """
        Check if a given item is a child of this node. calls "item in self"
        :param item: The item or name to check.
        :return: bool, True if the item is a child of this node, False otherwise.
        """
        return item in self

    def clear(self) -> int:
        """
        Remove all children from this node.
        :return: The number of removed children.
        """
        num_children = len(self)
        for _ in range(num_children):
            self.remove_child(0)
        return num_children

    def __len__(self):
        """
        Get the number of children of this node.
        :return: The number of children of this node.
        """
        return len(self.children)

    def __getitem__(self, item: Union[int, str, list[Union[int, str]]]) -> "BasicTreeNode":
        """
        Get a child node by its index or name.
        Throws KeyError or IndexError if the child is not found.
        :param item: int index or name of the child node.
        :return: Child node.
        """
        if isinstance(item, str):
            for child in self.children:
                if child.name == item:
                    return child
            raise KeyError(f"Node {self.name} has no child with name {item}")
        elif isinstance(item, int):
            return self.children[item]
        elif isinstance(item, list):
            next_node = self[item[0]]
            if following := item[1:]:
                return next_node[following]

    def __contains__(self, item: Union[str, "BasicTreeNode"]) -> bool:
        """
        Check if a given item is a child of this node.
        :param item: The item or name to check.
        :return: bool, True if the item is a child of this node, False otherwise.
        """
        # print(item)
        if isinstance(item, BasicTreeNode) or issubclass(type(item), BasicTreeNode):
            item = item.name
        for child_name in self.get_child_names():
            if child_name == item:
                return True
        return False

    def __repr__(self) -> str:
        """
        Get0 the  string representation of the node

        :return: String representation of the object.
        """
        return f"[{self.name} - {len(self.children)} children{' (' + self.parent.name + ')' if self.parent else ''}]"

    def __bool__(self):
        """
        this prevents that 'if not node' is true for a node that has no children
        :return:
        """
        return True


if __name__ == "__main__":
    pass
    # print(BasicTreeNode.from_csv(BASE_DATA_PATH / "temp/tree_create_test/a.csv").as_dict())
    # print(BasicTreeNode.from_csv(BASE_DATA_PATH / "temp/tree_create_test/b.csv").as_dict())
    # print(BasicTreeNode.from_csv(BASE_DATA_PATH / "temp/tree_create_test/b.csv", ["lvl0", "lvl1", "lvl2"]).as_dict())
    # print(BasicTreeNode.from_csv(BASE_DATA_PATH / "temp/tree_create_test/b2.csv", ["lvl0", "lvl1", "lvl2"]).as_dict())
