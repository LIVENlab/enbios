import csv
from base64 import b64encode
from copy import deepcopy, copy
from pathlib import Path
from typing import (
    Optional,
    Any,
    Literal,
    Union,
    Generator,
    TypeVar,
    Generic,
    Callable,
    Type,
)
from uuid import uuid4

from enbios.generic.enbios2_logging import get_logger
from enbios.generic.files import PathLike

T = TypeVar("T")

logger = get_logger(__name__)


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
    data: T The data of this node.
    """

    def __init__(
        self,
        name: str,
        children: Optional[list[Union["BasicTreeNode[T]", dict[str, Any]]]] = None,
        data: Optional[Union[T, dict]] = None,
        dataclass: Optional[Type] = None,
        data_factory: Optional[Callable[[dict], T]] = None,
        temp_data: Optional[dict[str, Any]] = None,
    ):
        """
        Initialize the HierarchyNode.

        :param name: The name of the node.
        :param children: A list of child nodes (default is an empty list).
        """
        if not name:
            raise ValueError("name must be a non-empty string")

        self._name: str = name
        self.children: list[BasicTreeNode[T]] = []
        if children:
            for child in children:
                if isinstance(child, dict):
                    child = BasicTreeNode.from_dict(
                        child, dataclass=dataclass, data_factory=data_factory
                    )
                self.add_child(child)
        self.parent: Optional[BasicTreeNode[T]] = None
        self.temp_data: dict[str, Any] = temp_data if temp_data else {}
        self._id: bytes = self.generate_id()
        self._data: Optional[Union[dict, T]]
        if data:
            if isinstance(data, dict):
                if dataclass:
                    self._data = dataclass(**data)
                else:
                    self._data = data
            else:
                self._data = data
        elif data_factory:
            self._data = data_factory(self.temp_data)
        else:
            self._data = None

    def generate_id(self) -> bytes:
        self._id = b64encode(uuid4().bytes)
        return self._id

    def set_data(self, data: T):
        self._data = data

    @property
    def data(self) -> T:
        assert self._data is not None, f"data should be set before (node: '{self.name}')"
        return self._data

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
        if self._name == name:
            return
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
        if not isinstance(node, BasicTreeNode):
            raise ValueError(f"Node {node} is of wrong type {type(node)}")
        if node is self or node in self:
            raise ValueError(f"Node {node} is already a child of {self}")
        if node.parent:
            raise ValueError(f"Node {node} already has a parent")
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

    def remove_child(self, node: Union["BasicTreeNode[T]", int, str]) -> "BasicTreeNode":
        """
        Remove a child node from this node.

        :param node: The node to be removed.
        :return: The node that was removed.
        """
        if isinstance(node, int) or isinstance(node, str):
            node = self[node]
        # if not isinstance(node, BasicTreeNode):
        #     raise ValueError(f"Node {node} is of wrong type {type(node)}")
        self.children.remove(node)
        node.parent = None
        return node

    def remove_self(self):
        if not self.parent:
            raise ValueError("Cannot remove root node")
        self.parent.remove_child(self)

    def as_dict(
        self,
        include_data: bool = False,
        remove_empty_childlist: bool = False,
        data_serializer: Optional[Callable[[T], Any]] = None,
    ) -> dict[str, Any]:
        """
        Convert the hierarchy from this node down into a dictionary.

        :return: The dictionary representing the hierarchy.
        """
        result = {
            "name": self.name,
            "children": [
                child.as_dict(
                    include_data,
                    remove_empty_childlist=remove_empty_childlist,
                    data_serializer=data_serializer,
                )
                for child in self.children
            ],
        }
        if len(self) == 0 and remove_empty_childlist:
            del result["children"]
        if include_data and self._data:
            result["data"] = (
                data_serializer(self._data) if data_serializer else self._data
            )
        return result

    @staticmethod
    def from_dict(
        data: dict,
        *,
        dataclass: Optional[Type] = None,
        data_factory: Optional[Callable[[dict], T]] = None,
    ) -> "BasicTreeNode":
        """
        Parse a dict and create a tree from it.
        :param data:
        :param dataclass:
        :param data_factory:
        :return: a node containing the whole tree
        """
        name = data.get("name", "")
        children = data.get("children")
        if dataclass:
            return BasicTreeNode(name, children, data=data, dataclass=dataclass)
        else:
            return BasicTreeNode(
                name,
                children,
                data_factory=data_factory,
                temp_data={
                    k: v for k, v in data.items() if k not in ["name", "children"]
                },
            )

    @staticmethod
    def from_compact_dict(
        input_dict: dict,
        root_name: Optional[str] = "root",
        data_factory: Optional[Callable[[dict], T]] = None,
    ) -> "BasicTreeNode":
        def generate_node(node_info: Union[dict, list]):
            if isinstance(node_info, dict):
                data = node_info.get("data")
                name = node_info.get("name")
                if (
                    not name
                ):  # If 'name' is not a key, then node_info represents children nodes
                    return [
                        BasicTreeNode(
                            name=key,
                            children=generate_node(value),
                            data_factory=data_factory,
                        )
                        for key, value in node_info.items()
                    ]
                else:  # If 'name' is a key, then node_info
                    # represents a single node with optional 'data' and 'children'
                    return [
                        BasicTreeNode(
                            name=name,
                            data=data,
                            children=generate_node(node_info.get("children", [])),
                            data_factory=data_factory,
                        )
                    ]
            elif isinstance(
                node_info, list
            ):  # A list represents a list of children nodes
                nodes = []
                for item in node_info:
                    if isinstance(item, dict):
                        nodes.extend(
                            generate_node(item)
                        )  # A dictionary in a list represents a single node
                        # or multiple children nodes
                    else:  # A string in a list represents a single node
                        nodes.append(BasicTreeNode(name=item, data_factory=data_factory))
                return nodes

        root = BasicTreeNode(
            name=root_name,
            children=generate_node(input_dict),
            data_factory=data_factory,
        )
        return root

    def location(self) -> list["BasicTreeNode[T]"]:
        """
        Get the path from the root to this node.

        :return: List of nodes representing the path from the root to this node.
        """
        nodes: list[BasicTreeNode[T]] = []
        current = self
        while True:
            nodes.append(current)
            if current.parent is None:
                break
            else:
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
            if not current.parent:
                break
            current = current.parent
        return list(reversed(nodes))

    def location_id(self) -> str:
        """
        Get the ids of the nodes in the path from the root to this node.

        :return: List of ids representing the path from the root to this node.
        """
        return "_".join(self.location_names())

    def assert_all_names_unique(self) -> Literal[True]:
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
        return True

    def make_names_unique(self, strategy: Literal["parent_name"] = "parent_name"):
        """
        Make all names in the tree unique.
        The strategy parameter determines how this is done.
        Currently, there is just one strategy,
        which is to prefix the name with the name of the parent
        :param strategy:
        """

        if strategy == "parent_name":
            name_map: dict[str, list[BasicTreeNode[T]]] = {}
            for node in self.iter_all_nodes():
                node.temp_data["orig_name"] = node.name
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
                        #  f"Cannot make names unique for {name}
                        #  because it is not unique at level {parent_level}")
                        new_name = node.name
                        p = node
                        for i in range(parent_level):
                            assert p.parent is not None
                            p = p.parent
                            new_name = f"{p.temp_data['orig_name']}_{new_name}"
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

    def find_subnode_by_name(
        self, name: str, recursive: bool = True
    ) -> Optional["BasicTreeNode[T]"]:
        """
        Find a child node by its name.

        :param name: The name of the node to be found.
        :param recursive: Whether to search recursively into the children's children
                          and so on (default is True).
        :return: The node if found, None otherwise.
        """

        def rec_find_child(node_: BasicTreeNode[T]) -> Optional["BasicTreeNode[T]"]:
            if node_.name == name:
                return node_
            for child in node_.children:
                found_node = rec_find_child(child)
                if found_node:
                    return found_node
            return None

        if not recursive:
            for node in [self] + self.children:
                if node.name == name:
                    return node
            return None

        return rec_find_child(self)

    def iter_leaves(self) -> Generator["BasicTreeNode[T]", None, None]:
        """
        Get all leaf nodes of this node.

        :return: List of all leaf nodes.
        """

        def rec_get_leaves(
            node: BasicTreeNode,
        ) -> Generator["BasicTreeNode", None, None]:
            if not node.children:
                yield node
            else:
                for _child in node.children:
                    yield from rec_get_leaves(_child)

        for child in self.children:
            yield from rec_get_leaves(child)

    @property
    def depth(self) -> int:
        """
        Get the depth of this node in the tree.
        Go down the tree until the deepest leaf is reached.
        :return: The depth of this node.
        """

        def calc_max_depth(node):
            if not node.children:
                return 1
            else:
                return 1 + max(
                    (calc_max_depth(child) for child in node.children), default=0
                )

        return calc_max_depth(self)

    def to_csv(
        self,
        csv_file: PathLike,
        *,
        include_data: Optional[bool] = False,
        data_serializer: Optional[Callable[[T], dict]] = None,
        exclude_data_keys: Optional[list[str]] = None,
        level_names: Optional[list[str]] = None,
        repeat_parent_name: bool = False,
        flat_hierarchy: Optional[bool] = False,
    ):
        if not exclude_data_keys:
            exclude_data_keys = []
        # Calculate max_depth based on root if not provided
        if include_data and not isinstance(self._data, dict) and not data_serializer:
            raise ValueError(
                "If include_data is True, and data not a dict, "
                "data_serializer must be provided"
            )

        if flat_hierarchy and level_names:
            logger.warning("flat hierarchy do not make use of level_names")
        if flat_hierarchy and repeat_parent_name:
            logger.warning("flat hierarchy do not make use of repeat_parent_name")
        _total_level_names = level_names if level_names else []

        def level_name(level: int) -> str:
            if level >= len(_total_level_names):
                _total_level_names.append(f"lvl_{level}")
            return _total_level_names[level]

        def rec_add_node_row(
            node: "BasicTreeNode[T]",
            include_data_: Optional[bool] = False,
            current_level: int = 0,
        ) -> list[dict[str, Union[str, float]]]:
            row = {}
            if include_data_ and node._data:
                row: dict[str, Any] = {}
                if data_serializer:
                    row = data_serializer(node._data)
                elif isinstance(node._data, dict):
                    row = node._data
                else:
                    logger.warning(
                        "Data is not a dict and no data_serializer provided, "
                        "skipping data"
                    )
                for delete in exclude_data_keys:
                    row.pop(delete, None)
            row[level_name(current_level)] = node.name
            row["level"] = node.level
            _sub_rows = []
            for child in node.children:
                _sub_rows.extend(
                    rec_add_node_row(child, include_data_, current_level + 1)
                )
            if _sub_rows:
                if repeat_parent_name:
                    for sub_row in _sub_rows:
                        sub_row[level_name(current_level)] = node.name
            return [row] + _sub_rows

        def rec_add_flat_node_row(
            node: "BasicTreeNode[T]",
            include_data_: Optional[bool] = False,
            current_level: int = 0,
        ) -> list[dict[str, Union[str, float]]]:
            row = {}
            if include_data_ and node._data:
                if data_serializer:
                    row = data_serializer(node._data)
                elif isinstance(node._data, dict):
                    row = node._data
                else:
                    logger.warning(
                        "Data is not a dict and no data_serializer provided, "
                        "skipping data"
                    )
            row["node_name"] = node.name
            row["level"] = node.level
            row["parent_name"] = node.parent.name if node.parent else ""
            rows = [row]
            for child in node.children:
                rows.extend(
                    rec_add_flat_node_row(child, include_data_, current_level + 1)
                )
            return rows

        # Write rows to csv
        if isinstance(csv_file, bytes):
            csv_file = csv_file.decode()

        with Path(csv_file).open("w", newline="") as csvfile:
            if flat_hierarchy:
                rows = rec_add_flat_node_row(self, include_data)
                headers = ["node_name", "level", "parent_name"]
            else:
                rows = rec_add_node_row(self, include_data)
                headers = ["level"] + _total_level_names

            for row in rows:
                for k in row:
                    if k not in headers:
                        headers.append(k)
            writer = csv.DictWriter(csvfile, headers)
            writer.writeheader()
            writer.writerows(rows)

    def to_sanky_tree(self, file_path: Path, value_key: str = "value"):
        """
        Write the hierarchy to a csv file.
        :param file_path: The path to the csv file.
        :param value_key: The key to use for the value of the links.
        """

        def rec_add_link_row(node, writer_: csv.DictWriter, level: int = 0):
            for child in node.children:
                writer_.writerow(
                    {
                        "source": node.name,
                        "target": child.name,
                        "value": getattr(child, value_key, 0),
                        "target_level": level + 1,
                    }
                )
                rec_add_link_row(child, writer_, level + 1)

        with file_path.open("w", encoding="utf-8") as fout:
            writer = csv.DictWriter(
                fout, fieldnames=["source", "target", "value", "target_level"]
            )
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

    def collect_all_nodes_at_level(self, level: int) -> list["BasicTreeNode[T]"]:
        """
        Collect all nodes at a given level.

        :param level: The level of the nodes to be collected.
        :return: List of nodes at the given level.
        """
        current = [self]
        for _ in range(level):
            current = [child for node in current for child in node.children]
        return current

    def get_sub_tree(self, max_level: int) -> "BasicTreeNode[T]":
        """
        Get a subtree of the current tree.
        Creates a copy of the tree.

        :param max_level: The maximum level of the subtree.
        :return: The subtree.
        """

        def rec_get_sub_tree(
            node: BasicTreeNode[T], max_level_: int, **kwargs
        ) -> BasicTreeNode[T]:
            if max_level_ == 0:
                return BasicTreeNode[T](
                    node.name, **{k: getattr(node, k) for k in kwargs}
                )
            else:
                sub_tree = BasicTreeNode[T](node.name)
                for child in node.children:
                    sub_tree.add_child(child.get_sub_tree(max_level_ - 1))
                return sub_tree

        return rec_get_sub_tree(self, max_level)

    def iter_all_nodes(self) -> Generator["BasicTreeNode[T]", None, None]:
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
                    f"Cannot level up from {self.name}/level: {self.level}. "
                    f"Requested level: {levels_orig}"
                )
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

        def reset_parents(node_: BasicTreeNode):
            node_.generate_id()
            for child in node_.children:
                child.parent = node_
                reset_parents(child)

        node = deepcopy(self)
        reset_parents(node)
        return node

    def copy_an_merge(
        self, child_names: list[str], parent_name: Optional[str] = None
    ) -> "BasicTreeNode[T]":
        """
        Copy this node and all children given in  child_names.
        :param child_names: The names of the children to be copied.
        :param parent_name: The name of the new root node. Default: self.name
        :return: node that contains all "copies" as children.
        """
        _root = BasicTreeNode[T](parent_name if parent_name else self.name)
        for child_name in child_names:
            _root.add_child(self.copy(child_name))
        return _root

    @staticmethod
    def from_csv(
        csv_file: Path,
        node_columns: Optional[list[str]] = None,
        merged_first_sub_row: Optional[bool] = True,
    ) -> "BasicTreeNode[T]":
        reader: csv.DictReader = csv.DictReader(csv_file.open("r", encoding="utf-8"))
        if not node_columns:
            if reader.fieldnames:
                node_columns = list(reader.fieldnames)
            else:
                raise ValueError("No fieldnames in csv.")

        root: Optional["BasicTreeNode[T]"] = None
        current_node: Optional["BasicTreeNode[T]"] = None
        for row in reader:
            for index, lvl_name in enumerate(node_columns):
                if not row[lvl_name] or (clean_value := row[lvl_name].strip()) == "":
                    continue
                if index == 0 and not root:
                    root = BasicTreeNode[T](clean_value)
                    current_node = root
                    continue
                if current_node:
                    if current_node.level < index - 1:
                        raise ValueError(
                            f"There are nodes missing inbetween "
                            f"{current_node.name} and {clean_value}"
                        )
                    elif current_node.level >= index:
                        current_node = current_node.level_up(
                            current_node.level - index + 1
                        )
                    current_node = current_node.add_child(BasicTreeNode(clean_value))

        if not root:
            raise ValueError("Could not generate tree from csv.")
        else:
            return root

    def recursive_apply_eager(
        self,
        func: Callable[["BasicTreeNode", Optional[Any]], Any],
        depth_first: bool = False,
        *args,
        **kwargs,
    ):
        if not depth_first:
            func(self, *args, **kwargs)

        for child in self.children:
            child.recursive_apply_eager(func, depth_first, *args, **kwargs)

        if depth_first:
            func(self, *args, **kwargs)

    def recursive_apply_lazy(
        self,
        func: Callable[["BasicTreeNode", Any], Any],
        depth_first: bool = False,
        *args,
        **kwargs,
    ) -> Generator[Any, Any, Any]:
        if not depth_first:
            yield func(self, *args, **kwargs)

        for child in self.children:
            for res in child.recursive_apply_lazy(func, depth_first, *args, **kwargs):
                yield res

        if depth_first:
            yield func(self, *args, **kwargs)

    def recursive_apply(
        self,
        func: Callable[["BasicTreeNode", Optional[Any]], Optional[Any]],
        depth_first: bool = False,
        lazy: bool = False,
        *args,
        **kwargs,
    ):
        """
        note that only when lazy is set to True, the function will return a generator.
        :param func: the function to apply to all nodes
        :param depth_first:
        :param lazy: does not evaluate the function until the generator is iterated.
        :param args:
        :param kwargs:
        :return:
        """
        if lazy:
            return self.recursive_apply_lazy(func, depth_first, *args, **kwargs)
        else:
            self.recursive_apply_eager(func, depth_first, *args, **kwargs)

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

    @property
    def id(self):
        return self._id

    def __len__(self):
        """
        Get the number of children of this node.
        :return: The number of children of this node.
        """
        return len(self.children)

    def __getitem__(
        self, item: Union[int, str, list[Union[int, str]]]
    ) -> "BasicTreeNode":
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
            if not item:
                return self
            next_node = self[item[0]]
            return next_node[item[1:]]

    def __contains__(self, item: Union[str, "BasicTreeNode"]) -> bool:
        """
        Check if a given item is a child of this node.
        :param item: The item or name to check.
        :return: bool, True if the item is a child of this node, False otherwise.
        """
        # print(item)
        if isinstance(item, BasicTreeNode) or issubclass(type(item), BasicTreeNode):
            assert not isinstance(item, str)
            item = item.name
        for child_name in self.get_child_names():
            if child_name == item:
                return True
        return False

    def __repr__(self) -> str:
        """
        Get the  string representation of the node

        :return: String representation of the object.
        """
        children_str = f"{len(self.children)} {'children' if len(self) != 1 else 'child'}"
        # this instead of self.parent, which in some cases (2 children with same 'name') would weirdly not exists
        parent_str = (
            f"{' (' + self.parent.name + ')' if getattr(self, 'parent', None) else ''}"
        )
        return f"[{self.name} - {children_str}{parent_str}]"

    def info(self) -> str:
        if not self._data:
            return repr(self)
        else:
            return repr(self) + f"\n{repr(self._data)}"

    def __bool__(self):
        """
        this prevents that 'if not node' is true for a node that has no children
        :return:
        """
        return True

    def to_mermaid_str(
        self,
        arrow_style="---",
        markdown_context: bool = False,
        leaves_style: str = "fill:#ccc",
    ) -> str:
        """
        Get a string representation of the tree in mermaid format.
        :return: String representation of the tree in mermaid format.
        """

        def safe_name(node: BasicTreeNode):
            return node.name.replace(" ", "_")

        def node_repr(node: BasicTreeNode):
            return f"{safe_name(node)}[{node.name}]"

        def style(node: BasicTreeNode):
            if leaves_style and not node.children:
                return ":::leaf"
            else:
                return ""

        def mm_connection(node: BasicTreeNode[T], _=None):
            return (
                f"{safe_name(node.parent)} {arrow_style} {node_repr(node)}{style(node)}"
                if node.parent
                else ""
            )

        lines = "\n".join([con for con in self.recursive_apply(mm_connection) if con])
        if leaves_style:
            lines += f"\nclassDef leaf {leaves_style}"
        if markdown_context:
            return f"```mermaid\ngraph BT;\n{lines}\n```"
        else:
            return f"graph BT;\n{lines}"
