from typing import Optional, Any


class BasicTreeNode:
    """
    The `HierarchyNode` class represents a node in a tree hierarchy. Each node has a name,
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

    def __init__(self, name: str, children: list["BasicTreeNode"] = ()):
        """
        Initialize the HierarchyNode.

        :param name: The name of the node.
        :param children: A list of child nodes (default is an empty list).
        """
        self.name: str = name
        self.children: list[BasicTreeNode] = []
        for child in children:
            self.add_child(child)
        self.parent: Optional[BasicTreeNode] = None



    @property
    def is_leaf(self):
        """
        Check whether this node is a leaf node (i.e., it has no children).

        :return: True if this node is a leaf node, False otherwise.
        """
        return not self.children

    def add_child(self, node: "BasicTreeNode"):
        """
        Add a child node to this node.

        :param node: The node to be added as a child.
        """
        self.children.append(node)
        node.parent = self



    def as_dict(self) -> dict[str, Any]:
        """
        Convert the hierarchy from this node down into a dictionary.

        :return: The dictionary representing the hierarchy.
        """

        def rec_build_dict(node: BasicTreeNode) -> dict:
            return {"children": {
                child.name: rec_build_dict(child)
                for child in node.children
            }}

        return {self.name: rec_build_dict(self)}

    def location(self) -> list["BasicTreeNode"]:
        """
        Get the path from the root to this node.

        :return: List of nodes representing the path from the root to this node.
        """
        nodes: list[BasicTreeNode] = []
        current = self
        while current:
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

    def join_tree(self, node: "BasicTreeNode", remove_from_original_root: bool = True):
        """
        Add the children of a given node to the current node.

        :param node: The node whose children are to be added.
        :param remove_from_original_root: Whether to remove the children from
                                          the given node after adding them
                                          to the current node (default is True).
        """
        for child in node.children:
            self.add_child(child)
        if remove_from_original_root:
            node.children.clear()

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

    def get_leaves(self) -> list["BasicTreeNode"]:
        """
        Get all leaf nodes of this node.

        :return: List of all leaf nodes.
        """

        def rec_get_leaves(node: BasicTreeNode) -> list["BasicTreeNode"]:
            if not node.children:
                return [node]
            else:
                _leaves: list[BasicTreeNode] = []
                for _child in node.children:
                    _leaves.extend(rec_get_leaves(_child))
                return _leaves

        all_leaves: list["BasicTreeNode"] = []
        for child in self.children:
            all_leaves.extend(rec_get_leaves(child))
        return all_leaves

    def depth(self) -> int:
        """
        Get the depth of this node in the tree.

        :return: The depth of this node.
        """
        def calc_max_depth(node):
            if not node.children:
                return 1
            else:
                return 1 + max((calc_max_depth(child) for child in node.children), default=0)

        return calc_max_depth(self)

    def __repr__(self) -> str:
        """
        Get0 the  string representation of the node

        :return: String representation of the object.
        """
        return f"[{self.name} - {len(self.children)} children {'(' + self.parent.name + ')' if self.parent else ''}]"
