import functools
from pathlib import Path
from typing import Optional, Any

from enbios2.experiment.tree_transformer import tree_to_csv
from enbios2.generic.tree.basic_tree import BasicTreeNode

@deprecated(reason="Just use BasicTreeNode with type-parameter")
class HierarchyNode(BasicTreeNode):
    """

    Attributes:
    ----------
    name : str
        The name of the node.

    children : list["HierarchyNode"]
        The child nodes of this node.

    value : float, optional
        The value associated with this node.

    parent : HierarchyNode, optional
        The parent node of this node. None if this node is the root.
    """

    def __init__(self,
                 name: str,
                 children: Optional[list["HierarchyNode"]] = (),
                 value: Optional[float] = None,
                 allow_resetting_value: bool = True):
        """
        Initialize the HierarchyNode.

        :param name: The name of the node.
        :param children: A list of child nodes (default is an empty list).
        :param value: The value associated with the node (default is None).
        :param allow_resetting_value: Whether to allow to set the value multiple times
        """
        # call super constructor
        super().__init__(name, children)
        self._value: float = value
        self.children: list["HierarchyNode"] = []  # Override children type
        for child in children:
            self.add_child(child)
        self.parent: Optional[HierarchyNode] = None
        self.allow_resetting_value = allow_resetting_value

    # def add_child(self, node: "HierarchyNode"):  # Override add_child method
    #     self.children.append(node)
    #     node.parent = self

    @property
    def value(self):
        return self._value

    @value.setter
    def value(self, value: float):
        if self.is_value_set and not self.allow_resetting_value:
            raise ValueError("Value already set")
        self._value = value

    @property
    def is_value_set(self):
        return self.value is not None

    def calc(self, ignore_missing_values: bool = False):
        """
        Calculate the value of the current node. If it has children,
        the value is the sum of the values of the children.

        :param ignore_missing_values: Whether to ignore missing values when
                                      calculating (default is False).
        :return: The calculated value.
        """

        if (not self.children) and (not self.is_value_set) and (not ignore_missing_values):
            raise ValueError(
                f"Hierarchy node '{self.name}': {self.location_names()} must either have children or have its value set")
        if self.value:
            return self.value
        elif self.children:
            self.value = functools.reduce(lambda total, node: total + node.calc(ignore_missing_values),
                                          self.children, 0.0)
            return self.value
        else:  # must be ignore_missing_values = True
            return 0

    def as_dict(self) -> dict[str, Any]:
        """
        Convert the hierarchy from this node down into a dictionary.

        :return: The dictionary representing the hierarchy.
        """

        def rec_build_dict(node: HierarchyNode) -> dict:
            return {
                "name": node.name,
                "children": {
                    child.name: rec_build_dict(child)
                    for child in node.children
                }, "value": node.value}

        return {self.name: rec_build_dict(self)}

    def to_csv(self, file_path: Path, **kwargs):
        """
        Write the hierarchy to a csv file.

        :param file_path: The path to the csv file.
        """
        include_attrs = {"value"}
        if "include_attrs" in kwargs:
            include_attrs.update(kwargs["include_attrs"])
            del kwargs["include_attrs"]
        tree_to_csv(self.as_dict()[self.name], file_path, list(include_attrs), **kwargs)

