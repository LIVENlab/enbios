import functools
from typing import Optional, Any


class HierarchyNode:

    def __init__(self, name: str, children: Optional[list["HierarchyNode"]] = (), value: Optional[float] = None):
        self.name: str = name
        self.children: list[HierarchyNode] = []
        for child in children:
            self.add_child(child)
        self.value: float = value
        self.parent: Optional[HierarchyNode] = None

    def __setattr__(self, key, value):
        if key == "value":
            super().__setattr__('value', value)
        else:
            super().__setattr__(key, value)

    @property
    def value_set(self):
        return self.value is not None

    def is_leaf(self):
        return not self.children

    def add_child(self, node: "HierarchyNode"):
        self.children.append(node)
        node.parent = self

    def calc(self, ignore_missing_values: bool = False):
        if (not self.children) and (not self.value_set) and (not ignore_missing_values):
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

        def rec_build_dict(node: HierarchyNode) -> dict:
            return {"children": {
                child.name: rec_build_dict(child)
                for child in node.children
            }, "value": node.value}

        return {self.name: rec_build_dict(self)}

    def location(self) -> list["HierarchyNode"]:
        nodes: list[HierarchyNode] = []
        current = self
        while current:
            nodes.append(current)
            current = current.parent
        return list(reversed(nodes))

    def location_names(self) -> list[str]:
        nodes: list[str] = []
        current = self
        while current:
            nodes.append(current.name)
            current = current.parent
        return list(reversed(nodes))

    def assert_all_names_unique(self):
        all_names: set = set()

        def rec_assert_unique_name(node: HierarchyNode):
            if node.name in all_names:
                print(node.name)
            assert node.name not in all_names, f"{node.name} seems to appear twice"
            all_names.add(node.name)
            for child in node.children:
                rec_assert_unique_name(child)

        rec_assert_unique_name(self)

    def join_tree(self, node: "HierarchyNode", remove_from_original_root: bool = True):
        for child in node.children:
            self.add_child(child)
        if remove_from_original_root:
            node.children.clear()

    def find_child_by_name(self, name: str, recursive: bool = True) -> Optional["HierarchyNode"]:

        def rec_find_child(node: HierarchyNode) -> Optional["HierarchyNode"]:
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

    def get_leaves(self) -> list["HierarchyNode"]:

        def rec_get_leaves(node: HierarchyNode) -> list["HierarchyNode"]:
            if not node.children:
                return [node]
            else:
                _leaves: list[HierarchyNode] = []
                for _child in node.children:
                    _leaves.extend(rec_get_leaves(_child))
                return _leaves

        all_leaves: list["HierarchyNode"] = []
        for child in self.children:
            all_leaves.extend(rec_get_leaves(child))
        return all_leaves

    def __repr__(self) -> str:
        return f"[{self.name} - {len(self.children)} children {'(' + self.parent.name + ')' if self.parent else ''}]"
