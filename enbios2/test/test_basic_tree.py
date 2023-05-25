from copy import copy

import pytest

from enbios2.generic.tree.basic_tree import BasicTreeNode


@pytest.fixture
def csv_file_path(tmp_path):
    return tmp_path / "test.csv"


@pytest.fixture
def tree_fixture():
    # create root node
    root = BasicTreeNode("root")
    # create child nodes
    child1 = BasicTreeNode("child1")
    child2 = BasicTreeNode("child2")
    # create duplicate named nodes
    dupe1 = BasicTreeNode("dupe")
    dupe2 = BasicTreeNode("dupe")

    # add child nodes to root
    root.add_child(child1)
    root.add_child(child2)

    # add duplicate named nodes to child nodes
    child1.add_child(dupe1)
    child2.add_child(dupe2)

    return root


def test_node_init():
    node = BasicTreeNode("node1")
    assert node.name == "node1"
    assert node.children == []
    assert node.parent is None
    assert node._data == {}


def test_node_add_child():
    parent_node = BasicTreeNode("parent")
    child_node = BasicTreeNode("child")
    parent_node.add_child(child_node)
    assert child_node in parent_node.children
    assert child_node.parent == parent_node


def test_node_add_child_raises():
    node1 = BasicTreeNode("node1")
    with pytest.raises(ValueError):
        node1.add_child(node1)


def test_is_leaf():
    node = BasicTreeNode("node1")
    assert node.is_leaf is True
    child_node = BasicTreeNode("child")
    node.add_child(child_node)
    assert node.is_leaf is False


def test_level():
    node1 = BasicTreeNode("node1")
    node2 = BasicTreeNode("node2")
    node1.add_child(node2)
    assert node1.level == 0
    assert node2.level == 1


def test_as_dict(tree_fixture):
    tree = tree_fixture
    tree_dict = tree.as_dict()

    # check if root name is correct
    assert tree_dict['name'] == "root"

    # check if children are correct
    assert set(tree_dict['children'].keys()) == {"child1", "child2"}

    # check if children's children are correct
    assert tree_dict['children']['child1']['children'].keys() == {"dupe"}
    assert tree_dict['children']['child2']['children'].keys() == {"dupe"}


def test_location():
    node1 = BasicTreeNode("node1")
    node2 = BasicTreeNode("node2")
    node1.add_child(node2)
    assert node1.location() == [node1]
    assert node2.location() == [node1, node2]


def test_location_names():
    node1 = BasicTreeNode("node1")
    node2 = BasicTreeNode("node2")
    node1.add_child(node2)
    assert node1.location_names() == ["node1"]
    assert node2.location_names() == ["node1", "node2"]


def test_location_id():
    node1 = BasicTreeNode("node1")
    node2 = BasicTreeNode("node2")
    node3 = BasicTreeNode("node3")
    node1.add_child(node2)
    node2.add_child(node3)
    assert node3.location_id() == "node1_node2_node3"


def test_assert_all_names_unique():
    node1 = BasicTreeNode("node1")
    node2 = BasicTreeNode("node2")
    node3 = BasicTreeNode("node3")
    node1.add_child(node2)
    node2.add_child(node3)
    node1.assert_all_names_unique()

    node4 = BasicTreeNode("node1")
    node1.add_child(node4)
    with pytest.raises(AssertionError):
        node1.assert_all_names_unique()


def test_make_names_unique(tree_fixture):
    tree = tree_fixture

    # initially the tree can have duplicate names
    with pytest.raises(AssertionError):
        tree.assert_all_names_unique()

    # make names unique
    try:
        tree.make_names_unique(strategy="parent_name")
    except ValueError as ve:
        print(f"Caught ValueError: {str(ve)}")
        assert False, "Could not make names unique"

    # now all names in the tree should be unique
    tree.assert_all_names_unique()


def test_join_tree():
    node1 = BasicTreeNode("node1")
    node2 = BasicTreeNode("node2")
    node3 = BasicTreeNode("node3")
    node2.add_child(node3)
    node1.join_tree(node2)
    assert node3 in node1.children
    assert node3.parent == node1
    assert node2.children == []


def test_find_child_by_name():
    node1 = BasicTreeNode("node1")
    node2 = BasicTreeNode("node2")
    node3 = BasicTreeNode("node3")
    node1.add_child(node2)
    node2.add_child(node3)
    assert node1.find_child_by_name("node3") == node3
    assert node1.find_child_by_name("node4") is None
    assert node1.find_child_by_name("node3", recursive=False) is None
    assert node1.find_child_by_name("node2", recursive=False) == node2


def test_get_leaves():
    node1 = BasicTreeNode("node1")
    node2 = BasicTreeNode("node2")
    node3 = BasicTreeNode("node3")
    node1.add_child(node2)
    node2.add_child(node3)
    leaves = list(node1.get_leaves())
    assert leaves == [node3]


def test_depth():
    node1 = BasicTreeNode("node1")
    node2 = BasicTreeNode("node2")
    node3 = BasicTreeNode("node3")
    node1.add_child(node2)
    node2.add_child(node3)
    assert node1.depth() == 3


@pytest.fixture
def csv_file_path(tmp_path):
    return tmp_path / "test.csv"


def test_to_csv(csv_file_path):
    node1 = BasicTreeNode("node1")
    node2 = BasicTreeNode("node2")
    node1.add_child(node2)
    node1.to_csv(csv_file_path)
    assert csv_file_path.exists()


def test_to_sanky_tree(csv_file_path):
    node1 = BasicTreeNode("node1")
    node2 = BasicTreeNode("node2")
    node1.add_child(node2)
    node1.to_sanky_tree(csv_file_path)
    assert csv_file_path.exists()


def test_get_child_names():
    node1 = BasicTreeNode("node1")
    node2 = BasicTreeNode("node2")
    node1.add_child(node2)
    assert node1.get_child_names() == ["node2"]


def test_get_num_children():
    node1 = BasicTreeNode("node1")
    node2 = BasicTreeNode("node2")
    node1.add_child(node2)
    assert node1.get_num_children() == 1


def test_collect_all_nodes_at_level():
    root = BasicTreeNode("root", children=[
        BasicTreeNode("child1", children=[
            BasicTreeNode("grandchild1")
        ]),
        BasicTreeNode("child2")
    ])

    assert len(root.collect_all_nodes_at_level(0)) == 1
    assert len(root.collect_all_nodes_at_level(1)) == 2
    assert len(root.collect_all_nodes_at_level(2)) == 1


def test_get_sub_tree():
    root = BasicTreeNode("root", children=[
        BasicTreeNode("child1", children=[
            BasicTreeNode("grandchild1")
        ]),
        BasicTreeNode("child2")
    ])
    sub_tree = root.get_sub_tree(1)

    assert sub_tree.name == root.name
    assert len(sub_tree.children) == 2
    assert all(child.is_leaf for child in sub_tree.children)


def test_iter_all_nodes():
    root = BasicTreeNode("root", children=[
        BasicTreeNode("child1", children=[
            BasicTreeNode("grandchild1")
        ]),
        BasicTreeNode("child2")
    ])

    all_nodes = list(root.iter_all_nodes())
    assert len(all_nodes) == 4


def test_from_csv(csv_file_path):
    csv_content = """level1,level2,level3
root,child1,grandchild1
    ,child2,
"""
    with open(csv_file_path, "w") as csv_file:
        csv_file.write(csv_content)

    root = BasicTreeNode.from_csv(csv_file_path)

    assert root.name == "root"
    assert len(root.children) == 2
    assert root.children[0].name == "child1"
    assert len(root.children[0].children) == 1
    assert root.children[1].name == "child2"
    assert len(root.children[1].children) == 0


def test_get_child():
    root = BasicTreeNode("root", children=[
        BasicTreeNode("child1"),
        BasicTreeNode("child2")
    ])

    assert root.get_child(0).name == "child1"
    assert root.get_child("child2").name == "child2"
    with pytest.raises(IndexError):
        root.get_child(2)
    with pytest.raises(KeyError):
        root.get_child("non_existent")


def test_contains():
    root = BasicTreeNode("root", children=[
        BasicTreeNode("child1"),
        BasicTreeNode("child2")
    ])
    assert root.contains("child1")
    assert not root.contains("non_existent")

    assert "child1" in root
    assert root.contains("child1")
    assert root.children[0] in root
    assert "non_existent" not in root


def test_len():
    root = BasicTreeNode("root", children=[
        BasicTreeNode("child1"),
        BasicTreeNode("child2")
    ])
    assert len(root) == 2


def test_getitem():
    root = BasicTreeNode("root", children=[
        BasicTreeNode("child1"),
        BasicTreeNode("child2")
    ])

    assert root[0].name == "child1"
    assert root["child2"].name == "child2"
    with pytest.raises(IndexError):
        _ = root[2]
    with pytest.raises(KeyError):
        _ = root["non_existent"]


def test_repr():
    root = BasicTreeNode("root", children=[
        BasicTreeNode("child1"),
        BasicTreeNode("child2")
    ])
    assert repr(root) == "[root - 2 children]"
    assert repr(root.children[0]) == "[child1 - 0 children (root)]"


def test_bool():
    root = BasicTreeNode("root")
    assert bool(root) is True


def test_copy():
    node = BasicTreeNode("root")
    copy = node.copy()
    assert copy != node
    assert copy.name == node.name
    assert copy._id != node._id
    assert copy.children == node.children


def test_recursive_apply():
    node = BasicTreeNode("root")
    child1 = BasicTreeNode("child1")
    node.add_child(child1)
    child2 = BasicTreeNode("child2")
    node.add_child(child2)

    node.recursive_apply(lambda n: setattr(n, 'name', n.name.upper()))
    assert node.name == "ROOT"
    assert child1.name == "CHILD1"
    assert child2.name == "CHILD2"


def test_copy_and_merge():
    node = BasicTreeNode("root")
    child_names = ["child1", "child2"]
    parent_name = "new_root"

    new_root = node.copy_an_merge(child_names, parent_name)
    assert new_root.name == parent_name
    assert len(new_root.children) == 2
    assert [child.name for child in new_root.children] == child_names


def test_remove_child():
    node = BasicTreeNode("root")
    child1 = node.add_child(BasicTreeNode("child1"))
    node.remove_child(child1)
    assert len(node.children) == 0
    assert child1.parent is None
    node.add_child(child1)
    assert child1.parent == node
    node.remove_child(0)
    assert len(node.children) == 0
    assert child1.parent is None
    with pytest.raises(IndexError):
        node.remove_child(0)


def test_clear():
    node = BasicTreeNode("root")
    child1 = node.add_child(BasicTreeNode("child1"))
    node.add_child(BasicTreeNode("child2"))
    node.add_child(BasicTreeNode("child3"))
    removed = node.clear()
    assert removed == 3
    assert len(node) == 0
    assert child1.parent is None


def test_level_up():
    node = BasicTreeNode("root")
    child1 = node.add_child(BasicTreeNode("child1"))
    child2 = child1.add_child(BasicTreeNode("child2"))
    assert child1.level_up(0) == child1
    assert child1.level_up(1) == node
    assert child2.level_up(2) == node
    with pytest.raises(ValueError):
        child1.level_up(2)


def test_make_names_unique():
    node = BasicTreeNode("root")
    child1 = node.add_child(BasicTreeNode("child1"))
    child1.add_child(BasicTreeNode("child1"))
    child2 = node.add_child(BasicTreeNode("child2"))
    child2.add_child(BasicTreeNode("child1"))
    child2.add_child(BasicTreeNode("child2"))
    node.make_names_unique()
    assert node.get_child_names() == ["root_child1", "root_child2"]
    assert child1.get_child_names() == ["child1_child1"]
    assert child2.get_child_names() == ["child2_child1", "child2_child2"]


def test_copy():
    root_ = BasicTreeNode("root")
    other = copy(root_)
    assert root_ is not other
    assert root_ != other
    assert root_._id != other._id
    assert root_.name == other.name

    child = root_.add_child(BasicTreeNode("child"))
    other = root_.copy("copy_node")
    assert other.name == "copy_node"
    assert child == root_.children[0]
    assert child is not other.children[0]
    assert root_.children[0].parent == root_
    assert other.children[0].parent == other


def test_set_name():
    node = BasicTreeNode("root")
    child1 = node.add_child(BasicTreeNode("child1"))
    child2 = node.add_child(BasicTreeNode("child2"))
    child2.name = "x"
    assert child2.name == "x"
    with pytest.raises(ValueError):
        child2.name = "child1"
