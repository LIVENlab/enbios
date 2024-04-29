from enbios.models.models import NodeOutput
from enbios.util.output_merge import merge_outputs


def create_output(label_id: int = 0) -> NodeOutput:
    return NodeOutput(unit="kg", magnitude=1, label=f"l_{label_id}")


co = create_output


def test_simple_merge1():
    merge = merge_outputs([
        [co()],
    ])
    assert len(merge[0]) == 1


def test_simple_merge2():
    merge = merge_outputs([
        [co()], [co()]
    ])

    assert len(merge[0]) == 1


def test_simple_merge3():
    merge = merge_outputs([
        [co()], [co(1)]
    ])

    assert len(merge[0]) == 2
    assignment = merge[1]
    assert assignment[0][0] == 0
    assert assignment[1][0] == 1


def test_simple_merge4():
    merge = merge_outputs([
        [co(), co(1)], [co(1)], [co(1), co(2), co(0)]
    ])

    assert len(merge[0]) == 3
    assignment = merge[1]
    assert assignment == [[0, 1], [1], [1, 2, 0]]
