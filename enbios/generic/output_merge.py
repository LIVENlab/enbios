from typing import Optional

from enbios.base.unit_registry import ureg
from enbios.base.models import NodeOutput, output_merge_type, LabeledQuantity


def merge_outputs(nodes_outputs: list[list[NodeOutput]]) -> output_merge_type:
    """
    merges the outputs of multiple nodes into a single list of outputs and gives back the indices of assignment
    :param nodes_outputs:
    :return:
        the merged outputs and the indices of assignment (with the original structure of nodes_outputs)
    """
    final_node_outputs: list[LabeledQuantity] = []
    nodes_outputs_assignments: list[list[int]] = []

    def find_node_output_index(given_output: NodeOutput) -> Optional[int]:
        for idx, labeled_q in enumerate(final_node_outputs):
            if given_output.label:
                if labeled_q.label == given_output.label:
                    return idx
            elif not labeled_q.label and labeled_q.quantity.is_compatible_with(
                given_output.unit
            ):
                return idx
        return None

    for node_outputs in nodes_outputs:
        node_outputs_assignment = []
        for output in node_outputs:
            assign_to: Optional[int] = find_node_output_index(output)
            if assign_to is not None:
                final_node_outputs[assign_to].quantity += (
                    ureg(output.unit) * output.magnitude
                )
                node_outputs_assignment.append(assign_to)
            else:
                final_node_outputs.append(
                    LabeledQuantity(
                        quantity=ureg.parse_expression(output.unit) * output.magnitude,
                        label=output.label,
                    )
                )
                node_outputs_assignment.append(len(final_node_outputs) - 1)
        nodes_outputs_assignments.append(node_outputs_assignment)

    node_outputs = [
        NodeOutput(
            unit=str(n.quantity.units), magnitude=n.quantity.magnitude, label=n.label
        )
        for n in final_node_outputs
    ]

    return node_outputs, nodes_outputs_assignments
