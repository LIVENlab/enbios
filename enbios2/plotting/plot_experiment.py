from pathlib import Path
from typing import Optional

from enbios2.base.experiment import Experiment
from enbios2.generic.enbios2_logging import get_logger

logger = get_logger(__file__)


def results_to_plot(exp: Experiment,
                    method_: str,
                    *,
                    scenario_name: Optional[str] = None,
                    image_file: Optional[Path] = None,
                    show: bool = False):
    try:
        import plotly.graph_objects as go
    except ImportError:
        logger.error("plotly not installed. Run 'pip install plotly'")
        return

    scenario = exp.get_scenario(scenario_name) if scenario_name else exp.scenarios[0]
    all_nodes = list(scenario.result_tree.iter_all_nodes())
    node_labels = [node.name for node in all_nodes]

    source = []
    target = []
    value = []
    for index_, node in enumerate(all_nodes):
        for child in node.children:
            source.append(index_)
            target.append(all_nodes.index(child))
            value.append(child.data[method_])

    fig = go.Figure(data=[go.Sankey(
        node=dict(
            pad=15,
            thickness=20,
            line=dict(color="black", width=0.5),
            label=node_labels,
            color="blue"
        ),
        link=dict(
            source=source,
            target=target,
            value=value
        ))])

    fig.update_layout(title_text=f"{scenario.alias} / {'_'.join(method_)}", font_size=10)
    if show:
        fig.show()
    if image_file:
        fig.write_image(image_file.as_posix(), width=1800, height=1600)
