from pathlib import Path
from typing import Optional, Union

import numpy as np
from matplotlib import pyplot as plt
from matplotlib.figure import Figure
from matplotlib.projections import PolarAxes
from pandas import DataFrame

from enbios2.base.result_select import ResultsSelector
from enbios2.base.experiment import Experiment
from enbios2.generic.enbios2_logging import get_logger
from enbios2.generic.files import PathLike
from enbios2.generic.tree.basic_tree import BasicTreeNode
from enbios2.models.experiment_models import ScenarioResultNodeData

logger = get_logger(__file__)


def bar_plot(
    experiment: Union[Experiment, ResultsSelector],
    scenarios: Optional[list[str]] = None,
    methods: Optional[list[str]] = None,
    short_method_names: bool = True,
    image_file: Optional[PathLike] = None,
) -> Figure:
    rs = ResultsSelector.get_result_selector(experiment, scenarios, methods)

    n_rows = len(rs.methods)
    n_cols = 1

    fig, axs = plt.subplots(
        n_rows, n_cols, figsize=(10, 5 * n_rows)
    )  # Assuming each subplot has a height of 5, adjust as needed

    # Check if there's only one subplot to handle the indexing appropriately
    if n_rows == 1:
        axs = [axs]

    for idx, method in enumerate(rs.methods):
        cmap = plt.colormaps.get_cmap("tab10")
        colors = cmap(np.linspace(0, 1, len(rs.scenarios)))
        rs.base_df.plot(kind="bar", x="scenario", y=method, ax=axs[idx], color=colors)
        axs[idx].set_ylabel(rs.method_label_names(short_method_names)[idx], fontsize=8)
        axs[idx].legend().set_visible(False)

    plt.tight_layout()
    if image_file:
        fig.write_image(Path(image_file).as_posix())
    return fig  # Return the figure object


def stacked_bar_plot(
    experiment: Union[Experiment, ResultsSelector],
    scenarios: Optional[list[str]] = None,
    methods: Optional[list[str]] = None,
    level: int = 1,
    short_method_names: bool = True,
    aliases: Optional[list[str]] = None,
    image_file: Optional[PathLike] = None,
) -> Figure:
    rs = ResultsSelector.get_result_selector(experiment, scenarios, methods)
    experiment = rs.experiment

    if level >= experiment.hierarchy_root.depth:
        logger.warning(
            f"Level {level} is higher or equal (>=) than the depth of the hierarchy "
            f"({experiment.hierarchy_root.depth}). "
            f"Limiting to {experiment.hierarchy_root.depth - 1}"
        )
        level = experiment.hierarchy_root.depth - 1
    # Define the number of rows and columns for the subplots
    n_rows = len(rs.methods)
    n_cols = 1

    # Create a new figure with a defined size (adjust as needed)
    # Explicitly create a Figure object
    fig, axs = plt.subplots(
        n_rows, n_cols, figsize=(10, 5 * n_rows)
    )  # Assuming each subplot has a height of 5, adjust as needed

    # Check if there's only one subplot to handle the indexing appropriately
    if n_rows == 1:
        axs = [axs]

    for idx, method in enumerate(rs.methods):
        ax = axs[idx]

        # Create the bar plot using the specific Axes object
        nodes: list[BasicTreeNode[ScenarioResultNodeData]] = []
        if aliases:
            for alias in aliases:
                node = experiment.hierarchy_root.find_subnode_by_name(alias)
                if not node:
                    raise ValueError(f"Alias {alias} not found in hierarchy")
                nodes.append(node)
        else:
            nodes = experiment.hierarchy_root.collect_all_nodes_at_level(level)

        node_names = [node.name for node in nodes]
        df = rs.collect_tech_results(node_names)
        df_pivot = df.pivot(index="scenario", columns="tech", values=method)
        df_pivot.plot(kind="bar", stacked=True, ax=ax)
        ax.set_ylabel(rs.method_label_names(short_method_names)[idx], fontsize=8)

    plt.tight_layout()
    if image_file:
        fig.write_image(Path(image_file).as_posix())
    return fig  # Return the figure object


def star_plot(
    experiment: Union[Experiment, ResultsSelector],
    scenarios: Optional[list[str]] = None,
    methods: Optional[list[str]] = None,
    *,
    fill: bool = True,
    r_ticks=(0.2, 0.4, 0.6, 0.8, 1.0),
    show_r_ticks: bool = True,
    show_grid: bool = True,
    col: int = 4,
    row: Optional[int] = None,
    short_method_names: bool = True,
    show_labels: bool = True,
    image_file: Optional[PathLike] = None,
) -> Figure:
    rs = ResultsSelector.get_result_selector(experiment, scenarios, methods)
    df = rs.normalized_df()

    labels = rs.method_label_names(short_method_names, False)

    if row is None:
        row = int(np.ceil(len(rs.scenarios) / col))
    # if only one row. limit the figure size to the number of scenarios
    if row == 1:
        col = len(rs.scenarios)
    # Create figure and axes
    fig, axs = plt.subplots(
        row, col, figsize=(6 * col, 6 * row), subplot_kw=dict(polar=True)
    )
    if row == 1 and col == 1:
        axs = [[axs]]  # to handle indexing later on
    elif row == 1:
        axs = [axs]
    elif col == 1:
        axs = [[a] for a in axs]

    # remove empty axes
    last_row_num_axes = len(rs.scenarios) % col
    # print(last_row_num_axes)
    if last_row_num_axes != 0:
        for c in range(col - last_row_num_axes):
            # print(c,"--")
            axs[-1][-1 - c].remove()
            # pass

    all_done = False
    for r in range(row):
        for c in range(col):
            ax: PolarAxes = axs[r][c]
            scenario_name: str = rs.scenarios[r * col + c]
            values = df.loc[df["scenario"] == scenario_name].values.tolist()[0][1:]
            values.append(values[0])

            angles = np.linspace(0, 2 * np.pi, len(values) - 1, endpoint=False).tolist()
            angles.append(0)
            # Plot data

            if fill:
                ax.fill(angles, values, color="blue", alpha=0.1)
            else:
                ax.plot(angles, values)

            # Fix axis to close the circle
            ax.set_theta_offset(np.pi / 2)
            ax.set_theta_direction(-1)
            ax.set_rlabel_position(30)
            ax.set_rticks(list(r_ticks))
            if not show_r_ticks:
                ax.set_yticklabels([])
            if not show_grid:
                ax.grid(False)
            angles.pop()
            ax.set_xticks(angles)
            if r == 0 and c == 0 and show_labels:
                ax.set_xticklabels(labels)
            else:
                ax.set_xticklabels([""] * len(angles))
            ax.set_rmax(1)
            ax.title.set_text(scenario_name)

            if r * col + c == len(rs.scenarios) - 1:
                all_done = True
                break
        if all_done:
            break

    plt.tight_layout()
    if image_file:
        fig.write_image(Path(image_file).as_posix())
    return fig


def single_star_plot(
    experiment: Union[Experiment, ResultsSelector],
    scenarios: Optional[list[str]] = None,
    methods: Optional[list[str]] = None,
    *,
    r_ticks=(0.2, 0.4, 0.6, 0.8, 1.0),
    show_r_ticks: bool = True,
    show_grid: bool = True,
    col: int = 4,
    row: Optional[int] = None,
    short_method_names: bool = True,
    show_labels: bool = True,
    image_file: Optional[PathLike] = None,
) -> Figure:
    """
    plots multiple scenarios into one star plot
    :param experiment:
    :param scenarios:
    :param methods:
    :param r_ticks:
    :param show_r_ticks:
    :param show_grid:
    :param col:
    :param row:
    :param short_method_names:
    :param show_labels:
    :param image_file:
    :return:
    """
    rs = ResultsSelector.get_result_selector(experiment, scenarios, methods)
    df = rs.normalized_df()

    labels = rs.method_label_names(short_method_names, False)

    # Create figure and axes
    fig, ax = plt.subplots(figsize=(6, 6), subplot_kw=dict(polar=True))

    cmap = plt.colormaps.get_cmap("tab10")
    for idx, scenario_name in enumerate(scenarios):
        values = df.loc[df["scenario"] == scenario_name].values.tolist()[0][1:]
        values.append(values[0])

        angles = np.linspace(0, 2 * np.pi, len(values) - 1, endpoint=False).tolist()
        angles.append(0)

        _color = list(cmap(idx)[:3]) + [0.3]

        ax.plot(angles, values)

    # Fix axis to close the circle
    ax.set_theta_offset(np.pi / 2)
    ax.set_theta_direction(-1)
    ax.set_rlabel_position(30)
    ax.set_rticks(list(r_ticks))

    if not show_grid:
        ax.grid(False)
    angles.pop()
    ax.set_xticks(angles)
    if show_labels:
        ax.set_xticklabels(labels)
    else:
        ax.set_xticklabels([""] * len(labels))
    ax.set_rmax(1)


def plot_heatmap(
    experiment: Union[Experiment, ResultsSelector],
    scenarios: Optional[list[str]] = None,
    methods: Optional[list[str]] = None,
    special_df: Optional[DataFrame] = None,
    image_file: Optional[PathLike] = None,
) -> Figure:
    rs = ResultsSelector.get_result_selector(experiment, scenarios, methods)
    if special_df is not None:
        rs.check_special_df(special_df)
        df = special_df
    else:
        df = rs.normalized_df()

    df = df.set_index("scenario").transpose()

    fig, ax = plt.subplots(figsize=(len(rs.scenarios) * 1.5, len(rs.methods) * 1.5))
    im = ax.imshow(df, cmap="summer")

    labels = rs.method_label_names(include_unit=False)
    ax.set_xticks(np.arange(len(rs.scenarios)), labels=rs.scenarios)
    ax.set_yticks(np.arange(len(rs.methods)), labels=labels)

    # Rotate the tick labels and set their alignment.
    plt.setp(ax.get_xticklabels(), rotation=45, ha="right", rotation_mode="anchor")
    ax.figure.colorbar(im, ax=ax, **{})
    for i in range(len(rs.scenarios)):
        for j in range(len(rs.methods)):
            ax.text(
                i,
                j,
                "%.2f" % df[df.columns[i]][j],
                ha="center",
                va="center",
                color="black",
            )

    fig.tight_layout()
    if image_file:
        fig.write_image(Path(image_file).as_posix())
    return fig


def plot_sankey(
    exp: Experiment,
    scenario_name: str,
    method_: str,
    *,
    image_file: Optional[PathLike] = None,
) -> Figure:
    try:  # type: ignore
        import plotly.graph_objects as go
    except ImportError:
        logger.error("plotly not installed. Run 'pip install plotly'")
        return

    scenario = exp.get_scenario(scenario_name)
    all_nodes = list(scenario.result_tree.iter_all_nodes())
    node_labels = [node.name for node in all_nodes]

    source = []
    target = []
    value = []
    for index_, node in enumerate(all_nodes):
        _: BasicTreeNode[ScenarioResultNodeData] = node
        for child in node.children:
            source.append(index_)
            target.append(all_nodes.index(child))
            value.append(child.data.results[method_])

    fig = go.Figure(
        data=[
            go.Sankey(
                node=dict(
                    pad=15,
                    thickness=20,
                    line=dict(color="black", width=0.5),
                    label=node_labels,
                    color="blue",
                ),
                link=dict(source=source, target=target, value=value),
            )
        ]
    )

    fig.update_layout(title_text=f"{scenario.alias} / {method_}", font_size=10)
    if image_file:
        fig.write_image(Path(image_file).as_posix(), width=1800, height=1600)
    return fig


def one_axes_scatter_plot(
    experiment: Union[Experiment, ResultsSelector],
    selected_scenario: str,
    methods: Optional[list[str]] = None,
    image_file: Optional[PathLike] = None,
) -> Figure:
    rs = ResultsSelector.get_result_selector(experiment, None, methods)
    df = rs.normalized_df()

    scenario_index = df[df.columns[0]].tolist().index(selected_scenario)
    n_rows = len(rs.methods)
    fig, axs = plt.subplots(
        len(rs.methods), 1, figsize=(10, 2 * n_rows)
    )  # Assuming each subplot has a height of 5, adjust as needed

    for method_index in range(n_rows):
        ax = axs[method_index]

        x = df[df.columns[method_index + 1]]
        y = np.random.normal(loc=0, scale=1e-286, size=len(df))
        colors = ["#FFA50090"] * len(x)
        colors[scenario_index] = "blue"
        ax.scatter(x, y, s=100, c=colors, marker="o")
        # plt.title("Scatter Plot with Dense Y Distribution")
        ax.set_xlabel(rs.methods[method_index])
        ax.set_yticks([])  # Hide y-axis labels
        ax.grid(True, which="both", linestyle="--", linewidth=0.5, axis="x")
        # no border on all edges
        for spine in ax.spines.values():
            spine.set_visible(False)
    plt.tight_layout()
    if image_file:
        fig.write_image(Path(image_file).as_posix())
    return fig  # Return the figure object
