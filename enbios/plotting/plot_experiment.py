from pathlib import Path
from typing import Optional, Union, Callable

import numpy as np
from matplotlib import pyplot as plt
from matplotlib.axes import Axes
from matplotlib.figure import Figure
from matplotlib.projections import PolarAxes
from pandas import DataFrame

from enbios.base.experiment import Experiment
from enbios.base.result_select import ResultsSelector
from enbios.generic.enbios2_logging import get_logger
from enbios.generic.files import PathLike
from enbios.generic.tree.basic_tree import BasicTreeNode
from enbios.models.experiment_models import ScenarioResultNodeData

logger = get_logger(__name__)


def bar_plot(
    experiment: Union[Experiment, ResultsSelector],
    scenarios: Optional[list[str]] = None,
    methods: Optional[list[str]] = None,
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

    for idx, method in enumerate(rs.method_names):
        cmap = plt.colormaps.get_cmap("tab10")
        colors = cmap(np.linspace(0, 1, len(rs.scenarios)))
        rs.base_df.plot(kind="bar", x="scenario", y=method, ax=axs[idx], color=colors)
        axs[idx].set_ylabel(rs.method_label_names()[idx], fontsize=8)
        axs[idx].legend().set_visible(False)

    plt.tight_layout()
    if image_file:
        fig.savefig(Path(image_file).as_posix())
    return fig  # Return the figure object


def stacked_bar_plot(
    experiment: Union[Experiment, ResultsSelector],
    scenarios: Optional[list[str]] = None,
    methods: Optional[list[str]] = None,
    level: int = 1,
    short_method_names: bool = True,
    nodes: Optional[list[str]] = None,
    image_file: Optional[PathLike] = None,
) -> Figure:
    rs = ResultsSelector.get_result_selector(experiment, scenarios, methods)

    nodes: list[str] = rs.validate_node_selection(level, nodes)
    # Define the number of rows and columns for the subplots
    n_rows = len(rs.method_names)
    n_cols = 1

    # Create a new figure with a defined size (adjust as needed)
    # Explicitly create a Figure object
    fig, axs = plt.subplots(
        n_rows, n_cols, figsize=(10, 5 * n_rows)
    )  # Assuming each subplot has a height of 5, adjust as needed

    # Check if there's only one subplot to handle the indexing appropriately
    if n_rows == 1:
        axs = [axs]

    for idx, method in enumerate(rs.method_names):
        ax = axs[idx]
        df: DataFrame = rs.collect_tech_results(nodes)
        df_pivot: DataFrame = df.pivot(index="scenario", columns="tech", values=method)
        df_pivot = df_pivot.reindex(df["scenario"].drop_duplicates().tolist())
        df_pivot.plot(kind="bar", stacked=True, ax=ax)
        ax.set_ylabel(rs.method_label_names(short_method_names)[idx], fontsize=8)

    plt.tight_layout()
    if image_file:
        fig.savefig(Path(image_file).as_posix())
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
    show_labels: bool = True,
    image_file: Optional[PathLike] = None,
) -> Figure:
    rs = ResultsSelector.get_result_selector(experiment, scenarios, methods)
    df = rs.normalized_df()

    if len(rs.methods) < 3:
        raise ValueError("Star-plots require at least 3 methods")

    labels = rs.method_label_names(False)

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
        fig.savefig(Path(image_file).as_posix())
    return fig


def single_star_plot(
    experiment: Union[Experiment, ResultsSelector],
    scenarios: Optional[list[str]] = None,
    methods: Optional[list[str]] = None,
    *,
    r_ticks=(0.2, 0.4, 0.6, 0.8, 1.0),
    show_r_ticks: bool = True,
    show_grid: bool = True,
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
    :param show_labels:
    :param image_file:
    :return:
    """
    rs = ResultsSelector.get_result_selector(experiment, scenarios, methods)
    df = rs.normalized_df()

    if len(rs.methods) < 3:
        raise ValueError("Star-plots require at least 3 methods")

    labels = rs.method_label_names(False)

    # Create figure and axes
    fig, ax = plt.subplots(figsize=(6, 6), subplot_kw=dict(polar=True))

    cmap = plt.colormaps.get_cmap("tab10")
    angles = 0
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
    if show_r_ticks:
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
    if image_file:
        fig.savefig(Path(image_file).as_posix())
    return fig


def plot_heatmap(
    experiment: Union[Experiment, ResultsSelector],
    scenarios: Optional[list[str]] = None,
    methods: Optional[list[str]] = None,
    special_df: Optional[DataFrame] = None,
    image_file: Optional[PathLike] = None,
    x_label_rotation: Optional[int] = 45,
) -> Figure:
    rs = ResultsSelector.get_result_selector(experiment, scenarios, methods)
    df = special_df if special_df is not None else rs.normalized_df()

    def set_plot_settings(ax, df, rs: ResultsSelector):
        im = ax.imshow(df, cmap="summer")
        labels = rs.method_label_names(include_unit=False)
        ax.set_xticks(np.arange(len(rs.scenarios)), labels=rs.scenarios)
        ax.set_yticks(np.arange(len(rs.method_names)), labels=labels)
        plt.setp(
            ax.get_xticklabels(),
            rotation=x_label_rotation,
            ha="right",
            rotation_mode="anchor",
        )
        ax.figure.colorbar(im, ax=ax)
        return ax

    def plot_values_on_grid(ax, df, rs: ResultsSelector):
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
        return ax

    df = df.set_index("scenario").transpose()

    fig, ax = plt.subplots(figsize=(len(rs.scenarios) * 1.5, len(rs.method_names) * 1.5))
    fig: Figure = fig
    set_plot_settings(ax, df, rs)
    plot_values_on_grid(ax, df, rs)
    fig.tight_layout()
    if image_file:
        fig.savefig(Path(image_file).as_posix())
    return fig


def plot_sankey(
    exp: Experiment,
    scenario_name: str,
    method_: str,
    default_bar_color: Optional[str] = "blue",
    color_map: Optional[dict[str, str]] = None,
    *,
    image_file: Optional[PathLike] = None,
) -> Figure:
    try:  # type: ignore
        import plotly.graph_objects as go
    except ImportError:
        logger.error("plotly not installed. Run 'pip install plotly'")
        raise

    scenario = exp.get_scenario(scenario_name)
    all_nodes = list(scenario.result_tree.iter_all_nodes())
    node_labels = [node.name for node in all_nodes]
    color_map = color_map or {}
    node_colors = [color_map.get(n, default_bar_color) for n in node_labels]
    method_ = method_.split(".")[-1]

    source = []
    target = []
    value = []
    for index_, node in enumerate(all_nodes):
        _: BasicTreeNode[ScenarioResultNodeData] = node
        for child in node.children:
            source.append(index_)
            target.append(all_nodes.index(child))
            value.append(child.data.results[method_].magnitude)

    fig = go.Figure(
        data=[
            go.Sankey(
                node=dict(
                    pad=15,
                    thickness=20,
                    line=dict(color="black", width=0.5),
                    label=node_labels,
                    color=node_colors,
                ),
                link=dict(source=source, target=target, value=value),
            )
        ]
    )

    fig.update_layout(title_text=f"{scenario.name} / {method_}", font_size=10)
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
        len(rs.method_names), 1, figsize=(10, 2 * n_rows)
    )  # Assuming each subplot has a height of 5, adjust as needed

    for method_index in range(n_rows):
        ax = axs[method_index]

        x = df[df.columns[method_index + 1]]
        y = np.random.normal(loc=0, scale=1e-286, size=len(df))
        colors = ["#FFA50090"] * len(x)
        colors[scenario_index] = "blue"
        ax.scatter(x, y, s=100, c=colors, marker="o")
        # plt.title("Scatter Plot with Dense Y Distribution")
        ax.set_xlabel(rs.method_names[method_index])
        ax.set_yticks([])  # Hide y-axis labels
        ax.grid(True, which="both", linestyle="--", linewidth=0.5, axis="x")
        # no border on all edges
        for spine in ax.spines.values():
            spine.set_visible(False)
    plt.tight_layout()
    if image_file:
        fig.savefig(Path(image_file).as_posix())
    return fig  # Return the figure object


def plot_multivalue_results(
    experiment: Union[Experiment, ResultsSelector],
    scenarios: Optional[list[str]] = None,
    level: int = 1,
    methods: Optional[list[str]] = None,
    nodes: Optional[list[str]] = None,
    image_file: Optional[PathLike] = None,
    err_method: Optional[Callable[[np.ndarray], float]] = None,
):
    rs = ResultsSelector.get_result_selector(experiment, scenarios, methods)
    experiment = rs.experiment

    nodes = rs.validate_node_selection(level, nodes)

    n_rows = len(rs.method_names) * len(rs.scenarios)
    n_cols = 1

    fig, axs = plt.subplots(
        n_rows, n_cols, figsize=(2 * len(nodes), 5 * n_rows)
    )  # Assuming each subplot has a height of 5, adjust as needed

    if n_rows == 1:
        axs = [axs]
    for sidx, scenario in enumerate(rs.scenarios):
        # print(scenario)
        for midx, method in enumerate(rs.methods):
            # print(method)
            ax: Axes = axs[sidx * midx + midx]

            df: DataFrame = rs.collect_tech_results(nodes, "multi_magnitude")
            for nidx, n in enumerate(nodes):
                value_array = df[method][nidx]
                y = np.mean(value_array)
                if not err_method:
                    y_err = np.std(value_array)
                else:
                    y_err = err_method(value_array)

                ax.errorbar(nidx, y, y_err, fmt="o", linewidth=2, capsize=6)
            x = np.arange(len(nodes))
            ax.set_xticks(x)
            ax.set_xticklabels(nodes)
            ax.set_title(f"Scenario {scenario}")
            ax.set_ylabel(f"{method}\n{rs.experiment.get_method_unit(method)}")

            # ax.set_ylabel
            # ax.set(xlim=(-0.5, len(x_labels) - 0.5), ylim=(0, max(value_array) + yerr_std + 1))

        plt.tight_layout()
        if image_file:
            fig.savefig(Path(image_file).as_posix())
        return fig  # Return the figure object
