from pathlib import Path
from typing import Optional, Union

import numpy as np
from deprecated.classic import deprecated
from matplotlib import pyplot as plt
from matplotlib.figure import Figure

from enbios2.analyse.util import ResultsSelector
from enbios2.base.experiment import Experiment
from enbios2.generic.enbios2_logging import get_logger
from enbios2.generic.files import PathLike
from enbios2.generic.tree.basic_tree import BasicTreeNode
from enbios2.models.experiment_models import ScenarioResultNodeData

logger = get_logger(__file__)


def plot_sankey(exp: Experiment,
                scenario_name: str,
                method_: str,
                *,
                image_file: Optional[PathLike] = None,
                show: bool = False) -> Figure:
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

    fig.update_layout(title_text=f"{scenario.alias} / {method_}", font_size=10)
    if show:
        fig.show()
    if image_file:
        fig.write_image(Path(image_file).as_posix(), width=1800, height=1600)
    return fig


def bar_plot(experiment: Union[Experiment, ResultsSelector], scenarios: Optional[list[str]] = None,
             methods: Optional[list[str]] = None) -> Figure:
    if isinstance(experiment, Experiment):
        dt = ResultsSelector(experiment, scenarios=scenarios, methods=methods)
    else:
        dt = experiment
        experiment = dt.experiment
    df = dt.base_df

    # Define the number of rows and columns for the subplots
    n_rows = len(dt.methods)
    n_cols = 1

    # Create a new figure with a defined size (adjust as needed)
    # Explicitly create a Figure object
    fig, axs = plt.subplots(n_rows, n_cols,
                            figsize=(10, 5 * n_rows))  # Assuming each subplot has a height of 5, adjust as needed

    # Check if there's only one subplot to handle the indexing appropriately
    if n_rows == 1:
        axs = [axs]

    for idx, method in enumerate(dt.methods):
        method_data = experiment.methods[method]
        label = "\n".join(list(method_data.id) + [method_data.bw_method.unit])

        # plot with tab10 colors
        cmap = plt.colormaps.get_cmap('tab10')
        colors = cmap(np.linspace(0, 1, len(dt.scenarios)))
        df.plot(kind='bar', x='scenario', y=method, ax=axs[idx], color=colors)
        # df.plot(kind='bar', x='scenario', y=method, ax=axs[idx], color = "tab10")
        axs[idx].set_ylabel(label, fontsize=8)
        axs[idx].legend().set_visible(False)

    plt.tight_layout()
    return fig  # Return the figure object


def stacked_bar_plot(experiment: Union[Experiment, ResultsSelector], scenarios: Optional[list[str]] = None,
                     methods: Optional[list[str]] = None, level: int = 1,
                     aliases: Optional[list[str]] = None) -> Figure:
    if isinstance(experiment, Experiment):
        dt = ResultsSelector(experiment, scenarios=scenarios, methods=methods)
    else:
        dt = experiment
        experiment = dt.experiment

    if level >= experiment.hierarchy_root.depth:
        logger.warning(
            f"Level {level} is higher or equal (>=) than the depth of the hierarchy "
            f"({experiment.hierarchy_root.depth}). Limiting to {experiment.hierarchy_root.depth - 1}")
        level = experiment.hierarchy_root.depth - 1
    # Define the number of rows and columns for the subplots
    n_rows = len(dt.methods)
    n_cols = 1

    # Create a new figure with a defined size (adjust as needed)
    # Explicitly create a Figure object
    fig, axs = plt.subplots(n_rows, n_cols,
                            figsize=(10, 5 * n_rows))  # Assuming each subplot has a height of 5, adjust as needed

    # Check if there's only one subplot to handle the indexing appropriately
    if n_rows == 1:
        axs = [axs]

    for idx, method in enumerate(dt.methods):
        # method_data = experiment.methods[method]
        # label = "\n".join(list(method_data.id) + [method_data.bw_method.unit])
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
        df = dt.collect_tech_results(node_names)
        df_pivot = df.pivot(index='scenario', columns='tech',
                            values=method)
        df_pivot.plot(kind='bar', stacked=True, ax=ax)

    plt.tight_layout()
    return fig  # Return the figure object


@deprecated(reason="See star_plot and update this code")
def single_star_plot(experiment: Experiment, scenario: str) -> Figure:
    """
    outdated. see the other implementation and addapt this
    :param experiment:
    :param scenario:
    :return:
    """
    dt = ResultsSelector(experiment)
    df = dt.normalized_df

    # Define data
    labels = dt.short_method_names()

    values = df.loc[df["scenario"] == scenario].values.tolist()[0][1:]
    # multiply by 100 to get percentage
    values = [v * 100 for v in values]

    labels.append(labels[0])
    values.append(values[0])

    num_vars = len(labels)
    angles = np.linspace(0, 2 * np.pi, num_vars, endpoint=False).tolist()

    fig, ax = plt.subplots(figsize=(6, 6), subplot_kw=dict(polar=True))

    # Plot data
    ax.fill(angles, values, color='blue', alpha=0.1)

    # Fix axis to close the circle
    ax.set_theta_offset(np.pi / 2)
    ax.set_theta_direction(-1)

    # Draw ylabels
    ax.set_rlabel_position(30)

    # Set more customizations
    # ticks = [0, 0.25, 0.5, 0.75, 1.0]
    ticks = [20, 40, 60, 80, 100]
    ax.set_yticks(ticks)
    ax.set_yticklabels(ticks)

    ax.set_xticks(angles)
    ax.set_xticklabels(labels)

    plt.title(scenario)
    plt.show()
    return fig


def star_plot(experiment: Experiment,
              *,
              fill: bool = True,
              r_ticks=(0.2, 0.4, 0.6, 0.8, 1.0),
              show_r_ticks: bool = True,
              show_grid: bool = True,
              col: int = 4,
              row: Optional[int] = None) -> Figure:
    # print(experiment.methods)
    dt = ResultsSelector(experiment)
    df = dt.normalized_df

    # set a value in a specific cell
    # df.loc[df["scenario"] == "Scenario 3", "EDIP 2003 no LT_non-renewable resources no LT_molybdenum no LT"] = 0.5
    # Define data
    labels = dt.short_method_names()
    # print("num label", len(labels))

    if row is None:
        row = int(np.ceil(len(dt.scenarios) / col))
    # if only one row. limit the figure size to the number of scenarios
    if row == 1:
        col = len(dt.scenarios)
    # Create figure and axes
    fig, axs = plt.subplots(row, col, figsize=(6 * col, 6 * row), subplot_kw=dict(polar=True))

    if row == 1 and col == 1:
        axs = [[axs]]  # to handle indexing later on
    elif row == 1:
        axs = [axs]
    elif col == 1:
        axs = [[a] for a in axs]

    # remove empty axes
    last_row_num_axes = len(dt.scenarios) % col
    # print(last_row_num_axes)
    if last_row_num_axes != 0:
        for c in range(col - last_row_num_axes):
            # print(c,"--")
            axs[-1][-1 - c].remove()
            # pass

    # print(row, col)

    all_done = False
    for r in range(row):
        for c in range(col):
            ax = axs[r][c]
            # get scenario at index r * col + c
            scenario_name: str = dt.scenarios[r * col + c]
            # scenario: Scenario = experiment.get_scenario(scenario_name)
            # values from df.normalized_df in row with the scenario name
            values = df.loc[df["scenario"] == scenario_name].values.tolist()[0][1:]
            values.append(values[0])
            # print(scenario_name)
            # print("num values", len(values))

            angles = np.linspace(0, 2 * np.pi, len(values) - 1, endpoint=False).tolist()
            angles.append(0)
            # print("num angles", len(angles))
            # Plot data
            if fill:
                ax.fill(angles, values, color='blue', alpha=0.1)
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
            if r == 0 and c == 0:
                ax.set_xticklabels(labels)
            else:
                ax.set_xticklabels([""] * len(angles))
            ax.set_rmax(1)
            ax.title = scenario_name
            if r * col + c == len(dt.scenarios) - 1:
                all_done = True
                break
        if all_done:
            break

    plt.tight_layout()
    plt.show()
    return fig


def plot_heatmap(experiment: Experiment):
    try:
        import seaborn as sns
    except ImportError:
        raise ImportError("The seaborn package is required to run this function.")

    dt = ResultsSelector(experiment)
    df_ = dt.normalized_df.set_index('scenario').transpose()

    # Create the heatmap
    plt.figure(figsize=(10, 6))
    sns.heatmap(df_, annot=True, cmap="YlGnBu", cbar=True, yticklabels=dt.short_method_names())
    plt.title("Heatmap of Indicators by Scenario")
    plt.show()
