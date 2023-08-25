from pathlib import Path
from typing import Optional

import numpy as np
from deprecated.classic import deprecated
from matplotlib import pyplot as plt
from matplotlib.figure import Figure
from pandas import DataFrame

from enbios2.analyse.util import DataTransformer
from enbios2.base.experiment import Experiment
from enbios2.generic.enbios2_logging import get_logger
from enbios2.generic.tree.basic_tree import BasicTreeNode
from enbios2.models.experiment_models import ScenarioResultNodeData

logger = get_logger(__file__)


def results_to_plot(exp: Experiment,
                    method_: str,
                    *,
                    scenario_name: Optional[str] = None,
                    image_file: Optional[Path] = None,
                    show: bool = False):
    try:  # type: ignore
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

    fig.update_layout(title_text=f"{scenario.alias} / {'_'.join(method_)}", font_size=10)
    if show:
        fig.show()
    if image_file:
        fig.write_image(image_file.as_posix(), width=1800, height=1600)


def plot_experiment_results(experiment: Experiment,
                            scenarios: Optional[list[str]] = None,
                            methods: Optional[list[str]] = None) -> Figure:
    try:  # type: ignore
        import seaborn
    except ImportError:
        raise

    dt = DataTransformer(experiment)
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

        # Create the bar plot using the specific Axes object
        seaborn.barplot(data=df, y=method, x="scenario", ax=axs[idx])
        axs[idx].set_ylabel(label, fontsize=8)

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
    dt = DataTransformer(experiment)
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
    dt = DataTransformer(experiment)
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

    dt = DataTransformer(experiment)
    df_ = dt.normalized_df.set_index('scenario').transpose()

    # Create the heatmap
    plt.figure(figsize=(10, 6))
    sns.heatmap(df_, annot=True, cmap="YlGnBu", cbar=True, yticklabels=dt.short_method_names())
    plt.title("Heatmap of Indicators by Scenario")
    plt.show()