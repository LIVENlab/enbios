from pathlib import Path
from typing import Generator

import pytest

from demos.demo_experiment import get_demo_experiment
from enbios import Experiment
from enbios.base.plot_experiment import bar_plot, stacked_bar_plot, star_plot, single_star_plot, plot_heatmap, \
    one_axes_scatter_plot
from enbios.base.result_select import ResultsSelector
from enbios.const import BASE_TEST_DATA_PATH


@pytest.fixture
def get_experiment_pickle() -> Experiment:
    return get_demo_experiment(4)


def test_result_selector1(get_experiment_pickle):
    rs = ResultsSelector.get_result_selector(get_experiment_pickle)
    assert list(rs.base_df.columns) == ['scenario', 'GWP1000', 'WCP', 'HToxicity']
    assert list(rs.base_df["scenario"]) == ['scenario 1', 'scenario 2', 'scenario 3', 'scenario 4']
    assert rs.methods == ['bw.GWP1000', 'bw.WCP', 'bw.HToxicity']


@pytest.fixture
def temp_file() -> Generator[Path, None, None]:
    tempfile = BASE_TEST_DATA_PATH / "temp/temp.png"
    yield tempfile
    tempfile.unlink(missing_ok=True)


def test_bar_plot1(get_experiment_pickle, temp_file):
    bar_plot(get_experiment_pickle, image_file=temp_file)
    assert temp_file.exists()


def test_bar_plot2(get_experiment_pickle, temp_file):
    bar_plot(get_experiment_pickle, image_file=temp_file,
             scenarios=["scenario 1"])
    assert temp_file.exists()


def test_bar_plot3(get_experiment_pickle, temp_file):
    bar_plot(get_experiment_pickle, image_file=temp_file,
             methods=["bw.WCP"])
    assert temp_file.exists()


def test_stacked_bar_plot(get_experiment_pickle, temp_file):
    stacked_bar_plot(get_experiment_pickle, image_file=temp_file)


def test_stacked_bar_plot2(get_experiment_pickle, temp_file):
    stacked_bar_plot(get_experiment_pickle, level=2, image_file=temp_file)


def test_stacked_bar_plot3(get_experiment_pickle, temp_file):
    stacked_bar_plot(
        get_experiment_pickle,
        methods=get_experiment_pickle.methods[:1],
        nodes=[
            "electricity production, solar tower power plant, 20 MW",
            "electricity production, solar thermal parabolic trough, 50 MW",
        ],
        image_file=temp_file
    )


def test_star_plot(get_experiment_pickle, temp_file):
    star_plot(get_experiment_pickle, fill=True)


def test_star_plot2(get_experiment_pickle, temp_file):
    single_star_plot(
        get_experiment_pickle, show_labels=True, image_file=temp_file
    )


def test_heatmap(get_experiment_pickle, temp_file):
    plot_heatmap(get_experiment_pickle, image_file=temp_file)


def test_scatter(get_experiment_pickle, temp_file):
    one_axes_scatter_plot(get_experiment_pickle,
                          selected_scenario=get_experiment_pickle.scenario_names[0],
                          image_file=temp_file)
