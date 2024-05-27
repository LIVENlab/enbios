from pathlib import Path

from demos.demo_experiment import get_demo_experiment
from enbios.base.experiment import Experiment
from enbios.base.plot_experiment import (
    bar_plot,
    stacked_bar_plot,
    star_plot,
    single_star_plot,
)

base_plot_destination_path = Path("data/plots")
# Open a pickled experiment object.
exp: Experiment = get_demo_experiment(num_scenarios=4)

bar_plot(exp, image_file=base_plot_destination_path / "bar_plot_1.png")

bar_plot(
    exp,
    scenarios=["scenario 1", "scenario 2"],
    image_file=base_plot_destination_path / "bar_plot_2.png",
)


bar_plot(
    exp, methods=["bw.GWP1000"], image_file=base_plot_destination_path / "bar_plot_3.png"
)

exp.results_to_csv(base_plot_destination_path / "experiment.csv", flat_hierarchy=True)

stacked_bar_plot(exp, image_file=base_plot_destination_path / "stacked_plot_1.png")

stacked_bar_plot(
    exp, level=2, image_file=base_plot_destination_path / "stacked_plot_2.png"
)

stacked_bar_plot(
    exp, level=2, image_file=base_plot_destination_path / "stacked_plot_2.png"
)

stacked_bar_plot(
    exp,
    methods=exp.methods[:1],
    nodes=[
        "electricity production, solar tower power plant, 20 MW",
        "electricity production, solar thermal parabolic trough, 50 MW",
    ],
    image_file=base_plot_destination_path / "stacked_plot_3.png",
)


star_plot(exp, fill=True, image_file=base_plot_destination_path / "star_plot_1.png")

single_star_plot(
    exp,
    show_labels=True,
    image_file=base_plot_destination_path / "single_star_plot_1.png",
)
