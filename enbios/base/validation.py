from collections import Counter
from logging import getLogger
from typing import TYPE_CHECKING, Optional

from enbios.base.adapters_aggregators.adapter import EnbiosAdapter
from enbios.base.adapters_aggregators.aggregator import EnbiosAggregator
from enbios.base.adapters_aggregators.builtin import BUILTIN_AGGREGATORS
from enbios.base.adapters_aggregators.loader import load_adapter, load_aggregator
from enbios.base.scenario import Scenario
from enbios.models.environment_model import Settings
from enbios.models.models import (
    ExperimentConfig,
    ExperimentScenarioData,
    AdapterModel,
    AggregationModel,
)

if TYPE_CHECKING:
    from enbios.base.experiment import Experiment

logger = getLogger(__name__)


def validate_adapters(
    experiment_adapters: list[AdapterModel],
) -> tuple[dict[str, EnbiosAdapter], list[str]]:
    """
    Validate the adapters in this experiment data

    :return: adapter-dict and method names
    """
    adapters = []
    methods = []
    for adapter_def in experiment_adapters:
        adapter = load_adapter(adapter_def)
        adapter.validate_definition(adapter_def)
        adapter.validate_config(adapter_def.config)
        adapters.append(adapter)
        adapter_methods = adapter.validate_methods(adapter_def.methods)
        methods.extend([f"{adapter.node_indicator()}.{m}" for m in adapter_methods])

    adapter_map = {adapter.name(): adapter for adapter in adapters}
    return adapter_map, methods


def validate_aggregators(
    experiment_aggregators: list[AggregationModel],
) -> dict[str, EnbiosAggregator]:
    """
    Validate the aggregators in this experiment data

    :return: a aggregator-name-Aggregator dict
    """
    aggregators = []
    for aggregator_def in experiment_aggregators:
        aggregator = load_aggregator(aggregator_def)
        aggregator.validate_config(aggregator_def.config)
        aggregators.append(aggregator)

    aggregator_names = [a.name() for a in aggregators]
    for builtin_name, aggregator_class in BUILTIN_AGGREGATORS.items():
        if builtin_name not in aggregator_names:
            aggregator = aggregator_class()

            aggregators.append(aggregator)

    return {aggregator.name(): aggregator for aggregator in aggregators}


def validate_scenarios(
    experiment_scenarios: Optional[list[ExperimentScenarioData]],
    default_scenario_name: str,
    experiment: "Experiment",
) -> list[Scenario]:
    scenarios: list[Scenario] = []

    # undefined scenarios. just one default scenario
    if not experiment_scenarios:
        experiment_scenarios = [
            ExperimentScenarioData(name=default_scenario_name, nodes={})
        ]

    # set names if not given
    for index, scenario_data in enumerate(experiment_scenarios):
        scenario_data.name_factory(index)

    # check for name duplicates
    name_count = Counter([s.name for s in experiment_scenarios])
    # get the scenarios that have the same name
    duplicate_names = [name for name, count in name_count.items() if count > 1]
    if duplicate_names:
        raise ValueError(f"Scenarios with the same name: {duplicate_names}")

    for index, scenario_data in enumerate(experiment_scenarios):
        scenario = validate_scenario(scenario_data, experiment)
        scenarios.append(scenario)
        scenario.prepare_tree()
    return scenarios


def validate_scenario(
    scenario_data: ExperimentScenarioData, experiment: "Experiment"
) -> Scenario:
    """
    Validate one scenario
    :param scenario_data:
    :param experiment:
    :return:
    """

    def validate_nodes(scenario_: ExperimentScenarioData) -> dict[str, float]:
        nodes = scenario_.nodes or {}
        outputs: dict[str, float] = {}

        for node_name_, node_output in nodes.items():
            node_ = experiment.get_structural_node(node_name_)
            adapter = experiment.get_node_adapter(node_)
            outputs[node_name_] = adapter.validate_scenario_node(
                node_name_, str(scenario_.name), node_output
            )
        return outputs

    validate_nodes(scenario_data)

    # fill up the missing activities with default values
    # todo bring something like this back...make the data come from the adapter
    # if not scenario_data.config.exclude_defaults:
    # for node_name in experiment.structural_nodes_names:
    #     if node_name not in scenario_data.nodes:
    #         node = experiment.get_structural_node(node_name)
    #         output = experiment.get_node_adapter(
    #             node
    #         ).get_node_output(node_name, scenario_data.name)
    #         if not output:
    #             raise EnbiosValidationException(
    #                 f"No output for node {node_name} in scenario {scenario_data.name}")
    assert scenario_data.name
    return Scenario(
        experiment=experiment,  # type: ignore
        name=scenario_data.name,
        # structural_nodes_outputs=scenario_nodes_outputs,
        config=scenario_data.config,
        result_tree=experiment.base_result_tree.copy(),
    )


def validate_run_scenario_setting(
    env_settings: Settings, experiment_config: ExperimentConfig, scenario_names: list[str]
):
    """
    Validate a run environmental variable that is setting the scenario
    """
    if env_settings.RUN_SCENARIOS:
        if experiment_config.run_scenarios:
            logger.info(
                "Environment variable 'RUN_SCENARIOS' is set "
                "and overwriting experiment config."
            )
        experiment_config.run_scenarios = env_settings.RUN_SCENARIOS
    if experiment_config.run_scenarios:
        for scenario in experiment_config.run_scenarios:
            if scenario not in scenario_names:
                raise ValueError(
                    f"Scenario '{scenario}' not found in experiment scenarios. "
                    f"Scenarios are: {scenario_names}"
                )
