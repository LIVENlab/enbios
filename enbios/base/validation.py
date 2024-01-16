from collections import Counter
from logging import getLogger
from typing import TYPE_CHECKING

from enbios.base.adapters_aggregators.adapter import EnbiosAdapter
from enbios.base.adapters_aggregators.aggregator import EnbiosAggregator
from enbios.base.adapters_aggregators.builtin import BUILTIN_AGGREGATORS
from enbios.base.adapters_aggregators.loader import load_adapter, load_aggregator
from enbios.base.scenario import Scenario
from enbios.models.environment_model import Settings
from enbios.models.experiment_base_models import ExperimentConfig, ExperimentScenarioData, NodeOutput, AdapterModel, \
    AggregationModel
from enbios.models.experiment_models import Activity_Outputs

if TYPE_CHECKING:
    from enbios.base.experiment import Experiment

logger = getLogger(__name__)


def validate_adapters(experiment_adapters: list[AdapterModel]) -> tuple[dict[str:EnbiosAdapter], list[str]]:
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


def validate_aggregators(experiment_aggregators: list[AggregationModel]) -> dict[str, EnbiosAggregator]:
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
    for builtin_name, aggregator in BUILTIN_AGGREGATORS.items():
        if builtin_name not in aggregator_names:
            aggregator = aggregator()

            aggregators.append(aggregator)

    return {aggregator.name(): aggregator for aggregator in aggregators}


def validate_scenarios(experiment_scenarios: list[ExperimentScenarioData],
                       default_scenario_name: str,
                       experiment: "Experiment") -> list[Scenario]:
    scenarios: list[Scenario] = []

    # undefined scenarios. just one default scenario
    if not experiment_scenarios:
        experiment_scenarios = [
            ExperimentScenarioData(name=default_scenario_name)
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


def validate_scenario(scenario_data: ExperimentScenarioData,
                      experiment: "Experiment") -> Scenario:
    """
    Validate one scenario
    :param scenario_data:
    :param experiment:
    :return:
    """

    def validate_activities(scenario_: ExperimentScenarioData) -> Activity_Outputs:
        activities = scenario_.activities or {}
        result: dict[str, float] = {}

        for activity_name, activity_output in activities.items():
            activity = experiment.get_activity(activity_name)
            adapter = experiment.get_node_adapter(activity)

            if isinstance(activity_output, dict):
                activity_output = NodeOutput(**activity_output)
            result[activity_name] = adapter.validate_node_output(
                activity_name, activity_output
            )
        return result

    scenario_activities_outputs: Activity_Outputs = validate_activities(scenario_data)
    defined_activities = list(scenario_activities_outputs.keys())

    # fill up the missing activities with default values
    if not scenario_data.config.exclude_defaults:
        for activity_name in experiment.activities_names:
            if activity_name not in defined_activities:
                scenario_activities_outputs[
                    activity_name
                ] = experiment.get_activity_default_output(activity_name)

    # todo shall we bring back. scenario specific methods??
    # resolved_methods: dict[str, ExperimentMethodPrepData] = {}
    # if _scenario.methods:
    #     for index_, method_ in enumerate(_scenario.methods):
    #         if isinstance(method_, str):
    #             global_method = self.methods.get(method_)
    #             assert global_method
    #             resolved_methods[global_method.name] = global_method

    return Scenario(
        experiment=experiment,  # type: ignore
        name=scenario_data.name,
        activities_outputs=scenario_activities_outputs,
        # methods={},
        config=scenario_data.config,
        result_tree=experiment.base_result_tree.copy(),
    )


def validate_run_scenario_setting(env_settings: Settings,
                                  experiment_config: ExperimentConfig,
                                  scenario_names: list[str]):
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
