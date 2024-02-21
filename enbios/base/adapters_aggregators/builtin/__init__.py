from typing import Type

from enbios.base.adapters_aggregators.adapter import EnbiosAdapter
from enbios.base.adapters_aggregators.builtin.sum_aggregator import SumAggregator
from enbios.base.adapters_aggregators.builtin.simple_assignment_adapter import (
    SimpleAssignmentAdapter,
)
from enbios.bw2.brightway_experiment_adapter import BrightwayAdapter

BUILTIN_ADAPTERS: dict[str, Type[EnbiosAdapter]] = {
    SimpleAssignmentAdapter.name(): SimpleAssignmentAdapter,
    BrightwayAdapter.name(): BrightwayAdapter,
}

BUILTIN_AGGREGATORS = {SumAggregator.name(): SumAggregator}
