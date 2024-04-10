from typing import Type

from enbios.base.adapters_aggregators.adapter import EnbiosAdapter
from enbios.base.adapters_aggregators.aggregator import EnbiosAggregator
from enbios.base.adapters_aggregators.builtin.sum_aggregator import SumAggregator
from enbios.base.adapters_aggregators.builtin.assignment_adapter import (
    AssignmentAdapter,
)
from enbios.bw2.brightway_experiment_adapter import BrightwayAdapter

BUILTIN_ADAPTERS: dict[str, Type[EnbiosAdapter]] = {
    AssignmentAdapter.name(): AssignmentAdapter,
    BrightwayAdapter.name(): BrightwayAdapter,
}

BUILTIN_AGGREGATORS: dict[str, Type[EnbiosAggregator]] = {
    SumAggregator.name(): SumAggregator
}
