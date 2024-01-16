from enbios.base.adapters_aggregators.builtin.sum_aggregator import SumAggregator
from enbios.base.adapters_aggregators.builtin.simple_assignment_adapter import (
    SimpleAssignmentAdapter,
)
from enbios.bw2.brightway_experiment_adapter import BrightwayAdapter

BUILTIN_ADAPTERS = {
    SimpleAssignmentAdapter.name(): SimpleAssignmentAdapter,
    BrightwayAdapter.name(): BrightwayAdapter,
}

BUILTIN_AGGREGATORS = {SumAggregator.name(): SumAggregator}
