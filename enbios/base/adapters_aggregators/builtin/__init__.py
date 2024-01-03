from enbios.base.adapters_aggregators.builtin.simple_assignment_adapter import SimpleAssignmentAdapter
from enbios.bw2.brightway_experiment_adapter import BrightwayAdapter

BUILT_IN_ADAPTERS = {
    SimpleAssignmentAdapter.name(): SimpleAssignmentAdapter,
    BrightwayAdapter.name(): BrightwayAdapter,
}

