
# ######################################################################################################################
# GRAPH PARTITIONING - SCENARIO (PARAMETERS), SINGLE SCENARIO PARTITION CATEGORIES, IN-SCENARIO OBSERVATIONS VARIATION
# ######################################################################################################################
import networkx as nx
from nexinfosys.model_services import get_case_study_registry_objects


def get_contexts(objects):
    """

    :param objects:
    :return:
    """
    pass


def get_graph_partitioning_categories(objects):
    """
    Obtain categories known to partition the graph (into components)

    The most important one is usually "GEO"

    :param objects:
    :return: A list of partition categories
    """
    return ["GEO"]  # TODO Ensure that processors get category "GEO"


# ######################################################################################################################
# PARAMETERS - SCENARIOS. Global parameters. Scenarios to make
# ######################################################################################################################

def get_parameters(objects):
    """
    Obtain all parameters
    Obtain the variation ranges (or constant values) for each parameter

    :param objects:
    :return: A list (or dict) of parameters
    """
    # TODO
    return None


def map_parameters(params, objects):
    """
    Scan all occurrences of parameters
    Put a list in each parameter pointing to the parameter occurrence (expressions)

    :param params:
    :param objects:
    :return:
    """


def get_observation_variation_categories(objects):
    """
    Obtain categories by which, for a given scenario and partition, ie, a decoupled subsystem
    produce variation in quantitative observations, not in the system structure.

    The most important ones are TIME and OBSERVER

    :param objects:
    :return:
    """
    return ["TIME", "SOURCE"]

# ######################################################################################################################
# GENERATORS
# ######################################################################################################################


def get_scenario_generator(params, objects):
    """
    Obtain a scenario generator
    For each iteration it will return a dict with parameter name as key and parameter value as value

    :param params:
    :param objects:
    :return:
    """


def get_partition_generator(scenario, partition_categories, objects):
    """
    Obtain an enumerator of partitions for a given scenario (the set may change from scenario to scenario)
    For each iteration it will return a list of dicts made category names and category values
    (the list may be just of an element)

    :param scenario:
    :param partition_categories:
    :param objects:
    :return:
    """


def get_obs_variation_generator(obs_variation_categories, msm):
    """
    Obtain an iterator on the possible combinations of observation variations, given a MuSIASEM model

    Ideally this would be between one and less than ten [1, 10)
    For each iteration it will return a dict of categories

    :param obs_variation_categories:
    :param msm:
    :return:
    """


# ######################################################################################################################
# DATA STRUCTURES
# ######################################################################################################################


def build_msm_from_parsed(scenario, partition, parse_execution_results):
    """
    Construct MuSIASEM entities given the scenario and the requested partition

    :param scenario:
    :param partition:
    :param parse_execution_results:
    :return:
    """
    # TODO Everything!!!
    # TODO Consider evaluation of parameters in expressions

    return None


def cleanup_unused_processors(msm):
    """
    Remove or disable processors not involved in the solving and indicators
    For instance, unit processors and its children

    Elaborating a mark signaling when we have a unit processor is needed, then filtering out is trivial

    :param msm:
    :return:
    """
    # TODO


def reset_msm_solution_observations(msm):
    """
    Because the solving process in a MuSIASEM model is iterative, and each iteration may add observations,
    a reset is needed before iterations begin.



    :param msm:
    :return:
    """


def put_solution_into_msm(msm, fg):
    """

    :param msm:
    :param fg:
    :return:
    """


def get_flow_graph(msm, obs_variation):
    """
    Obtain a flow graph from the MuSIASEM model
    Assign quantitative observations filtering using the observations variation parameter

    :param msm:
    :param obs_variation:
    :return:
    """
    # TODO
    return nx.DiGraph()


# ######################################################################################################################
# INDICATORS
# ######################################################################################################################


def get_empty_indicators_collector():
    """
    Prepare a data structure where indicators

    It may be an in memory structure or a handle to a persistent structure

    :return:
    """
    # TODO
    return dict()


def compute_local_indicators(msm):
    """
    Compute indicator local to processors

    The standard would be metabolic ratios, wherever it may be possible

    :param msm: Input and output MuSIASEM model, augmented with the local indicators, that would be attached to the Processors
    :return: <nothing>
    """


def compute_global_indicators(msm):
    """
    Compute global indicators -those using data from more than one Processor-
    Store results inside the MuSIASEM structure

    :param msm:
    :return: <Nothing>
    """


def collect_indicators(indicators, msm):
    """
    Extract categories, observations and indicators into the "indicators" analysis structure

    :param indicators:
    :param msm:
    :return: <Nothing>
    """


# ######################################################################################################################
# SOLVER
# ######################################################################################################################


def solve(g):
    """
    Given a flow graph, find the values for the unknown interfaces

    How to treat the situation when an interface, with given value, could get a solution?
    * We have a top-down value
    * A bottom-up value is also available
    * Take note of the solution?

    :param g:
    :return:
    """
    #
    # Elaborate square matrix?




def solver_one(state):
    """
    Solves a MuSIASEM case study AND computes/collects indicators

    Receives as input a registry of parsed/elaborated MuSIASEM objects

    STORES in "state" an indicators structure (a Dataset?) with all the results

    :param state:
    :return: A list of issues
    """

    # Obtain the different state elements
    glb_idx, p_sets, hh, datasets, mappings = get_case_study_registry_objects(state)

    # Obtain parameters and their variation ranges (initially decoupled)
    params_list = get_parameters(state)

    # Map params to the objects where they appear
    map_parameters(params_list, state)  # Modifies "params_list"

    # Obtain "partition" categories
    # Partition categories are categories provoking totally decoupled systems. The most important one is GEO
    partition_categories = get_graph_partitioning_categories(state)

    # Obtain "observation variation" categories
    # Observation variation categories are those which for a given scenario and partition (i.e., fixed subsystem)
    # produce variation in quantitative observations, not in the system structure. The most important are TIME and OBSERVER
    obs_variation_categories = get_observation_variation_categories(state)

    # Empty indicators collector
    indicators = get_empty_indicators_collector()

    for scenario in get_scenario_generator(params_list, state):
        # "scenario" contains a list of parameters and their values
        for partition in get_partition_generator(scenario, partition_categories, state):
            # "partition" contains a list of categories and their specific values
            # Build MSM for the partition categories
            msm = build_msm_from_parsed(scenario, partition, state)
            # TODO Remove processors not in the calculations (unit processors)
            cleanup_unused_processors(msm)  # Modify "msm"
            for obs_variation in get_obs_variation_generator(obs_variation_categories, state):
                # TODO "obs_variation" contains a list of categories and their specific values
                # TODO Build flow graph with observations filtered according to "obs_variation".
                # Nodes keep link to interface AND a value if there is one.
                # Edges keep link to: hierarchy OR flow OR scale change (and context)
                reset_msm_solution_observations(msm)
                fg = get_flow_graph(msm, obs_variation)
                for sub_fg in nx.weakly_connected_component_subgraphs(fg):
                    # Solve the sub_fg. Attach solutions to Nodes of "sub_fg"
                    solve(sub_fg)  # Modify "fg"
                    put_solution_into_msm(msm, sub_fg)  # Modify "msm"
                compute_local_indicators(msm)
                compute_global_indicators(msm)
                collect_indicators(indicators, msm)  # Elaborate output matrices
    return indicators
