import pytest



def test_fail_get_structural_node(basic_experiment):
    # get_structural_node should fail (arbitrary node name)
    with pytest.raises(Exception):
        basic_experiment.get_structural_node("arbitrary-node")

#  get_adapter_by_name should return the correct adapter
def test_get_adapter_by_name(basic_experiment):
    # get_adapter_by_name should return the correct adapter
    adapter = basic_experiment.get_adapter_by_name("bw")
    assert adapter is not None
    assert adapter.name() == "brightway-adapter"
    assert adapter == basic_experiment.get_adapter_by_name("brightway-adapter")

#  get_adapter_by_name should raise an exception for invalid adapter name
def test_fail_get_adapter_by_name(basic_experiment):
    # get_adapter_by_name should raise an exception for invalid adapter name
    with pytest.raises(Exception):
        basic_experiment.get_adapter_by_name("invalid-adapter")

#  Retrieving a non-existing structural node should raise an exception
def test_fail_get_non_existing_structural_node(basic_experiment):
    # get_structural_node should fail for a non-existing node name
    with pytest.raises(Exception):
        basic_experiment.get_structural_node("non-existing-node")

#  get_scenario should return the correct scenario
def test_get_scenario(basic_experiment):
    # get_scenario should return the correct scenario
    assert basic_experiment.get_scenario(basic_experiment.DEFAULT_SCENARIO_NAME)


def test_fail_get_scenario(basic_experiment):
    # get_scenario  should fail (arbitrary adapter name)
    with pytest.raises(ValueError):
        assert basic_experiment.get_scenario("non-existing scenario")

