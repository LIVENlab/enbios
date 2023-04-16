import json

from nexinfosys.models.musiasem_concepts_helper import build_hierarchy
from nexinfosys.model_services import IExecutableCommand, get_case_study_registry_objects


class HierarchyCommand(IExecutableCommand):
    """
    Serves to specify a hierarchy (composition or taxonomy) of Observables
    Observables can be Processors, Factors or external categories
    """
    def __init__(self, name: str):
        self._name = name
        self._content = None

    def execute(self, state: "State"):
        """
        Create a Hierarchy. The exact form of this hierarchy is different depending on the concept:
        * FactorTypes and Categories use Hierarchies, which are intrinsic.
            The hierarchy name is passed to the containing Hierarchy object
        * Processors use Part-Of Relations. In this case, the hierarchy name is lost
        Names of Processor and FactorTypes are built both in hierarchical and simple form
        The hierarchical is all the ancestors from root down to the current node, separated by "."
        The simple name is just the current node. If there is already another concept with that name, the simple name
        is not stored (STORE BOTH CONCEPTS by the same name, and design some tie breaking mechanism??)
        """
        glb_idx, p_sets, hh, datasets, mappings = get_case_study_registry_objects(state)
        # Extract base information
        name = self._content["name"]
        type_name = self._content["type"]
        first_level = self._content["h"]

        # Build the hierarchy
        build_hierarchy(name, type_name, registry=glb_idx, h=first_level)

        return None, None

    def estimate_execution_time(self):
        return 0

    def json_serialize(self):
        # Directly return the content
        return self._content

    def json_deserialize(self, json_input):
        # TODO Check validity
        issues = []
        if isinstance(json_input, dict):
            self._content = json_input
        else:
            self._content = json.loads(json_input)
        return issues
