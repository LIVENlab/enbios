import json

from nexinfosys.common.helper import create_dictionary
from nexinfosys.models.musiasem_concepts import FactorTypesRelationUnidirectionalLinearTransformObservation, Observer
from nexinfosys.models.musiasem_concepts_helper import find_or_create_observable
from nexinfosys.model_services import IExecutableCommand, get_case_study_registry_objects


class ScaleConversionCommand(IExecutableCommand):
    """
    Useful to convert quantities from one scale to other, using a linear transform
    The transform is unidirectional. To define a bidirectional conversion, another scale conversion command is needed
    """
    def __init__(self, name: str):
        self._name = name
        self._content = None

    def execute(self, state: "State"):
        """
        Create a set of linear scale conversions, from factor type to factor type
        """
        some_error = False
        issues = []

        glb_idx, p_sets, hh, datasets, mappings = get_case_study_registry_objects(state)

        origin_factor_types = self._content["origin_factor_types"]
        destination_factor_types = self._content["destination_factor_types"]
        scales = self._content["scales"]

        # Check that we have valid factor type names
        fts = create_dictionary()
        for ft_name in origin_factor_types + destination_factor_types:
            # Obtain (maybe Create) the mentioned Factor Types
            p, ft, f = find_or_create_observable(glb_idx, ft_name, Observer.no_observer_specified, None,
                                                 proc_external=None, proc_attributes=None, proc_location=None,
                                                 fact_roegen_type=None, fact_attributes=None, fact_incoming=None,
                                                 fact_external=None, fact_location=None
                                                 )
            if not ft:
                some_error = True
                issues.append((3, "Could not obtain/create the Factor Type '"+ft_name+"'"))
            fts[ft_name] = ft

        if some_error:
            return issues, None

        for sc in scales:
            origin = fts[sc["origin"]]
            destination = fts[sc["destination"]]
            scale = sc["scale"]
            FactorTypesRelationUnidirectionalLinearTransformObservation.create_and_append(origin, destination, scale, Observer.no_observer_specified)

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
