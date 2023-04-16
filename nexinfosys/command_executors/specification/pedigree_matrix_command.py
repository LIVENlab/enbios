import json

from nexinfosys.models.musiasem_concepts import PedigreeMatrix
from nexinfosys.model_services import IExecutableCommand, get_case_study_registry_objects


class PedigreeMatrixCommand(IExecutableCommand):
    """
    Declaration of a pedigree matrix, which can be used

    """
    def __init__(self, name: str):
        self._name = name
        self._content = None

    def execute(self, state: "State"):
        """
        Create the PedigreeMatrix object to which QQ observation may refer

        """
        some_error = False
        issues = []

        glb_idx, _, _, _, _ = get_case_study_registry_objects(state)

        # Prepare PedigreeMatrix object instance
        phases = self._content["phases"]
        codes = self._content["codes"]
        name = self._content["name"]

        pm = PedigreeMatrix(name)
        pm.set_phases(codes, phases)

        # Insert the PedigreeMatrix object into the state
        glb_idx.put(pm.key(), pm)
        import jsonpickle
        s = jsonpickle.encode(pm)
        pm2 = jsonpickle.decode(s)

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
