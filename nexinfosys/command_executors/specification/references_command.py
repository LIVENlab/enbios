import json

from nexinfosys.command_generators.spreadsheet_command_parsers.specification import ref_prof
from nexinfosys.models.musiasem_concepts import Reference
from nexinfosys.model_services import IExecutableCommand, get_case_study_registry_objects


class ReferencesCommand(IExecutableCommand):
    """ It is a mere data container
        Depending on the type, the format can be controlled with a predefined schema
    """
    def __init__(self, name: str):
        self._name = name
        self._content = None

    def execute(self, state: "State"):
        """
        Process each of the references, simply storing them as Reference objects
        """
        glb_idx, _, _, _, _ = get_case_study_registry_objects(state)

        issues = []

        # Receive a list of validated references
        # Store them as independent objects
        for ref in self._content["references"]:
            if "ref_id" not in ref:
                issues.append((3, "'ref_id' field not found: "+str(ref)))
            else:
                ref_id = ref["ref_id"]
                ref_type = ref["type"]
                existing = glb_idx.get(Reference.partial_key(ref_id, ref_type))
                if len(existing) == 1:
                    issues.append((2, "Overwriting reference '"+ref_id+"' of type '"+ref_type+"'"))
                elif len(existing) > 1:
                    issues.append((3, "The reference '"+ref_id+"' of type '"+ref_type+"' is defined more than one time ("+str(len(existing))+")"))

                reference = Reference(ref_id, ref_type, ref)
                glb_idx.put(reference.key(), reference)

        return issues, None

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
