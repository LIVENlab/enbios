from nexinfosys.model_services import IExecutableCommand


class NestedCommandsCommand(IExecutableCommand):
    """
    This command is just a "placeholder" command, with no effect
    """
    def __init__(self, name: str):
        self._name = name
        self._content = None

    def execute(self, state: "State"):
        return None, None

    def estimate_execution_time(self):
        return 0

    def json_serialize(self):
        return None  # NULLIFY CONTENT because commands have been serialized previously (FLAT APPROACH) self._content

    def json_deserialize(self, json_input):
        self._content = None
        return []
