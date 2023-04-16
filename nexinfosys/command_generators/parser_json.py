import json
import logging

from nexinfosys.command_executors import create_command


def commands_generator_from_native_json(input, state, sublist, stack):
    """
    It allows both a single command ("input" is a dict) or a sequence of commands ("input" is a list)

    :param input: Either a dict (for a single command) or list (for a sequence of commands)
    :param state: State used to check variables
    :param sublist: List of command names to consider. If not defined, ALL commands
    :param stack: Stack of nested files. Just pass it...
    :return: A generator of IExecutableCommand
    """
    def build_and_yield(d):
        # Only DICT is allowed, and the two mandatory attributes, "command" and "content"
        if isinstance(d, dict) and "command" in d and "content" in d:
            if "label" in d:
                n = d["label"]
            else:
                n = None
            logging.debug(f"{d['command']} build and yield")
            yield create_command(d["command"], n, d["content"])

    json_commands = json.loads(input)  # Convert JSON string to dictionary
    if isinstance(json_commands, list):  # A sequence of primitive commands
        for i in json_commands:  # For each member of the list
            yield from build_and_yield(i)
    else:  # A single primitive command
        yield from build_and_yield(json_commands)
