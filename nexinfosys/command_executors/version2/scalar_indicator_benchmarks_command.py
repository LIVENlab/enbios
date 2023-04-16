from typing import Dict, Any

from nexinfosys.command_executors import BasicCommand, subrow_issue_message
from nexinfosys.command_field_definitions import get_command_fields_from_class
from nexinfosys.command_generators import IType
from nexinfosys.common.helper import create_dictionary
from nexinfosys.models.musiasem_concepts import Benchmark


class ScalarIndicatorBenchmarksCommand(BasicCommand):
    def __init__(self, name: str):
        BasicCommand.__init__(self, name, get_command_fields_from_class(self.__class__))

    def _process_row(self, fields: Dict[str, Any], subrow=None) -> None:
        """
        Create and register Benchmark object

        :param fields:
        """
        name = fields["benchmark"]
        benchmark_group = fields["benchmark_group"]
        stakeholders = fields["stakeholders"]
        b = self._glb_idx.get(Benchmark.partial_key(name=name))
        if len(b) == 1:
            b = b[0]
        elif len(b) == 0:
            b = Benchmark(name, benchmark_group, stakeholders.split(",") if stakeholders else [])
            self._glb_idx.put(b.key(), b)
        else:
            self._add_issue(IType.ERROR,
                            f"There are {len(b)} instances of the Benchmark '{name}'"+subrow_issue_message(subrow))
            return

        # Add range, if not repeated
        category = fields["category"]
        if category not in b.ranges:
            b.ranges[category] = create_dictionary(
                data=dict(range=fields["range"],
                          unit=fields["unit"],
                          category=category,
                          label=fields["label"],
                          description=fields["description"])
            )
        else:
            self._add_issue(IType.WARNING,
                            f"Range with category '{category}' repeated"+subrow_issue_message(subrow))

