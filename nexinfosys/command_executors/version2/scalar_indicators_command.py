from typing import Dict, Any

from nexinfosys.command_executors import BasicCommand
from nexinfosys.command_field_definitions import get_command_fields_from_class
from nexinfosys.command_generators import IType
from nexinfosys.common.helper import strcmp
from nexinfosys.models.musiasem_concepts import Indicator, IndicatorCategories, Benchmark


class ScalarIndicatorsCommand(BasicCommand):
    def __init__(self, name: str):
        BasicCommand.__init__(self, name, get_command_fields_from_class(self.__class__))

    def _process_row(self, fields: Dict[str, Any], subrow=None) -> None:
        """
        Create and register Indicator object

        :param fields:
        """
        benchmark_names = fields["benchmarks"]

        benchmarks = []
        if benchmark_names:
            for benchmark_name in benchmark_names.split(","):
                if benchmark_name:
                    benchmark = self._glb_idx.get(Benchmark.partial_key(benchmark_name))
                    if len(benchmark) == 1:
                        benchmark = benchmark[0]
                    elif len(benchmark) == 0:
                        self._add_issue(IType.ERROR,
                                        f"Benchmark {benchmark_name} does not exist (it must be declared previously in a "
                                        "ScalarBenchmark command worksheet")
                        return
                    elif len(benchmark) > 1:
                        self._add_issue(IType.ERROR,
                                        f"Benchmark {benchmark_name} exists {len(benchmark)} times."
                                        " Only one occurrence is allowed.")
                        return
                else:
                    benchmark = None
                if benchmark:
                    benchmarks.append(benchmark)

        attributes = self._transform_text_attributes_into_dictionary(fields["attributes"], subrow)

        if strcmp(fields.get("local"), "Yes") or strcmp(fields.get("local"), "Local"):
            indicator_type = IndicatorCategories.factors_expression
        elif strcmp(fields.get("local"), "No") or strcmp(fields.get("local"), "Global"):
            indicator_type = IndicatorCategories.case_study
        else:
            indicator_type = IndicatorCategories.system

        indicator = Indicator(fields["indicator_name"],
                              fields["formula"],
                              None,
                              fields.get("processors_selector"),
                              benchmarks,
                              indicator_type,
                              fields.get("description"),
                              fields["indicators_group"],
                              fields["unit"],
                              fields["unit_label"],
                              fields["source"],
                              fields["account_na"],
                              attributes)
        self._glb_idx.put(indicator.key(), indicator)
