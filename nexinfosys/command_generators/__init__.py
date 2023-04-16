from enum import Enum
from attr import attrs, attrib, validators
from openpyxl.utils import get_column_letter

import nexinfosys
from nexinfosys.common.helper import create_dictionary


class IType(Enum):
    INFO = 1
    WARNING = 2
    ERROR = 3


@attrs
class IssueLocation:
    sheet_name = attrib()
    sheet_number = attrib(default=None)
    row = attrib(default=None)
    column = attrib(default=None)

    def __str__(self):
        return f'(sheet_name="{self.sheet_name}", sheet_no={self.sheet_number}, row={self.row}, ' \
               f'column={get_column_letter(self.column) if self.column else "-"})'


@attrs
class Issue:
    # (1) Info, (2) Warning, (3) Error
    itype = attrib()  # type: IType
    # An english description of what happened
    description = attrib()  # type: str
    # Command type
    ctype = attrib(default=None)  # type: str
    # Where is the issue. The expression of the location depends. For spreadsheet it is sheet name, row and column
    location = attrib(default=None)  # type: IssueLocation

    def __str__(self):
        return f'(level={self.itype.name}, msg="{self.description}", cmd="{self.ctype}", {self.location})'

    @itype.validator
    def check(self, attribute, value):
        if not isinstance(value, nexinfosys.command_generators.IType):
            raise ValueError("itype should be an instance of enum IType")


# List of Global Functions (used by "parser_field_parsers.py" and "parser_ast_evaluators.py")
global_functions = create_dictionary(data={i["name"]: i for i in
                [{"name": "cos",
                  "full_name": "math.cos",
                  "kwargs": {}},
                 {"name": "sin",
                  "full_name": "math.sin",
                  "kwargs": {}},
                 {"name": "InterfaceType",
                  "full_name": "nexinfosys.command_generators.parser_ast_evaluators.get_interface_type",
                  "kwargs": {},
                  "special_kwargs": {"PartialRetrievalDictionary": "prd"}},
                 {"name": "NISName",
                  "full_name": "nexinfosys.command_generators.parser_ast_evaluators.get_nis_name",
                  "kwargs": {}},
                 {"name": "Processor",
                  "full_name": "nexinfosys.command_generators.parser_ast_evaluators.get_processor",
                  "kwargs": {},
                  "special_kwargs": {"PartialRetrievalDictionary": "prd"}},
                 {"name": "LCIAMethod",
                  "full_name": "nexinfosys.command_generators.parser_ast_evaluators.lcia_method",
                  "kwargs": {},
                  "special_kwargs": {"IndicatorState": "state",
                                     "LCIAMethods": "lcia_methods_dict"}},
                 {"name": "UDIF",  # "User Defined Indicator Function"
                  "full_name": "nexinfosys.command_generators.parser_ast_evaluators.call_udif_function",
                  "kwargs": {},
                  "special_kwargs": {"IndicatorState": "state"}},
                 ]
                })

# Functions used in expressions for global indicators
global_functions_extended = create_dictionary(data={i["name"]: i for i in
                [{"name": "sum",
                  "full_name": "nexinfosys.solving.flow_graph_outputs.aggregate_sum",
                  "kwargs": {},
                  "aggregate": True,
                  "special_kwargs": {"ProcessorsMap": "processors_map", "ProcessorsDOM": "processors_dom",
                                     "LCIAMethods": "lcia_methods", "PartialRetrievalDictionary": "registry",
                                     "DataFrameGroup": "df_group", "IndicatorDictionaries": "indicators_tmp",
                                     "ProcessorNames": "processor_names"}},
                 {"name": "avg",
                  "full_name": "nexinfosys.solving.flow_graph_outputs.aggregate_avg",
                  "kwargs": {},
                  "aggregate": True,
                  "special_kwargs": {"ProcessorsMap": "processors_map", "ProcessorsDOM": "processors_dom",
                                     "LCIAMethods": "lcia_methods", "PartialRetrievalDictionary": "registry",
                                     "DataFrameGroup": "df_group", "IndicatorDictionaries": "indicators_tmp",
                                     "ProcessorNames": "processor_names"}},
                 {"name": "max",
                  "full_name": "nexinfosys.solving.flow_graph_outputs.aggregate_max",
                  "kwargs": {},
                  "aggregate": True,
                  "special_kwargs": {"ProcessorsMap": "processors_map", "ProcessorsDOM": "processors_dom",
                                     "LCIAMethods": "lcia_methods", "PartialRetrievalDictionary": "registry",
                                     "DataFrameGroup": "df_group", "IndicatorDictionaries": "indicators_tmp",
                                     "ProcessorNames": "processor_names"}},
                 {"name": "min",
                  "full_name": "nexinfosys.solving.flow_graph_outputs.aggregate_min",
                  "kwargs": {},
                  "aggregate": True,
                  "special_kwargs": {"ProcessorsMap": "processors_map", "ProcessorsDOM": "processors_dom",
                                     "LCIAMethods": "lcia_methods", "PartialRetrievalDictionary": "registry",
                                     "DataFrameGroup": "df_group", "IndicatorDictionaries": "indicators_tmp",
                                     "ProcessorNames": "processor_names"}},
                 {"name": "count",
                  "full_name": "nexinfosys.solving.flow_graph_outputs.aggregate_count",
                  "kwargs": {},
                  "aggregate": True,
                  "special_kwargs": {"ProcessorsMap": "processors_map", "ProcessorsDOM": "processors_dom",
                                     "LCIAMethods": "lcia_methods", "PartialRetrievalDictionary": "registry",
                                     "DataFrameGroup": "df_group", "IndicatorDictionaries": "indicators_tmp",
                                     "ProcessorNames": "processor_names"}},
                 {"name": "nancount",
                  "full_name": "nexinfosys.solving.flow_graph_outputs.aggregate_nan_count",
                  "kwargs": {},
                  "aggregate": True,
                  "special_kwargs": {"ProcessorsMap": "processors_map", "ProcessorsDOM": "processors_dom",
                                     "LCIAMethods": "lcia_methods", "PartialRetrievalDictionary": "registry",
                                     "DataFrameGroup": "df_group", "IndicatorDictionaries": "indicators_tmp",
                                     "ProcessorNames": "processor_names"}},
                 ]
                })

_ = [k for k in global_functions.keys()]+[k for k in global_functions_extended.keys()]
extended_dict_of_function_names = create_dictionary(data={k: None for k in _})
_ = extended_dict_of_function_names
