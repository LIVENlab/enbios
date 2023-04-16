######################
#  LIST OF COMMANDS  #
######################
from typing import List

from nexinfosys import Command, regex_var_name, regex_hvar_name, regex_cplex_var, CommandType
from nexinfosys.command_generators.spreadsheet_command_parsers.external_data.etl_external_dataset_spreadsheet_parse import \
    parse_etl_external_dataset_command
from nexinfosys.command_generators.spreadsheet_command_parsers.external_data.mapping_spreadsheet_parse import \
    parse_mapping_command
from nexinfosys.command_generators.spreadsheet_command_parsers.specification.data_input_spreadsheet_parse import \
    parse_data_input_command
from nexinfosys.command_generators.spreadsheet_command_parsers.specification.hierarchy_spreadsheet_parser import \
    parse_hierarchy_command
from nexinfosys.command_generators.spreadsheet_command_parsers.specification.metadata_spreadsheet_parse import \
    parse_metadata_command
from nexinfosys.command_generators.spreadsheet_command_parsers.specification.pedigree_matrix_spreadsheet_parse import \
    parse_pedigree_matrix_command
from nexinfosys.command_generators.spreadsheet_command_parsers.specification.references_spreadsheet_parser import \
    parse_references_command
from nexinfosys.command_generators.spreadsheet_command_parsers.specification.scale_conversion_spreadsheet_parse import \
    parse_scale_conversion_command
from nexinfosys.command_generators.spreadsheet_command_parsers.specification.structure_spreadsheet_parser import \
    parse_structure_command
from nexinfosys.command_generators.spreadsheet_command_parsers.specification.upscale_spreadsheet_parse import \
    parse_upscale_command
from nexinfosys.command_generators.spreadsheet_command_parsers_v2.dataset_data_spreadsheet_parse import \
    parse_dataset_data_command
from nexinfosys.command_generators.spreadsheet_command_parsers_v2.dataset_qry_spreadsheet_parse import \
    parse_dataset_qry_command

# BE CAREFUL: the order of the commands in the list MATTERS because it is used to prioritize the detection
# of the command based on the worksheet name and the allowed_names / alt_regex of the command.
# E.g. the command "cat_hier_mapping" allows a worksheet name with the pattern "HierarchiesMappings.*" and the command
# "cat_hierarchies" allows the pattern "Hierarchies.*". An actual worksheet named "HierarchiesMappings Spain" would
# match both patterns, so the command of the larger pattern should come first in the list below.
commands: List[Command] = [
    Command(name="dummy", allowed_names=["Dummy"], is_v1=True,
            cmd_type=CommandType.misc,
            execution_class_name="nexinfosys.command_executors.specification.dummy_command.DummyCommand"),

    # Version 1 and maybe also Version 2

    Command(name="references", allowed_names=["References"], is_v1=True, is_v2=True,
            cmd_type=CommandType.metadata,
            execution_class_name="nexinfosys.command_executors.specification.references_command.ReferencesCommand",
            parse_function=parse_references_command,
            alt_regex=r"(References|Ref)[ _]" + regex_var_name),

    # Version 1 and Version 2

    Command(name="metadata", allowed_names=["Metadata"], is_v1=True, is_v2=True,
            cmd_type=CommandType.metadata,
            execution_class_name="nexinfosys.command_executors.specification.metadata_command.MetadataCommand",
            parse_function=parse_metadata_command),

    Command(name="pedigree_matrix", allowed_names=["Pedigree"], is_v1=True, is_v2=True,
            cmd_type=CommandType.metadata,
            execution_class_name="nexinfosys.command_executors.specification.pedigree_matrix_command.PedigreeMatrixCommand",
            parse_function=parse_pedigree_matrix_command,
            alt_regex=r"(PedigreeV1|Ped|NUSAP\.PM)[ _]+" + regex_var_name),

    # Version 2 only

    Command(name="cat_hier_mapping",
            allowed_names=["CodeHierarchiesMapping", "HierarchiesMapping"],
            is_v2=True, cmd_type=CommandType.input,
            execution_class_name="nexinfosys.command_executors.version2.hierarchy_mapping_command.HierarchyMappingCommand"),

    Command(name="cat_hierarchies", allowed_names=["CodeHierarchies", "Hierarchies"],
            is_v2=True, cmd_type=CommandType.input,
            execution_class_name="nexinfosys.command_executors.version2.hierarchy_categories_command.HierarchyCategoriesCommand"),

    # Command(name="attribute_types", allowed_names=["AttributeTypes"], is_v2=True,
    #         cmd_type=CommandType.misc,
    #         execution_class_name="nexinfosys.command_executors.version2.attribute_types_command.AttributeTypesCommand"),

    Command(name="parameters", allowed_names=["Parameters", "Params"], is_v2=True,
            cmd_type=CommandType.input,
            execution_class_name="nexinfosys.command_executors.external_data.parameters_command.ParametersCommand"),

    Command(name="datasetdef", allowed_names=["DatasetDef"], is_v2=True,
            cmd_type=CommandType.input,
            execution_class_name="nexinfosys.command_executors.version2.dataset_definition_command.DatasetDefCommand"),

    # Command(name="attribute_sets", allowed_names=["AttributeSets"], is_v2=True,
    #         cmd_type=CommandType.misc,
    #         execution_class_name="nexinfosys.command_executors.version2.attribute_sets_command.AttributeSetsCommand"),

    Command(name="interface_types", allowed_names=["InterfaceTypes"], is_v2=True,
            cmd_type=CommandType.core,
            execution_class_name="nexinfosys.command_executors.version2.interface_types_command.InterfaceTypesCommand"),

    Command(name="processors", allowed_names=["BareProcessors"], is_v2=True,
            cmd_type=CommandType.core,
            execution_class_name="nexinfosys.command_executors.version2.processors_command.ProcessorsCommand"),

    Command(name="interfaces_and_qq", allowed_names=["Interfaces"], is_v2=True,
            cmd_type=CommandType.core,
            execution_class_name="nexinfosys.command_executors.version2.interfaces_command.InterfacesAndQualifiedQuantitiesCommand"),

    Command(name="relationships", allowed_names=["Relationships", "Relationship", "Flows"], is_v2=True,
            cmd_type=CommandType.core,
            execution_class_name="nexinfosys.command_executors.version2.relationships_command.RelationshipsCommand"),

    Command(name="processor_scalings", allowed_names=["ProcessorScalings", "ProcessorScaling"], is_v2=True,
            cmd_type=CommandType.core,
            execution_class_name="nexinfosys.command_executors.version2.processor_scalings_command.ProcessorScalingsCommand"),

    Command(name="scale_conversion_v2", allowed_names=["ScaleChangeMap", "ScaleChangeMaps"], is_v2=True,
            cmd_type=CommandType.core,
            execution_class_name="nexinfosys.command_executors.version2.scale_conversion_v2_command.ScaleConversionV2Command"),

    Command(name="problem_statement", allowed_names=["ProblemStatement"], is_v2=True,
            cmd_type=CommandType.analysis,
            execution_class_name="nexinfosys.command_executors.version2.problem_statement_command.ProblemStatementCommand"),

    Command(name="scalar_indicator_benchmarks", allowed_names=["ScalarBenchmarks"], is_v2=True,
            cmd_type=CommandType.analysis,
            execution_class_name="nexinfosys.command_executors.version2.scalar_indicator_benchmarks_command.ScalarIndicatorBenchmarksCommand"),

    Command(name="scalar_indicators", allowed_names=["ScalarIndicators"], is_v2=True,
            cmd_type=CommandType.analysis,
            execution_class_name="nexinfosys.command_executors.version2.scalar_indicators_command.ScalarIndicatorsCommand"),

    Command(name="matrix_indicators", allowed_names=["MatrixIndicators"], is_v2=True,
            cmd_type=CommandType.analysis,
            execution_class_name="nexinfosys.command_executors.version2.matrix_indicators_command.MatrixIndicatorsCommand"),

    Command(name="lcia_methods", allowed_names=["LCIAMethods"], is_v2=True,
            cmd_type=CommandType.analysis,
            execution_class_name="nexinfosys.command_executors.version2.lcia_methods_command.LCIAMethodsCommand"),

    Command(name="datasetdata", allowed_names=["DatasetData"], is_v2=True,
            cmd_type=CommandType.input,
            execution_class_name="nexinfosys.command_executors.version2.dataset_data_command.DatasetDataCommand",
            parse_function=parse_dataset_data_command),

    Command(name="datasetqry", allowed_names=["DatasetQry"], is_v2=True,
            cmd_type=CommandType.input,
            execution_class_name="nexinfosys.command_executors.version2.dataset_query_command.DatasetQryCommand",
            parse_function=parse_dataset_qry_command),

    Command(name="ref_pedigree_matrices", allowed_names=["PedigreeMatrices"], is_v2=True,
            cmd_type=CommandType.metadata,
            execution_class_name="nexinfosys.command_executors.version2.pedigree_matrices_command.PedigreeMatricesReferencesCommand"),

    Command(name="ref_provenance", allowed_names=["RefProvenance"], is_v2=True,
            cmd_type=CommandType.metadata,
            execution_class_name="nexinfosys.command_executors.version2.references_v2_command.ProvenanceReferencesCommand"),

    Command(name="ref_geographical", allowed_names=["RefGeographic", "RefGeography"], is_v2=True,
            cmd_type=CommandType.metadata,
            execution_class_name="nexinfosys.command_executors.version2.references_v2_command.GeographicReferencesCommand"),

    Command(name="ref_bibliographic", allowed_names=["RefBibliographic", "RefBibliography"], is_v2=True,
            cmd_type=CommandType.metadata,
            execution_class_name="nexinfosys.command_executors.version2.references_v2_command.BibliographicReferencesCommand"),

    Command(name="list_of_commands",
            allowed_names=["ListOfCommands"],
            is_v2=True, cmd_type=CommandType.convenience,
            execution_class_name=None),

    Command(name="import_commands",
            allowed_names=["ImportCommands"],
            is_v2=True, cmd_type=CommandType.convenience,
            execution_class_name=None),

    # Version 1 only

    Command(name="data_input", allowed_names=["DataInput"], is_v1=True,
            cmd_type=CommandType.input,
            execution_class_name="nexinfosys.command_executors.specification.data_input_command.DataInputCommand",
            parse_function=parse_data_input_command,
            alt_regex=r"(Processors|Proc)[ _]+" + regex_var_name),

    Command(name="hierarchy", allowed_names=["Hierarchy"], is_v1=True,
            cmd_type=CommandType.core,
            execution_class_name="nexinfosys.command_executors.specification.hierarchy_command.HierarchyCommand",
            parse_function=parse_hierarchy_command,
            alt_regex=r"(Taxonomy|Tax|Composition|Comp)[ _]([cpf])[ ]" + regex_var_name),

    Command(name="upscale", allowed_names=["Upscale"], is_v1=True,
            cmd_type=CommandType.core,
            execution_class_name="nexinfosys.command_executors.specification.upscale_command.UpscaleCommand",
            parse_function=parse_upscale_command,
            alt_regex=r"(Upscale|Up)[ _](" + regex_var_name + "[ _]" + regex_var_name + ")?"),

    Command(name="structure", allowed_names=["Structure"], is_v1=True,
            cmd_type=CommandType.core,
            execution_class_name="nexinfosys.command_executors.specification.structure_command.StructureCommand",
            parse_function=parse_structure_command,
            alt_regex=r"(Grammar|Structure)([ _]+" + regex_var_name + ")?"),

    Command(name="scale_conversion", allowed_names=["Scale"], is_v1=True,
            cmd_type=CommandType.core,
            execution_class_name="nexinfosys.command_executors.specification.scale_conversion_command.ScaleConversionCommand",
            parse_function=parse_scale_conversion_command,
            alt_regex=r"Scale"),

    Command(name="etl_dataset", allowed_names=["Dataset"], is_v1=True,
            cmd_type=CommandType.input,
            execution_class_name="nexinfosys.command_executors.external_data.etl_external_dataset_command.ETLExternalDatasetCommand",
            parse_function=parse_etl_external_dataset_command,
            alt_regex=r"(Dataset|DS)[ _]" + regex_hvar_name),

    Command(name="mapping", allowed_names=["Mapping"], is_v1=True,
            cmd_type=CommandType.core,
            execution_class_name="nexinfosys.command_executors.external_data.mapping_command.MappingCommand",
            parse_function=parse_mapping_command,
            alt_regex=r"(Mapping|Map)([ _]" + regex_cplex_var + "[ _]" + regex_cplex_var + ")?")

]


valid_v2_command_names: List[str] = [label for cmd in commands if cmd.is_v2 for label in cmd.allowed_names]
