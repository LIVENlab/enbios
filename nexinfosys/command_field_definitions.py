######################################
#  LIST OF FIELDS FOR THE COMMANDS   #
######################################
import logging
from typing import Dict, List, Type

from nexinfosys import CommandField
from nexinfosys.command_definitions import valid_v2_command_names, commands
from nexinfosys.command_generators.parser_field_parsers import simple_ident, unquoted_string, alphanums_string, \
    hierarchy_expression_v2, key_value_list, key_value, expression_with_parameters, \
    time_expression, indicator_expression, code_string, simple_h_name, domain_definition, unit_name, url_parser, \
    processor_names, value, list_simple_ident, reference, processor_name, processors_selector_expression, \
    interfaces_list_expression, attributes_list_expression, indicators_list_expression, number_interval, pair_numbers, \
    external_ds_name, level_name, expression_with_parameters_or_list_simple_ident, signed_float
from nexinfosys.common.constants import SubsystemType, Scope
from nexinfosys.common.helper import first, class_full_name
from nexinfosys.model_services import IExecutableCommand
from nexinfosys.models.musiasem_concepts import Processor, Factor, RelationClassType, FactorType

data_types = ["Number", "Boolean", "URL", "UUID", "Datetime", "String", "UnitName", "Code", "Geo"]
concept_types = ["Dimension", "Measure", "Attribute", "Dataset"]
parameter_types = ["Number", "Code", "Boolean", "String"]
element_types = ["Parameter", "Processor", "InterfaceType", "Interface"]
spheres = ["Biosphere", "Technosphere"]
roegen_types = ["Flow", "Fund"]
orientations = ["Input", "Output"]
yes_no = ["Yes", "No"]  # Default "Yes"
no_yes = ["No", "Yes"]  # Default "No"
functional_or_structural = ["Functional", "Structural", "Notional"]
scalar_indicator_type = ["Yes", "No", "Local", "System", "Global"]  # Default "Yes"
instance_or_archetype = ["Yes", "No", "Instance",
                         "Archetype"]  # Yes/No -> Instead of "instance/archetype", now the field is "Accounted"
copy_interfaces_mode = ["No", "FromParent", "FromChildren", "Bidirectional"]
source_cardinalities = ["One", "Zero", "ZeroOrOne", "ZeroOrMore", "OneOrMore"]
target_cardinalities = source_cardinalities
processor_scaling_types = ["CloneAndScale", "Scale", "Clone", "CloneScaled"]
agent_types = ["Person", "Software", "Organization"]
geographic_resource_types = ["dataset"]
geographic_topic_categories = ["Farming", "Biota", "Boundaries", "Climatology", "Meteorology", "Atmosphere", "Economy",
                               "Elevation", "Environment", "GeoscientificInformation", "Health", "Imagery", "BaseMaps",
                               "EarthCover", "Intelligence", "Military", "InlandWaters", "Location", "Oceans",
                               "Planning", "Cadastre", "Society", "Structure", "Transportation", "Utilities",
                               "Communication"]
bib_entry_types = ["article", "book", "booklet", "conference", "inbook", "incollection", "inproceedings",
                   "manual", "mastersthesis", "misc", "phdtesis", "proceedings", "techreport", "unpublished"]
bib_months = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
benchmark_groups = ["Feasibility", "Viability", "Desirability"]
aggregators_list = ["Sum", "Avg", "Count", "SumNA", "CountAv", "AvgNA", "PctNA"]

attributeRegex = "@.+"

# Version 2 only
command_fields: Dict[str, List[CommandField]] = {

    "cat_hierarchies": [
        CommandField(allowed_names=["Source"], name="source", parser=simple_h_name),
        CommandField(allowed_names=["HierarchyGroup"], name="hierarchy_group", parser=simple_ident),
        CommandField(allowed_names=["Hierarchy", "HierarchyName"], name="hierarchy_name", mandatory=True,
                     parser=simple_ident),
        CommandField(allowed_names=["Level", "LevelCode"], name="level", parser=alphanums_string),
        CommandField(allowed_names=["ReferredHierarchy"], name="referred_hierarchy", parser=simple_h_name),
        CommandField(allowed_names=["Code"], name="code", parser=code_string),
        # NOTE: Removed because parent code must be already a member of the hierarchy being defined
        # CommandField(allowed_names=["ReferredHierarchyParent"], name="referred_hierarchy_parent", parser=simple_ident),
        CommandField(allowed_names=["ParentCode"], name="parent_code", parser=code_string),
        CommandField(allowed_names=["Label"], name="label", parser=unquoted_string),
        CommandField(allowed_names=["Description"], name="description", parser=unquoted_string),
        CommandField(allowed_names=["Expression", "Formula"], name="expression", parser=hierarchy_expression_v2),
        CommandField(allowed_names=["GeolocationRef"], name="geolocation_ref", parser=reference),
        CommandField(allowed_names=["GeolocationCode"], name="geolocation_code", parser=code_string),
        CommandField(allowed_names=[attributeRegex], name="attributes", many_appearances=True, parser=value),
        CommandField(allowed_names=["Attributes"], name="attributes", parser=key_value_list)
    ],

    "cat_hier_mapping": [
        CommandField(allowed_names=["OriginDataset"], name="source_dataset", parser=external_ds_name),
        CommandField(allowed_names=["OriginHierarchy"], name="source_hierarchy", mandatory=True, parser=simple_ident),
        CommandField(allowed_names=["OriginCode"], name="source_code", mandatory=True, parser=code_string),
        CommandField(allowed_names=["DestinationHierarchy"], name="destination_hierarchy", mandatory=True,
                     parser=simple_ident),
        CommandField(allowed_names=["DestinationCode"], name="destination_code", mandatory=True, parser=code_string),
        CommandField(allowed_names=["Weight"], name="weight", mandatory=False, default_value="1",
                     parser=expression_with_parameters),
    ],

    "attribute_types": [
        CommandField(allowed_names=["AttributeType", "AttributeTypeName"], name="attribute_type_name", mandatory=True,
                     parser=simple_ident),
        CommandField(allowed_names=["Type"], name="data_type", mandatory=True, allowed_values=data_types,
                     parser=simple_ident),
        CommandField(allowed_names=["ElementTypes"], name="element_types", default_value=element_types[0],
                     allowed_values=element_types, parser=list_simple_ident),
        CommandField(allowed_names=["Domain"], name="domain", parser=domain_definition)
        # "domain_definition" for Category and NUmber. Boolean is only True or False. Other data types cannot be easily constrained (URL, UUID, Datetime, Geo, String)
    ],

    "datasetdef": [
        CommandField(allowed_names=["Dataset", "DatasetName"], name="dataset_name", mandatory=True,
                     parser=simple_ident),
        CommandField(allowed_names=["DatasetDataLocation"], name="dataset_data_location", parser=url_parser),
        CommandField(allowed_names=["ConceptType"], name="concept_type", mandatory=True, allowed_values=concept_types,
                     parser=simple_ident),
        CommandField(allowed_names=["Concept", "ConceptName"], name="concept_name", mandatory=True,
                     parser=simple_ident),
        CommandField(allowed_names=["ConceptDataType", "DataType"], name="concept_data_type", mandatory=True,
                     allowed_values=data_types, parser=simple_ident),
        CommandField(allowed_names=["ConceptDomain", "Domain"], name="concept_domain", parser=domain_definition),
        CommandField(allowed_names=["ConceptDescription", "Description"], name="concept_description",
                     parser=unquoted_string),
        CommandField(allowed_names=[attributeRegex], name="attributes", many_appearances=True, parser=value),
        CommandField(allowed_names=["ConceptAttributes", "Attributes"], name="attributes", parser=key_value_list)
    ],

    "attribute_sets": [
        CommandField(allowed_names=["AttributeSetName"], name="attribute_set_name", mandatory=True,
                     parser=simple_ident),
        CommandField(allowed_names=[attributeRegex], name="attributes", many_appearances=True, parser=value),
        CommandField(allowed_names=["Attributes"], name="attributes", parser=key_value_list)
    ],

    "parameters": [
        CommandField(allowed_names=["Parameter", "ParameterName"], name="name", mandatory=True, parser=simple_ident),
        CommandField(allowed_names=["Type"], name="type", mandatory=True, allowed_values=parameter_types,
                     parser=simple_ident),
        CommandField(allowed_names=["Domain"], name="domain", parser=domain_definition),
        CommandField(allowed_names=["Value"], name="value", parser=expression_with_parameters),
        CommandField(allowed_names=["Group"], name="group", parser=simple_ident),
        CommandField(allowed_names=["Description"], name="description", parser=unquoted_string),
        CommandField(allowed_names=[attributeRegex], name="attributes", many_appearances=True, parser=value),
        CommandField(allowed_names=["Attributes"], name="attributes", parser=key_value_list)
    ],

    "interface_types": [
        CommandField(allowed_names=["InterfaceTypeHierarchy"], name="interface_type_hierarchy", parser=simple_ident),
        CommandField(allowed_names=["InterfaceType"], name="interface_type", mandatory=True, parser=simple_ident),
        CommandField(allowed_names=["ParentInterfaceType"], name="parent_interface_type", parser=simple_ident),
        CommandField(allowed_names=["Sphere"], name="sphere", mandatory=True, allowed_values=spheres,
                     parser=simple_ident),
        CommandField(allowed_names=["RoegenType"], name="roegen_type", mandatory=True, allowed_values=roegen_types,
                     parser=simple_ident),
        CommandField(allowed_names=["Description"], name="description", parser=unquoted_string),
        CommandField(allowed_names=["Source"], name="qq_source", parser=reference),
        # Cristina (in "MuSIASEM Interface List" worksheet)
        CommandField(allowed_names=["Unit"], name="unit", mandatory=True, parser=unit_name),
        CommandField(allowed_names=["OppositeSubsystemType", "OppositeProcessorType"], name="opposite_processor_type",
                     allowed_values=SubsystemType.get_names(), parser=simple_ident),
        CommandField(allowed_names=["Level"], name="level", parser=level_name, attribute_of=FactorType),
        CommandField(allowed_names=["Formula", "Expression"], name="formula", parser=unquoted_string),
        CommandField(allowed_names=[attributeRegex], name="attributes", many_appearances=True, parser=value),
        CommandField(allowed_names=["Attributes"], name="attributes", parser=key_value_list)
    ],

    "processors": [
        CommandField(allowed_names=["ProcessorGroup"], name="processor_group", parser=simple_ident),
        CommandField(allowed_names=["Processor"], name="processor", mandatory=True, parser=processor_name),
        CommandField(allowed_names=["ParentProcessor"], name="parent_processor", parser=processor_name),
        # CommandField(allowed_names=["CopyInterfaces"], name="copy_interfaces_mode",
        #              default_value=copy_interfaces_mode[0], allowed_values=copy_interfaces_mode, parser=simple_ident),
        # CommandField(allowed_names=["CloneProcessor"], name="clone_processor", parser=simple_ident),
        CommandField(allowed_names=["SubsystemType", "ProcessorContextType", "ProcessorType"], name="subsystem_type",
                     default_value=SubsystemType.get_names()[0], allowed_values=SubsystemType.get_names(),
                     parser=simple_ident,
                     attribute_of=Processor),
        CommandField(allowed_names=["System"], name="processor_system", default_value="default",
                     parser=simple_ident, attribute_of=Processor),
        CommandField(allowed_names=["FunctionalOrStructural"], name="functional_or_structural",
                     default_value=functional_or_structural[0], allowed_values=functional_or_structural,
                     parser=simple_ident, attribute_of=Processor),
        CommandField(allowed_names=["Accounted", "InstanceOrArchetype"], name="instance_or_archetype",
                     default_value=instance_or_archetype[0], allowed_values=instance_or_archetype, parser=simple_ident,
                     attribute_of=Processor),
        CommandField(allowed_names=["ParentProcessorWeight"], name="parent_processor_weight",
                     parser=expression_with_parameters),
        CommandField(allowed_names=["BehaveAs"], name="behave_as_processor", parser=processor_name),
        CommandField(allowed_names=["Level"], name="level", parser=level_name, attribute_of=Processor),
        CommandField(allowed_names=["Stock"], name="stock", default_value=no_yes[0], allowed_values=no_yes,
                     deprecated=True,
                     parser=simple_ident, attribute_of=Processor),
        # CommandField(allowed_names=["Alias", "SpecificName"], name="alias", parser=simple_ident),
        CommandField(allowed_names=["Description"], name="description", parser=unquoted_string),
        CommandField(allowed_names=["GeolocationRef"], name="geolocation_ref", parser=reference),
        CommandField(allowed_names=["GeolocationCode"], name="geolocation_code", parser=code_string),
        CommandField(allowed_names=["GeolocationLatLong"], name="geolocation_latlong", parser=pair_numbers),
        CommandField(allowed_names=[attributeRegex], name="attributes", many_appearances=True, parser=value),
        CommandField(allowed_names=["Attributes"], name="attributes", parser=key_value_list)
    ],

    "interfaces_and_qq": [
        CommandField(allowed_names=["Processor"], name="processor", mandatory=True, parser=processor_name),
        CommandField(allowed_names=["InterfaceType"], name="interface_type", parser=simple_ident),
        CommandField(allowed_names=["Interface"], name="interface", parser=simple_ident),
        CommandField(allowed_names=["Sphere"], name="sphere", allowed_values=spheres, parser=simple_ident,
                     attribute_of=Factor),
        CommandField(allowed_names=["RoegenType"], name="roegen_type", allowed_values=roegen_types, parser=simple_ident,
                     attribute_of=Factor),
        CommandField(allowed_names=["Orientation"], name="orientation", mandatory=True, allowed_values=orientations,
                     parser=simple_ident, attribute_of=Factor),
        CommandField(allowed_names=["OppositeSubsystemType", "OppositeProcessorType"], name="opposite_processor_type",
                     allowed_values=SubsystemType.get_names(), parser=simple_ident, attribute_of=Factor),
        CommandField(allowed_names=["GeolocationRef"], name="geolocation_ref", parser=reference),
        CommandField(allowed_names=["GeolocationCode"], name="geolocation_code", parser=code_string),
        # CommandField(allowed_names=["Alias", "SpecificName"], name="alias", parser=simple_ident),
        CommandField(allowed_names=["I" + attributeRegex], name="interface_attributes", many_appearances=True,
                     parser=value),
        CommandField(allowed_names=["Range"], name="range", parser=number_interval),
        CommandField(allowed_names=["RangeUnit"], name="range_unit", parser=unit_name),
        CommandField(allowed_names=["InterfaceAttributes"], name="interface_attributes", parser=key_value_list),
        # Qualified Quantification
        CommandField(allowed_names=["Value"], name="value", parser=expression_with_parameters),
        CommandField(allowed_names=["Unit"], name="unit", parser=unit_name),
        CommandField(allowed_names=["RelativeTo"], name="relative_to", parser=unquoted_string),
        CommandField(allowed_names=["Uncertainty"], name="uncertainty", parser=unquoted_string),
        CommandField(allowed_names=["Assessment"], name="assessment", parser=unquoted_string),
        # TODO
        # CommandField(allowed_names=["Pedigree"], name="pedigree", parser=pedigree_code),
        # CommandField(allowed_names=["RelativeTo"], name="relative_to", parser=simple_ident_plus_unit_name),
        CommandField(allowed_names=["PedigreeMatrix"], name="pedigree_matrix", parser=reference),
        CommandField(allowed_names=["Pedigree"], name="pedigree", parser=unquoted_string),
        CommandField(allowed_names=["Time"], name="time", parser=time_expression),
        CommandField(allowed_names=["Source"], name="qq_source", parser=reference),
        CommandField(allowed_names=["N" + attributeRegex], name="number_attributes", many_appearances=True,
                     parser=key_value),
        CommandField(allowed_names=["NumberAttributes"], name="number_attributes", parser=key_value_list),
        CommandField(allowed_names=["Comments"], name="comments", parser=unquoted_string)
    ],

    "relationships": [
        CommandField(allowed_names=["OriginProcessors", "OriginProcessor"], name="source_processor", mandatory=True,
                     parser=processor_names),
        CommandField(allowed_names=["OriginInterface"], name="source_interface", parser=simple_ident),
        CommandField(allowed_names=["DestinationProcessors", "DestinationProcessor"], name="target_processor",
                     mandatory=True, parser=processor_names),
        CommandField(allowed_names=["DestinationInterface"], name="target_interface", parser=simple_ident),
        CommandField(allowed_names=["BackInterface"], name="back_interface", parser=simple_ident),
        CommandField(allowed_names=["RelationType"], name="relation_type", mandatory=True,
                     allowed_values=RelationClassType.relationships_command_labels(), parser=unquoted_string),
        CommandField(allowed_names=["Weight"], name="flow_weight", parser=expression_with_parameters),
        CommandField(allowed_names=["ChangeOfTypeScale"], name="change_type_scale", parser=expression_with_parameters),
        CommandField(allowed_names=["OriginCardinality"], name="source_cardinality",
                     default_value=source_cardinalities[0],
                     allowed_values=source_cardinalities, parser=simple_ident),
        CommandField(allowed_names=["DestinationCardinality"], name="target_cardinality",
                     default_value=target_cardinalities[0],
                     allowed_values=target_cardinalities, parser=simple_ident),
        CommandField(allowed_names=["Attributes"], name="attributes", parser=key_value_list)
    ],

    "processor_scalings": [
        CommandField(allowed_names=["InvokingProcessor"], name="invoking_processor", mandatory=True,
                     parser=processor_name),
        CommandField(allowed_names=["RequestedProcessor"], name="requested_processor", mandatory=True,
                     parser=processor_name),
        CommandField(allowed_names=["ScalingType"], name="scaling_type", mandatory=True,
                     allowed_values=processor_scaling_types, parser=simple_ident),
        CommandField(allowed_names=["InvokingInterface"], name="invoking_interface", mandatory=True,
                     parser=simple_ident),
        CommandField(allowed_names=["RequestedInterface"], name="requested_interface", mandatory=True,
                     parser=simple_ident),
        CommandField(allowed_names=["Scale"], name="scale", mandatory=True, parser=expression_with_parameters),
        # BareProcessor fields
        CommandField(allowed_names=["NewProcessorName"], name="new_processor_name", parser=processor_name),
        CommandField(allowed_names=["NewProcessorGroup"], name="processor_group", parser=simple_ident,
                     attribute_of=Processor),
        CommandField(allowed_names=["NewParentProcessor"], name="parent_processor", parser=processor_name),
        CommandField(allowed_names=["NewSubsystemType"], name="subsystem_type",
                     default_value=SubsystemType.get_names()[0], allowed_values=SubsystemType.get_names(),
                     parser=simple_ident,
                     attribute_of=Processor),
        CommandField(allowed_names=["NewProcessorLevel"], name="level", parser=level_name, attribute_of=Processor),
        # DISABLED because this fields apply to processors Cloned and Scaled, so they will always have a parent,
        #          and children inherit the system of the parent.
        # CommandField(allowed_names=["NewSystem"], name="processor_system", default_value="_default_system",
        #              parser=simple_ident, attribute_of=Processor),
        CommandField(allowed_names=["NewDescription"], name="description", parser=unquoted_string,
                     attribute_of=Processor),
        CommandField(allowed_names=["NewGeolocationRef"], name="geolocation_ref", parser=reference,
                     attribute_of=Processor),
        CommandField(allowed_names=["NewGeolocationCode"], name="geolocation_code", parser=code_string,
                     attribute_of=Processor),
        CommandField(allowed_names=[attributeRegex], name="attributes", many_appearances=True, parser=value),
        CommandField(allowed_names=["NewAttributes"], name="attributes", parser=key_value_list)
        # CommandField(allowed_names=["UpscaleParentContext"], name="upscale_parent_context", parser=upscale_context),
        # CommandField(allowed_names=["UpscaleChildContext"], name="upscale_child_context", parser=upscale_context)
    ],

    "scale_conversion_v2": [
        CommandField(allowed_names=["OriginHierarchy"], name="source_hierarchy", parser=simple_ident),
        CommandField(allowed_names=["OriginInterfaceType"], name="source_interface_type", parser=simple_ident),
        CommandField(allowed_names=["DestinationHierarchy"], name="target_hierarchy", parser=simple_ident),
        CommandField(allowed_names=["DestinationInterfaceType"], name="target_interface_type", parser=simple_ident),
        CommandField(allowed_names=["OriginContext"], name="source_context", parser=processor_names),
        CommandField(allowed_names=["DestinationContext"], name="target_context", parser=processor_names),
        CommandField(allowed_names=["Scale"], name="scale", mandatory=True, parser=expression_with_parameters),
        CommandField(allowed_names=["OriginUnit"], name="source_unit", parser=unit_name),
        CommandField(allowed_names=["DestinationUnit"], name="target_unit", parser=unit_name)
    ],

    "import_commands": [
        CommandField(allowed_names=["Workbook", "WorkbookLocation"], name="workbook_name", parser=url_parser),
        CommandField(allowed_names=["Worksheets"], name="worksheets", parser=unquoted_string)
    ],

    "list_of_commands": [
        CommandField(allowed_names=["Worksheet", "WorksheetName"], name="worksheet", mandatory=True,
                     parser=unquoted_string),
        CommandField(allowed_names=["Command"], name="command", mandatory=True, allowed_values=valid_v2_command_names,
                     parser=simple_ident),
        CommandField(allowed_names=["Comment", "Description"], name="comment", parser=unquoted_string)
    ],

    "ref_provenance": [
        # Reduced, from W3C Provenance Recommendation (https://www.w3.org/TR/prov-overview/)
        CommandField(allowed_names=["RefID", "Reference"], name="ref_id", mandatory=True, parser=simple_ident),
        # The reference "RefID" should be mentioned
        CommandField(allowed_names=["ProvenanceFileURL"], name="provenance_file_url", parser=url_parser),
        CommandField(allowed_names=["AgentType"], name="agent_type", mandatory=True, allowed_values=agent_types,
                     parser=simple_ident),
        CommandField(allowed_names=["Agent"], name="agent", mandatory=True, parser=unquoted_string),
        CommandField(allowed_names=["Activities"], name="activities", mandatory=True, parser=unquoted_string),
        CommandField(allowed_names=["Entities"], name="entities", parser=unquoted_string)
    ],

    "ref_geographical": [
        # A subset of fields from INSPIRE regulation for metadata: https://eur-lex.europa.eu/legal-content/EN/TXT/PDF/?uri=CELEX:32008R1205&from=EN
        # Fields useful to elaborate graphical displays. Augment in the future as demanded
        CommandField(allowed_names=["RefID", "Reference"], name="ref_id", mandatory=True, parser=simple_ident),
        CommandField(allowed_names=["GeoLayerURL", "DataLocation", "ResourceLocator"], name="data_location",
                     parser=url_parser),
        CommandField(allowed_names=["Title"], name="title", mandatory=True, parser=unquoted_string),
        CommandField(allowed_names=["Description", "Abstract"], name="description", parser=unquoted_string),  # Syntax??
        CommandField(allowed_names=["BoundingBox"], name="bounding_box", parser=unquoted_string),  # Syntax??
        CommandField(allowed_names=["TopicCategory"], name="topic_category",
                     default_value=geographic_topic_categories[0],
                     allowed_values=geographic_topic_categories, parser=unquoted_string),  # Part D.2
        CommandField(allowed_names=["TemporalExtent", "Date"], name="temporal_extent", parser=unquoted_string),
        # Syntax??
        CommandField(allowed_names=["PointOfContact"], name="point_of_contact", parser=unquoted_string),
        CommandField(allowed_names=["Type"], name="type", default_value=geographic_resource_types[0],
                     allowed_values=geographic_resource_types, parser=unquoted_string)  # Part D.1. JUST "Dataset"
    ],

    "ref_bibliographic": [
        # From BibTex. Mandatory fields depending on EntryType, at "https://en.wikipedia.org/wiki/BibTeX" (or search: "Bibtex entry field types")
        CommandField(allowed_names=["RefID", "Reference"], name="ref_id", mandatory=True, parser=simple_ident),
        CommandField(allowed_names=["BibFileURL"], name="bib_file_url", parser=url_parser),
        CommandField(allowed_names=["EntryType"], name="entry_type", mandatory=True, allowed_values=bib_entry_types,
                     parser=unquoted_string),
        CommandField(allowed_names=["Address"], name="address", parser=unquoted_string),
        CommandField(allowed_names=["Annote"], name="annote", parser=unquoted_string),
        CommandField(allowed_names=["Author"], name="author",
                     mandatory="entry_type not in ('booklet', 'manual', 'misc', 'proceedings')",
                     parser=unquoted_string),
        CommandField(allowed_names=["BookTitle"], name="booktitle",
                     mandatory="entry_type in ('incollection', 'inproceedings')", parser=unquoted_string),
        CommandField(allowed_names=["Chapter"], name="chapter", mandatory="entry_type in ('inbook')",
                     parser=unquoted_string),
        CommandField(allowed_names=["CrossRef"], name="crossref", parser=unquoted_string),
        CommandField(allowed_names=["Edition"], name="edition", parser=unquoted_string),
        CommandField(allowed_names=["Editor"], name="editor", mandatory="entry_type in ('book', 'inbook')",
                     parser=unquoted_string),
        CommandField(allowed_names=["HowPublished"], name="how_published", parser=unquoted_string),
        CommandField(allowed_names=["Institution"], name="institution", mandatory="entry_type in ('techreport')",
                     parser=unquoted_string),
        CommandField(allowed_names=["Journal"], name="journal", mandatory="entry_type in ('article')",
                     parser=unquoted_string),
        CommandField(allowed_names=["Key"], name="key", parser=unquoted_string),
        CommandField(allowed_names=["Month"], name="month", default_value=bib_months[0], allowed_values=bib_months,
                     parser=simple_ident),
        CommandField(allowed_names=["Note"], name="note", parser=unquoted_string),
        CommandField(allowed_names=["Number"], name="number", parser=unquoted_string),
        CommandField(allowed_names=["Organization"], name="organization", parser=unquoted_string),
        CommandField(allowed_names=["Pages"], name="pages", mandatory="entry_type in ('inbook')",
                     parser=unquoted_string),
        CommandField(allowed_names=["Publisher"], name="publisher",
                     mandatory="entry_type in ('book', 'inbook', 'incollection')", parser=unquoted_string),
        CommandField(allowed_names=["School"], name="school", mandatory="entry_type in ('mastersthesis', 'phdtesis')",
                     parser=unquoted_string),
        CommandField(allowed_names=["Series"], name="series", parser=unquoted_string),
        CommandField(allowed_names=["Title"], name="title", mandatory="entry_type not in ('misc')",
                     parser=unquoted_string),
        CommandField(allowed_names=["Type"], name="type", parser=unquoted_string),
        CommandField(allowed_names=["URL"], name="url", parser=url_parser),
        CommandField(allowed_names=["Volume"], name="volume", mandatory="entry_type in ('article')",
                     parser=unquoted_string),
        CommandField(allowed_names=["Year"], name="year",
                     mandatory="entry_type in ('article', 'book', 'inbook', 'incollection', 'inproceedings', 'mastersthesis', 'phdthesis', 'proceedings', 'techreport')",
                     parser=unquoted_string)
    ],

    # Used only for help elaboration
    "datasetqry": [
        CommandField(allowed_names=["InputDataset"], name="inputdataset", parser=external_ds_name),
        CommandField(allowed_names=["AvailableAtDateTime"], name="availableatdatetime", parser=unquoted_string),
        CommandField(allowed_names=["StartTime"], name="starttime", parser=time_expression),
        CommandField(allowed_names=["EndTime"], name="endtime", parser=time_expression),
        CommandField(allowed_names=["ResultDimensions"], name="resultdimensions", parser=simple_ident),
        CommandField(allowed_names=["ResultMeasures"], name="resultmeasures", parser=simple_ident),
        CommandField(allowed_names=["ResultMeasuresAggregation"], name="resultmeasuresaggregation",
                     default_value=aggregators_list[0], allowed_values=aggregators_list, parser=simple_ident),
        CommandField(allowed_names=["ResultMeasureName"], name="resultmeasurename", parser=simple_ident),
        CommandField(allowed_names=["OutputDataset"], name="outputdataset", parser=simple_ident),
    ],

    # Analysis commands

    "problem_statement": [
        CommandField(allowed_names=["Scenario"], name="scenario_name", parser=simple_ident),
        CommandField(allowed_names=["Parameter"], name="parameter", mandatory=True, parser=simple_ident),
        CommandField(allowed_names=["Value"], name="parameter_value", mandatory=True,
                     parser=expression_with_parameters_or_list_simple_ident),  # list_simple_ident
        CommandField(allowed_names=["Description"], name="description", parser=unquoted_string)
    ],

    "scalar_indicator_benchmarks": [
        CommandField(allowed_names=["BenchmarkGroup"], name="benchmark_group", default_value=benchmark_groups[0],
                     allowed_values=benchmark_groups, parser=simple_ident),
        CommandField(allowed_names=["Stakeholders"], name="stakeholders", parser=list_simple_ident),
        CommandField(allowed_names=["Benchmark"], name="benchmark", mandatory=True, parser=simple_ident),
        CommandField(allowed_names=["Range"], name="range", mandatory=True, parser=number_interval),
        CommandField(allowed_names=["Unit"], name="unit", mandatory=True, parser=unit_name),
        CommandField(allowed_names=["Category"], name="category", mandatory=True, parser=simple_ident),
        CommandField(allowed_names=["Label"], name="label", mandatory=True, parser=unquoted_string),
        CommandField(allowed_names=["Description"], name="description", parser=unquoted_string)
    ],

    # Modified to consider Cristina's annotations
    "scalar_indicators": [
        CommandField(allowed_names=["IndicatorsGroup"], name="indicators_group", parser=simple_ident),
        # IndicatorType (Indicators)
        CommandField(allowed_names=["Indicator"], name="indicator_name", mandatory=True, parser=simple_ident),
        # IndicatorName (Indicators)
        CommandField(allowed_names=["Scope", "Local"], name="local", mandatory=True, allowed_values=scalar_indicator_type,
                     parser=simple_ident),
        CommandField(allowed_names=["Processors"], name="processors_selector", parser=processors_selector_expression),
        CommandField(allowed_names=["Formula", "Expression"], name="formula", mandatory=True,
                     parser=indicator_expression),
        # A call to LCIAMethod function, with parameter IndicatorMethod (Indicators)
        CommandField(allowed_names=["Unit"], name="unit", mandatory=True, parser=unit_name),
        # IndicatorUnit (Indicators)
        CommandField(allowed_names=["AccountNA"], name="account_na", mandatory=False, allowed_values=no_yes,
                     parser=simple_ident),  # IndicatorUnit (Indicators)
        # TODO Disabled: apply the formula to ALL processors (and ignore those where it cannot be evaluated)
        #  CommandField(allowed_names=["Processors"], name="processors_selector", parser=processors_selector_expression)
        CommandField(allowed_names=["Benchmarks", "Benchmark"], name="benchmarks", parser=list_simple_ident),
        CommandField(allowed_names=["UnitLabel"], name="unit_label", mandatory=False, parser=unquoted_string),
        CommandField(allowed_names=["Description"], name="description", parser=unquoted_string),
        CommandField(allowed_names=["Reference", "Source"], name="source", mandatory=False, parser=reference),
        CommandField(allowed_names=[attributeRegex], name="attributes", many_appearances=True, parser=value),
        CommandField(allowed_names=["Attributes"], name="attributes", parser=key_value_list)
        # SAME (Indicators)
    ],

    "matrix_indicators": [
        CommandField(allowed_names=["Indicator"], name="indicator_name", mandatory=True, parser=simple_ident),
        CommandField(allowed_names=["Scope"], name="scope", default_value=Scope.get_names()[0],
                     allowed_values=Scope.get_names(), parser=simple_ident),
        CommandField(allowed_names=["Processors"], name="processors_selector", parser=processors_selector_expression),
        CommandField(allowed_names=["Interfaces"], name="interfaces_selector", parser=interfaces_list_expression),
        CommandField(allowed_names=["Indicators"], name="indicators_selector", parser=indicators_list_expression),
        CommandField(allowed_names=["Attributes"], name="attributes_selector", parser=attributes_list_expression),
        CommandField(allowed_names=["Description"], name="description", parser=unquoted_string)
    ],

    # NEW command, implementation of Cristina's suggestions
    "lcia_methods": [
        CommandField(allowed_names=["LCIAMethod"], name="lcia_method", mandatory=True, parser=unquoted_string),
        # IndicatorMethod (Indicators), SAME (LCIAmethod)
        CommandField(allowed_names=["LCIACategory"], name="lcia_category", mandatory=False, parser=unquoted_string),
        CommandField(allowed_names=["LCIAIndicator"], name="lcia_indicator", mandatory=True, parser=unquoted_string),
        # IndicatorName (Indicators), SAME (LCIAmethod)
        CommandField(allowed_names=["LCIAIndicatorUnit"], name="lcia_indicator_unit", mandatory=True,
                     parser=unquoted_string),  # IndicatorName (Indicators), SAME (LCIAmethod)
        CommandField(allowed_names=["LCIAHorizon"], name="lcia_horizon", mandatory=True, parser=unquoted_string),
        # IndicatorTemporal (Indicators)
        CommandField(allowed_names=["Interface"], name="interface", mandatory=True, parser=simple_ident),
        # SAME (LCIAmethod)
        CommandField(allowed_names=["Compartment"], name="compartment", mandatory=False, parser=unquoted_string),
        # SAME (LCIAmethod)
        CommandField(allowed_names=["Subcompartment"], name="subcompartment", mandatory=False, parser=unquoted_string),
        # SAME (LCIAmethod)
        CommandField(allowed_names=["InterfaceUnit"], name="interface_unit", mandatory=True, parser=unit_name),
        # Not present, but needed to warrant independence from specification of InterfaceTypes
        CommandField(allowed_names=["LCIACoefficient"], name="lcia_coefficient", mandatory=True,  # SAME (LCIAmethod)
                     parser=signed_float)
    ],

}


def get_command_fields_from_class(execution_class: Type[IExecutableCommand]) -> List[CommandField]:
    execution_class_name: str = class_full_name(execution_class)
    cmd = first(commands, condition=lambda c: c.execution_class_name == execution_class_name)
    if cmd:
        return command_fields.get(cmd.name, [])

    return []


# command_field_names = {}
# for fields in command_fields.values():
#     for field in fields:
#         for name in field.allowed_names:
#             command_field_names[name] = field.name
_command_field_names = {name: f.name for fields in command_fields.values() for f in fields for name in f.allowed_names}
