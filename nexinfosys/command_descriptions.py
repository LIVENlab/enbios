
c_descriptions = {
    # Metadata
    ("metadata", "title"): "Metadata",
    ("metadata", "description"): "A set of fields allowing the contextualization of a case study file with respect to other case studies, by using fields of the Dublin Core schema, so a “metadata.xml” file, defined in the DMP, can be produced, packaged and published along with the case study.",
    ("metadata", "semantics"): "The metadata record created by the Metadata command can be used to search for the case study ones archived because it has the list of basic Dublin Core metadata fields.",
    ("metadata", "examples"): [],
    # PedigreeMatrix
    ("pedigree_matrix", "title"): "Pedigree Matrix",
    ("pedigree_matrix", "description"): "When specifying quantities for interfaces it is allowed to qualify them using NUSAP fields. The PedigreeMatrix field references a Pedigree Matrix which contains the key to translate the encoded Pedigree. Elements in columns are referenced using an integer number starting from 0 and up to the number of elements of that column. Each column of the matrix is assigned a label.",
    ("pedigree_matrix", "semantics"): "A Pedigree Matrix is just a record containing a set of aspects, where each aspect is a scale made of a list of levels. The Pedigree Matrix can later be referenced (when specifying Interfaces) to translate a code.",
    ("pedigree_matrix", "examples"): [],
    # CodeHierarchies
    ("cat_hierarchies", "title"): "Code Hierarchies (or Code Lists)",
    ("cat_hierarchies", "description"): "The purpose of Hierarchies command is the definition of hierarchies of codes, which can be used in mappings or to constrain the definition of attributes in entities. The format permits defining several (all) hierarchies in the same worksheet, although it is also possible to split definitions at convenience. External datasets bring into the case study hierarchies which do not need to appear explicitly in the workbook.",
    ("cat_hierarchies", "semantics"): "The command can be used to define Hierarchies, the Codes inside them, and HierarchyGroups containing one or more Hierarchies. These concepts are borrowed from SDMX, with some simplifications. For instance, SDMX uses Code Lists and Hierarchical Code Lists, the command uses just Hierarchies which, depending on the values of certain fields (HierarchyGroup and ReferredHierarchy) construct a Code List or a Hierarchical Code List. Hierarchies can be used to define the domain of an attribute or a Concept in a Dataset; Codes in hierarchies can be used to define values of attributes.",
    ("cat_hierarchies", "examples"): [],
    # CodeHierarchiesMapping
    ("cat_hier_mapping", "title"): "Code Hierarchies (or Code List) -directed- Mapping",
    ("cat_hier_mapping", "description"): "Once -at least- two hierarchies are defined, with this command it is possible to specify a correspondence between codes of one of them, called origin, into codes of the second, called destination. The mapping has a direction, from origin to destination. To define the reverse mapping, an additional definition is needed. The correspondence can be one to one (simple relabeling), many to one, or many to many. The format permits defining several (all) mappings in the same worksheet, although it is also possible to split definitions at convenience.",
    ("cat_hier_mapping", "semantics"): "Considering formal codes those defined by external datasets and semantic codes those defined to account inside MuSIASEM case studies, mappings of hierarchies can be applied in different situations:<ul>"
                                       "<li>Formal to semantic bridging, and formal to formal. Mapping can be used to add dimensions to external datasets having a dimension matching the origin hierarchy. Using DatasetQuery the resulting dataset can have the destination hierarchy as a new dimension.</li>"
                                       "<li>Semantic to formal. Using the analytical tools, mappings can transform internal codification schemes to others.</li>"
                                       "<li>Semantic to semantic. These mappings do not apply in this case. Relations between processors and changes of scale for interface types are the mechanisms to achieve this.</li>"
                                       "</ul>",
    ("cat_hier_mapping", "examples"): [],
    # DatasetDef
    ("datasetdef", "title"): "(custom) Dataset Definition",
    ("datasetdef", "description"): "It is quite usual that people involved in elaborating case studies need to customize a dataset initially provided by a statistical agency because either data are not available or they are considered incorrect. Also, exists the possibility of integrating the information of multiple sources or even not having any information at all. Whatever the case, this command allows defining the structure of a custom dataset which can be integrated into different commands as a full-fledged dataset, by a specifying the concepts (a term borrowed from SDMX) composing it, and the location of data.",
    ("datasetdef", "semantics"): "A Dataset Definition defines the structure of the Dataset with the aim of making it usable in later commands.",
    ("datasetdef", "examples"): [],
    # DatasetData
    ("datasetdata", "title"): "Dataset Data",
    ("datasetdata",
     "description"): "Custom datasets need the means to specify the data contained in it. As it was mentioned in the Dataset Definition command (DatasetDataLocation field), they can store its data in a CSV file, out of the spreadsheet or in the same spreadsheet, using this command.\nTo do so, create the command 'DatasetData' appending the dataset name, e.g., 'DatasetData WaterPerCountry', then in the header line write the names of the concepts (dimensions and measures) matching those in a preceding DatasetDef command.",
    ("datasetdata",
     "semantics"): "Data in this command is attached to a dataset whose structure has been declared previously (using a 'DatasetDef' command). The completed dataset (structure+data) can now be used in different ways: feed it into a Dataset Query command, directly export it or Dataset Expansion in MuSIASEM commands allowing it (BareProcessors, InterfaceTypes, Interfaces, ProcessorScalings, Relationships, ScalarBenchmarks, ScalarIndicators, ScaleChangeMap).",
    ("datasetdata", "examples"): [],
    # Parameters
    ("parameters", "title"): "Parameters",
    ("parameters", "description"): "Specify numeric values which can change. These values have a name which can be used in expressions assigned to factors in processors.",
    ("parameters", "semantics"): "Each row defines a separate Parameter. When one of them is modified, expressions in which they are used are updated.",
    ("parameters", "examples"): [],
    # InterfaceTypes
    ("interface_types", "title"): "Interface Types",
    ("interface_types", "description"): "Previous to the definition of Interfaces, a type has to be defined for each of them. The command allows defining hierarchies of InterfaceTypes. Each row in the command defines an InterfaceType, a group of rows can define a complete hierarchy. All hierarchies of InterfaceTypes (therefore all InterfaceTypes) of the case study can be defined in a single worksheet.",
    ("interface_types", "semantics"): "InterfaceTypes, organized in Hierarchies, are declared by this command. Interfaces must address one of them as their defining type. Hierarchies of InterfaceTypes can be used to specify scale changes with the ScaleChangeMap command.",
    ("interface_types", "examples"): [],
    # Processors
    ("processors", "title"): "Processors (bare or 'still without interfaces')",
    ("processors", "description"): "The first MuSIASEM primitive concept to be instantiated into a case study is the Processor. Bare Processors command serves that purpose, bringing into the model unadorned Processors (thus the use of 'Bare'). Using previously defined Datasets, “Bare Processors” can use data in them by using the dataset expansion notation, consisting in writing a dataset name and the name of a dimension in this dataset. Please see “Dataset expansion” in the section “Syntactic elements”.",
    ("processors", "semantics"): "When the command finishes its execution, a set of Processors, potentially organized in hierarchies and in different groups, can be decorated with Interfaces and connected to other Processors.",
    ("processors", "examples"): [],
    # Interfaces
    ("interfaces_and_qq", "title"): "Interfaces (and Qualified Quantities)",
    ("interfaces_and_qq", "description"): "This command obeys two purposes, the definition of Interfaces, and attaching Quantities to these Interfaces. Interfaces are at the border of Processors, bridging the exterior of the Processor with its interior. These interfaces can be quantified directly or the magnitudes can be computed from other interfaces connected through some kind of relation, especially “flow”. Using previously defined Datasets, “Interfaces” can use data in them by using the dataset expansion notation, consisting in writing a dataset name and the name of a dimension or measure in this dataset. Please see “Dataset expansion” in the section “Syntactic elements”.",
    ("interfaces_and_qq", "semantics"): "A set of Interfaces can be defined, optionally accompanied by quantification of these Interfaces. For the same Time, more than one Quantity can be attached one Interface, by changing “Source”.",
    ("interfaces_and_qq", "examples"): [],
    # Relationships
    ("relationships", "title"): "Relationships",
    ("relationships", "description"): "It is to specify relationships between processors or interfaces in processors to other processors or interfaces (respectively), with the relations of different meaning types (like structures, flows of goods or waste, or flows of information).",
    ("relationships", "semantics"): "Relationships are considered by solvers first to let information flow (scale relation), and to aggregate (bottom-up) or disaggregate (top-down). Because the relation concept can grow, solvers could be prepared to study structural (not only quantitative) aspects of functions (resilience, diversity, openness) exploiting “part-of”, “associate” may be used to control relations between structurals, and so on.",
    ("relationships", "examples"): [],
    # ProcessorScalings
    ("processor_scalings", "title"): "Processor Scalings",
    ("processor_scalings", "description"): "As a convenience when specifying a case study, MuSIASEM defines the possibility to size processors relative to one of its interfaces, and relate the size of these defining interfaces either in a part-of hierarchy of processors or between sibling processors. It also defines how to clone unit processors to quickly build hierarchies of processors, which later can take advantage of the hierarchical scaling.",
    ("processor_scalings", "semantics"): "“Processor scaling” enables conveniently constructing models from processor templates, and defining interfaces relative to others, in cascade. This, combined with the “RelativeTo” field in “Interfaces” command and the “InstanceOrArchetype” field in “BareProcessors” command, permits to define a chain of scales which can be resolved once the end of a chain is quantified.",
    ("processor_scalings", "examples"): [],
    # ScaleChangeMap
    ("scale_conversion_v2", "title"): "Scale Change Map",
    ("scale_conversion_v2", "description"): "InterfaceTypes can be grouped and structured in Hierarchies. “Scale change map” enables the specification of adaptive (depending on Context) linear transformations between two Hierarchies of InterfaceTypes, as a materialization of the multiscaling concept in MuSIASEM.",
    ("scale_conversion_v2", "semantics"): "Scale changer maps are used, when solving, to cascade information expressed using InterfaceTypes of a Hierarchy into InterfaceTypes of another. For instance, if food types were used for quantities and a transform food type to macronutrients equivalence, the total macronutrient could be obtained. Also, prices of input goods depending on contexts could be implicitly obtained, and the payback could be accounted into the origin (“seller”) using a flow relationship.",
    ("scale_conversion_v2", "examples"): [],
    # ScalarBenchmarks
    ("scalar_indicator_benchmarks", "title"): "(Scalar) Benchmarks",
    ("scalar_indicator_benchmarks", "description"): "Benchmarks are simple categorizations used to assess how fit are scalar indicators relative to expected or normal values obtained from experience in preceding systems used as calibration or reference.",
    ("scalar_indicator_benchmarks", "semantics"): "The benchmark can be used multiple times for different scalar indicators or occurrences of the same scalar local indicators for different processors, to qualitatively visualize performance. Also, one or more stakeholders may be interested or adhered to a benchmark. This can serve to elaborate later decision structures organized by stakeholder.",
    ("scalar_indicator_benchmarks", "examples"): [],
    # ScalarIndicators
    ("scalar_indicators", "title"): "Scalar Indicators",
    ("scalar_indicators", "description"): "Indicators are quantities (scalar or matrix) designed to measure the performance of a case study using formulas in which observations are aggregated and operated. “Scalar indicators” command enables defining expressions operating on either local or global quantities, and resolving to a single number.",
    ("scalar_indicators", "semantics"): "Each scalar indicator tries to match the variables mentioned in the formula. Local indicators look for Interfaces in it, while formulas in global indicators can refer to a set of interfaces in a set of processors (double set), which must be aggregated then operated with arithmetic operators. Local indicators are attached to each processor, global indicators are kept in a separate global area.",
    ("scalar_indicators", "examples"): [],
    # MatrixIndicators
    ("matrix_indicators", "title"): "Matrix Indicators",
    ("matrix_indicators", "description"): "Indicators are quantities (scalar or matrix) designed to measure the performance of a case study using formulas in which observations are aggregated and operated. “Matrix indicators” command enables building matrices gathering information from a set of processors. The rows of the matrix are the processors, while in the columns both interfaces and indicators can be arranged.",
    ("matrix_indicators", "semantics"): "Matrix indicators result in Datasets aimed at summarizing aspects of the case study, for human analysis. They can be tailored to form End Use or Environmental Pressure matrices.",
    ("matrix_indicators", "examples"): [],
    # ProblemStatement
    ("problem_statement", "title"): "Problem Statement",
    ("problem_statement", "description"): "The problem statement serves to specify one or more scenarios to solve in batch. Scenarios are characterized by a name and a set of parameter values. When this command is not specified, the solver assumes a single scenarios using the values of parameters as they were defined in Parameters commands.",
    ("problem_statement", "semantics"): "If ProblemStatement defines one or more scenarios (characterized by a set of parameters), the solver will repeat the calculations for each of the parameter values specified by them. The starting values for parameters are those defined in the Parameters command. Each scenario then sets or overwrites parameters with the specified values. If no ProblemStatement command exists, the solver will assume a single scenario with the values of parameters in the Parameters command.",
    ("problem_statement", "examples"): [],
    # ImportCommands
    ("import_commands", "title"): "Import Commands",
    ("import_commands", "description"): "If a workbook contains commands building part of a case study which is recognized as base for the expression of functions in the Nexus, using this command they do not need to be copied every time but a reference will include them as if they were explicitly in the current workbook.",
    ("import_commands", "semantics"): "It considers the externally specified command workbooks as if they were in the current workbook, analogously to “include”, “import”, “package” statements of languages like C, Java, Python, R. Circular references are not allowed.",
    ("import_commands", "examples"): [],
    # ListOfCommands
    ("list_of_commands", "title"): "List of Commands",
    ("list_of_commands", "description"): "Command worksheets must have a fixed “worksheet name” prefix plus a text or parameters. “list of commands” is a special command allowing to freely name commands which do not have parameters in the name.<br>NOTE: the order of execution cannot be changed, i.e., commands will be executed as the order of worksheets in the workbook, not on the list.",
    ("list_of_commands", "semantics"): "Names of worksheets in the list will be interpreted as the specified command.",
    ("list_of_commands", "examples"): [],
    # RefGeographic
    ("ref_geographical", "title"): "Geographic Dataset Reference",
    ("ref_geographical", "description"): "References are containers of information which can be cited in certain fields of commands, using a special ID field. Geographical dataset references are one of the supported reference types, and can be used in “GeolocationRef” fields of Code Hierarchies, Processors and Interfaces, to assign a spatial dataset to each entity. The fields simplify the available names and conventions of ISO 19115 profile defined by Inspire.",
    ("ref_geographical", "semantics"): "Geographical references define the metadata of a geographical dataset, plus maybe the data itself. When cited in the “GeolocationRef” field of Code Hierarchies, the “Code” field automatically defines the geometrical shape, because the code is assumed to be the key to access that information, or the specific “GeolocationCode” can be specified if there is a change. For “BareProcessors”, “Interfaces” and “Relationships” commands, this referencing (“GeolocationRef” field again) informs about the dataset, and to determine which feature in corresponds to the MuSIASEM concept (Processor, Interface, Relationship) a geometrical shape an ID has to be specified using the “GeolocationCode” field.  This information can later be used to prepare visualizations with geographical maps or processes using this geographical information.",
    ("ref_geographical", "examples"): [],
    # RefBibliographic
    ("ref_bibliographic", "title"): "Bibliographic Reference",
    ("ref_bibliographic", "description"): "References are containers of information which can be cited in certain fields of commands, using a special ID field. Bibliographic references are one of the supported reference types, which can be used in “Source” fields, when information has been gathered directly from the cited document. The fields follow the names and conventions of BibTeX entries, <a href=""https://en.wikibooks.org/wiki/LaTeX/Bibliography_Management"" target=""_blank"">https://en.wikibooks.org/wiki/LaTeX/Bibliography_Management</a>.",
    ("ref_bibliographic", "semantics"): "Bibliographic references when cited in the “Source” field of “Interfaces” command are considered a type of Observer, serving to put in context the quantification provided for a specific Interface.",
    ("ref_bibliographic", "examples"): [],
    # RefProvenance
    ("ref_provenance", "title"): "Provenance description Reference",
    ("ref_provenance", "description"): "References are containers of information which can be cited in certain fields of commands, using a special ID field. Provenance references are one of the supported reference types, which can be used in “Source” fields, when information has been gathered from the mentioned reference. The fields are a simplification of the ontology W3C “Provenance Recommendation”, using the bare starting point terms, <a href=""https://www.w3.org/TR/2013/REC-prov-o-20130430/#description-starting-point-terms"" target=""_blank"">https://www.w3.org/TR/2013/REC-prov-o-20130430/#description-starting-point-terms</a>.",
    ("ref_provenance", "semantics"): "Provenance references when cited in the “Source” field of “Interfaces” command are considered a type of Observer, serving to put in context the quantification provided for a specific Interface.",
    ("ref_provenance", "examples"): [],
    # DatasetQry
    ("datasetqry", "title"): "Dataset Query",
    ("datasetqry", "description"): "It is possible to assimilate a dataset from supported external data sources (like Eurostat). The available command will perform five steps: <ul>"
                                   "<li>Import: Downloads the dataset from the data source, using a cache to speed up the operation.</li>"
                                   "<li>Filter: used to keep measures in cells matching any of the categories specified in the filter, from native or new dimensions.</li>"
                                   "<li>Join new dimensions: Using previously defined category mappings, this part of the command can create new dimensions with internal (case study) categories.</li> "
                                   "<li>Compose output dataset dimensions: Enumerate which dimensions will remain in the output dataset"
                                   "<li>Aggregates: once the measures are obtained they can be aggregated grouping by one or more dimensions, to elaborate a data cube having fewer dimensions. The aggregation functions are for instance a sum or an arithmetic average.</li>"
                                   "</ul>",
    ("datasetqry", "semantics"): "The command can be considered to perform a simple SQL “Select” operation consisting in importing an already available dataset, from one of the supported data sources (Eurostat, FAO, OECD, …) (the FROM clause of the SQL Select), filtering it (the WHERE clause), then selecting both output dimensions and aggregation functions for measures that will be in the output (SELECT part of the clause, with all the output dimensions plus measures affected by aggregation functions, and accompanied with GROUP BY clause enumerating the output dimensions). The result is stored in an internal dataset which can be expanded or exported as another output result.",
    ("datasetqry", "examples"): []
}
