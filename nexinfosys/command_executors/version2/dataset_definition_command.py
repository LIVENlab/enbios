import json

from nexinfosys.command_generators import Issue, IssueLocation, parser_field_parsers, IType
from nexinfosys.command_generators.parser_ast_evaluators import dictionary_from_key_value_list
from nexinfosys.command_generators.parser_field_parsers import url_parser
from nexinfosys.common.helper import create_dictionary, load_dataset, prepare_dataframe_after_external_read, strcmp, \
    any_error_issue
from nexinfosys.model_services import IExecutableCommand, get_case_study_registry_objects
from nexinfosys.models import CodeImmutable
from nexinfosys.models.musiasem_concepts import Hierarchy
from nexinfosys.models.musiasem_concepts_helper import convert_hierarchy_to_code_list
from nexinfosys.models.statistical_datasets import Dataset, Dimension, CodeList


class DatasetDefCommand(IExecutableCommand):
    def __init__(self, name: str):
        self._name = name
        self._content = None

    def execute(self, state: "State"):
        def process_line(item):
            # Read variables
            dsd_dataset_name = item.get("dataset_name", None)
            dsd_dataset_data_location = item.get("dataset_data_location", None)
            dsd_concept_type = item.get("concept_type", None)
            if dsd_concept_type.lower() == "dataset":
                dsd_concept_type = None
            dsd_concept_name = item.get("concept_name", None)
            dsd_concept_data_type = item.get("concept_data_type", None)
            dsd_concept_domain = item.get("concept_domain", None)
            dsd_concept_description = item.get("concept_description", None)
            dsd_attributes = item.get("concept_attributes", None)
            if dsd_attributes:
                try:
                    attributes = dictionary_from_key_value_list(dsd_attributes, glb_idx)
                except Exception as e:
                    issues.append(Issue(itype=IType.ERROR,
                                        description=str(e),
                                        location=IssueLocation(sheet_name=name, row=r, column=None)))
                    return
            else:
                attributes = {}

            if dsd_dataset_name in ds_names:
                issues.append(Issue(itype=IType.ERROR,
                                    description="The dataset '"+dsd_dataset_name+"' has been already defined",
                                    location=IssueLocation(sheet_name=name, row=r, column=None)))
                return

            # Internal dataset definitions cache
            ds = current_ds.get(dsd_dataset_name, None)
            if True:  # Statistical dataset format
                if not ds:
                    ds = Dataset()
                    ds.code = dsd_dataset_name  # Name
                    ds.database = None
                    ds.attributes = {}
                    current_ds[dsd_dataset_name] = ds
                if not dsd_concept_type:
                    if ds.attributes.get("_location"):
                        issues.append(Issue(itype=IType.WARNING,
                                            description=f"Location of data for dataset {ds.code} previously declared. "
                                                        f"Former: {attributes.get('_location')}, "
                                                        f"Current: {dsd_dataset_data_location}",
                                            location=IssueLocation(sheet_name=name, row=r, column=None)))
                        attributes = ds.attributes
                    else:
                        attributes["_dataset_first_row"] = r
                    attributes["_location"] = dsd_dataset_data_location  # Location
                    ds.description = dsd_concept_description
                    ds.attributes = attributes  # Set attributes
                else:  # If concept_type is defined => add a concept
                    # Check if the concept name already appears --> Error
                    for d1 in ds.dimensions:
                        if strcmp(d1.code, dsd_concept_name):
                            issues.append(Issue(itype=IType.ERROR,
                                                description=f"Concept {dsd_concept_name} already declared for dataset {ds.code}",
                                                location=IssueLocation(sheet_name=name, row=r, column=None)))
                            break

                    d = Dimension()
                    d.dataset = ds
                    d.description = dsd_concept_description
                    d.code = dsd_concept_name
                    d.is_measure = False if dsd_concept_type.lower() == "dimension" else True
                    if not d.is_measure and dsd_concept_data_type.lower() == "time":
                        d.is_time = True
                    else:
                        d.is_time = False
                    if dsd_concept_type.lower() == "attribute":
                        attributes["_attribute"] = True
                    else:
                        attributes["_attribute"] = False
                    if dsd_concept_data_type.lower() == "category":
                        # TODO "hierarchies" variable really does not register hierarchies (see "hierarchy_command.py" or "hierarchy_categories_command.py", no insertion is made)
                        # h = hierarchies.get(dsd_concept_domain, None)
                        h = glb_idx.get(Hierarchy.partial_key(name=dsd_concept_domain))
                        if len(h) == 0:
                            issues.append(Issue(itype=IType.ERROR,
                                                description="Could not find hierarchy of Categories '" + dsd_concept_domain + "'",
                                                location=IssueLocation(sheet_name=name, row=r, column=None)))
                            return
                        elif len(h) > 1:
                            issues.append(Issue(itype=IType.ERROR,
                                                description="Found more than one instance of Categories '" + dsd_concept_domain + "'",
                                                location=IssueLocation(sheet_name=name, row=r, column=None)))
                            return
                        else:  # len(h) == 1
                            h = h[0]
                        d.hierarchy = h
                        # Reencode the Hierarchy as a CodeList
                        cl = convert_hierarchy_to_code_list(h)
                        d.code_list = cl

                    attributes["_datatype"] = dsd_concept_data_type
                    attributes["_domain"] = dsd_concept_domain
                    d.attributes = attributes

        # -------------------------------------------------------------------------------------------------------------
        issues = []
        glb_idx, p_sets, hh, datasets, mappings = get_case_study_registry_objects(state)
        name = self._content["command_name"]

        # List of available dataset names. The newly defined datasets must not be in this list
        ds_names = [ds.name for ds in datasets]

        # List of available Category hierarchies
        hierarchies = create_dictionary()
        for h in hh:
            hierarchies[h.name] = hh

        # Datasets being defined in this Worksheet
        current_ds = create_dictionary()  # type: Dict[str, Dataset]

        # Process parsed information
        for line in self._content["items"]:
            r = line["_row"]
            # If the line contains a reference to a dataset or hierarchy, expand it
            # If not, process it directly
            is_expansion = False
            if is_expansion:
                pass
            else:
                process_line(line)

        # Any error?
        error = any_error_issue(issues)

        # Load the data for those datasets that are not local (data defined later in the same spreadsheet)
        for ds in current_ds.values():
            if "_location" not in ds.attributes:
                error = True
                issues.append(Issue(itype=IType.ERROR,
                                    description="Location of data not specified, for dataset '" + ds.code + "'",
                                    location=IssueLocation(sheet_name=name, row=r, column=None)))
            else:
                loc = ds.attributes["_location"]
                ast = parser_field_parsers.string_to_ast(url_parser, loc)
                if ast["scheme"] != "data":
                    df = load_dataset(loc)
                    if df is None:
                        error = True
                        issues.append(Issue(itype=IType.ERROR,
                                            description=f"Could not obtain data for dataset '{ds.code}' at '{loc}'",
                                            location=IssueLocation(sheet_name=name, row=r, column=None)))
                    else:
                        iss = prepare_dataframe_after_external_read(ds, df, name)
                        issues.extend(iss)
                        # Everything ok? Store the dataframe!
                        if not any_error_issue(iss):
                            ds.data = df

        if not error:
            # If no error happened, add the new Datasets to the Datasets in the "global" state
            for ds in current_ds:
                r = current_ds[ds].attributes["_dataset_first_row"]
                df = current_ds[ds].data
                if df is not None:
                    # Loop over "ds" concepts.
                    # - "dimension" concepts of type "string" generate a CodeHierarchy
                    # - Check that the DataFrame contains ALL declared concepts. If not, generate issue
                    cid = create_dictionary(data={col: col for col in df.columns})
                    col_names = list(df.columns)
                    for c in current_ds[ds].dimensions:
                        if c.code in df.columns:
                            col_names[df.columns.get_loc(cid[c.code])] = c.code  # Rename column
                            dsd_concept_data_type = c.attributes["_datatype"]
                            if dsd_concept_data_type.lower() == "string" and not c.is_measure:  # Freely defined dimension
                                cl = df[cid[c.code]].unique().tolist()
                                # Filter "nan", with c == c (a Python idiom achieving this...)
                                cl = [c if c == c else "" for c in cl]
                                c.code_list = CodeList.construct(
                                    c.code, c.code, [""],
                                    codes=[CodeImmutable(c, c, "", []) for c in cl]
                                )
                        else:
                            issues.append(Issue(itype=IType.ERROR,
                                                description=f"Concept '{c.code}' not defined for '{ds}' in {loc}",
                                                location=IssueLocation(sheet_name=name, row=r, column=None)))
                    df.columns = col_names
                datasets[ds] = current_ds[ds]

        return issues, None

    def estimate_execution_time(self):
        return 0

    def json_serialize(self):
        # Directly return the metadata dictionary
        return self._content

    def json_deserialize(self, json_input):
        # TODO Check validity
        issues = []
        if isinstance(json_input, dict):
            self._content = json_input
        else:
            self._content = json.loads(json_input)

        if "description" in json_input:
            self._description = json_input["description"]
        return issues
