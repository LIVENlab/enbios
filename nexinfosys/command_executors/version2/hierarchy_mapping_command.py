import json
from dotted.collection import DottedDict

from nexinfosys.command_generators import Issue, IssueLocation, IType
from nexinfosys.model_services import IExecutableCommand, get_case_study_registry_objects
from nexinfosys.common.helper import obtain_dataset_metadata, create_dictionary
from nexinfosys.models.musiasem_concepts import Mapping


# Combinatory of lists (for many-to-many mappings)
#
# import itertools
# a = [[(0.5, "a"),(4, "b"),(7, "c")],[(1, "b")],[(0.2, "z"),(0.1, "y"),(0.7, "x")]]
# list(itertools.product(*a))
#

def fill_map_with_all_origin_categories(dim, mapping):
    # Check all codes exist
    mapped_codes = set([d["o"] for d in mapping])
    all_codes = set([c for c in dim.code_list])
    for c in all_codes - mapped_codes:  # Loop over "unmapped" origin codes
        # This sentence MODIFIES map, so it is not necessary to return it
        mapping.append({"o": c, "to": [{"d": None, "w": 1.0}]})  # Map to placeholder, with weight 1

    return mapping


class HierarchyMappingCommand(IExecutableCommand):
    def __init__(self, name: str):
        self._name = name
        self._content = None

    def execute(self, state: "State"):
        def process_line(item):
            # Read variables
            mh_src_dataset = item.get("source_dataset", None)
            mh_src_hierarchy = item.get("source_hierarchy", None)
            mh_src_code = item.get("source_code", None)
            mh_dst_hierarchy = item.get("destination_hierarchy", None)
            mh_dst_code = item.get("destination_code", None)
            mh_weight = item.get("weight", 1.0)

            # Mapping name
            name = ((mh_src_dataset + ".") if mh_src_dataset else "") + mh_src_hierarchy + " -> " + mh_dst_hierarchy

            if name in mappings:
                issues.append(Issue(itype=IType.ERROR,
                                    description="The mapping '"+name+"' has been declared previously. Skipped.",
                                    location=IssueLocation(sheet_name=name, row=r, column=None)))
                return

            if name in local_mappings:
                d = local_mappings[name]
            else:
                d = DottedDict()
                local_mappings[name] = d
                d.name = name
                d.origin_dataset = mh_src_dataset
                d.origin_hierarchy = mh_src_hierarchy
                d.destination_hierarchy = mh_dst_hierarchy
                d.mapping = create_dictionary()

            # Specific code
            if mh_src_code in d.mapping:
                to_dict = d.mapping[mh_src_code]
            else:
                to_dict = create_dictionary()
            if mh_dst_code in to_dict:
                issues.append(Issue(itype=IType.ERROR,
                                    description="The mapping of '" + mh_src_code + "' into '" + mh_dst_code + "' has been already defined",
                                    location=IssueLocation(sheet_name=name, row=r, column=None)))
                return
            else:
                to_dict[mh_dst_code] = (mh_weight, r)  # NOTE: This could be an object instead of just a FLOAT or expression
                d.mapping[mh_src_code] = to_dict

        issues = []
        glb_idx, p_sets, hh, datasets, mappings = get_case_study_registry_objects(state)
        name = self._content["command_name"]

        local_mappings = create_dictionary()

        # Process parsed information
        for line in self._content["items"]:
            r = line["_row"]
            # If the line contains a reference to a dataset or hierarchy, expand it
            # If not, process it directly
            is_expansion = False
            if is_expansion:
                # TODO Iterate through dataset and/or hierarchy elements, producing a list of new items
                pass
            else:
                process_line(line)

        # Mappings post-processing
        for d in local_mappings:
            # Convert the mapping into:
            # [{"o": "", "to": [{"d": "", "w": ""}]}]
            # [ {o: origin category, to: [{d: destination category, w: weight assigned to destination category}] } ]
            mapping = []
            ds_rows = []  # Rows in which a dataset is mentioned
            for orig in local_mappings[d].mapping:
                lst = []
                for dst in local_mappings[d].mapping[orig]:
                    t = local_mappings[d].mapping[orig][dst]
                    lst.append(dict(d=dst, w=t[0]))
                    if local_mappings[d].origin_dataset:
                        ds_rows.append(t[1])
                mapping.append(dict(o=orig, to=lst))
            from nexinfosys.ie_imports.data_source_manager import DataSourceManager
            if local_mappings[d].origin_dataset:
                if not DataSourceManager.obtain_dataset_source(local_mappings[d].origin_dataset, datasets):
                    for r in ds_rows:
                        issues.append(Issue(itype=IType.ERROR,
                                            description=f"The dataset '{local_mappings[d].origin_dataset}' was not found",
                                            location=IssueLocation(sheet_name=name, row=r, column=None)))
                    continue
                dims, attrs, meas = obtain_dataset_metadata(local_mappings[d].origin_dataset, None, datasets)
                if local_mappings[d].origin_hierarchy not in dims:
                    issues.append(Issue(itype=IType.ERROR,
                                        description="The origin dimension '" + local_mappings[d].origin_hierarchy + "' does not exist in dataset '" + local_mappings[d].origin_dataset + "'",
                                        location=IssueLocation(sheet_name=name, row=r, column=None)))
                    continue
                else:
                    dim = dims[local_mappings[d].origin_hierarchy]
                    mapping = fill_map_with_all_origin_categories(dim, mapping)
            #
            origin_dataset = local_mappings[d].origin_dataset
            origin_hierarchy = local_mappings[d].origin_hierarchy
            destination_hierarchy = local_mappings[d].destination_hierarchy
            # Create Mapping and add it to Case Study mappings variable
            mappings[d] = Mapping(d, DataSourceManager.obtain_dataset_source(origin_dataset, datasets), origin_dataset, origin_hierarchy, destination_hierarchy, mapping)

        # TODO
        # Use the function to perform many to many mappings, "augment_dataframe_with_mapped_columns"
        # Put it to work !!!

        # One or more mapping in sequence could be specified?. The key is "source hierarchy+dest hierarchy"
        # Read mapping parameters

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
