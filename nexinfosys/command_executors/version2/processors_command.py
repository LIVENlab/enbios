from typing import Dict, Any, Optional

from nexinfosys.command_executors import BasicCommand, CommandExecutionError, subrow_issue_message
from nexinfosys.command_field_definitions import get_command_fields_from_class
from nexinfosys.command_generators import IType
from nexinfosys.common.helper import strcmp
from nexinfosys.ie_exports.geolayer import read_geojson
from nexinfosys.models.musiasem_concepts import ProcessorsSet, ProcessorsRelationPartOfObservation, \
    Processor, \
    Geolocation, GeographicReference
from nexinfosys.models.musiasem_concepts_helper import find_or_create_processor, \
    obtain_name_parts, find_processors_matching_name


def get_object_view(d):
    class objectview(object):
        def __init__(self, d2):
            self.__dict__ = d2
    return objectview(d)


class ProcessorsCommand(BasicCommand):
    def __init__(self, name: str):
        BasicCommand.__init__(self, name, get_command_fields_from_class(self.__class__))

    # def _execute(self, state: "State"):
    #     """
    #     Create empty Processors, potentially related with PartOf relationship
    #
    #     The Name of a new Processor can be:
    #     * Simple name
    #     * Hierarchical name
    #     * Clone previously declared processor (and children), where "previously declared processor" can be simple or complex
    #
    #     If name is hierarchic, a formerly declared processor is assumed.
    #     If a parent processor is specified, the processor will be related to the new parent (if parents are different -> multiple functionality)
    #     In a hierarchy, children inherit interfaces from parents
    #                     if parents do not have interfaces, do they "receive" interfaces from immediate child?
    #                     - Only if children have them already (separate copy)
    #                     - If children do not have them already (connected copy)
    #
    #     If CLONE(<existing processor>) is specified, a copy of the <existing processor> (and children) will be created.
    #     In this case, the Parent field is mandatory (if not, no unique name will be available). For a pair "parent processor - clone processor", this operation is unique (unless, a naming syntax for the copy is invented)
    #     If child processors had interface, copy them into the parent processor (the default move is downwards, from parent to child)
    #
    #     Later, when interfaces are attached to processors,
    #     :param state:
    #     :return:
    #     """
    #     def parse_and_unfold_line(item):
    #         # Consider multiplicity because of:
    #         # - A dataset (only one). First a list of dataset concepts used in the line is obtained.
    #         #   Then the unique tuples formed by them are obtained.
    #         # - Processor name.
    #         #   - A set of processors (wildcard or filter by attributes)
    #         #   - A set of interfaces (according to another filter?)
    #         # - Multiple types of relation
    #         # - Both (first each dataset record applied -expanded-, then the name evaluation is applied)
    #         # - UNRESOLVED: expressions are resolved partially. Parts where parameters
    #         # expressions depending on parameters. Only the part of the expression depending on varying things
    #         # - The processor name could be a concatenation of multiple literals
    #         #
    #         # Look for multiple items in r_source_processor_name, r_source_interface_name,
    #         #                            r_target_processor_name, r_target_interface_name
    #         if item["_complex"]:
    #             asts = parse_line(item, fields)
    #             if item["_expandable"]:
    #                 # It is an expandable line
    #                 # Look for fields which are specified to be variable in order to originate the expansion
    #                 res = classify_variables(asts, datasets, hh, parameters)
    #                 ds_list = res["datasets"]
    #                 ds_concepts = res["ds_concepts"]
    #                 h_list = res["hierarchies"]
    #                 if len(ds_list) >= 1 and len(h_list) >= 1:
    #                     issues.append(create_issue(IType.ERROR, "Dataset(s): "+", ".join([d.name for d in ds_list])+", and hierarchy(ies): "+", ".join([h.name for h in h_list])+", have been specified. Either a single dataset or a single hiearchy is supported."))
    #                     return
    #                 elif len(ds_list) > 1:
    #                     issues.append(create_issue(IType.ERROR, "More than one dataset has been specified: "+", ".join([d.name for d in ds_list])+", just one dataset is supported."))
    #                     return
    #                 elif len(h_list) > 1:
    #                     issues.append(create_issue(IType.ERROR, "More than one hierarchy has been specified: " + ", ".join([h.name for h in h_list])+", just one hierarchy is supported."))
    #                     return
    #                 const_dict = obtain_dictionary_with_literal_fields(item, asts)
    #                 if len(ds_list) == 1:
    #                     # If a measure is requested and not all dimensions are used, aggregate or
    #                     # issue an error (because it is not possible to reduce without aggregation).
    #                     # If only dimensions are used, then obtain all the unique tuples
    #                     ds = ds_list[0]
    #                     measure_requested = False
    #                     all_dimensions = set([c.code for c in ds.dimensions if not c.is_measure])
    #                     for con in ds_concepts:
    #                         for c in ds.dimensions:
    #                             if strcmp(c.code, con):
    #                                 if c.is_measure:
    #                                     measure_requested = True
    #                                 else:  # Dimension
    #                                     all_dimensions.remove(c.code)
    #                     only_dimensions_requested = len(all_dimensions) == 0
    #
    #                     if measure_requested and not only_dimensions_requested:
    #                         issues.append(create_issue(IType.ERROR, "It is not possible to use a measure if not all dimensions are used (cannot assume implicit aggregation)"))
    #                         return
    #                     elif not measure_requested and not only_dimensions_requested:
    #                         # TODO Reduce the dataset to the unique tuples (consider the current case -sensitive or not-sensitive-)
    #                         data = None
    #                     else:  # Take the dataset as-is
    #                         data = ds.data
    #
    #                     for row in data.iterrows():
    #                         item2 = const_dict.copy()
    #
    #                         d = {}
    #                         for c in ds_concepts:
    #                             d["{" + ds.code + "." + c + "}"] = row[c]
    #                         # Expand in all fields
    #                         for f in fields:
    #                             if f not in const_dict:
    #                                 # Replace all
    #                                 string = item[f]
    #                                 # TODO Could iterate through the variables in the field (not IN ALL FIELDS of the row)
    #                                 for item in sorted(d.keys(), key=len, reverse=True):
    #                                     string = re.sub(item, d[item], string)
    #                                 item2[f] = string
    #                         # Now, look for wildcards where it is allowed
    #                         r_source_processor_name = string_to_ast(processor_names, item2.get("source_processor", None))
    #                         r_target_processor_name = string_to_ast(processor_names, item2.get("target_processor", None))
    #                         if ".." in r_source_processor_name or ".." in r_target_processor_name:
    #                             if ".." in r_source_processor_name:
    #                                 source_processor_names = obtain_matching_processors(r_source_processor_name, all_processors)
    #                             else:
    #                                 source_processor_names = [r_source_processor_name]
    #                             if ".." in r_target_processor_name:
    #                                 target_processor_names = obtain_matching_processors(r_target_processor_name, all_processors)
    #                             else:
    #                                 target_processor_names = [r_target_processor_name]
    #                             for s in source_processor_names:
    #                                 for t in target_processor_names:
    #                                     item3 = item2.copy()
    #                                     item3["source_processor"] = s
    #                                     item3["target_processor"] = t
    #                                     print("Multiple by dataset and wildcard: " + str(item3))
    #                                     yield item3
    #                         else:
    #                             print("Multiple by dataset: " + str(item3))
    #                             yield item2
    #                 elif len(h_list) == 1:
    #                     pass
    #                 else:  # No dataset, no hierarchy of categories, but still complex, because of wildcards
    #                     wildcard_in_source = ".." in item.get("source_processor", "")
    #                     wildcard_in_target = ".." in item.get("target_processor", "")
    #                     if wildcard_in_source or wildcard_in_target:
    #                         r_source_processor_name = string_to_ast(processor_names, item.get("source_processor", None))
    #                         r_target_processor_name = string_to_ast(processor_names, item.get("target_processor", None))
    #                         if wildcard_in_source:
    #                             source_processor_names = obtain_matching_processors(r_source_processor_name, all_processors)
    #                         else:
    #                             source_processor_names = [item["source_processor"]]
    #                         if wildcard_in_target:
    #                             target_processor_names = obtain_matching_processors(r_target_processor_name, all_processors)
    #                         else:
    #                             target_processor_names = [item["target_processor"]]
    #                         for s in source_processor_names:
    #                             for t in target_processor_names:
    #                                 item3 = const_dict.copy()
    #                                 item3["source_processor"] = s
    #                                 item3["target_processor"] = t
    #                                 print("Multiple by wildcard: "+str(item3))
    #                                 yield item3
    #                     else:
    #                         # yield item
    #                         raise Exception("If 'complex' is signaled, it should not pass by this line")
    #         else:
    #             # print("Single: "+str(item))
    #             yield item
    #
    #     issues = []
    #     glb_idx, p_sets, hh, datasets, mappings = get_case_study_registry_objects(state)
    #     command_name = self._content["command_name"]
    #
    #     # CommandField definitions for the fields of Interface command
    #     fields: Dict[str, CommandField] = {f.name: f for f in get_command_fields_from_class(self.__class__)}
    #     # Obtain the names of all parameters
    #     parameters = [p.name for p in glb_idx.get(Parameter.partial_key())]
    #     # Obtain the names of all processors
    #     all_processors = get_processor_names_to_processors_dictionary(glb_idx)
    #
    #     # Process parsed information
    #     for line in self._content["items"]:
    #         row = line["_row"]
    #         for sub_line in parse_and_unfold_line(line):
    #             self._process_row(sub_line)
    #
    #     return issues, None  # Issues, Output

    def _process_row(self, field_values: Dict[str, Any], subrow=None) -> None:

        # Transform text of "attributes" into a dictionary
        field_values["attributes"] = self._transform_text_attributes_into_dictionary(field_values.get("attributes"),
                                                                                     subrow)

        # Process specific fields

        # Obtain the parent: it must exist. It could be created dynamically but it's important to specify attributes
        if field_values.get("parent_processor"):
            try:
                parent_processor = self._get_processor_from_field("parent_processor")
                # parents = find_processors_matching_name(parent_processor)
                # if len(parents) > 1:
                #     self._add_issue(IType.WARNING,
                #                     f"Parent processor '{parent_processor}' not unique. Matches: {', '.join(p.hierarchical_names[0] for p in parents)}. Skipped." + subrow_issue_message(subrow))
                #     return
            except CommandExecutionError:
                self._add_issue(IType.ERROR, f"Specified parent processor, '{field_values.get('parent_processor')}', does not exist"+subrow_issue_message(subrow))
                return
        else:
            parent_processor = None

        behave_as_processor: Optional[Processor] = None
        if field_values.get("behave_as_processor"):
            try:
                behave_as_processor = self._get_processor_from_field("behave_as_processor")
            except CommandExecutionError:
                self._add_issue(IType.WARNING, f"Specified 'behave as' processor, '{field_values.get('behave_as_processor')}', does not exist, value ignored"+subrow_issue_message(subrow))

        # Find or create processor and REGISTER it in "glb_idx"
        # TODO Now, only Simple name allowed
        # TODO Improve allowing hierarchical names, and hierarchical names with wildcards
        pgroup = field_values.get("processor_group")

        # Get internal and user-defined attributes in one dictionary
        attributes = {c.name: field_values[c.name] for c in self._command_fields if c.attribute_of == Processor}
        attributes.update(field_values["attributes"])
        attributes["processor_group"] = pgroup

        # Needed to support the new name of the field, "Accounted" (previously it was "InstanceOrArchetype")
        # (internally the values have the same meaning, "Instance" for a processor which has to be accounted,
        # "Archetype" for a processor which hasn't)
        v = attributes.get("instance_or_archetype", None)
        if strcmp(v, "Yes"):
            v = "Instance"
        elif strcmp(v, "No"):
            v = "Archetype"
        if v:
            attributes["instance_or_archetype"] = v

        name = field_values["processor"]
        p_names, _ = obtain_name_parts(name)

        geolocation = Geolocation.create(field_values["geolocation_ref"], field_values["geolocation_code"])

        ps = find_processors_matching_name(name, self._glb_idx)
        more_than_one = len(ps) > 1
        simple = len(p_names) == 1
        exists = True if len(ps) == 1 else False
        # SIMPLE? EXISTS? PARENT? ACTION:
        # Yes     Yes     Yes     NEW; HANG FROM PARENT
        # Yes     Yes     No      Warning: repeated
        # Yes     No      Yes     NEW; HANG FROM PARENT
        # Yes     No      No      NEW
        # No      Yes     Yes     Warning: cannot hang from parent
        # No      Yes     No      Warning: repeated AND not simple not allowed
        # No      No      Yes     Warning: cannot create more than one processor AND not simple not allowed
        # No      No      No      Warning: cannot create more than one processor AND not simple not allowed

        create_new = False
        if not simple:
            if not parent_processor:
                self._add_issue(IType.WARNING,
                                f"When a processor does not have parent, the name must be simple. Skipped." + subrow_issue_message(subrow))
                return
        else:
            if exists and not parent_processor:
                self._add_issue(IType.WARNING,
                                f"Repeated declaration of {name}. Skipped." + subrow_issue_message(subrow))
                return
            create_new = True

        if create_new:
            p = find_or_create_processor(
                state=self._glb_idx,
                name=name,
                proc_attributes=attributes,
                proc_location=geolocation)
        else:
            if exists:
                p = ps[0]

        # Add to ProcessorsGroup, if specified
        if pgroup:
            p_set = self._p_sets.get(pgroup, ProcessorsSet(pgroup))
            self._p_sets[pgroup] = p_set
            if p_set.append(p, self._glb_idx):  # Appends codes to the pset if the processor was not member of the pset
                p_set.append_attributes_codes(field_values["attributes"])

        # If geolocation specified, check if it exists
        # Inside it, check it the code exists
        if p.geolocation and p.geolocation.reference:
            # Geographical reference
            gr = self._glb_idx.get(GeographicReference.partial_key(name=p.geolocation.reference))
            if len(gr) == 0:
                self._add_issue(IType.ERROR, f"Geographical reference {p.geolocation.reference} not found "+subrow_issue_message(subrow))
                return
            if p.geolocation.reference and not p.geolocation.code:
                self._add_issue(IType.ERROR, f"Geographical reference was specified but not the code in it "+subrow_issue_message(subrow))
                return
            geo_id = p.geolocation.code
            try:
                url = gr[0].attributes["data_location"]
            except:
                self._add_issue(IType.ERROR, f"URL not found in geographical reference {p.geolocation.reference} "+subrow_issue_message(subrow))
                return
            try:
                j, ids = read_geojson(url)  # READ the file!! (or get it from cache). Could take some time...
            except:
                self._add_issue(IType.ERROR, f"URL {url} in reference {p.geolocation.reference} could not be read "+subrow_issue_message(subrow))
                return
            if geo_id not in ids:
                self._add_issue(IType.WARNING, f"Could not find code {geo_id} in file {url}, geographical reference {p.geolocation.reference} "+subrow_issue_message(subrow))

        # Add Relationship "part-of" if parent was specified
        # The processor may have previously other parent processors that will keep its parentship
        if parent_processor:
            # Create "part-of" relationship
            if len(self._glb_idx.get(ProcessorsRelationPartOfObservation.partial_key(parent_processor, p))) > 0:
                self._add_issue(IType.WARNING,
                                f"{p.name} is already part-of {parent_processor.name}. Skipped." + subrow_issue_message(subrow))
                return

            o1 = ProcessorsRelationPartOfObservation.create_and_append(parent_processor, p, None, behave_as=behave_as_processor, weight=field_values.get("parent_processor_weight"))  # Part-of
            self._glb_idx.put(o1.key(), o1)
            for hname in parent_processor.full_hierarchy_names(self._glb_idx):
                p_key = Processor.partial_key(f"{hname}.{p.name}", p.ident)
                if attributes:
                    p_key.update({k: ("" if v is None else v) for k, v in attributes.items()})
                self._glb_idx.put(p_key, p)

