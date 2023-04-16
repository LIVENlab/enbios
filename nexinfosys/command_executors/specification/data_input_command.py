import json

from nexinfosys import case_sensitive
from nexinfosys.common.helper import create_dictionary
from nexinfosys.model_services import IExecutableCommand, State, get_case_study_registry_objects
from nexinfosys.command_generators import parser_field_parsers
from nexinfosys.models.musiasem_concepts_helper import create_or_append_quantitative_observation
from nexinfosys.models.musiasem_concepts import FlowFundRoegenType, ProcessorsSet, PedigreeMatrix, Reference


class DataInputCommand(IExecutableCommand):
    """
    Serves to specify quantities (and their qualities) for observables
    If observables (Processor, Factor, FactorInProcessor) do not exist, they are created. This makes this command very powerfu: it may express by itself a MuSIASEM 1.0 structure

    """
    def __init__(self, name: str):
        self._name = name
        self._content = None

    def execute(self, state: "State"):
        """ The execution creates one or more Processors, Factors, FactorInProcessor and Observations
            It also creates "flat" Categories (Code Lists)
            It also Expands referenced Datasets
            Inserting it into "State"
        """
        def process_row(row):
            """
            Process a dictionary representing a row of the data input command. The dictionary can come directly from
            the worksheet or from a dataset.

            Implicitly uses "glb_idx"

            :param row: dictionary
            """
            # From "ff_type" extract: flow/fund, external/internal, incoming/outgoing
            # ecosystem/society?
            ft = row["ff_type"].lower()
            if ft == "int_in_flow":
                roegen_type = FlowFundRoegenType.flow
                internal = True
                incoming = True
            elif ft == "int_in_fund":
                roegen_type = FlowFundRoegenType.fund
                internal = True
                incoming = True
            elif ft == "ext_in_fund":
                roegen_type = FlowFundRoegenType.fund
                internal = False
                incoming = True
            elif ft == "int_out_flow":
                roegen_type = FlowFundRoegenType.flow
                internal = True
                incoming = False
            elif ft == "ext_in_flow":
                roegen_type = FlowFundRoegenType.flow
                internal = False
                incoming = True
            elif ft == "ext_out_flow":
                roegen_type = FlowFundRoegenType.flow
                internal = False
                incoming = False
            elif ft == "env_out_flow":
                roegen_type = FlowFundRoegenType.flow
                internal = False
                incoming = False
            elif ft == "env_in_flow":
                roegen_type = FlowFundRoegenType.flow
                internal = False
                incoming = True
            elif ft == "env_in_fund":
                roegen_type = FlowFundRoegenType.fund
                internal = False
                incoming = True

            # Split "taxa" attributes. "scale" corresponds to the observation
            p_attributes = row["taxa"].copy()
            if "scale" in p_attributes:
                other_attrs = create_dictionary()
                other_attrs["scale"] = p_attributes["scale"]
                del p_attributes["scale"]
            else:
                other_attrs = None

            # Check existence of PedigreeMatrix, if used
            if "pedigree_matrix" in row:
                pm = glb_idx.get(PedigreeMatrix.partial_key(name=row["pedigree_matrix"]))
                if len(pm) != 1:
                    issues.append((3, "Could not find Pedigree Matrix '"+row["pedigree_matrix"]+"'"))
                    del row["pedigree_matrix"]
                else:
                    try:
                        lst = pm[0].get_modes_for_code(row["pedigree"])
                    except:
                        issues.append((3, "Could not decode Pedigree '"+row["pedigree"]+"' for Pedigree Matrix '"+row["pedigree_matrix"]+"'"))
                        del row["pedigree"]
                        del row["pedigree_matrix"]
            else:
                if "pedigree" in row:
                    issues.append((3, "Pedigree specified without accompanying Pedigree Matrix"))
                    del row["pedigree"]

            # Source
            if "source" in row:
                try:
                    ast = parser_field_parsers.string_to_ast(parser_field_parsers.reference, row["source"])
                    ref_id = ast["ref_id"]
                    references = glb_idx.get(Reference.partial_key(ref_id), ref_type="provenance")
                    if len(references) == 1:
                        source = references[0]
                except:
                    source = row["source"]
            else:
                source = None

            # Geolocation
            if "geolocation" in row:
                try:
                    ast = parser_field_parsers.string_to_ast(parser_field_parsers.reference, row["geolocation"])
                    ref_id = ast["ref_id"]
                    references = glb_idx.get(Reference.partial_key(ref_id), ref_type="geographic")
                    if len(references) == 1:
                        geolocation = references[0]
                except:
                    geolocation = row["geolocation"]
            else:
                geolocation = None

            # CREATE FactorType, A Type of Observable, IF it does not exist
            # AND ADD Quantitative Observation
            p, ft, f, o = create_or_append_quantitative_observation(
                glb_idx,
                factor=row["processor"]+":"+row["factor"],
                value=row["value"] if "value" in row else None,
                unit=row["unit"],
                observer=source,
                spread=row["uncertainty"] if "uncertainty" in row else None,
                assessment=row["assessment"] if "assessment" in row else None,
                pedigree=row["pedigree"] if "pedigree" in row else None,
                pedigree_template=row["pedigree_matrix"] if "pedigree_matrix" in row else None,
                relative_to=row["relative_to"] if "relative_to" in row else None,
                time=row["time"] if "time" in row else None,
                geolocation=None,
                comments=row["comments"] if "comments" in row else None,
                tags=None,
                other_attributes=other_attrs,
                proc_aliases=None,
                proc_external=False,  # TODO
                proc_attributes=p_attributes,
                proc_location=None,
                ftype_roegen_type=roegen_type,
                ftype_attributes=None,
                fact_external=not internal,
                fact_incoming=incoming,
                fact_location=geolocation
            )
            if p_set.append(p, glb_idx):  # Appends codes to the pset if the processor was not member of the pset
                p_set.append_attributes_codes(row["taxa"])

            # author = state.get("_identity")
            # if not author:
            #     author = "_anonymous"
            #
            # oer = glb_idx.get(Observer.partial_key(author))
            # if not oer:
            #     oer = Observer(author, "Current user" if author != "_anonymous" else "Default anonymous user")
            #     glb_idx.put(oer.key(), oer)
            # else:
            #     oer = oer[0]
        # -----------------------------------------------------------------------------------------------------

        glb_idx, p_sets, hh, datasets, mappings = get_case_study_registry_objects(state)

        # TODO Check semantic validity, elaborate issues
        issues = []

        p_set_name = self._name.split(" ")[1] if self._name.lower().startswith("processor") else self._name
        if self._name not in p_sets:
            p_set = ProcessorsSet(p_set_name)
            p_sets[p_set_name] = p_set
        else:
            p_set = p_sets[p_set_name]

        # Store code lists (flat "hierarchies")
        for h in self._content["code_lists"]:
            # TODO If some hierarchies already exist, check that they grow (if new codes are added)
            if h not in hh:
                hh[h] = []
            hh[h].extend(self._content["code_lists"][h])

        dataset_column_rule = parser_field_parsers.dataset_with_column

        processor_attributes = self._content["processor_attributes"]

        # Read each of the rows
        for i, r in enumerate(self._content["factor_observations"]):
            # Create processor, hierarchies (taxa) and factors
            # Check if the processor exists. Two ways to characterize a processor: name or taxa
            """
            ABOUT PROCESSOR NAME
            The processor can have a name and/or a set of qualifications, defining its identity
            If not defined, the name can be assumed to be the qualifications, concatenated
            Is assigning a name for all processors a difficult task? 
            * In the specification moment, it can get in the middle
            * When operating it is not so important
            * If taxa identify uniquely the processor, name is optional, automatically obtained from taxa
            * The benefit is that it can help reducing hierarchic names
            * It may help in readability of the case study
            
            """
            # If a row contains a reference to a dataset, expand it
            if "_referenced_dataset" in r:
                if r["_referenced_dataset"] in datasets:
                    ds = datasets[r["_referenced_dataset"]]  # Obtain dataset
                else:
                    ds = None
                    issues.append((3, "Dataset '" + r["_referenced_dataset"] + "' is not declared. Row "+str(i+1)))
            else:
                ds = None
            if ds:
                # Obtain a dict to map columns to dataset columns
                fixed_dict = {}
                var_dict = {}
                var_taxa_dict = {}
                for k in r:  # Iterate through columns in row "r"
                    if k == "taxa":
                        for t in r[k]:
                            if r[k][t].startswith("#"):
                                var_taxa_dict[t] = r[k][t][1:]
                        fixed_dict["taxa"] = r["taxa"].copy()
                    elif k in ["_referenced_dataset", "_processor_type"]:
                        continue
                    elif not r[k].startswith("#"):
                        fixed_dict[k] = r[k]  # Does not refer to the dataset
                    else:  # Starts with "#"
                        if k != "processor":
                            var_dict[k] = r[k][1:]  # Dimension
                        else:
                            fixed_dict[k] = r[k]  # Special
                # Check that the # names are in the Dataset
                if not case_sensitive:
                    s1 = {v.lower(): v for v in list(var_dict.values())+list(var_taxa_dict.values())}
                    s2 = {v.lower(): v for v in ds.data.columns}
                else:
                    s1 = {v: v for v in list(var_dict.values())+list(var_taxa_dict.values())}
                    s2 = {v: v for v in ds.data.columns}
                diff = set(s1.keys()).difference(set(s2.keys()))
                if diff:
                    # There are request fields in var_dict NOT in the input dataset "ds.data"
                    if len(diff) > 1:
                        v = "is"
                    else:
                        v = "are"
                    issues.append((3, "'"+', '.join(diff)+"' "+v+" not present in the requested dataset '"+r["_referenced_dataset"]+"'. Columns are: "+', '.join(ds.data.columns)+". Row " + str(i+1)))
                else:
                    # Iterate the dataset (a pd.DataFrame), row by row
                    for r_num, r2 in ds.data.iterrows():
                        r_exp = fixed_dict.copy()
                        if not case_sensitive:
                            r_exp.update({k: str(r2[s2[v.lower()]]) for k, v in var_dict.items()})
                        else:
                            r_exp.update({k: str(r2[s2[v]]) for k, v in var_dict.items()})
                        if var_taxa_dict:
                            taxa = r_exp["taxa"]
                            if not case_sensitive:
                                taxa.update({k: r2[s2[v.lower()]] for k, v in var_taxa_dict.items()})
                            else:
                                taxa.update({k: r2[s2[v]] for k, v in var_taxa_dict.items()})
                            if r_exp["processor"].startswith("#"):
                                r_exp["processor"] = "_".join([str(taxa[t]) for t in processor_attributes if t in taxa])
                        process_row(r_exp)
            else:  # Literal values
                process_row(r)

        return issues, None

    def estimate_execution_time(self):
        return 0

    def json_serialize(self):
        # Directly return the content
        return self._content

    def json_deserialize(self, json_input):
        # TODO Check validity
        issues = []
        if isinstance(json_input, dict):
            self._content = json_input
        else:
            self._content = json.loads(json_input)
        return issues
