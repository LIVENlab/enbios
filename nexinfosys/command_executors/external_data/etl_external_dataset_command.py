import json
import logging
from collections import OrderedDict

import pandas as pd
import numpy as np

import nexinfosys
from nexinfosys.model_services import IExecutableCommand, get_case_study_registry_objects
from nexinfosys.common.helper import obtain_dataset_metadata, strcmp, create_dictionary, \
    augment_dataframe_with_mapped_columns, translate_case
from nexinfosys.models.musiasem_concepts import Hierarchy
from nexinfosys.models.musiasem_concepts_helper import convert_code_list_to_hierarchy


def obtain_reverse_codes(mapped, dst):
    """
    Given the list of desired dst codes and an extensive map src -> dst,
    obtain the list of src codes

    :param mapped: Correspondence between src codes and dst codes [{"o", "to": [{"d", "e"}]}]
    :param dst: Iterable of destination codes
    :return: List of origin codes
    """
    src = set()
    dest_set = set([d.lower() for d in dst])  # Destination categories
    # Obtain origin categories referencing "dest_set" destination categories
    for k in mapped:
        for t in k["to"]:
            if t["d"] and t["d"].lower() in dest_set:
                src.add(k["o"])
    return list(src)  # list(set([k[0].lower() for k in mapped if k[1].lower() in dest_set]))


def pctna(x):
    """
    Aggregation function computing the percentage of NaN values VS total number of elements, in a group "x"
    """
    return 100.0 * np.count_nonzero(np.isnan(x)) / x.size


class ETLExternalDatasetCommand(IExecutableCommand):
    def __init__(self, name: str):
        self._name = name
        self._content = None

    def execute(self, state: "State"):
        """
        First bring the data considering the filter
        Second, group, third aggregate
        Finally, store the result in State
        """
        issues = []
        # Obtain global variables in state
        glb_idx, p_sets, hh, datasets, mappings = get_case_study_registry_objects(state)

        # DS Source + DS Name
        source = self._content["dataset_source"]
        dataset_name = self._content["dataset_name"]

        # Result name
        result_name = self._content["result_name"]
        if result_name in datasets or state.get(result_name):
            issues.append((2, "A dataset called '"+result_name+"' is already stored in the registry of datasets"))

        # Dataset metadata
        dims, attrs, meas = obtain_dataset_metadata(dataset_name, source)
        # Obtain filter parameters
        params = create_dictionary()  # Native dimension name to list of values the filter will allow to pass
        joined_dimensions = []
        for dim in self._content["where"]:
            lst = self._content["where"][dim]
            native_dim = None
            if dim.lower() in ["startperiod", "endperiod"]:
                native_dim = dim
                lst = [lst]
            elif dim not in dims:
                # Check if there is a mapping. If so, obtain the native equivalent(s). If not, ERROR
                for m in mappings:
                    if strcmp(mappings[m].destination, dim) and \
                            strcmp(mappings[m].source, source) and \
                            strcmp(mappings[m].dataset, dataset_name) and \
                            mappings[m].origin in dims:
                        joined_dimensions.append(mappings[m].destination)  # Store dimension in the original case
                        native_dim = mappings[m].origin
                        lst = obtain_reverse_codes(mappings[m].map, lst)
                        break
            else:
                # Get the dimension name with the original case
                native_dim = dims[dim].name
            if native_dim:
                if native_dim not in params:
                    f = set()
                    params[native_dim] = f
                else:
                    f = params[native_dim]
                f.update(lst)

        # Convert param contents from set to list
        for p in params:
            params[p] = [i for i in params[p]]

        # Obtain the filtered Dataset <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<
        ds = nexinfosys.data_source_manager.get_dataset_filtered(source, dataset_name, params)
        df = ds.data

        # Join with mapped dimensions (augment it)
        # TODO Prepare an "m" containing ALL the mappings affecting "df"
        # TODO df2 = augment_dataframe_with_mapped_columns(df, m, ["value"])
        # TODO Does it allow adding the new column for the dimension, in case it is requested? Probably yes, but test it
        for m in mappings:
            if strcmp(mappings[m].source, source) and \
                    strcmp(mappings[m].dataset, dataset_name) and \
                    mappings[m].origin in dims:
                # TODO Change by many-to-many mapping
                # TODO augment_dataframe_with_mapped_columns(df, maps, measure_columns)
                # Elaborate a many to one mapping
                tmp = []
                for el in mappings[m].map:
                    for to in el["to"]:
                        if to["d"]:
                            tmp.append([el["o"], to["d"]])
                df_dst = pd.DataFrame(tmp, columns=['sou_rce', mappings[m].destination])
                for di in df.columns:
                    if strcmp(mappings[m].origin, di):
                        d = di
                        if not nexinfosys.case_sensitive:
                            df[d+"_l"] = df[d].str.lower()
                            d = d + "_l"
                        break
                df = pd.merge(df, df_dst, how='left', left_on=d, right_on='sou_rce')
                del df['sou_rce']
                if not nexinfosys.case_sensitive:
                    del df[d]

        # Aggregate (If any dimension has been specified)
        if len(self._content["group_by"]) > 0:
            # Column names where data is
            # HACK: for the case where the measure has been named "obs_value", use "value"
            values = [m.lower() if m.lower() != "obs_value" else "value" for m in self._content["measures"]]
            out_names = self._content["measures_as"]
            rows = translate_case(self._content["group_by"], params)  # Group by dimension names
            lcase_rows = [d.lower() for d in rows]
            # Now joined_dimensions
            for d in joined_dimensions:
                if d.lower() in lcase_rows:
                    # Find and replace
                    for i, d2 in enumerate(rows):
                        if strcmp(d, d2):
                            rows[i] = d
                            break

            aggs = []  # Aggregation functions
            agg_names = {}
            for f in self._content["agg_funcs"]:
                if f.lower() in ["avg", "average"]:
                    aggs.append(np.average)
                    agg_names[np.average] = "avg"
                elif f.lower() in ["sum"]:
                    aggs.append(np.sum)
                    agg_names[np.sum] = "sum"
                elif f.lower() in ["count"]:
                    aggs.append(np.size)
                    agg_names[np.size] = "count"
                elif f.lower() in ["sumna"]:
                    aggs.append(np.nansum)
                    agg_names[np.nansum] = "sumna"
                elif f.lower() in ["countav"]:  # countav=="Count Available"
                    aggs.append("count")  # Count number of non-NaN elements
                    agg_names["count"] = "countav"
                elif f.lower() in ["avgav", "avgna"]:  # avgna=="Average without
                    aggs.append(np.nanmean)
                    agg_names[np.nanmean] = "avgna"
                elif f.lower() in ["pctna"]:  # % of NaN vs total elements
                    aggs.append(pctna)
                    agg_names[pctna] = "pctna"

            # Calculate Pivot Table. The columns are a combination of values x aggregation functions
            # For instance, if two values ["v2", "v2"] and two agg. functions ["avg", "sum"] are provided
            # The columns will be: [["average", "v2"], ["average", "v2"], ["sum", "v2"], ["sum", "v2"]]
            try:
                # Check that all "rows" on which pivot table aggregates are present in the input "df"
                # If not either synthesize them (only if there is a single filter value) or remove (if not present
                df_columns_dict = create_dictionary(data={c: c for c in df.columns})
                for r in rows.copy():
                    if r not in df_columns_dict:
                        found = False
                        for k in params:
                            if strcmp(k, r):
                                found = True
                                if len(params[k]) == 1:
                                    df[r] = params[k][0]
                                else:
                                    rows.remove(r)
                                    issues.append((2, "Dimension '" + r + "' removed from the list of dimensions because it is not present in the raw input dataset."))
                                break
                        if not found:
                            rows.remove(r)
                            issues.append((2, "Dimension '" + r + "' removed from the list of dimensions because it is not present in the raw input dataset."))
                # Put proper DIMENSION names
                for ir, r in enumerate(rows):
                    if r in df_columns_dict:
                        rows[ir] = df_columns_dict[r]

                # Create and register Hierarchy objects from origin Dataset dimensions: state, ds
                ds_columns_dict = create_dictionary(data={c.code: c.code for c in ds.dimensions})
                for r in rows:
                    if r in ds_columns_dict:
                        # Create hierarchy local to the dataset
                        for d in ds.dimensions:
                            if strcmp(r, d.code):
                                if d.code_list:
                                    h = convert_code_list_to_hierarchy(d.code_list)
                                    h.name = result_name + "_" + r
                                    glb_idx.put(h.key(), h)
                                    break

                # Pivot table using Group by
                # if True:
                groups = df.groupby(by=rows, as_index=False)  # Split
                d = OrderedDict([])
                lst_names = []
                if len(values) == len(aggs):
                    for i, t in enumerate(zip(values, aggs)):
                        v, agg = t
                        if len(out_names) == len(values):
                            if out_names[i]:
                                lst_names.append(out_names[i])
                            else:
                                lst_names.append(agg_names[agg] + "_" + v)
                        else:
                            lst_names.append(agg_names[agg] + "_" +v)
                        lst = d.get(v, [])
                        lst.append(agg)
                        d[v] = lst
                else:
                    for v in values:
                        lst = d.get(v, [])
                        for agg in aggs:
                            lst.append(agg)
                            lst_names.append(agg_names[agg] + "_" +v)
                        d[v] = lst
                # Print NaN values for each value column
                for v in set(values):
                    cnt = df[v].isnull().sum()
                    logging.debug("NA count for col '"+v+"': "+str(cnt)+" of "+str(df.shape[0]))
                # AGGREGATE !!
                df2 = groups.agg(d)

                # Rename the aggregated columns
                df2.columns = rows + lst_names
                # else:
                #     # Pivot table
                #     df2 = pd.pivot_table(df,
                #                          values=values,
                #                          index=rows,
                #                          aggfunc=[aggs[0]], fill_value=np.NaN, margins=False,
                #                          dropna=True)
                #     # Remove the multiindex in columns
                #     df2.columns = [col[-1] for col in df2.columns.values]
                #     # Remove the index
                #     df2.reset_index(inplace=True)
                # The result, all columns (no index), is stored for later use
                ds.data = df2
            except Exception as e:
                issues.append((3, "There was a problem with the grouping: "+repr(e)))

        # Store the dataset in State
        datasets[result_name] = ds

        return issues, None

    def estimate_execution_time(self):
        return 0

    def json_serialize(self):
        # Directly return the metadata dictionary
        return self._content

    def json_deserialize(self, json_input):
        # TODO Read and check keys validity
        issues = []
        if isinstance(json_input, dict):
            self._content = json_input
        else:
            self._content = json.loads(json_input)

        return issues


