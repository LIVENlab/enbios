import json
import logging
from collections import OrderedDict
import traceback
import numpy as np
import pandas as pd
from typing import List

import nexinfosys
from nexinfosys.common.helper import strcmp, create_dictionary, obtain_dataset_metadata, \
    translate_case

from nexinfosys.models import CodeImmutable
from nexinfosys.models.statistical_datasets import Dataset, Dimension, CodeList

from nexinfosys.model_services import IExecutableCommand, get_case_study_registry_objects


# TODO Currently is just a copy of "ETLExternalDatasetCommand"
# TODO The new command has to elaborate a MDataset
# TODO It has two new parameters: "InputDataset" and "AvailableAtDateTime"
# TODO Time dimension can be specified as "Time" or as "StartTime" "EndTime"
# TODO Result parameter column also change a bit
# TODO See "DatasetQry" command in "MuSIASEM case study commands" Google Spreadsheet
#
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
    return 100.0 * np.count_nonzero(np.isnan(x.astype(np.float))) / x.size


class DatasetQryCommand(IExecutableCommand):
    def __init__(self, name: str):
        self._name = name
        self._content = None

    def _create_new_dataset(self, name: str, in_ds: Dataset, df: pd.DataFrame, dimension_names: List[str], measure_names: List[str]):
        """
        Create a dataset using the original ds
        :param in_ds:
        :param df:
        :return:
        """
        ds = Dataset()

        ds.data = df
        ds.code = name
        ds.description = "Result of DatasetQry command"
        ds.attributes = {}
        ds.metadata = None
        ds.database = None

        for dimension in dimension_names:  # type: str
            d = Dimension()
            d.code = dimension
            d.description = None
            d.attributes = None
            d.is_time = "period" in dimension.lower()
            d.is_measure = False
            cl = df[dimension].unique().tolist()
            d.code_list = CodeList.construct(
                dimension, dimension, [""],
                codes=[CodeImmutable(c, c, "", []) for c in cl]
            )
            d.dataset = ds

        for measure in measure_names:  # type: str
            d = Dimension()
            d.code = measure
            d.description = None
            d.attributes = None
            d.is_time = False
            d.is_measure = True
            d.code_list = None
            d.dataset = ds

        return ds

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
        dataset_datetime = self._content["dataset_datetime"]

        # Result name
        result_name = self._content["result_name"]
        if result_name in datasets or state.get(result_name):
            issues.append((2, "A dataset called '"+result_name+"' is already stored in the registry of datasets"))

        # Dataset metadata
        dims, attrs, measures = obtain_dataset_metadata(dataset_name, source, datasets)

        # Obtain filter parameters
        params = create_dictionary()  # Native dimension name to list of values the filter will allow to pass
        joined_dimensions = []
        for dim in self._content["where"]:
            lst = self._content["where"][dim]
            native_dim = None
            if dim.lower() in ["startperiod", "starttime", "endperiod", "endtime"]:
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
        ds = nexinfosys.data_source_manager.get_dataset_filtered(source, dataset_name, params, datasets)
        df = ds.data

        # Join with mapped dimensions (augment it)
        mapping_dict = create_dictionary()
        for m in mappings:
            if strcmp(mappings[m].source, source) and \
                    strcmp(mappings[m].dataset, dataset_name) and \
                    mappings[m].origin in dims:
                # mapping_tuples.append((mappings[m].origin, mappings[m].destination, mappings[m].map))
                mapping_dict[mappings[m].origin] = (mappings[m].destination, {d["o"]: d["to"] for d in mappings[m].map})

        # If accelerated version not available, use slow version
        try:
            if nexinfosys.get_global_configuration_variable("ENABLE_CYTHON_OPTIMIZATIONS") == "True":
                from nexinfosys.restful_service.helper_accel import augment_dataframe_with_mapped_columns2 as augment_df
            else:
                raise Exception("Just to import the slow version")
        except:
            from nexinfosys.common.helper import augment_dataframe_with_mapped_columns as augment_df

        df = augment_df(df, mapping_dict, ["value"])

        # Aggregate (If any dimension has been specified)
        if len(self._content["group_by"]) > 0:
            # Column names where data is
            # HACK: for the case where the measure has been named "obs_value", use "value"
            values = [m.lower() if m.lower() != "obs_value" else "value" for m in self._content["measures"]]
            v2 = []
            for v in values:
                for c in df.columns:
                    if v.lower() == c.lower():
                        v2.append(c)
                        break
            values = v2

            # TODO: use metadata name (e.g. "OBS_VALUE") instead of hardcoded "value"
            # values = self._content["measures"]
            out_names = self._content["measures_as"]
            group_by_dims = translate_case(self._content["group_by"], df.columns)  # Group by dimension names
            lcase_group_by_dims = [d.lower() for d in group_by_dims]
            # Now joined_dimensions
            for d in joined_dimensions:
                if d.lower() in lcase_group_by_dims:
                    # Find and replace
                    for i, d2 in enumerate(group_by_dims):
                        if strcmp(d, d2):
                            group_by_dims[i] = d
                            break

            agg_funcs = []  # Aggregation functions
            agg_names = {}
            for f in self._content["agg_funcs"]:
                if f.lower() in ["avg", "average"]:
                    agg_funcs.append(np.average)
                    agg_names[np.average] = "avg"
                elif f.lower() in ["sum"]:
                    agg_funcs.append(np.sum)
                    agg_names[np.sum] = "sum"
                elif f.lower() in ["count"]:
                    agg_funcs.append(np.size)
                    agg_names[np.size] = "count"
                elif f.lower() in ["sumna"]:
                    agg_funcs.append(np.nansum)
                    agg_names[np.nansum] = "sumna"
                elif f.lower() in ["countav"]:
                    agg_funcs.append("count")
                    agg_names["count"] = "countav"
                elif f.lower() in ["avgna"]:
                    agg_funcs.append(np.nanmean)
                    agg_names[np.nanmean] = "avgna"
                elif f.lower() in ["pctna"]:
                    agg_funcs.append(pctna)
                    agg_names[pctna] = "pctna"

            # Calculate Pivot Table. The columns are a combination of values x aggregation functions
            # For instance, if two values ["v2", "v2"] and two agg. functions ["avg", "sum"] are provided
            # The columns will be: [["average", "v2"], ["average", "v2"], ["sum", "v2"], ["sum", "v2"]]
            try:
                # Check that all "group_by_dims" on which pivot table aggregates are present in the input "df"
                # If not either synthesize them (only if there is a single filter value) or remove (if not present
                for r in group_by_dims.copy():
                    df_columns_dict = create_dictionary(data={c: None for c in df.columns})
                    if r not in df_columns_dict:
                        found = False
                        for k in params:
                            if strcmp(k, r):
                                found = True
                                if len(params[k]) == 1:
                                    df[k] = params[k][0]
                                else:
                                    group_by_dims.remove(r)
                                    issues.append((2, "Dimension '" + r + "' removed from the list of dimensions because it is not present in the raw input dataset."))
                                break
                        if not found:
                            group_by_dims.remove(r)
                            issues.append((2, "Dimension '" + r + "' removed from the list of dimensions because it is not present in the raw input dataset."))

                # Create and register Hierarchy objects from origin Dataset dimensions: state, ds
                ds_columns_dict = create_dictionary(data={c.code: c.code for c in ds.dimensions})
                for r in group_by_dims:
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
                if True:
                    groups = df.groupby(by=group_by_dims, as_index=False)  # Split
                    d = OrderedDict([])
                    lst_names = []
                    if len(values) == len(agg_funcs):
                        for i, (value, agg_func) in enumerate(zip(values, agg_funcs)):
                            if len(out_names) == len(values) and out_names[i]:
                                lst_names.append(out_names[i])
                            else:
                                lst_names.append(agg_names[agg_func] + "_" +value)
                            lst = d.get(value, [])
                            lst.append(agg_func)
                            d[value] = lst
                    else:
                        for value in values:
                            lst = d.get(value, [])
                            for agg_func in agg_funcs:
                                lst.append(agg_func)
                                lst_names.append(agg_names[agg_func] + "_" +value)
                            d[value] = lst
                    # Print NaN values for each value column
                    for value in set(values):
                        cnt = df[value].isnull().sum()
                        logging.debug("NA count for col '"+value+"': "+str(cnt)+" of "+str(df.shape[0]))
                    # AGGREGATE !!
                    df2 = groups.agg(d)

                    # Rename the aggregated columns
                    df2.columns = group_by_dims + lst_names
                # else:
                #     # Pivot table
                #     df2 = pd.pivot_table(df,
                #                          values=values,
                #                          index=group_by_dims,
                #                          aggfunc=[agg_funcs[0]], fill_value=np.NaN, margins=False,
                #                          dropna=True)
                #     # Remove the multiindex in columns
                #     df2.columns = [col[-1] for col in df2.columns.values]
                #     # Remove the index
                #     df2.reset_index(inplace=True)
                # The result, all columns (no index), is stored for later use
                ds = self._create_new_dataset(result_name, ds, df2, group_by_dims, out_names)
            except Exception as e:
                traceback.print_exc()
                issues.append((3, "There was a problem: "+str(e)))

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
