"""
Helper functions demanding acceleration using Cython

Currently, only the function to map "Many to One" and "Many to Many":
"augment_dataframe_with_mapped_columns2"

Because it cannot be debugged directly, use of the pure Python version:
"augment_dataframe_with_mapped_columns"

is recommended to find bugs. Changes should be applied to both functions.

Modules using these functions must include, before importing the module, the following lines:

import pyximport
pyximport.install(reload_support=True, language_level=3)

"""
import itertools

import numpy as np
import pandas as pd
from pandas import DataFrame
from typing import Dict, Tuple, List

def augment_dataframe_with_mapped_columns2(
        df: DataFrame,
        dict_of_maps: Dict[str, Tuple[str, List[Dict]]],
        measure_columns: List[str]) -> DataFrame:
    """
    Elaborate a pd.DataFrame from the input DataFrame "df" and
    "dict_of_maps" which is a dictionary of "source_column" to a tuple ("destination_column", map)
    where map is of the form:
        [ {origin category: [{d: destination category, w: weight assigned to destination category}] } ]

    Support not only "Many to One" (ManyToOne) but also "Many to Many" (ManyToMany)

    :param df: pd.DataFrame to process
    :param dict_of_maps: dictionary from "source" to a tuple of ("destination", "map"), see previous introduction
    :param measure_columns: list of measure column names in "df"
    :return: The pd.DataFrame resulting from the mapping
    """
    mapped_cols = {}  # A dict from mapped column names to column index in "m"
    measure_cols = {}  # A dict from measure columns to column index in "m". These will be the columns affected by the mapping weights
    non_mapped_cols = {}  # A dict of non-mapped columns (the rest)
    for i, c in enumerate(df.columns):
        if c in dict_of_maps:
            mapped_cols[c] = i
        elif c in measure_columns:
            measure_cols[c] = i
        else:
            non_mapped_cols[c] = i

    # "np.ndarray" from "pd.DataFrame" (no index, no column labels, only values)
    m = df.values

    # First pass is just to obtain the size of the target ndarray
    ncols = 2*len(mapped_cols) + len(non_mapped_cols) + len(measure_cols)
    nrows = 0
    for r in range(m.shape[0]):
        # Obtain each code combination
        n_subrows = 1
        for c_name, c in mapped_cols.items():
            map_ = dict_of_maps[c_name][1]
            code = m[r, c]
            n_subrows *= len(map_[code])
        nrows += n_subrows

    # Second pass, to elaborate the elements of the destination array
    new_cols_base = len(mapped_cols)
    non_mapped_cols_base = 2*len(mapped_cols)
    measure_cols_base = non_mapped_cols_base + len(non_mapped_cols)

    # Output matrix column names
    col_names = [col for col in mapped_cols]
    col_names.extend([dict_of_maps[col][0] for col in mapped_cols])
    col_names.extend([col for col in non_mapped_cols])
    col_names.extend([col for col in measure_cols])
    assert len(col_names) == ncols

    # Output matrix
    mm = np.empty((nrows, ncols), dtype=object)

    # For each ROW of ORIGIN matrix
    row = 0  # Current row in output matrix
    for r in range(m.shape[0]):
        # Obtain combinations from current codes
        lst = []
        n_subrows = 1
        for c_name, c in mapped_cols.items():
            map_ = dict_of_maps[c_name][1]
            code = m[r, c]
            n_subrows *= len(map_[code])
            lst.append(map_[code])

        combinations = list(itertools.product(*lst))

        for icomb in range(n_subrows):
            combination = combinations[icomb]
            # Mapped columns
            # At the same time, compute the weight for the measures
            w = 1.0
            for i, col in enumerate(mapped_cols.keys()):
                mm[row+icomb, i] = m[r, mapped_cols[col]]
                mm[row+icomb, new_cols_base+i] = ifnull(combination[i]["d"], '')
                if combination[i]["w"]:
                    w *= float(combination[i]["w"])

            # Fill the measures
            for i, col in enumerate(measure_cols.keys()):
                mm[row+icomb, measure_cols_base+i] = w * m[r, measure_cols[col]]

            # Non-mapped columns
            for i, col in enumerate(non_mapped_cols.keys()):
                mm[row+icomb, non_mapped_cols_base+i] = m[r, non_mapped_cols[col]]

        row += n_subrows

    # Now elaborate a DataFrame back
    tmp = pd.DataFrame(data=mm, columns=col_names)

    return tmp

def ifnull(var, val):
    if var is None:
        return val
    return var
