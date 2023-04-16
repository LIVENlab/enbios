# -*- coding: utf-8 -*-
import ast
import base64
import collections
import io
import itertools
import json
import logging
import mimetypes
import os
import re
import tempfile
import urllib
import urllib.request
import uuid
from enum import Enum
from functools import partial, reduce
from operator import add, mul, sub, truediv
from typing import IO, List, Tuple, Dict, Any, Optional, Iterable, Callable, TypeVar, Type, Union, SupportsFloat
from urllib.parse import urlparse
from uuid import UUID

import jsonpickle
import numpy as np
import pandas as pd
import pycurl
from multidict import MultiDict, CIMultiDict
from pandas import DataFrame

import nexinfosys
from nexinfosys import case_sensitive, SDMXConcept, get_global_configuration_variable
from nexinfosys.common.decorators import deprecated
from nexinfosys.ie_imports.google_drive import download_xlsx_file_id
from nexinfosys.models import log_level

logger = logging.getLogger(__name__)
logger.setLevel(log_level)


def append_line(str, file=None):
    if not file:
        file = "/app/borrame.txt"
    f = open(file, "a+")
    f.write(str+"\n")
    f.close()


# #####################################################################################################################
# >>>> JSON FUNCTIONS <<<<
# #####################################################################################################################

def _json_serial(obj):
    """JSON serializer for objects not serializable by default json code"""
    from datetime import datetime
    from nexinfosys.command_generators import Issue  # Imported here to break circularity of imports

    if isinstance(obj, datetime):
        serial = obj.isoformat()
        return serial
    elif isinstance(obj, CaseInsensitiveDict):
        return str(obj)
    elif isinstance(obj, np.int64):
        return int(obj)
    elif isinstance(obj, Issue):
        return obj.__repr__()
    raise TypeError(f"Type {type(obj)} not serializable")


JSON_INDENT = 4
ENSURE_ASCII = False


def generate_json(o):
    return json.dumps(o,
                      default=_json_serial,
                      sort_keys=True,
                      indent=JSON_INDENT,
                      ensure_ascii=ENSURE_ASCII,
                      separators=(',', ': ')
                      ) if o else None


class Encodable:
    """
    Abstract class with the method encode() that should be implemented by a subclass to be encoded into JSON
    using the json.dumps() method together with the option cls=CustomEncoder.
    """
    def encode(self) -> Dict[str, Any]:
        raise NotImplementedError("users must define encode() to use this base class")

    @staticmethod
    def parents_encode(obj: "Encodable", cls: type) -> Dict[str, Any]:
        """
        Get the state of all "cls" parent classes for the selected instance "obj"
        :param obj: The instance. Use "self".
        :param cls: The base class which parents we want to get. Use "__class__".
        :return: A dictionary with the state of the instance "obj" for all inherited classes.

        """
        d = {}
        for parent in cls.__bases__:
            if issubclass(parent, Encodable) and parent is not Encodable:
                d.update(parent.encode(obj))
        return d


class CustomEncoder(json.JSONEncoder):
    """
    Encoding class used by json.dumps(). It should be passed as the "cls" argument.
    Example: print(json.dumps({'A': 2, 'b': 4}), cls=CustomEncoder)
    """
    def default(self, obj):
        # Does the object implement its own encoder?
        if isinstance(obj, Encodable):
            return obj.encode()

        # Use the default encoder for handled types
        if isinstance(obj, (list, dict, str, int, float, bool, type(None))):
            return json.JSONEncoder.default(self, obj)

        # For other unhandled types, like set, use universal json encoder "jsonpickle"
        return jsonpickle.encode(obj, unpicklable=False)


# #####################################################################################################################
# >>>> CASE SeNsItIvE or INSENSITIVE names (flows, funds, processors, ...) <<<<
# #####################################################################################################################
# from nexinfosys.models.musiasem_concepts import Taxon  IMPORT LOOP !!!!! AVOID !!!!


class CaseInsensitiveDict(collections.MutableMapping, Encodable):
    """
    A dictionary with case insensitive Keys.
    Prepared also to support TUPLES as keys, required because compound keys are required
    """
    def __init__(self, data=None, **kwargs):
        from collections import OrderedDict
        self._store = OrderedDict()
        if data is None:
            data = {}
        self.update(data, **kwargs)

    def encode(self):
        return self.get_data()

    def get_original_data(self):
        return {casedkey: mappedvalue for casedkey, mappedvalue in self._store.values()}

    def get_data(self):
        return {key: self._store[key][1] for key in self._store}

    def __setitem__(self, key, value):
        # Use the lowercased key for lookups, but store the actual
        # key alongside the value.
        if not isinstance(key, tuple):
            self._store[key.lower()] = (key, value)
        else:
            self._store[tuple([k.lower() for k in key])] = (key, value)

    def __getitem__(self, key):
        if not isinstance(key, tuple):
            return self._store[key.lower()][1]
        else:
            return self._store[tuple([k.lower() for k in key])][1]

    def __delitem__(self, key):
        if not isinstance(key, tuple):
            del self._store[key.lower()]
        else:
            del self._store[tuple([k.lower() for k in key])]

    def __iter__(self):
        return (casedkey for casedkey, mappedvalue in self._store.values())

    def __len__(self):
        return len(self._store)

    def lower_items(self):
        """Like iteritems(), but with all lowercase keys."""
        return (
            (lowerkey, keyval[1])
            for (lowerkey, keyval)
            in self._store.items()
        )

    def __contains__(self, key):  # "in" operator to check if the key is present in the dictionary
        if not isinstance(key, tuple):
            return key.lower() in self._store
        else:
            return tuple([k.lower() for k in key]) in self._store

    def __eq__(self, other):
        if isinstance(other, collections.Mapping):
            other = CaseInsensitiveDict(other)
        else:
            return NotImplemented
        # Compare insensitively
        return dict(self.lower_items()) == dict(other.lower_items())

    # Copy is required
    def copy(self):
        return CaseInsensitiveDict(self._store.values())

    def __repr__(self):
        return str(dict(self.items()))


def create_dictionary(case_sens=case_sensitive, multi_dict=False, data=dict()):
    """
    Factory to create dictionaries

    :param case_sens: True to create a case sensitive dictionary, False to create a case insensitive one
    :param multi_dict: True to create a "MultiDict", capable of storing several values
    :param data: Dictionary with which the new dictionary is initialized
    :return:
    """

    if not multi_dict:
        if case_sens:
            tmp = {}
            tmp.update(data)
            return tmp  # Normal, "native" dictionary
        else:
            return CaseInsensitiveDict(data)
    else:
        if case_sens:
            return MultiDict(data)
        else:
            return CIMultiDict(data)


def strcmp(s1, s2):
    """
    Compare two strings for equality or not, considering a flag for case sensitiveness or not

    It also removes leading and trailing whitespace from both strings, so it is not sensitive to this possible
    difference, which can be a source of problems

    :param s1:
    :param s2:
    :return:
    """
    # Handling empty or None strings
    if not s1:
        return True if not s2 else False
    if not s2:
        return False

    if case_sensitive:
        return s1.strip() == s2.strip()
    else:
        return s1.strip().lower() == s2.strip().lower()


def istr(s1: str) -> str:
    """ Return a lowercase version of a string if program works ignoring case sensitiveness """
    if case_sensitive:
        return s1
    else:
        return s1.lower()

# #####################################################################################################################
# >>>> DYNAMIC IMPORT <<<<
# #####################################################################################################################


def import_names(package, names):
    """
    Dynamic import of a list of names from a module

    :param package: String with the name of the package
    :param names: Name or list of names, string or list of strings with the objects inside the package
    :return: The list (or not) of objects under those names
    """
    if not isinstance(names, list):
        names = [names]
        not_list = True
    else:
        not_list = False

    try:
        tmp = __import__(package, fromlist=names)
    except:
        tmp = None

    if tmp:
        tmp2 = [getattr(tmp, name) for name in names]
        if not_list:
            tmp2 = tmp2[0]
        return tmp2
    else:
        return None

# #####################################################################################################################
# >>>> KEY -> VALUE STORE, WITH PARTIAL KEY INDEXATION <<<<
# #####################################################################################################################


@deprecated  # DEPRECATED!!!: Use PartialRetrievalDictionary
class PartialRetrievalDictionary2:
    """
    Implementation using pd.DataFrame, very slow!!!

    The key is a dictionary, the value an object. Allows partial search.

    >> IF "case_sensitive==False" -> VALUES are CASE INSENSITIVE <<<<<<<<<<<<<<<<<

        It is prepared to store different key compositions, with a pd.DataFrame per key
        When retrieving (get), it can match several of these, so it can return results from different pd.DataFrames

        pd.DataFrame with MultiIndex:

import pandas as pd
df = pd.DataFrame(columns=["a", "b", "c"])  # Empty DataFrame
df.set_index(["a", "b"], inplace=True)  # First columns are the MultiIndex
df.loc[("h", "i"), "c"] = "hi"  # Insert values in two cells
df.loc[("h", "j"), "c"] = "hj"
df.loc[(slice(None), "j"), "c"]  # Retrieve using Partial Key
df.loc[("h", slice(None)), "c"]
    """
    def __init__(self):
        self._dfs = {}  # Dict from sorted key-dictionary-keys (the keys in the key dictionary) to DataFrame
        self._df_sorted = {}  # A Dict telling if each pd.DataFrame is sorted
        self._key_lst = []  # List of sets of keys, used in "get" when matching partial keys

    def put(self, key, value):
        """
        Store a value using a dictionary key which can have None values

        :param key:
        :param value:
        :return:
        """
        # Sort keys
        keys = [k for k in key]
        s_tuple = tuple(sorted(keys))
        if case_sensitive:
            df_key = tuple([key[k] if key[k] is not None else slice(None) for k in s_tuple])
        else:
            df_key = tuple([(key[k] if k.startswith("__") else key[k].lower()) if key[k] is not None else slice(None) for k in s_tuple])

        if s_tuple not in self._dfs:
            # Append to list of sets of keys
            self._key_lst.append(set(keys))
            # Add New DataFrame to the dictionary of pd.DataFrame
            cols = [s for s in s_tuple]
            cols.append("value")
            df = pd.DataFrame(columns=cols)
            self._dfs[s_tuple] = df
            df.set_index([s for s in s_tuple], inplace=True)
        else:
            # Use existing pd.DataFrame
            df = self._dfs[s_tuple]

        # Do the insertion into the pd.DataFrame
        df.loc[df_key, "value"] = value

        # Flag the pd.DataFrame as unsorted
        self._df_sorted[s_tuple] = False

    def get(self, key, key_and_value: bool=False):
        """
        Return elements of different kinds, matching the totally or partially specified key.

        :param key: A dictionary with all or part of the key of elements to be retrieved
        :param key_and_value: If True, return a list of tuple (key, value). If not, return a list of values
        :return: A list of elements which can be (key, value) or "value", depending on the parameter "key_and_value"
        """
        keys = [k for k in key]
        s_tuple = tuple(sorted(keys))
        if s_tuple not in self._dfs:
            # Try partial match
            s = set(keys)
            df = []
            for s2 in self._key_lst:
                if s.issubset(s2):
                    s_tuple = tuple(sorted(s2))
                    df.append((self._dfs[s_tuple], s_tuple))
        else:
            df = [(self._dfs[s_tuple], s_tuple)]

        if df:
            res = []
            for df_, s_tuple in df:
                try:
                    if case_sensitive:
                        df_key = tuple([key[k] if k in key else slice(None) for k in s_tuple])
                    else:
                        df_key = tuple([(key[k] if k.startswith("__") else key[k].lower()) if k in key else slice(None) for k in s_tuple])

                    if s_tuple not in self._df_sorted or not self._df_sorted[s_tuple]:
                        df_.sort_index(ascending=True, inplace=True)
                        self._df_sorted[s_tuple] = True

                    tmp = df_.loc[df_key, "value"]
                    if isinstance(tmp, pd.Series):
                        for i, v in enumerate(tmp):
                            if key_and_value:
                                k = {rk: rv for rk, rv in zip(s_tuple, tmp.index[i])}
                                res.append((k, v))
                            else:
                                res.append(v)
                    else:  # Single result, standardize to always (k, v)
                        if key_and_value:
                            k = {rk: rv for rk, rv in zip(s_tuple, df_key)}
                            res.append((k, tmp))
                        else:
                            res.append(tmp)
                except (IndexError, KeyError):
                    pass
                except Exception as e:
                    pass
            return res
        else:
            return []

    def delete(self, key):
        """
        Remove elements matching each of the keys, total or partial, passed to the method

        :param key:
        :return:
        """

        def delete_single(key_):
            """
            Remove elements matching the total or partial key

            :param key_:
            :return:
            """
            """
            Return elements of different kinds, matching the totally or partially specified key.
    
            :param key: A dictionary with all or part of the key of elements to be retrieved
            :param key_and_value: If True, return a list of tuple (key, value). If not, return a list of values
            :return: A list of elements which can be (key, value) or "value", depending on the parameter "key_and_value"
            """
            keys = [k_ for k_ in key_]
            s_tuple = tuple(sorted(keys))
            if s_tuple not in self._dfs:
                # Try partial match
                s = set(keys)
                df = []
                for s2 in self._key_lst:
                    if s.issubset(s2):
                        s_tuple = tuple(sorted(s2))
                        df.append((self._dfs[s_tuple], s_tuple))
            else:
                df = [(self._dfs[s_tuple], s_tuple)]

            if df:
                res = 0
                for df_, s_tuple in df:
                    if case_sensitive:
                        df_key = tuple([key_[k_] if k_ in key_ else slice(None) for k_ in s_tuple])
                    else:
                        df_key = tuple([(key_[k_] if k_.startswith("__") else key_[k_].lower()) if k_ in key_ else slice(None) for k_ in s_tuple])
                    try:
                        tmp = df_.loc[df_key, "value"]
                        if isinstance(tmp, pd.Series):
                            df_.drop(tmp.index, inplace=True)
                            res += len(tmp)
                        else:  # Single result, standardize to always (k, v)
                            df_.drop(df_key, inplace=True)
                            res += 1
                    except (IndexError, KeyError):
                        pass
                return res
            else:
                return 0

        if isinstance(key, list):
            res_ = 0
            for k in key:
                res_ += delete_single(k)
            return res_
        else:
            return delete_single(key)

    def to_pickable(self):
        # Convert to a jsonpickable structure
        out = {}
        for k in self._dfs:
            df = self._dfs[k]
            out[k] = df.to_dict()
        return out

    def from_pickable(self, inp):
        # Convert from the jsonpickable structure (obtained by "to_pickable") to the internal structure
        self._dfs = {}
        self._key_lst = []
        for t in inp:
            t_ = ast.literal_eval(t)
            self._key_lst.append(set(t_))
            inp[t]["value"] = {ast.literal_eval(k): v for k, v in inp[t]["value"].items()}
            df = pd.DataFrame(inp[t])
            df.index.names = t_
            self._dfs[t_] = df

        return self  # Allows the following: prd = PartialRetrievalDictionary().from_pickable(inp)


class PartialRetrievalDictionary:
    """
    A (k, v) storage where "k" is Dict of (str, str) and "v" is an object:

    reg = PartialRetrievalDictionary()
    reg.put(dict(a="1", b="3"), [4, 5, 9])

    The retrieval method, "get", is passed a partial key also in the form of a Dict of (str, str) and returns a list
    of objects matching the partial key:

    reg.get(dict(a="1")) -> [[4, 5, 9]]

    NOTE: when inserting, both "k" and "v" (object) should be unique

    """
    def __init__(self):
        # A dictionary of key-name to dictionaries, where the dictionaries are each of the values of the key and the
        # value is a set of IDs having that value
        # dict(key-name, dict(key-value, set(obj-IDs with that key-value))
        self._keys = {}
        # Dictionary from ID to the tuple (composite-key-elements dict, object)
        self._objs = {}
        self._rev_objs = {}  # From object to ID
        # Counter
        self._id_counter = 0

    def get(self, key, key_and_value=False, full_key=False, just_oid=False, just_key=False):
        """
        Retrieve one or more objects matching "key"
        If "key_and_value" is True, return not only the value, also matching key (useful for multiple matching keys)
        If "full_key" is True, zero or one objects should be the result
        :param key:
        :param full_key:
        :return: A list of matching elements
        """
        if True:
            # Lower case values
            # Keys can be all lower case, because they will be internal Key components, not specified by users
            if case_sensitive:
                key2 = {k.lower(): v for k, v in key.items()}
            else:
                key2 = {k.lower(): v if k.startswith("__") else v.lower() if v else None for k, v in key.items()}
        else:
            key2 = key

        # List of sets, one per element of "key". If a key is "None", set of all values for that key
        sets = [self._keys.get(k, {}).get(v, set())
                if v is not None else
                set().union(*self._keys.get(k, {}).values())
                for k, v in key2.items()]

        # To speed up intersection of all sets, use smaller set as reference
        min_len = 1e30
        min_len_set_idx = None
        for i, s in enumerate(sets):
            if len(s) < min_len:
                min_len = len(s)
                min_len_set_idx = i
        min_len_set = sets[min_len_set_idx]
        del sets[min_len_set_idx]
        # Compute intersections
        result = min_len_set.intersection(*sets)
        if just_oid:
            return result

        # Obtain list of results
        if full_key and len(result) > 1:
            raise Exception("Zero or one results were expected. "+str(len(result)+" obtained."))
        if not just_key:
            if not key_and_value:
                return [self._objs[oid][1] for oid in result]
            else:
                return [self._objs[oid] for oid in result]
        else:
            return [self._objs[oid][0] for oid in result]

    def get_one(self, key, key_and_value=False, full_key=False, just_oid=False):
        results = self.get(key, key_and_value, full_key, just_oid)
        return results[0] if results else None

    def put(self, key: Dict[Tuple, object], value):
        """
        Insert implies the key does not exist
        Update implies the key exists
        Upsert does not care

        :param key:
        :param value:
        :return:
        """
        ptype = 'i'  # 'i', 'u', 'ups' (Insert, Update, Upsert)
        if True:
            # Lower case values
            # Keys can be all lower case, because they will be internal Key components, not specified by users
            if case_sensitive:
                key2 = {k.lower(): v for k, v in key.items()}
            else:
                key2 = {k.lower(): v if k.startswith("__") else v.lower() if isinstance(v, str) else v for k, v in key.items()}
        else:
            key2 = key
        # Arrays containing key: values "not-present" and "present"
        not_present = []  # List of tuples (dictionary of key-values, value to be stored)
        present = []  # List of sets storing IDs having same key-value
        for k, v in key2.items():
            d = self._keys.get(k, {})
            if len(d) == 0:
                self._keys[k] = d
            if v not in d:  # A new value "v" for subkey "k"
                not_present.append((d, v))
            else:
                present.append(d.get(v))

        if len(not_present) > 0:
            is_new = True
        else:
            if len(present) > 1:
                is_new = len(present[0].intersection(*present[1:])) == 0
            elif len(present) == 1:
                is_new = len(present) == 0
            else:
                is_new = False

        # Insert, Update or Upsert
        if is_new:  # It seems to be an insert
            # Check
            if ptype == 'u':
                raise Exception("Key does not exist")
            # Insert
            if value in self._rev_objs:
                oid = self._rev_objs[value]
            else:
                self._id_counter += 1
                oid = self._id_counter
                self._objs[oid] = (key, value)
                self._rev_objs[value] = oid

            # Insert

            # Add the subkey value to the subkey dictionaries where it is not present
            for d, v in not_present:
                s = set()
                d[v] = s
                s.add(oid)
            # Register the object into subkey-value dictionaries
            for s in present:
                s.add(oid)
        else:
            if ptype == 'i':
                raise Exception("Key '+"+str(key2)+"' already exists")
            # Update
            # Find the ID for the key
            res = self.get(key, just_oid=True)
            if len(res) != 1:
                raise Exception("Only one result expected")
            # Update value (key is the same, ID is the same)
            self._objs[res[0]] = value

    def delete(self, key):
        def delete_single(key):
            if True:
                # Lower case values
                # Keys can be all lower case, because they will be internal Key components, not specified by users
                if case_sensitive:
                    key2 = {k.lower(): v for k, v in key.items()}
                else:
                    key2 = {k.lower(): v if k.startswith("__") else v.lower() for k, v in key.items()}
            else:
                key2 = key

            # Get IDs
            oids = self.get(key, just_oid=True)
            if len(oids) > 0:
                # From key_i: value_i remove IDs (set difference)
                for k, v in key2.items():
                    d = self._keys.get(k, None)
                    if d:
                        s = d.get(v, None)
                        if s:
                            s2 = s.difference(oids)
                            d[v] = s2
                            if not s2:
                                del d[v]  # Remove the value for the key

                # Delete oids
                for oid in oids:
                    del self._objs[oid]

                return len(oids)
            else:
                return 0

        if isinstance(key, list):
            res_ = 0
            for k in key:
                res_ += delete_single(k)
            return res_
        else:
            return delete_single(key)

    def to_pickable(self):
        # Convert to a jsonpickable structure
        return dict(keys=self._keys, objs=self._objs, cont=self._id_counter)

    def from_pickable(self, inp):
        self._keys = inp["keys"]
        self._objs = {int(k): v for k, v in inp["objs"].items()}
        self._rev_objs = {v[1]: k for k, v in self._objs.items()}
        self._id_counter = inp["cont"]

        return self  # Allows the following: prd = PartialRetrievalDictionary().from_pickable(inp)


# #####################################################################################################################
# >>>> EXTERNAL DATASETS <<<<
# #####################################################################################################################


def get_statistical_dataset_structure(source, dset_name, local_datasets=None):
    from nexinfosys.ie_imports.data_sources.ad_hoc_dataset import AdHocDatasets
    # Register AdHocDatasets
    if local_datasets:
        # Register AdHocSource, which needs the current state
        adhoc = AdHocDatasets(local_datasets)
        nexinfosys.data_source_manager.register_datasource_manager(adhoc)

    # Obtain DATASET: Datasource -> Database -> DATASET -> Dimension(s) -> CodeList (no need for "Concept")
    dset = nexinfosys.data_source_manager.get_dataset_structure(source, dset_name, local_datasets)

    # Generate "dims", "attrs" and "meas" from "dset"
    dims = create_dictionary()  # Each dimension has a name, a description and a code list
    attrs = create_dictionary()
    meas = create_dictionary()
    for dim in dset.dimensions:
        if dim.is_measure:
            meas[dim.code] = None
        else:
            # Convert the code list to a dictionary
            if dim.get_hierarchy():
                cl = dim.get_hierarchy().to_dict()
            else:
                cl = None
            dims[dim.code] = SDMXConcept("dimension", dim.code, dim.is_time, "", cl)

    time_dim = False

    lst_dim = []

    for l in dims:
        lst_dim_codes = []
        if dims[l].istime:
            time_dim = True
        else:
            lst_dim.append((dims[l].name, lst_dim_codes))

        if dims[l].code_list:
            for c, description in dims[l].code_list.items():
                lst_dim_codes.append((c, description))

    if time_dim:
        lst_dim.append(("StartPeriod", None))
        lst_dim.append(("EndPeriod", None))

    # Unregister AdHocDatasets
    if local_datasets:
        nexinfosys.data_source_manager.unregister_datasource_manager(adhoc)

    return lst_dim, (dims, attrs, meas)


def check_dataset_exists(dset_name, local_datasets=None):
    from nexinfosys.ie_imports.data_source_manager import DataSourceManager
    if len(dset_name.split(".")) == 2:
        source, d_set = dset_name.split(".")
    else:
        d_set = dset_name
    res = DataSourceManager.obtain_dataset_source(d_set, local_datasets)
    return res is not None


def obtain_dataset_metadata(dset_name, source=None, local_datasets=None):
    from nexinfosys.ie_imports.data_source_manager import DataSourceManager
    d_set = dset_name
    if not source:
        if len(dset_name.split(".")) == 2:
            source, d_set = dset_name.split(".")
        else:
            source = DataSourceManager.obtain_dataset_source(d_set, local_datasets)

    _, metadata = get_statistical_dataset_structure(source, d_set, local_datasets)

    return metadata

# #####################################################################################################################
# >>>> DECORATORS <<<<
# #####################################################################################################################


class Memoize:
    """
    Cache of function calls (non-persistent, non-refreshable)
    """
    def __init__(self, fn):
        self.fn = fn
        self.memo = {}

    def __call__(self, *args):
        if args not in self.memo:
            self.memo[args] = self.fn(*args)
        return self.memo[args]


class Memoize2(object):
    """cache the return value of a method

    This class is meant to be used as a decorator of methods. The return value
    from a given method invocation will be cached on the instance whose method
    was invoked. All arguments passed to a method decorated with memoize must
    be hashable.

    If a memoized method is invoked directly on its class the result will not
    be cached. Instead the method will be invoked like a static method:
    class Obj(object):
        @memoize
        def add_to(self, arg):
            return self + arg
    Obj.add_to(1) # not enough arguments
    Obj.add_to(1, 2) # returns 3, result is not cached
    """

    def __init__(self, func):
        self.func = func

    def __get__(self, obj, objtype=None):
        if obj is None:
            return self.func
        return partial(self, obj)

    def __call__(self, *args, **kw):
        obj = args[0]
        try:
            cache = obj.__cache
        except AttributeError:
            cache = obj.__cache = {}
        key = (self.func, args[1:], frozenset(kw.items()))
        try:
            res = cache[key]
        except KeyError:
            res = cache[key] = self.func(*args, **kw)
        return res


# #####################################################################################################################
# >>>> MAPPING <<<<
# #####################################################################################################################


# Cython version is in module "helper_accel.pyx"
def augment_dataframe_with_mapped_columns(
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
    for c in measure_columns:
        tmp[c] = tmp[c].astype(np.float)

    return tmp


def is_boolean(v):
    return v.lower() in ["true", "false"]


def to_boolean(v):
    return v.lower() == "true"


def is_integer(v):
    try:
        int(v)
        return True
    except ValueError:
        return False


def to_integer(v):
    return int(v)


def is_float(v):
    try:
        float(v)
        return True
    except ValueError:
        return False


def to_float(v):
    return float(v)


def is_datetime(v):
    try:
        from dateutil.parser import parse
        parse(v)
        return True
    except ValueError:
        return False


def to_datetime(v):
    from dateutil.parser import parse
    return parse(v)


def is_url(v):
    """
    From https://stackoverflow.com/a/36283503
    """
    min_attributes = ('scheme', 'netloc')
    qualifying = min_attributes
    token = urllib.parse.urlparse(v)
    return all([getattr(token, qualifying_attr) for qualifying_attr in qualifying])


def to_url(v):
    return urllib.parse.urlparse(v)


def is_uuid(v):
    try:
        UUID(v, version=4)
        return True
    except ValueError:
        return False


def to_uuid(v):
    return UUID(v, version=4)


def is_category(v):
    # TODO Get all hierarchies, get all categories from all hierarchies, find if "v" is one of them
    return False


def to_category(v):
    # TODO
    return None #  Taxon  # Return some Taxon


def is_geo(v):
    # TODO Check if "v" is a GeoJSON, or a reference to a GeoJSON
    return None


def to_geo(v):
    return None


def is_str(v):
    return True


def to_str(v):
    return str(v)

# #####################################################################################################################
# >>>> LOAD DATASET FROM URL INTO PD.DATAFRAME <<<<
# #####################################################################################################################


def wv_create_client_and_resource_name(location, wv_user=None, wv_password=None, wv_host_name=None):
    import webdav.client as wc
    if not wv_host_name:
        wv_host_name = get_global_configuration_variable("FS_SERVER") \
            if get_global_configuration_variable("FS_SERVER") else "nextcloud.data.magic-nexus.eu"

    parts = location.split("/")
    for i, p in enumerate(parts):
        if p == wv_host_name:
            url = "/".join(parts[:i + 1])
            fname = "/" + "/".join(parts[i + 1:])
            break

    options = {
        "webdav_hostname": url,
        "webdav_login": wv_user if wv_user else get_global_configuration_variable("FS_USER"),
        "webdav_password": wv_password if wv_password else get_global_configuration_variable("FS_PASSWORD")
    }
    client = wc.Client(options)
    return client, fname


def wv_check_path(location, wv_user=None, wv_password=None, wv_host_name=None):
    """
    Check if the location exists, and if it is a directory or a file

    :param location:
    :param user:
    :param password:
    :return: 0 -> does not exist; 1 -> directory; 2 -> file
    """
    client, fname = wv_create_client_and_resource_name(location, wv_user, wv_password, wv_host_name)
    if client.check(fname):
        retval = 1 if client.is_dir(fname) else 2
    else:
        retval = 0
    client.free()
    return retval


def wv_create_directory(location, wv_user=None, wv_password=None, wv_host_name=None):
    """
    Create directory (if it exists do nothing)

    :param location:
    :param wv_user:
    :param wv_password:
    :param wv_host_name:
    :return:

    """
    client, fname = wv_create_client_and_resource_name(location, wv_user, wv_password, wv_host_name)
    client.mkdir(fname)
    client.free()


def wv_upload_file(data: io.BytesIO, location: str, wv_user=None, wv_password=None, wv_host_name=None):
    """
    Uploads a bytes array into a location on a WebDAV server
    Directory must exist
    :param data: The data to upload
    :param location:
    :param wv_user:
    :param wv_password:
    :param wv_host_name:
    :return:
    """
    client, fname = wv_create_client_and_resource_name(location, wv_user, wv_password, wv_host_name)
    client.upload_from(data, fname)
    client.free()


def wv_download_file(location, wv_user=None, wv_password=None, wv_host_name=None):
    """
    WebDAV download a file

    :param location:
    :param wv_user:
    :param wv_password:
    :param wv_host_name:
    :return:
    """
    client, fname = wv_create_client_and_resource_name(location, wv_user, wv_password, wv_host_name)
    with tempfile.NamedTemporaryFile(delete=True) as temp:
        client.download_sync(remote_path=fname, local_path=temp.name)
        f = open(temp.name, "rb")
        data = io.BytesIO(f.read())
        f.close()
    client.free()
    return data


def download_with_pycurl(location):
    headers = {}

    def header_function(header_line):
        # HTTP standard specifies that headers are encoded in iso-8859-1.
        # On Python 2, decoding step can be skipped.
        # On Python 3, decoding step is required.
        header_line = header_line.decode('iso-8859-1')

        # Header lines include the first status line (HTTP/1.x ...).
        # We are going to ignore all lines that don't have a colon in them.
        # This will botch headers that are split on multiple lines...
        if ':' not in header_line:
            return

        # Break the header line into header name and value.
        name, value = header_line.split(':', 1)

        # Remove whitespace that may be present.
        # Header lines include the trailing newline, and there may be whitespace
        # around the colon.
        name = name.strip()
        value = value.strip()

        # Header names are case insensitive.
        # Lowercase name here.
        name = name.lower()

        # Now we can actually record the header name and value.
        # Note: this only works when headers are not duplicated, see below.
        headers[name] = value

    data = io.BytesIO()
    c = pycurl.Curl()
    c.setopt(c.URL, location)
    c.setopt(c.FOLLOWLOCATION, True)
    c.setopt(c.HEADERFUNCTION, header_function)
    c.setopt(c.WRITEDATA, data)
    c.setopt(pycurl.SSL_VERIFYPEER, 0)
    c.setopt(pycurl.SSL_VERIFYHOST, 0)
    c.perform()
    status = c.getinfo(c.RESPONSE_CODE)
    c.close()

    return status, headers, data


def download_file(location, wv_user=None, wv_password=None, wv_host_name=None):
    """
    Download a file from the specified URL location.

    It recognizes MAGIC's Nextcloud (WebDAV) instance AND Google Drive URL's
    Of course, it should work with Zenodo.
    Google Drive URL's are assumed to be Spreadsheets (both XLSX and Google Calc are considered)

    :param location:
    :param wv_host_name: WebDav host name part. Example: "nextcloud.data.magic-nexus.eu"
    :return: A BytesIO object with the contents of the file
    """
    pr = urlparse(location)
    if pr.scheme != "":
        # Load from remote site
        fragment = ""
        if "#" in location:
            pos = location.find("#")
            fragment = location[pos + 1:]
            location = location[:pos]
        if not wv_host_name:
            wv_host_name = get_global_configuration_variable("FS_SERVER")\
                if get_global_configuration_variable("FS_SERVER") else "nextcloud.data.magic-nexus.eu"
        if pr.netloc.lower() == wv_host_name:
            data = wv_download_file(location, wv_user, wv_password, wv_host_name)
        elif pr.netloc.lower() == "docs.google.com" or pr.netloc.lower() == "drive.google.com":
            # TODO From mail: Aug 11, 2021, 6:47 AM
            #   with subject: [Action Required] Drive API requires updates to your code before Sep 13, 2021
            #   13 of September
            #      https://developers.google.com/drive/api/v3/reference/files
            #      https://developers.google.com/drive/api/v3/resource-keys
            #

            # Google Drive. CSV and XLSX files supported (if Google Sheets, an Export to XLSX is done)
            # Extract file id from the URL
            import re
            m = re.match(r".*[^-\w]([-\w]{33,})[^-\w]?.*", location)
            file_id = m.groups()[0]
            if "com/file" not in location:
                url = f"https://docs.google.com/spreadsheets/d/{file_id}/export?format=xlsx"  # &id={file_id}"
            else:
                url = f"https://docs.google.com/uc?export=download&id={file_id}"
            # resp = requests.get(url, headers={'Cache-Control': 'no-cache', 'Pragma': 'no-cache'}, allow_redirects=True)  # headers={'Cache-Control': 'no-cache', 'Pragma': 'no-cache'}
            status_code, headers, data = download_with_pycurl(url)
            logging.debug(f'curl -L "{url}" >> out.xlsx')
            if status_code == 200 and "text/html" not in headers["content-type"]:
            # if resp.status_code == 200 and "text/html" not in resp.headers["Content-Type"]:
            #     data = io.BytesIO(resp.content)
                pass
            else:
                # Disabled remote file could not be accessed due to permissions, please check them
                credentials_file = get_global_configuration_variable("GAPI_CREDENTIALS_FILE")
                token_file = get_global_configuration_variable("GAPI_TOKEN_FILE")
                if os.path.exists(credentials_file) and os.path.exists(token_file):
                    try:
                        data = download_xlsx_file_id(credentials_file, token_file, file_id)
                    except:
                        print(f"Google Drive file download failed, please check credentials "
                              f"files: {credentials_file} and {token_file}")
                        data = None
                else:
                    print("Google Drive file download not possible, please check permissions, "
                          "it should be public (can be read only)")
                    data = None

            # with open("/home/rnebot/Downloads/out2.xlsx", "wb") as nf:
            #     nf.write(data.getvalue())
        else:
            data = urllib.request.urlopen(location).read()
            data = io.BytesIO(data)
    else:
        data = urllib.request.urlopen(location).read()
        data = io.BytesIO(data)

    return data


def load_dataset(location: str=None):
    """
    Loads a dataset into a DataFrame
    If the dataset is present, it decompresses it in memory to obtain one of the four datasets per file
    If the dataset is not downloaded, downloads it and decompresses into the corresponding version directory
    :param location: URL of the dataset data
    :return: pd.DataFrame
    """

    if not location:
        df = None
    else:
        data = download_file(location)
        fragment = ""
        if "#" in location:
            pos = location.find("#")
            fragment = location[pos + 1:]
            location = location[:pos-1]
        # Then, try to read it
        t = mimetypes.guess_type(location, strict=True)
        if t[0] == "text/csv" or "com/file" in location:
            data.seek(0)
            df = pd.read_csv(data, comment="#")
        elif t[0] == "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet" or t[0] == "application/vnd.ms-excel":
            if fragment:
                df = pd.read_excel(data, sheet_name=fragment)
            else:
                df = pd.read_excel(data)

    return df


def any_error_issue(issues):
    """
    Just iterate through a list of Issues (of the three types) to check if there is any error

    :param issues:
    :return:
    """
    any_error = False
    for i in issues:
        from nexinfosys.command_generators import Issue, IType
        if isinstance(i, dict):
            if i["type"] == 3:
                any_error = True
        elif isinstance(i, tuple):
            if i[0] == 3:  # Error
                any_error = True
        elif isinstance(i, Issue):
            if i.itype == IType.ERROR:  # Error
                any_error = True
    return any_error


def prepare_dataframe_after_external_read(ds, df, cmd_name):
    from nexinfosys.command_generators import IType, Issue, IssueLocation  # Declared here to avoid circular "import"
    issues = []
    dims = set()  # Set of dimensions, to index Dataframe on them
    cols = []  # Same columns, with exact case (matching Dataset object)
    orig_cols = []  # Original column names appearing in the target Dataset
    for c in df.columns:
        for d in ds.dimensions:
            if strcmp(c, d.code):
                orig_cols.append(c)
                cols.append(d.code)  # Exact case
                if not d.is_measure:
                    dims.add(d.code)
                    num_column = df.dtypes[c] in [np.int64, np.float]
                    # NaN values in dimensions replaced by ""
                    df[c] = df[c].fillna('')
                    # Convert to string if it is numeric
                    if num_column:
                        df[c] = df[c].astype(str)
                break
        else:
            del df[c]
            issue = Issue(itype=IType.WARNING,
                          description=f"Column '{c}' not found in the definition of Dataset '{ds.code}'. Skipping it.",
                          location=IssueLocation(sheet_name=cmd_name,
                                                 row=None, column=None))
            issues.append(issue)
    df = df[orig_cols]  # Remove unused columns
    df.columns = cols  # Rename columns
    df.set_index(list(dims), inplace=True)  # Index by Dimension Concepts
    # Check Index: unique
    tmp = np.where(df.index.duplicated(keep=False))
    if len(tmp[0]) > 0:
        for i in np.nditer(tmp):
            issue = Issue(itype=IType.ERROR,
                          description=f"Duplicated row '{i+2}' in the data of dataset '{ds.code}' with value: {df.iloc[i]}",
                          location=IssueLocation(sheet_name=cmd_name,
                                                 row=i+2, column=None))
            issues.append(issue)
    df.reset_index(inplace=True)  # If not, information stored in the index is lost (maybe after serialization/deserialization)

    return issues


# #####################################################################################################################
# >>>> DATAFRAME <<<<
# #####################################################################################################################

def get_dataframe_copy_with_lowercase_multiindex(dataframe: DataFrame) -> DataFrame:
    """
    Create a copy of an input MultiIndex dataframe where all the index values have been lowercased.
    :param dataframe: a MultiIndex dataframe
    :return: A copy of the input dataframe with lowercased index values.
    """
    # df = dataframe.copy()
    levels = [dataframe.index.get_level_values(n).astype(str).str.lower() for n in range(dataframe.index.nlevels)]
    df = pd.DataFrame(index=pd.MultiIndex.from_arrays(levels))
    # df.index = pd.MultiIndex.from_arrays(levels)
    return df

# #####################################################################################################################
# >>>> OTHER STUFF <<<<
# #####################################################################################################################


def str2bool(v: str):
    return str(v).lower() in ("yes", "true", "t", "1")


def ascii2uuid(s: str) -> str:
    """
    Convert an ASCII string to an UUID hexadecimal string
    :param s: an ASCII string
    :return: an UUID hexadecimal string
    """
    return str(uuid.UUID(bytes=base64.a85decode(s)))


T = TypeVar('T')
S = TypeVar('S')


def ifnull(var: Optional[T], val: Optional[S]) -> Union[T, Optional[S]]:
    """ Returns first value if not None, otherwise returns second value """
    if var is None:
        return val
    return var


def head(in_list: List[T]) -> Optional[T]:
    """
    Returns the head element of the list or None if the list is empty.
    :param in_list: The input list
    :return: The head element of the list or None
    """
    if in_list:
        return in_list[0]
    else:
        return None


def first(iterable: Iterable[T],
          condition: Callable[[T], bool] = lambda x: True,
          default: Optional[T] = None) -> Optional[T]:
    """
    Returns the first item in the `iterable` that satisfies the `condition`.
    If the condition is not given, returns the first item of the iterable.

    Returns the `default` value if no item satisfying the condition is found.

    >>> first( (1,2,3), condition=lambda x: x % 2 == 0)
    2
    >>> first(range(3, 100))
    3
    >>> first( () )
    None
    >>> first( (), default="Some" )
    Some
    """
    return next((x for x in iterable if condition(x)), default)


def translate_case(current_names: List[str], new_names: List[str]) -> List[str]:
    """
    Translate the names in the current_names list according the existing names in the new_names list that
    can have a different case.
    :param current_names: a list of names to translate
    :param new_names: a list of new names to use
    :return: the current_names list where some or all of the names are translated
    """
    new_names_dict = {name.lower(): name for name in new_names}
    translated_names = [new_names_dict.get(name.lower(), name) for name in current_names]
    return translated_names


def values_of_nested_dictionary(d: Dict)-> List:
    for v in d.values():
        if not isinstance(v, Dict):
            yield v
        else:
            yield from values_of_nested_dictionary(v)


def name_and_id_dict(obj: object) -> Optional[Dict]:
    if obj:
        return {"name": obj.name, "id": obj.uuid}
    else:
        return None


def get_value_or_list(current_value, additional_value):
    """
    Add a new value to another existing value/s. If a value doesn't exist it returns the new value otherwise
    returns a list with the existing value/s and the new value.
    :param current_value: the current value
    :param additional_value: the new value
    :return: a single value or a list
    """
    if current_value:
        if isinstance(current_value, list):
            return current_value + [additional_value]
        else:
            return [current_value, additional_value]
    else:
        return additional_value


def class_full_name(c: Type) -> str:
    """ Get the full name of a class """
    module = c.__module__
    if module is None:
        return c.__name__
    else:
        return module + '.' + c.__name__


def object_full_name(o: object) -> str:
    """ Get the full class name of an object """
    return class_full_name(o.__class__)


def split_and_strip(s: str, sep=",") -> List[str]:
    """Split a string representing a comma separated list of strings into a list of strings
    where each element has been stripped. If the string has no elements an empty list is returned."""
    string_list: List[str] = []
    if s is not None and isinstance(s, str):
        string_list = [s.strip() for s in s.split(sep)]

    return string_list


def precedes_in_list(lst: List[T], elem1: Optional[T], elem2: Optional[T]) -> bool:
    """ Check if an element comes before another inside a list """
    return elem1 in lst and (elem2 not in lst or lst.index(elem1) < lst.index(elem2))


def replace_string_from_dictionary(s: str, d: Dict[str, str]) -> str:
    """
    Replace in a string all the words defined as keys in the dictionary with the associated value.
    Note: values of the dictionary are sorted by length before replacement in order to avoid replacing on top of a replaced word.

    :param s: input string to transform with replacements
    :param d: dictionary with old_word -> new_word scheme
    :return: a string with replacements applied
    """
    for k, v in sorted(d.items(), key=lambda item: len(item[1])):
        logging.debug(v)
        if case_sensitive:
            s = re.sub(r"\b" + k + r"\b", v, s)
        else:
            s = re.sub(r"\b" + k + r"\b", v, s, flags=re.IGNORECASE)
    return s

# #####################################################################################################################
# >>>> CUSTOM DATA TYPES <<<<
# #####################################################################################################################

FloatOrStringT = Union[str, float]


class FloatOrString:
    @staticmethod
    def to_float(value: Optional[FloatOrStringT]) -> Optional[FloatOrStringT]:
        if value is None:
            return None
        try:
            return float(value)
        except ValueError:
            return value

    @staticmethod
    def multiply(a: Optional[FloatOrStringT], b: Optional[FloatOrStringT]) -> Optional[FloatOrStringT]:
        value_a = FloatOrString.to_float(a)
        value_b = FloatOrString.to_float(b)

        if value_a is None:
            return value_b

        if value_b is None:
            return value_a

        if isinstance(value_a, float) and isinstance(value_b, float):
            return value_a * value_b
        else:
            return f"({value_a})*({value_b})"

    @staticmethod
    def multiply_with_float(a: FloatOrStringT, b: float) -> FloatOrStringT:
        value_a = FloatOrString.to_float(a)

        if isinstance(value_a, float):
            return value_a * b
        else:
            return f"({value_a})*{b}"


class UnitConversion:
    @staticmethod
    def convert(value: FloatOrStringT, from_unit: str, to_unit: str) -> FloatOrStringT:
        ratio = UnitConversion.ratio(from_unit, to_unit)
        return FloatOrString.multiply_with_float(value, ratio)

    @staticmethod
    def ratio(from_unit: str, to_unit: str) -> float:
        return nexinfosys.ureg(from_unit).to(nexinfosys.ureg(to_unit)).magnitude

    @staticmethod
    def get_scaled_weight(weight: FloatOrStringT,
                          source_from_unit: str, source_to_unit: Optional[str],
                          target_from_unit: Optional[str], target_to_unit: str) -> FloatOrStringT:
        ratio = 1.0
        if source_to_unit:
            ratio *= UnitConversion.ratio(source_from_unit, source_to_unit)

        if target_from_unit:
            ratio *= UnitConversion.ratio(target_from_unit, target_to_unit)

        return FloatOrString.multiply_with_float(weight, ratio)


class ArithmeticOperator(Enum):
    ADD = ("+", add)
    MUL = ("*", mul)
    SUB = ("-", sub)
    DIV = ("/", truediv)
    ABS = ("abs", abs)

    def __init__(self, symbol: str, operation: Callable):
        self.symbol = symbol
        self.operation = operation


class ArithmeticOperand:
    def __init__(self, name: str, value: float):
        self.name = name
        self.value = value

    def __str__(self):
        return self.name


class ArithmeticExpression:
    def __init__(self, operator: ArithmeticOperator, operands: List[Union[ArithmeticOperand, 'ArithmeticExpression']], value: float):
        self.operator = operator
        self.operands = operands
        self.value = value

    def __repr__(self):
        return self.__str__()

    def __str__(self):
        if len(self.operands) == 1:
            return f"{self.operator.symbol}({self.operands[0]})"
        else:
            return self.operator.symbol.join([f"({o})" for o in self.operands])


def brackets(exp: str) -> str:
    """ Surround a string expression with brackets """
    return "(" + exp + ")"


class FloatExp(SupportsFloat):
    """ Wrapper of the Float data type which includes a name and a string expression that will grow
        when operating with other objects """

    ValueWeightPair = Tuple[Optional['FloatExp'], Optional['FloatExp']]

    def __init__(self, val: Union[float, int], name: Optional[str] = None,
                 exp: Union[str, ArithmeticExpression] = None):
        assert (isinstance(val, float) or isinstance(val, int))
        assert (name is None or isinstance(name, str))
        assert (exp is None or isinstance(exp, str) or isinstance(exp, ArithmeticExpression))

        self.val = float(val)

        if exp is None:
            self.exp = ArithmeticOperand(str(self.val), self.val)
        elif isinstance(exp, str):
            self.exp = ArithmeticOperand(exp, self.val)
        else:
            self.exp = exp

        self.name = str(self.exp) if name is None else name

    def __float__(self) -> float:
        """ Needed for abc SupportsFloat used in match.isclose() """
        return self.val

    def __round__(self, n=None):
        exp = f"round({self.name}{'' if n is None else ',' + str(n)})"
        return FloatExp(float(round(self.val, n)), exp, self.exp)

    def __abs__(self):
        return self._operate_unary(ArithmeticOperator.ABS)

    @staticmethod
    def get_float(f: Union[float, 'FloatExp']) -> float:
        return f.val if isinstance(f, FloatExp) else f

    @staticmethod
    def compute_weighted_addition(addends: List[ValueWeightPair]) -> Optional['FloatExp']:
        filtered_addends = [(v, w) for v, w in addends if v]

        if len(filtered_addends) == 1:
            value, weight = filtered_addends[0]
            if weight and weight != 1.0:
                return value * weight
            else:
                return FloatExp(value.val, value.name, value.name)
        elif len(filtered_addends) > 1:
            values = [value * weight
                      if weight and weight != 1.0
                      else FloatExp(value.val, value.name, value.name)
                      for value, weight in filtered_addends]
            value = reduce(add, [value.val for value in values])
            exp = ArithmeticExpression(ArithmeticOperator.ADD, [v.exp for v in values], value)
            return FloatExp(value, str(exp), exp)
        else:
            return None

    def _operate_unary(self, operator: ArithmeticOperator) -> 'FloatExp':
        value = operator.operation(self.val)
        new_exp = ArithmeticExpression(operator, [ArithmeticOperand(self.name, self.val)], value)
        return FloatExp(value, str(new_exp), new_exp)

    def _operate_binary(self, operator: ArithmeticOperator, other: 'FloatExp') -> 'FloatExp':
        value = operator.operation(self.val, other.val)
        new_exp = ArithmeticExpression(operator, [ArithmeticOperand(self.name, self.val),
                                                  ArithmeticOperand(other.name, other.val)], value)
        return FloatExp(value, str(new_exp), new_exp)

    def __add__(self, other: 'FloatExp') -> 'FloatExp':
        return self._operate_binary(ArithmeticOperator.ADD, other)

    def __sub__(self, other: 'FloatExp') -> 'FloatExp':
        return self._operate_binary(ArithmeticOperator.SUB, other)

    def __mul__(self, other: 'FloatExp') -> 'FloatExp':
        return self._operate_binary(ArithmeticOperator.MUL, other)

    def __truediv__(self, other: 'FloatExp') -> 'FloatExp':
        return self._operate_binary(ArithmeticOperator.DIV, other)

    def __eq__(self, other: Union[float, 'FloatExp']) -> bool:
        return self.val == FloatExp.get_float(other)

    def __gt__(self, other: Union[float, 'FloatExp']) -> bool:
        return self.val > FloatExp.get_float(other)

    def __str__(self) -> str:
        return f'Value = {self.val}, Name = {self.name}, Expression = "{self.exp}"'

    def __repr__(self):
        return str(self)


def get_interfaces_and_weights_from_expression(exp: Union[ArithmeticOperand, ArithmeticExpression]) -> List[Tuple[str, float]]:
    def is_interface(s: str) -> bool:
        return s.find(":") > 0

    def find_interface(exp: Union[ArithmeticOperand, ArithmeticExpression]) -> Optional[Tuple[str, float]]:
        if isinstance(exp, ArithmeticOperand):
            if is_interface(exp.name):
                return exp.name, 1.0
            else:
                logging.debug(f"Operand not interface: {exp.name}")
        elif exp.operator == ArithmeticOperator.MUL:
            interfaces_list: List[Tuple[str, float]] = []
            weight: float = 1.0
            for o in exp.operands:
                i = find_interface(o)
                if i:
                    interfaces_list.append(i)
                else:
                    weight *= o.value

            if len(interfaces_list) == 1:
                return interfaces_list[0][0], interfaces_list[0][1] * weight
            elif len(interfaces_list) > 1:
                logging.debug(f"Error multiple interfaces: {interfaces_list}")
        else:
            logging.debug(f"Error operator not allowed: {exp.operator}")

        return None

    interfaces: List[Tuple[str, float]] = []
    if isinstance(exp, ArithmeticExpression) and exp.operator == ArithmeticOperator.ADD:
        for operand in exp.operands:
            i = find_interface(operand)
            if i:
                interfaces.append(i)
    else:
        i = find_interface(exp)
        if i:
            interfaces.append(i)

    return interfaces


def add_label_columns_to_dataframe(ds_name, df, prd):
    """
    Add columns containing labels describing codes in the input Dataframe
    The labels must be in the CodeHierarchies or CodeLists

    :param ds_name: Dataset name
    :param df: pd.Dataframe to enhance
    :param prd: PartialRetrievalDictionary
    :return: Enhanced pd.Dataframe
    """
    from nexinfosys.models.musiasem_concepts import Hierarchy
    # Merge with Taxonomy LABELS, IF available
    for col in df.columns:
        hs = prd.get(Hierarchy.partial_key(ds_name + "_" + col))
        hs2 = prd.get(Hierarchy.partial_key(col))
        if len(hs) == 1 or len(hs2) == 1:
            if len(hs) == 1:
                h = hs[0]
            else:
                h = hs2[0]
            nodes = h.get_all_nodes()
            tmp = []
            for nn in nodes:
                t = nodes[nn]
                tmp.append([t[0].lower(), t[1]])  # CSens
            if not nexinfosys.case_sensitive and df[col].dtype == 'O':
                df[col + "_l"] = df[col].str.lower()
                col = col + "_l"

                # Dataframe of codes and descriptions
                df_dst = pd.DataFrame(tmp, columns=['sou_rce', col + "_desc"])
                df = pd.merge(df, df_dst, how='left', left_on=col, right_on='sou_rce')
                del df['sou_rce']
                if not nexinfosys.case_sensitive:
                    del df[col]

    return df


def change_tuple_value(t: Tuple, index: int, value: Any) -> Tuple:
    lst = list(t)
    lst[index] = value
    return tuple(lst)


# if __name__ == '__main__':
#     import random
#     import string
#     from timeit import default_timer as timer
#
#     class Dummy:
#         def __init__(self, a):
#             self._a = a
#
#     def rndstr(n):
#         return random.choices(string.ascii_uppercase + string.digits, k=n)
#
#     prd = PartialRetrievalDictionary2()
#     ktypes = [("a", "b", "c"), ("a", "b"), ("a", "d"), ("a", "f", "g")]
#     # Generate a set of keys and empty objects
#     vals = []
#     print("Generating sample")
#     for i in range(30000):
#         # Choose random key
#         ktype = ktypes[random.randrange(len(ktypes))]
#         # Generate the element
#         vals.append(({k: ''.join(rndstr(6)) for k in ktype}, Dummy(rndstr(12))))
#
#     print("Insertion started")
#     df = pd.DataFrame()
#     start = timer()
#     # Insert each element
#     for v in vals:
#         prd.put(v[0], v[1])
#     stop = timer()
#     print(stop-start)
#
#     print("Reading started")
#
#     # Select all elements
#     start = timer()
#     # Insert each element
#     for v in vals:
#         r = prd.get(v[0], False)
#         if len(r) == 0:
#             raise Exception("Unexpected!")
#     stop = timer()
#     print(stop-start)
#
#     print("Deleting started")
#
#     # Select all elements
#     start = timer()
#     # Insert each element
#     for v in vals:
#         r = prd.delete(v[0])
#         if r == 0:
#             raise Exception("Unexpected!")
#     stop = timer()
#     print(stop-start)
#
#     print("Finished!!")


if __name__ == '__main__':
    # f = open("/home/rnebot/GoogleDrive/AA_MAGIC/FAOSTAT_analysis.xlsx", "rb")
    # data = io.BytesIO(f.read())
    # f.close()
    # wv_upload_file(data, "https://nextcloud.data.magic-nexus.eu/remote.php/webdav/NIS_beta/CS_format_examples/FAOSTAT_analysis.xlsx", "NIS_agent", "NIS_agent@1", "nextcloud.data.magic-nexus.eu")
    ret = download_file("https://sandbox.zenodo.org/record/536704/files/url_demo_2.zip?download=1#msm/geolayer.xlsx")
    # ret = download_file("https://nextcloud.data.magic-nexus.eu/remote.php/webdav/NIS_beta/CS_format_examples/08_caso_energia_eu_new_commands.xlsx", "NIS_agent", "NIS_agent@1")
    print(f"Longit: {len(ret.getvalue())}")
