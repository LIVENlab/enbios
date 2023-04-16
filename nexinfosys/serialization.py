import ast
import logging

from sqlalchemy.orm import class_mapper
import pandas as pd
import numpy as np
import json
import sys
import blosc

# Some ideas from function "model_to_dict" (Google it, StackOverflow Q&A)
from nexinfosys.common.helper import PartialRetrievalDictionary, create_dictionary
from nexinfosys.models import MODEL_VERSION
from nexinfosys.models.musiasem_methodology_support import serialize_from_object, deserialize_to_object
from nexinfosys.model_services import State, get_case_study_registry_objects


def serialize(o_list):
    """
    Receives a list of SQLAlchemy objects to serialize
    The objects can be related between them by OneToMany and ManyToOne relations
    Returns a list of dictionaries with their properties and two special
    properties, "_nis_class_name" and "_nis_object_id" allowing the reconstruction

    Raise exception if some of the objects refers to an object OUT of the graph

    :param o_list:
    :return:
    """

    def fullname(o):
        return o.__module__ + "." + o.__class__.__name__

    # Dictionary to map obj to ID
    d_ref = {o: i for i, o in enumerate(o_list)}

    # Expand the list to referred objects
    cont = len(o_list)
    proc_lst = o_list
    while True:
        o_list2 = []
        for i, o in enumerate(proc_lst):
            # Relationships
            relationships = [(name, rel) for name, rel in class_mapper(o.__class__).relationships.items()]
            for name, relation in relationships:
                if str(relation.direction) != "symbol('ONETOMANY')":
                    ref_obj = o.__dict__.get(name)
                    if ref_obj:
                        if ref_obj not in d_ref:
                            d_ref[ref_obj] = cont
                            o_list2.append(ref_obj)
                            cont += 1

        o_list.extend(o_list2)
        proc_lst = o_list2
        if len(o_list2) == 0:
            break

    # Do the transformation to list of dictionaries
    d_list = []
    for i, o in enumerate(o_list):
        d = {c.key: getattr(o, c.key) for c in o.__table__.columns}
        d["_nis_class_name"] = fullname(o)
        d["_nis_object_id"] = i
        # Relationships
        relationships = [(name, rel) for name, rel in class_mapper(o.__class__).relationships.items()]
        for name, relation in relationships:
            if str(relation.direction) != "symbol('ONETOMANY')":
                ref_obj = o.__dict__.get(name)
                if ref_obj:
                    if ref_obj in d_ref:
                        d[name] = d_ref[ref_obj]
                else:
                    d[name] = -1  # None

        d_list.append(d)

    return d_list


def deserialize(d_list):
    """
    Receives a list of dictionaries representing SQLAlchemy object previously serialized

    :param d_list:
    :return:
    """
    def instantiate(full_class_name: str, c_dict: dict):
        import importlib
        if full_class_name in c_dict:
            class_ = c_dict[full_class_name]
        else:
            module_name, class_name = full_class_name.rsplit(".", 1)
            class_ = getattr(importlib.import_module(module_name), class_name)
            c_dict[full_class_name] = class_

        return class_()

    o_list = []
    c_list = {}  # Dictionary of classes (full_class_name, class)
    # Construct instances
    for d in d_list:
        o_list.append(instantiate(d["_nis_class_name"], c_list))
    # Now populate them
    ids = []
    for i, d in enumerate(d_list):
        o = o_list[i]
        o.__dict__.update({c.key: d[c.key] for c in o.__table__.columns})
        # Relationships
        relationships = [(name, rel) for name, rel in class_mapper(o.__class__).relationships.items()]
        for name, relation in relationships:
            if str(relation.direction) != "symbol('ONETOMANY')":
                ref_idx = d[name]
                if ref_idx < 0:
                    setattr(o, name, None)
                else:
                    setattr(o, name, o_list[ref_idx])
                    for t in relation.local_remote_pairs:
                        o_id_name = t[0].name  # Or t[0].key
                        r_id_name = t[1].name  # Or t[1].key
                        ids.append((o, o_id_name, o_list[ref_idx], r_id_name))

    for t in ids:
        k = getattr(t[2], t[3])
        if k:
            setattr(t[0], t[1], k)

    return o_list


def serialize_state(state: State):
    """
    Serialization prepared for a given organization of the state

    :return:
    """

    def serialize_dataframe(df):
        return df.to_json(orient="split", index=False), \
               json.dumps({i[0]: str(i[1]) for i in df.dtypes.to_dict().items()})
        # list(df.index.names), df.to_dict()

    logging.debug("  serialize_state IN")

    import copy
    # "_datasets"
    ns_ds = {}
    # Save and nullify "_datasets" before deep copy
    for ns in state.list_namespaces():
        _, _, _, datasets, _ = get_case_study_registry_objects(state, ns)
        ns_ds[ns] = datasets
        state.set("_datasets", create_dictionary(), ns)  # Nullify datasets

    logging.debug("  serialize_state IN 2")  # DELETEME

    # !!! WARNING: It destroys "state", so a DEEP COPY is performed !!!
    tmp = sys.getrecursionlimit()
    sys.setrecursionlimit(10000)
    state2 = copy.deepcopy(state)
    sys.setrecursionlimit(tmp)

    logging.debug("  serialize_state IN 3")  # DELETEME

    # Restore "_datasets"
    for ns in state.list_namespaces():
        state.set("_datasets", ns_ds[ns], ns)  # Nullify datasets

    # Iterate all namespaces
    for ns in state2.list_namespaces():
        glb_idx, p_sets, hh, _, mappings = get_case_study_registry_objects(state2, ns)
        if glb_idx:
            tmp = glb_idx.to_pickable()
            state2.set("_glb_idx", tmp, ns)
        _, _, _, datasets, _ = get_case_study_registry_objects(state, ns)
        datasets2 = create_dictionary()
        logging.debug("  serialize_state IN 4")  # DELETEME
        # TODO Serialize other DataFrames.
        # Process Datasets
        for ds_name in datasets:
            ds = datasets[ds_name]
            if isinstance(ds.data, pd.DataFrame):
                tmp = serialize_dataframe(ds.data)
            else:
                tmp = None
                # ds.data = None
            logging.debug(f"  serialize_state IN ds {ds_name}")  # DELETEME

            # DB serialize the datasets
            lst2 = serialize(ds.get_objects_list())
            lst2.append(tmp)  # Append the serialized DataFrame
            datasets2[ds_name] = lst2

        logging.debug("  serialize_state IN 5")  # DELETEME

        state2.set("_datasets", datasets2, ns)
    logging.debug("  serialize_state serialize preprocessed state ")
    tmp = serialize_from_object(state2)  # <<<<<<<< SLOWEST !!!! (when debugging)
    logging.debug("  serialize_state length: "+str(len(tmp))+" OUT")
    tmp = blosc.compress(bytearray(tmp, "utf-8"), cname="zlib", typesize=8)
    logging.debug("  serialize_state compressed length: "+str(len(tmp))+" OUT")

    return tmp


def deserialize_state(st: str, state_version: int = MODEL_VERSION):
    """
    Deserializes an object previously serialized using "serialize_state"

    It can receive also a "State" modified for the serialization to restore it

    :param state_version: version number of the internal models
    :param st:
    :return:
    """
    def deserialize_dataframe(t):
        if t:
            dtypes = {i[0]: np.dtype(i[1]) for i in json.loads(t[1]).items()}
            df = pd.read_json(t[0], orient="split", dtype=dtypes)  # pd.DataFrame(t[1])
            # df.index.names = t[0]
            return df
        else:
            return pd.DataFrame()  # Return empty pd.Dataframe

    logging.debug("  deserialize_state")
    if isinstance(st, bytes):
        st = blosc.decompress(st).decode("utf-8")
    if isinstance(st, str):
        # TODO: use state_version to convert a previous version to the latest one
        #  This means transforming the old json to the latest json
        if state_version == MODEL_VERSION:
            state = deserialize_to_object(st)
        else:
            raise Exception(f"The model version {state_version} is not supported. Current version is {MODEL_VERSION}.")
    else:
        raise Exception(f"Serialized state must be a string: currently is of type {type(st)}")

    # Iterate all namespaces
    for ns in state.list_namespaces():
        lcia_methods = state.get("_lcia_methods", ns)
        if lcia_methods:
            _ = {ast.literal_eval(k): v for k,v in lcia_methods.items()}
            state.set("_lcia_methods", _, ns)
        glb_idx = state.get("_glb_idx", ns)
        if isinstance(glb_idx, dict):
            glb_idx = PartialRetrievalDictionary().from_pickable(glb_idx)
            state.set("_glb_idx", glb_idx)
        glb_idx, p_sets, hh, datasets, mappings = get_case_study_registry_objects(state, ns)
        if isinstance(glb_idx, dict):
            logging.debug("glb_idx is DICT, after deserialization!!!")
        # TODO Deserialize DataFrames
        # In datasets
        for ds_name in datasets:
            lst = datasets[ds_name]
            ds = deserialize(lst[:-1])[0]
            ds.data = deserialize_dataframe(lst[-1])
            datasets[ds_name] = ds

    return state
