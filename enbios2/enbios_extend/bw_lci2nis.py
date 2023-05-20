import re
from dataclasses import dataclass
from typing import Optional

import bw2data as bd
import bw2io as bi
import pandas as pd
import pint
from bw2data.backends import Activity
from bw2io import SingleOutputEcospold2Importer

from enbios2.const import BASE_DATA_PATH

output_path = BASE_DATA_PATH / "/enbios/bw2nis"

ureg = pint.UnitRegistry()


def long_to_short_unit(long_unit):
    try:
        unit = ureg[long_unit]
        short_unit = '{:~}'.format(unit).split()[1:]
    except (KeyError, ValueError):
        short_unit = long_unit  # If the long_unit is not recognized, return it as-is.
    return short_unit


def export_solved_inventory(activity: Activity, method: tuple[str, ...],
                            out_path: Optional[str] = None) -> pd.DataFrame:
    """
    All credits to Ben Portner.
    :param activity:
    :param method:
    :param out_path:
    :return:
    """
    # print("calculating lci")
    lca = activity.lca(method, 1)
    lca.lci()
    array = lca.inventory.sum(axis=1)
    lca.load_lci_data()
    if hasattr(lca, 'dicts'):
        mapping = lca.dicts.biosphere
    else:
        mapping = lca.biosphere_mm
    data = []
    for key, row in mapping.items():
        amount = array[row, 0]
        data.append((bd.get_activity(key), row, amount))
    data.sort(key=lambda x: abs(x[2]))
    df = pd.DataFrame([{
        'row_index': row,
        'amount': amount,
        'name': flow.get('name'),
        'unit': flow.get('unit'),
        'categories': str(flow.get('categories'))
    } for flow, row, amount in data])
    if out_path:
        df.to_excel(out_path)
    return df


def export_solved_inventory(activity: Activity, method: tuple[str, ...],
                            out_path: Optional[str] = None) -> pd.DataFrame:
    """
    All credits to Ben Portner.
    :param activity:
    :param method:
    :param out_path:
    :return:
    """
    # print("calculating lci")
    lca = activity.lca(method, 1)
    lca.lci()
    array = lca.inventory.sum(axis=1)
    if hasattr(lca, 'dicts'):
        mapping = lca.dicts.biosphere
    else:
        mapping = lca.biosphere_dict
    data = []
    for key, row in mapping.items():
        amount = array[row, 0]
        data.append((bd.get_activity(key), row, amount))
    data.sort(key=lambda x: abs(x[2]))
    df = pd.DataFrame([{
        'row_index': row,
        'amount': amount,
        'name': flow.get('name'),
        'unit': flow.get('unit'),
        'categories': str(flow.get('categories'))
    } for flow, row, amount in data])
    if out_path:
        df.to_excel(out_path)
    return df


def interface_type_template():
    return {
        "InterfaceTypeHierarchy": "LCI",
        "InterfaceType": None,
        "Sphere": None,
        "RoegenType": "Flow",
        "ParentInterfaceType": "",
        "Formula": "",
        "Description": "",
        "Unit": None,
        "OppositeSubsystemType": None,
        "Attributes": "",
        "@EcoinventName": None
    }


def base_processors_template():
    return {
        "ProcessorGroup": "NewStructurals",
        "Processor": None,
        "ParentProcessor": "",
        "SubsystemType": "",
        "System": "",
        "FunctionalOrStructural": "Structural",
        "Accounted": "No",
        "Stock": "",
        "Description": "",
        "GeolocationRef": "",
        "GeolocationCode": "",
        "GeolocationLatLong": "",
        "Attributes": "",
        "@EcoinventName": None,
        "@EcoinventFilename": None,
        "@EcoinventCarrierName": None,
        "@region": ""
    }


def interfaces_template():
    return {
        "Processor": None,
        "InterfaceType": None,
        "Interface": None,
        "Sphere": "",
        "RoegenType": "",
        "Orientation": None,
        "OppositeSubsystemType": "",
        "GeolocationRef": "",
        "GeolocationCode": "",
        "I@compartment": None,
        "I@subcompartment": None,
        "Value": None,
        "Unit": "",
        "RelativeTo": None,
        "Uncertainty": "",
        "Assessment": "",
        "PedigreeMatrix": "",
        "Pedigree": "",
        "Time": "Year",
        "Source": None,
        "NumberAttributes": "",
        "Comments": "",
    }


def get_nis_name(original_name):
    """
    Convert the original_name to a name valid for NIS
    from nexinfosys

    :param original_name:
    :return:
    """
    if not isinstance(original_name, str):
        original_name = str(original_name)
    if original_name.strip() == "":
        return ""
    else:
        prefix = original_name[0] if original_name[0].isalpha() else "_"
        remainder = original_name[1:] if original_name[0].isalpha() else original_name

        return prefix + re.sub("[^0-9a-zA-Z_]", "_", remainder)


@dataclass
class NisSheetDfs:
    interface_types_df: pd.DataFrame
    bare_processors_df: pd.DataFrame
    interfaces_df: pd.DataFrame


def read_exising_nis_file(file_path) -> NisSheetDfs:
    """
    read sheets and convert to dataframes
    :param file_path:
    :return:
    """
    with pd.ExcelFile(file_path) as xls:
        interface_types_df = pd.read_excel(xls, "InterfaceTypes", header=0)
        bare_processors_df = pd.read_excel(xls, "BareProcessors", header=0)
        interfaces_df = pd.read_excel(xls, "Interfaces", header=0)
    return NisSheetDfs(interface_types_df, bare_processors_df, interfaces_df)


def insert_activity_processor(activity: Activity, nis_dataframes: NisSheetDfs, lci_result: pd.DataFrame):
    # insert new interface_types
    unique_interface_types = set(nis_dataframes.interface_types_df["@EcoinventName"].unique())
    lci_interface_types = set(lci_result["name"].unique())
    # difference between the two sets
    missing_interface_types = lci_interface_types - unique_interface_types
    # get the rows, where the interface type is missing
    rows_to_add = lci_result[lci_result["name"].isin(missing_interface_types)]

    interface_type_new_rows = []
    for new_type in rows_to_add.iterrows():
        print(new_type)
        row = new_type[1]
        if row["amount"] == 0.0:
            continue
        new_row = {**interface_type_template(), **{
            "InterfaceType": get_nis_name(row["name"]),
            "Sphere": "Biosphere",
            "Unit": row["unit"],
            # opposite = "Environment" if props["sphere"].lower() == "biosphere" else ""
            "OppositeSubsystemType": "Environment",
            "@EcoinventName": row["name"]
        }}
        interface_type_new_rows.append(new_row)

    unique_bareprocessors = set(nis_dataframes.bare_processors_df["Processor"].unique())
    # # difference between the two sets
    # missing_interface_types = lci_interface_types - unique_interface_types
    # # get the rows, where the interface type is missing
    # rows_to_add = lci_result[lci_result["name"].isin(missing_interface_types)]

    bareprocessor_new_rows = []
    if activity["name"] not in unique_bareprocessors:
        new_row = {**base_processors_template(), **{
            "Processor": get_nis_name(activity["name"]),
            "@EcoinventName": activity["name"],
            "@EcoinventFilename": "no-file",
            "@EcoinventCarrierName": "???"
        }}
        bareprocessor_new_rows.append(new_row)

    new_interface_rows = []
    print(lci_result)

    # and add the first row for it
    main_name = get_nis_name("_".join(activity._data["synonyms"]))
    main_row = {
        "Orientation": "Output",
    }
    for row in lci_result.iterrows():
        print(row)
        data = row[1]
        new_interface_rows.append({**interfaces_template(),
                                   "Processor": get_nis_name(activity["name"]),
                                   "InterfaceType": data["name"],
                                   "Interface": None,
                                   "Orientation": "Input",
                                   "I@compartment": None,
                                   "I@subcompartment": None,
                                   "Value": data["amount"],
                                   "Unit": None,
                                   "RelativeTo": main_name,  #
                                   "Source": "BW",
                                   })

    # interface_type_new_rows
    # bareprocessor_new_rows


if __name__ == "__main__":
    projects = bd.projects

    bd.projects.set_current("ecoi_dbs")

    generate = False
    if generate:
        bi.bw2setup()
        if "ei39" not in bd.databases:
            im = SingleOutputEcospold2Importer(
                (BASE_DATA_PATH / f"ecoinvent/ecoinvent 3.9_cutoff_ecoSpold02/datasets").as_posix(),
                "ei39")
        im.apply_strategies()
        if im.statistics()[2] == 0:
            im.write_database()
        else:
            print("unlinked exchanges")
    #
    # activities = bd.Database("cutoff391",).search("heat and power co-generation, oil", filter={"location": "PT"})
    # activity = activities[0]
    # print(activity._document.code)
    activity = bd.Database("cutoff391", ).get("a8fe0b37705fe611fac8004ca6cb1afd")
    #
    #
    nis_file = BASE_DATA_PATH / "enbios/_1_/output/output.xlsx"
    #
    dataframes = read_exising_nis_file(nis_file)
    #
    lci_result = export_solved_inventory(activity,
                                         ('CML v4.8 2016', 'acidification',
                                          'acidification (incl. fate, average Europe total, A&B)'),
                                         "test.xlsx")
    insert_activity_processor(activity, dataframes, lci_result)
