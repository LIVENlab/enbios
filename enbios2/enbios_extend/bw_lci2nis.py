from dataclasses import dataclass
from pathlib import Path

import openpyxl
import pandas as pd
from bw2data.backends import Activity
import bw2data as bd
import bw2io as bi
from bw2io import SingleOutputEcospold2Importer

base_data_path = Path("/mnt/SSD/projects/LIVENLab/enbios2/data")
output_path = base_data_path / "/enbios/bw2nis"


def export_solved_inventory(activity: Activity, method: tuple[str, ...], out_path: str) -> pd.DataFrame:
    """
    All credits to Ben Portner.
    :param activity:
    :param method:
    :param out_path:
    :return:
    """
    lca = activity.lca(method, 1)
    print("calculating lci")
    lca.lci()
    cutoff = None
    array = lca.inventory.sum(axis=1)
    if cutoff is not None and not (0 < cutoff < 1):
        print(f"Ignoring invalid cutoff value {cutoff}")
        cutoff = None

    total = array.sum()
    include = lambda x: abs(x / total) >= cutoff if cutoff is not None else True
    if hasattr(lca, 'dicts'):
        mapping = lca.dicts.biosphere
    else:
        mapping = lca.biosphere_dict
    data = []
    for key, row in mapping.items():
        amount = array[row, 0]
        if include(amount):
            data.append((bd.get_activity(key), row, amount))
    data.sort(key=lambda x: abs(x[2]))
    df = pd.DataFrame([{
        'row_index': row,
        'amount': amount,
        'name': flow.get('name'),
        'unit': flow.get('unit'),
        'categories': str(flow.get('categories'))
    } for flow, row, amount in data
    ])
    df.to_excel(out_path)
    return df


def create_nis_rows(data):
    interface_types = {
        "InterfaceTypeHierarchy": "LCI",
        "InterfaceType": None,
        "Sphere": None,
        "RoegenType": "Flow",
        "ParentInterfaceType": None,
        "Formula": "",
        "Description": "",
        "Unit": None,
        "OppositeSubsystemType": None,
        "Attributes": "",
        "@EcoinventName": None
    }

    base_processors = {
        "ProcessorGroup": None,
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

    interfaces = {
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
        "Unit": None,
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


@dataclass
class NisSheetDfs:
    interface_types_df: pd.DataFrame
    bare_processors_df: pd.DataFrame
    interfaces_df: pd.DataFrame


def read_exising_nis_file(file_path) -> NisSheetDfs:
    # read Excel sheet
    workbook = openpyxl.load_workbook(file_path)
    return NisSheetDfs(pd.DataFrame(workbook["InterfaceTypes"].values),
                       pd.DataFrame(workbook["BareProcessors"].values),
                       pd.DataFrame(workbook["Interfaces"].values))


def insert_activity_processor(activity: Activity, nis_dataframes: NisSheetDfs, lci_result: pd.DataFrame):
    # insert new interface_types
    nis_dataframes.interface_types_df
    pass


if __name__ == "__main__":
    projects = bd.projects

    if False:

        bd.projects.set_current("uab_bw_ei39")
        print(bd.databases)
        bi.bw2setup()

        eis = [
            {
                "folder": "ecoinvent 3.9.1_cutoff_ecoSpold02",
                "db_name": "ei391"
            },
            {
                "folder": "ecoinvent 3.9_cutoff_ecoSpold02",
                "db_name": "ei39"
            }
        ]
        for ei in eis:
            if ei["db_name"] not in bd.databases:
                print(f"importing {ei['db_name']}")
                im = SingleOutputEcospold2Importer(
                    (base_data_path / f"ecoinvent/{ei['folder']}/datasets").as_posix(),
                    ei["db_name"])
                im.apply_strategies()
                if im.statistics()[2] == 0:
                    im.write_database()
                else:
                    print("unlinked exchanges")

    activity = bd.Database("ei391").random()

    nis_file = "/mnt/SSD/projects/LIVENLab/enbios2/data/enbios/_1_/output/output.xlsx"

    dataframes = read_exising_nis_file(nis_file)

    lci_result = export_solved_inventory(activity,
                                         ('CML v4.8 2016', 'acidification',
                                          'acidification (incl. fate, average Europe total, A&B)'),
                                         "test.xlsx")

    insert_activity_processor(activity, dataframes, lci_result)
