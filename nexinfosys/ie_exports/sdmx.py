"""
  Given one of the output datasets in State, convert it to SDMX format
  * Metadata
  * Data

  * pandaSDMX is only for reading. It has not been updated since two years
  * Eurostat has an IT tools page: https://ec.europa.eu/eurostat/web/sdmx-infospace/sdmx-it-tools
    * DSW (Data Structure Wizard). To create and maintain DSDs
    * SDMX Converter. Provides a web service to convert data to SDMX-ML. Input DSD is needed.
  * SdmxSource (http://www.sdmxsource.org/) is reference implementation now. Also two years without updates (last 9th June 2016)

DSPL (DataSet Publishing Language) from Google provides a Python package. "SDMX Converter" is able to use this.
"""

from typing import List, Dict

from nexinfosys.models.musiasem_concepts_helper import convert_code_list_to_hierarchy
from nexinfosys.models.statistical_datasets import CodeList, Dataset

def convert_code_list_to_hierarchy(cl: CodeList) -> List[Dict]:
    """
    It receives a CodeList and elaborates an equivalent Hierarchy, returning it

    :param cl: The input CodeList
    :return: The equivalent Hierarchy
    """
    codes = []
    # h = Hierarchy(name=cl.code)
    # h._description = cl.description

    # CodeList is organized in levels. Create all Levels and all Nodes (do not interlink Nodes)
    for cll in cl.levels:
        for ct in cll.codes:
            codes.append(dict(code=ct.code, label=ct.description, description=ct.description, level=cll.code, parent_code=None))

    return codes


def get_dataset_metadata(ds_name: str, ds: Dataset):
    """
    From a list of dataset names, which should exist in-memory (State)
    obtain a list of their metadata.
    Metadata is a dictionary of two elements: "conceptscheme" and "codelist"
     - "conceptscheme" is a list of the concepts. Each concept has a name, a description, a concept type (Dimension, Measure or Attribute), a data type, a domain and dictionary of attributes
     - "codelist" is a list of all codes used in concepts of the dataset. Each is a dictionary of the elements:
        concept, code, level, parentcode, label
    :param state:
    :param datasets:
    :return:
    """
    codelists = []
    concepts = []
    for concept in ds.dimensions:
        if not concept.attributes:
            attributes = {}
        else:
            attributes = concept.attributes
        concepts.append(dict(name=concept.code,
                             description=concept.description,
                             concept_type="dimension" if not concept.is_measure else "attribute" if attributes.get("_attribute", False) else "measure",
                             data_type=attributes.get("_datatype"),
                             domain="",
                             attributes=attributes)
                        )
        if concept.code_list:
            cl = convert_code_list_to_hierarchy(concept.code_list)
            for c in cl:
                c["concept"] = concept.code
            codelists.extend(cl)

    return dict(dataset=ds_name, conceptscheme=concepts, codelists=codelists)
