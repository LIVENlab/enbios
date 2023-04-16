"""
Serialize a model once it has been built
Before solving and/or after solving. Clone operations executed. Scales not performed.

It could be something like:

Metadata:
{
{,
Parameters:
[
 {
 },
],
CodeHierarchies:
[
 {
 },
],
Observers:
[
 {
 },
],
InterfaceTypes:
[
 {
 },
],
InterfaceTypeConverts: (if conversions not done already)
[
 {
 },
],
Processors:
[
 {
  Interfaces:
  [
   {...,
    Observations:
    [
     {
     },
    ]
   }
  ]
 }
],
Relationships:
[
 {
  Origin: {name, uuid}
  Destination: {name, uuid}
  Type
 }
]
"""
import json
from collections import OrderedDict

import jsonpickle
from typing import Dict, List, Union, Optional, Any

from nexinfosys.common.helper import create_dictionary, CustomEncoder, values_of_nested_dictionary
from nexinfosys.model_services import State
from nexinfosys.models.musiasem_concepts import Processor, Parameter, Hierarchy, Taxon, Observer, FactorType, \
    ProcessorsRelationPartOfObservation, ProcessorsRelationUpscaleObservation, ProcessorsRelationIsAObservation, \
    FactorsRelationDirectedFlowObservation, FactorTypesRelationUnidirectionalLinearTransformObservation
from nexinfosys.solving import BasicQuery


JsonStructureType = Dict[str, Optional[Union[type, "JsonStructureType"]]]


def objects_list_to_string(objects_list: List[object], object_type: type) -> str:
    str_list = []

    if objects_list:
        if object_type is Hierarchy:
            # Just get the Hierarchy objects of type Taxon
            objects_list = [o for o in objects_list if o.hierarchy_type == Taxon]
            # Sort the list to show the "code lists" first
            objects_list.sort(key=lambda h: not h.is_code_list)

        for obj in objects_list:
            str_list.append(json.dumps(obj, cls=CustomEncoder))

    return ", ".join(str_list)


def create_json_string_from_objects(objects: Dict[type, List[object]], json_structure: JsonStructureType) -> str:

    def json_structure_to_string(sections_and_types: JsonStructureType) -> str:
        str_list = []
        for section_name, output_type in sections_and_types.items():
            if not isinstance(output_type, dict):
                str_list.append(f'"{section_name}": [{objects_list_to_string(objects.get(output_type), output_type)}]')
            else:
                str_list.append(f'"{section_name}": {{{json_structure_to_string(output_type)}}}')

        return ", ".join(str_list)

    return json_structure_to_string(json_structure)


# ------ MAIN FUNCTION ------

def model_to_json(state: State, structure: Dict):
    # Get objects from state
    query = BasicQuery(state)
    objects = query.execute(values_of_nested_dictionary(structure), filt="")

    return create_json_string_from_objects(objects, structure)


def export_model_to_json(state: State) -> str:

    # Get Metadata dictionary
    metadata = state.get("_metadata")
    metadata_string = None
    if metadata:
        # Change lists of 1 element to a simple value
        metadata = {k: v[0] if len(v) == 1 else v for k, v in metadata.items()}
        metadata_string = '"Metadata": ' + json.dumps(metadata, cls=CustomEncoder)

    json_structure: JsonStructureType = OrderedDict(
        {"Parameters": Parameter,
         "CodeHierarchies": Hierarchy,
         "Observers": Observer,
         "InterfaceTypes": FactorType,
         "InterfaceTypeConverts": FactorTypesRelationUnidirectionalLinearTransformObservation,
         "Processors": Processor,
         "Relationships": OrderedDict(
             {"PartOf": ProcessorsRelationPartOfObservation,
              "Upscale": ProcessorsRelationUpscaleObservation,
              "IsA": ProcessorsRelationIsAObservation,
              "DirectedFlow": FactorsRelationDirectedFlowObservation
              }
         )
         }
    )

    json_string = model_to_json(state, json_structure)

    if metadata_string:
        json_string = f'{metadata_string}, {json_string}'

    return f'{{{json_string}}}'

