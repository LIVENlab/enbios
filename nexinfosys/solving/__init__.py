from typing import List, Union, Dict, Set
from abc import ABCMeta, abstractmethod

from nexinfosys.models.musiasem_concepts import Processor, Observer, FactorType, Factor, \
    FactorQuantitativeObservation, FactorTypesRelationUnidirectionalLinearTransformObservation, \
    ProcessorsRelationPartOfObservation, ProcessorsRelationUndirectedFlowObservation, \
    ProcessorsRelationUpscaleObservation, FactorsRelationDirectedFlowObservation, Hierarchy, Parameter, \
    ProcessorsRelationIsAObservation, FactorsRelationScaleObservation
from nexinfosys.model_services import get_case_study_registry_objects, State
from nexinfosys.common.helper import create_dictionary, PartialRetrievalDictionary


class IQueryObjects(metaclass=ABCMeta):
    @abstractmethod
    def execute(self, object_classes: List, filt: Union[dict, str]) -> str:
        """
        Query state to obtain objects of types enumerated in "object_classes", applying a filter
        In general, interface to pass state, select criteria (which kinds of objects to retrieve) and
        filter criteria (which of these objects to obtain).

        :param object_classes: A list with the names/codes of the types of objects to obtain
        :param filt: A way of expressing a filter of objects of each of the classes to be retrieved
        :return:
        """
        pass


class BasicQuery(IQueryObjects):
    def __init__(self, state: State):
        self._state = state
        self._registry, self._p_sets, self._hierarchies, self._datasets, self._mappings = get_case_study_registry_objects(state)

    def execute(self, object_classes: List[Union[type, str]], filt: Union[dict, str]) -> Dict[type, List[object]]:
        requested = {}
        supported_types = [Observer, Processor, FactorType, Factor,
                           # FactorQuantitativeObservation, --> Use find_quantitative_observations() instead
                           FactorTypesRelationUnidirectionalLinearTransformObservation,
                           ProcessorsRelationPartOfObservation, ProcessorsRelationUpscaleObservation,
                           ProcessorsRelationUndirectedFlowObservation, ProcessorsRelationIsAObservation,
                           FactorsRelationScaleObservation,
                           FactorsRelationDirectedFlowObservation, Hierarchy, Parameter]
        supported_types_names = {t.__name__.lower(): t for t in supported_types}

        for object_class_name in [o.lower() if isinstance(o, str) else o.__name__.lower() for o in object_classes]:

            if object_class_name in supported_types_names:
                # Get the type from the name
                object_class = supported_types_names[object_class_name]

                # Obtain all objects of specified type
                objects = set(self._registry.get(object_class.partial_key()))

                # Filter list of objects
                # TODO: apply filter to all types
                if object_class == Observer:
                    if "observer_name" in filt:
                        objects = [o for o in objects if o.name.lower() == filt["observer_name"]]

                # Store result
                requested[object_class] = objects
            else:
                raise Exception("Class not supported in BasicQuery.execute(): " + str(object_class_name))

        return requested


def get_processor_names_to_processors_dictionary(state: PartialRetrievalDictionary):
    """
    Obtain a dictionary with all processor names (a processor may have multiple names) and
    the corresponding Processor object

    :param state:
    :return:
    """
    ps = state.get(Processor.partial_key())
    ps = set(ps)  # Avoid repeating Processor objects
    d = create_dictionary()
    for p in ps:
        for n in p.full_hierarchy_names(state):
            d[n] = p
    return d


def get_processor_id(p: Processor):
    return p.name.lower()


def get_processor_ident(p: Processor):
    return p.ident


def get_processor_label(p: Processor):
    return p.name.lower()


def get_processor_unique_label(p: Processor, reg: PartialRetrievalDictionary):
    return p.full_hierarchy_names(reg)[0]


def get_factor_id(f_: Union[Factor, Processor], ft: FactorType=None, prd: PartialRetrievalDictionary=None):
    if isinstance(f_, Factor):
        if prd:
            name = f_.processor.full_hierarchy_names(prd)[0]
        else:
            name = f_.processor.name
        return (name + ":" + f_.taxon.name).lower()
    elif isinstance(f_, Processor) and isinstance(ft, FactorType):
        if prd:
            name = f_.full_hierarchy_names(prd)[0]
        else:
            name = f_.name
        return (name + ":" + ft.name).lower()


def get_factor_type_id(ft: (FactorType, Factor)):
    if isinstance(ft, FactorType):
        return ":"+ft.name.lower()
    elif isinstance(ft, Factor):
        return ":" + ft.taxon.name.lower()


def processor_to_dict(p: Processor, reg: PartialRetrievalDictionary):
    return dict(name=get_processor_id(p), uname=get_processor_unique_label(p, reg), ident=p.ident)


def factor_to_dict(f_: Factor):
    return dict(name=get_factor_id(f_), rep=str(f_), ident=f_.ident)
