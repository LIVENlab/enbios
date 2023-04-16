"""
Given a State, elaborate an equivalent RDF or OWL file which can be exploited outside

To view it (suggestions):

* https://github.com/essepuntato/LODE
* http://vowl.visualdataweb.org/webvowl.html


RDF Basics:
Triple: (s,p,o)
s (subject), ->p (predicate or property)-> o (object). Node-arc-Node
s: URIRef, Blank Node
p: URIRef
o: URIRef, Blank Node, Literal

Where...
* URIRef. A full URL or a QName (a schema prefix, ":", a local name)
* Blank nodes. Nodes with no intrinsic name. Blank Node Identifier (an ad-hoc identifier)
* URI references URIRef. Vocabulary is "URI based"
* Literals. Literal (unicode) + datatype or language tag (RFC 3066)

Concept: "Nodes": set of Subjects and Objects of a graph

Concept: "Ground RDF": graph with no Blank nodes

Concept: "Name": URIRef or Literal

RDF Principles:
* XML-based syntax
* XML-schema datatypes

Resource. A COMPACT way to access a SUBJECT to read or write its properties
Domain
Range

NamespaceManager: associates Namespace with Prefixed
Each RDFLIB Graph has a "namespace_manager":
 - Populated when reading an RDF
 - New can be added using graph.bind()
 - SPARQL queries can be initialized using "initNs" parameter in method "query" of a GRAPH

Persistence:
 - Memory
 - Sleepycat (BSDDB or BSDDB3)
 - SPARQLStore
 - SPARQLUpdateStore

"""
import os
import sys
from enum import Enum
import owlready2
import io

from rdflib.resource import Resource

from nexinfosys.model_services import State, get_case_study_registry_objects

from nexinfosys.model_services.workspace import execute_file_return_issues
from nexinfosys.initialization import prepare_and_reset_database_for_tests
from nexinfosys.models.musiasem_concepts import FactorType, Processor, Factor, FactorsRelationScaleObservation, \
    FactorsRelationDirectedFlowObservation, ProcessorsRelationPartOfObservation, \
    FactorTypesRelationUnidirectionalLinearTransformObservation, FactorQuantitativeObservation
from nexinfosys.serialization import deserialize_state, serialize_state


class Ontology(Enum):
    # Basic ontologies
    XML = 'http://www.w3.org/XML/1998/namespace'
    XSD = 'http://www.w3.org/2001/XMLSchema#'
    RDF = 'http://www.w3.org/1999/02/22-rdf-syntax-ns#'
    RDFS = 'http://www.w3.org/2000/01/rdf-schema#'
    OWL = 'http://www.w3.org/2002/07/owl#'
    PROV = 'http://www.w3.org/ns/prov#'
    DC = 'http://purl.org/dc/elements/1.1/'
    DCT = 'http://purl.org/dc/terms/'
    SCHEMA = 'http://schema.org/'

    # special interest namespaces
    SKOS = 'http://www.w3.org/2004/02/skos/core#'
    FOAF = 'http://xmlns.com/foaf/0.1/'
    MA = 'http://www.w3.org/ns/ma-ont#'
    SIOC = 'http://rdfs.org/sioc/ns#'
    PO = 'http://purl.org/ontology/po/'

    # lexical namespaces
    LEMON = 'http://lemon-model.net/lemon#'
    LEXINFO = 'http://www.lexinfo.net/ontology/2.0/lexinfo#'
    LANG = 'http://id.loc.gov/vocabulary/iso639-1/'

    # wikidata, dbpedia, geo
    DBO = 'http://dbpedia.org/ontology/'
    GEO = 'http://www.opengis.net/ont/geosparql#'
    WDT = 'http://www.wikidata.org/prop/direct/'
    WD = 'http://www.wikidata.org/entity/'
    P = 'http://www.wikidata.org/prop/'
    PS = 'http://www.wikidata.org/prop/statement/'
    PQ = 'http://www.wikidata.org/prop/qualifier/'
    GN = 'http://sws.geonames.org/'

    @classmethod
    def to_fully_qualified(cls, prefix: str) -> str:
        """ Look up short prefix and return fully-qualified URL if known.
        :param prefix: the prefix to be resolved.
        :returns the fully-qualified URL or the prefix if unknown.
        """
        prefix = prefix.upper()
        if hasattr(Ontology, prefix):
            return getattr(cls, prefix).value
        return prefix

    @classmethod
    def to_prefix(cls, uri: str) -> str:
        """
        """
        try:
            return Ontology(uri).name.lower()
        except Exception as e:
            pass


NAMESPACES = {item.value: item.name.lower() for item in Ontology}

PREFIXES = '\n'.join([''] + ['PREFIX {value}: <{key}>'.format(value=item.name.lower(),
                                                              key=item.value)
                             for item in Ontology])


def to_fully_qualified(attribute: str) -> str:
    """ QName originates from the XML world, where it is used to reduce I/O by shortening
    namespaces (e.g. http://www.weblyzard.com/wl/2013#) to a prefix (e.g. wl)
    followed by the local part (e.g. jonas_type). The namespace-prefix relations
    are thereby defined in the XML-Head. This relation mapping does not exist in JSON,
    which is why we have to have the full qualified name (namespace + local part)
    to define an attribute. While the more readable way would be to just use the URI,
    the official standardized format is {namespace}localpart, having the major
    advantage of non-ambiguous namespace identification. Further, Java expects Qnames
    in this annotation format, which enables to simply use Qname.valueOf(key).
    :param attribute: the attribute to resolve
    :returns a fully-qualified version of the input attribute.
    """
    if len(attribute.split(':')) <= 1 or attribute.startswith('{'):
        return attribute

    namespace, attr_name = attribute.split(':')
    return '{%s}%s' % (Ontology.to_fully_qualified(namespace), attr_name)


def prefix_uri(uri: str, allow_partial: bool=False) -> str:
    """ Replace a sub-path from the uri with the most specific prefix as defined
    in the Namespace.
    :param uri: The URI to modify.
    :type uri: str
    :returns: The modified URI if applicable
    :rtype: str
    """
    if not uri.startswith('http'):
        return uri
    # replace most specific/longest prefix, hence sorted
    for namespace in sorted(list([ns.value for ns in Ontology]), key=len, reverse=True):
        if namespace in uri:
            replaced = uri.replace(
                namespace, '{}:'.format(Ontology.to_prefix(namespace)))
            if '/' in replaced or '#' in replaced:
                if not allow_partial:
                    # slashes or hashes in prefixed URIs not allowed
                    continue
            return replaced
    return uri


def replace_prefix(uri):
    """ Replace a prefix with the fully-qualified namespace URL.
    :param uri: The URI to modify.
    :type uri: str
    :returns: The modified URI if applicable
    :rtype: str
    """
    for namespace in sorted(list(NAMESPACES.keys()), key=len, reverse=True):
        prefix = '{}:'.format(NAMESPACES[namespace])
        if uri.startswith(prefix):
            return uri.replace(prefix, namespace)
    return uri


def parse_language_tagged_string(value: str) -> tuple:
    """ Check if a string value has a language tag @xx or @xxx
    and returns the string without the value tag and language
    as tuple. If no language tag -> language is None
    :param value
    :returns:
    """
    lang = None
    if len(value) > 1 and value[0] == value[-1] == '"':
        value = value[1:-1]
    if len(value) > 6 and value[-6] == '@':
        lang = value[-5:]
        value = value[:-6]
    elif len(value) > 3 and value[-3] == '@':
        lang = value[-2:]
        value = value[:-3]
    elif len(value) > 4 and value[-4] == '@':
        lang = value[-3:]
        value = value[:-4]
    return value, lang


def generate_rdf_from_object_model(state: State):
    """
    Using the base ontology "musiasem.owl", generate a file

    :param state:
    :return:
    """
    onto = owlready2.get_ontology("file:////home/rnebot/Dropbox/nis-backend/nexinfosys/ie_exports/musiasem.owl").load()
    glb_idx, p_sets, hh, datasets, mappings = get_case_study_registry_objects(state)
    # onto = get_ontology("file:///musiasem.owl").load()

    m = {}  # Map from objects of the model in memory to the equivalent OWL individuals

    # InterfaceTypes
    fts = glb_idx.get(FactorType.partial_key())
    for ft in fts:
        ft_individual = onto.InterfaceType(ft.name)
        ft_individual.label = ft.name
        ft_individual.has_roegen_type = ft.roegen_type
        ft_individual.has_sphere = ft.sphere
        ft_individual.has_processor_type = ft.opposite_processor_type
        ft_individual.has_unit = ft.unit
        if ft.range:
            tmp = onto.InterfaceRange()
            tmp.has_interval = ft.range
            tmp.has_interval_unit = ft.range_unit
            ft_individual.has_range = tmp

        ft_individual.has_interface = []

        m[ft] = ft_individual

    # Processors and Systems
    procs = glb_idx.get(Processor.partial_key())
    for proc in procs:
        proc_individual = onto.Processor(proc.full_hierarchy_names(glb_idx)[0])
        proc_individual.has_interface = []
        proc_individual.has_part_directly = []
        proc_individual.has_system = proc.processor_system  # Valor string
        proc_individual.has_subsystem_type = proc.subsystem_type  # Valor entre una lista (enum)
        proc_individual.has_processor_type = proc.functional_or_structural  # Valor entre una lista (enum)
        proc_individual.is_accounted = proc.instance_or_archetype  # Boolean
        proc_individual.has_level = proc.level  # Valor string
        if proc.geolocation:
            tmp = onto.Geolocation()
            proc.geolocation.code
            proc.geolocation.ref
            proc_individual.location = tmp
        m[proc] = proc_individual

    # Interfaces and Quantities
    fs = glb_idx.get(Factor.partial_key())
    for f in fs:
        f_individual = onto.Interface()
        f_individual.label = f.name
        f_individual.has_roegen_type = [f.roegen_type]
        f_individual.has_sphere = [f.sphere]
        f_individual.has_orientation = [f.orientation]
        f_individual.has_processor_type = [f.opposite_processor_type]
        f_individual.has_unit = [f.unit]
        if f.range:
            tmp = onto.InterfaceRange()
            tmp.has_interval = [f.range]
            tmp.has_interval_unit = [f.range_unit]
            f_individual.has_range = tmp

        f_individual.has_qqobservation = []
        m[f] = f_individual

        # Link Interface -> AND <- Processor
        f_individual.has_processor = [m[f.processor]]
        m[f.processor].has_interface.append(f_individual)

        # Link Interface -> AND <- InterfaceType
        f_individual.has_interface_type = [m[f.taxon]]
        m[f.taxon].has_interface.append(f_individual)

        # Qualified Quantities
        for q in f.observations:
            if isinstance(q, FactorQuantitativeObservation):
                q_individual = onto.QualifiedQuantity()
                q_individual.has_value = [q.value]
                q_individual.has_unit = [q.unit]
                q_individual.has_time = [q.time]
                q_individual.has_source = [q.source]

                m[q] = q_individual
                # Link Interface -> AND <- QualifiedQuantity
                q_individual.has_interface = [m[f]]
                m[f].has_qqobservation.append(q_individual)

    # Relationships

    #  Part-Of
    part_ofs = glb_idx.get(ProcessorsRelationPartOfObservation.partial_key())
    for po in part_ofs:
        po_individual = onto.PartOf()
        po_individual.has_parent = [m[po.parent_processor]]
        po_individual.has_child = [m[po.child_processor]]
        po_individual.has_weight = 1

        m[po.child_processor].has_parent.append(m[po.parent_processor])
        m[po.parent_processor].has_part_directly.append(m[po.child_processor])

    #  Exchange
    exchanges = glb_idx.get(FactorsRelationDirectedFlowObservation.partial_key())
    for ex in exchanges:
        ex_individual = onto.SequentialFlowLinkage()
        ex_individual.has_origin = [m[ex.source]]
        ex_individual.has_child = [m[ex.target]]
        ex_individual.has_weight = ex.weight
        # ex_individual.has_observer

    #  Scale
    scale = glb_idx.get(FactorsRelationScaleObservation.partial_key())
    #  ScaleChange
    scale_change = glb_idx.get(FactorTypesRelationUnidirectionalLinearTransformObservation.partial_key())

    # Processor Properties
    #  System (maybe a Class)
    #  Geolocation (maybe a Class)
    #  is_functional (boolean)
    #  is_structural (boolean)
    #  is_notional (boolean)
    #  ...
    # InterfaceType Properties
    #  RoegenType (NOT a class)
    #
    # Interface Properties
    #  Orientation
    #  Quantification (a Class)
    #
    # Relationship Properties
    #  Source (another individual)
    #  Target (another individual)
    #  Weight (property)

    # Formal 2 Semantic
    #  Datasets
    #  Hierarchies
    #  Mappings

    out = io.BytesIO()
    onto.save(out, format="n3")

    s = out.getvalue().decode("utf-8")
    with open("/home/rnebot/Downloads/prueba.txt", "wt") as f:
        f.write(s)

    return s


def ensure_dir(file_path):
    directory = os.path.dirname(file_path)
    if not os.path.exists(directory):
        os.makedirs(directory)


if __name__ == '__main__':
    # graph = Graph(identifier="g1")
    # graph.parse("/home/rnebot/Dropbox/nis-backend/nexinfosys/ie_exports/musiasem.owl", format="xml")
    # # graph.serialize("", format="n3")
    # n = Namespace("http://ngd.org/")
    # person = Resource(graph, n["person#p1"])
    # bn = BNode()
    # aref = URIRef("Hola")
    # # graph.add((aref, n.Code, Literal("x01")))
    # result = graph.parse("http://www.w3.org/2000/10/swap/test/meet/blue.rdf")
    # for p in graph:
    #     print(f"{type(p)}{p}")
    #
    # # graph.add()  # graph.input(file_input1) or graph.load(...) or graph.parse()
    # graph.close()
    # graph.add((aref, n.Code, Literal("x01")))
    # for p in graph:
    #     print(f"{type(p)}{p}")
    # print(f"{Literal('01', datatype=XSD.integer)!=Literal('1', datatype=XSD.integer)}")
    # print(f"{n.Person},{n['hola%20adios']}")
    # print("Hola")
    # sys.exit(1)

    input_file = "/home/rnebot/Dropbox/nis-internal-tests/pruebaSoyBeansNIS.xlsx"
    state_file = "/home/rnebot/Dropbox/nis-internal-tests/pruebaSoyBeansNIS.serialized_state"
    if not os.path.exists(state_file):
        prepare_and_reset_database_for_tests(prepare=True)
        isess, issues = execute_file_return_issues(input_file, generator_type="spreadsheet")
        ensure_dir(state_file)
        # Save state
        s = serialize_state(isess.state)
        with open(state_file, "wb") as f:
            f.write(s)
        state = isess.state
    else:
        with open(state_file, "rb") as f:
            s = f.read()
            state = deserialize_state(s)

    generate_rdf_from_object_model(state)

