import geojson
import urllib

from nexinfosys.common.helper import create_dictionary
from nexinfosys.model_services import get_case_study_registry_objects
from nexinfosys.models.musiasem_concepts import Processor, GeographicReference

in_files = create_dictionary()  # URL -> (json, idx)


def read_geojson(url):
    """
    Read a GeoJSON file and index it by ID

    :param url:
    :return: A tuple with the deserialized GeoJSON file and an index of ID to position in the features list
    """
    if url not in in_files:
        f = urllib.request.urlopen(url)
        j = geojson.loads(f.read())
        id_dict = create_dictionary()
        for i, f in enumerate(j["features"]):
            fid = f["id"]
            id_dict[fid] = i
        in_files[url] = (j, id_dict)
    else:
        j, id_dict = in_files[url]

    return j, id_dict


def generate_geojson(s):
    """

    :param s:
    :return:
    """

    features = []
    # Obtain principal structures
    glb_idx, p_sets, hierarchies, datasets, mappings = get_case_study_registry_objects(s)
    ps = glb_idx.get(Processor.partial_key())
    for p in ps:
        # TODO If there is Geolocation information, obtain the region
        #      Collect also the attributes of the processor and of all the interfaces, from "flow graph solution"
        # url = "https://raw.githubusercontent.com/eurostat/Nuts2json/master/2016/4326/20M/nutsrg_1.json"
        if not p.geolocation:
            continue

        gr = glb_idx.get(GeographicReference.partial_key(name=p.geolocation.reference))
        url = gr[0].attributes["data_location"]
        geo_id = p.geolocation.code
        j, ids = read_geojson(url)
        # Obtain element of interest (GeoJSON)
        tmp = j["features"][ids[geo_id]]
        feature = tmp.copy()
        # Add processor properties
        feature["properties"]["processor"] = dict(name=p.name, h_name=p.full_hierarchy_names(glb_idx)[0], subsystem_type=p.subsystem_type, system=p.processor_system, level=p.level, functional_or_structural=p.functional_or_structural, instance_or_archetype=p.instance_or_archetype, stock=p.stock)
        # Add interface properties
        interfaces = []
        for i in p.factors:
            idict = dict()
            interfaces.append(idict)
        feature["properties"]["interfaces"] = interfaces
        # TODO Add relationships?

        features.append(feature)

    return dict(type="FeatureCollection", features=features)


