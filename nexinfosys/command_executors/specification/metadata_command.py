import json

from xml.sax.saxutils import escape

from nexinfosys import metadata_fields
from nexinfosys.common.helper import create_dictionary
from nexinfosys.model_services import IExecutableCommand


def generate_dublin_core_xml(content):
    """
    Generate an XML string with a Simple Dublin Core Record from a Case Study Metadata Command Content
    :param content:
    :return:
    """
    controlled = create_dictionary()
    for t in metadata_fields:
        controlled[t[4]] = t

    s = """<?xml version="1.0"?>
<caseStudyMetadata xmlns="http://magic-nexus.org/dmp/" xmlns:dc="http://purl.org/dc/elements/1.1/">
"""
    for key in content:
        k = controlled[key][1]
        if k:
            for l in content[key]:
                s += "    <dc:" + k + ">" + escape(str(l)) + "</dc:" + k + ">\n"

    s += "</caseStudyMetadata>\n"

    return s


class MetadataCommand(IExecutableCommand):
    def __init__(self, name: str):
        self._name = name
        self._metadata_dictionary = {}

    def execute(self, state: "State"):
        """ The execution creates an instance of a Metadata object, and assigns the name "metadata" to the variable,
            inserting it into "State" 
        """
        issues = []
        cs = state.get("_case_study")
        cs_version = state.get("_case_study_version")
        state.set("_metadata", self._metadata_dictionary)
        if cs:
            # Modify case study attributes
            cs_version.name = ""
            if "case_study_name" in self._metadata_dictionary and self._metadata_dictionary["case_study_name"]:
                cs_version.name += "- ".join(self._metadata_dictionary["case_study_name"])
            if "title" in self._metadata_dictionary and self._metadata_dictionary["title"]:
                if cs_version.name:
                    cs_version.name += "; "
                cs_version.name += "- ".join(self._metadata_dictionary["title"])
            if "doi" in self._metadata_dictionary and self._metadata_dictionary["doi"]:
                cs.oid = self._metadata_dictionary["doi"][0]
            if "description" in self._metadata_dictionary and self._metadata_dictionary["description"]:
                cs.description = '; '.join(self._metadata_dictionary["description"])
            if "dimensions" in self._metadata_dictionary and self._metadata_dictionary["dimensions"]:
                cs.areas = ""
                lst = [i.lower() for i in self._metadata_dictionary["dimensions"]]
                if "water" in lst:
                    cs.areas += "W"
                if "energy" in lst:
                    cs.areas += "E"
                if "food" in lst:
                    cs.areas += "F"
                if "land" in lst:
                    cs.areas += "L"
                if "climate" in lst:
                    cs.areas += "C"
            if "geographical_level" in self._metadata_dictionary and self._metadata_dictionary["geographical_level"]:
                cs.geographic_level = ""
                lst = [i.lower() for i in self._metadata_dictionary["geographical_level"]]
                if "local" in lst:
                    cs.geographic_level += "L"
                if "regional" in lst or "region" in lst:
                    cs.geographic_level += "R"
                if "country" in lst:
                    cs.geographic_level += "C"
                if "europe" in lst:
                    cs.geographic_level += "E"
                if "sector" in lst or "sectoral" in lst:
                    cs.geographic_level += "S"
            if "restriction_level" in self._metadata_dictionary and self._metadata_dictionary["restriction_level"]:
                cs.restriction_level = ""
                lst = [i.lower() for i in self._metadata_dictionary["restriction_level"]]
                if "internal" in lst:
                    cs.restriction_level += "I"
                if "confidential" in lst:
                    cs.restriction_level += "C"
                if "public" in lst:
                    cs.restriction_level += "P"
            if "version" in self._metadata_dictionary and self._metadata_dictionary["version"]:
                cs.version = str(self._metadata_dictionary["version"])

            if "case_study_code" not in self._metadata_dictionary:
                # Generate internal code according to DMP
                correlative = 1  # TODO Obtain unique -MAGIC project level- correlative number
                cs.internal_code = "CS"+str(correlative)+"_"+cs.geographic_level+"_"+cs.areas+"_"+cs.restriction_level+"-"+cs.version
                self._metadata_dictionary["case_study_code"] = cs.internal_code
            else:
                cs.internal_code = self._metadata_dictionary["case_study_code"][0]

        return issues, None  # Issues, output

    def estimate_execution_time(self):
        return 0

    def json_serialize(self):
        # Directly return the metadata dictionary
        return self._metadata_dictionary

    def json_deserialize(self, json_input):
        # TODO Check validity
        issues = []
        if isinstance(json_input, (dict, list)):
            self._metadata_dictionary = json_input
        else:
            self._metadata_dictionary = json.loads(json_input)
        return issues