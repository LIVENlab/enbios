from collections import namedtuple

Domain = namedtuple("Domain", "type values")
Field = namedtuple("Field", "name domain mandatory")

ref_prof = [{"type": "geographic",  # Reduced, from ISO19139
             "fields":
                 [Field(name="title", domain=Domain(type="string", values=None), mandatory="True"),
                  Field(name="date", domain=Domain(type="string", values=None), mandatory="True"),
                  Field(name="boundingbox", domain=Domain(type="string", values=None), mandatory="False"),
                  # Field(name="language", domain=Domain(type="string", values=None), mandatory="True"),
                  # Field(name="charset", domain=Domain(type="string", values=None), mandatory="False"),
                  Field(name="topiccategory", domain=Domain(type="string", values=None), mandatory="True"),
                  # Field(name="metadatalanguage", domain=Domain(type="string", values=None), mandatory="False"),
                  Field(name="description", domain=Domain(type="string", values=None), mandatory="True"),
                  # Field(name="metadatacharset", domain=Domain(type="string", values=None), mandatory="False"),
                  Field(name="metadatapointofcontact", domain=Domain(type="string", values=None), mandatory="False"),
                  Field(name="annote", domain=Domain(type="string", values=None), mandatory="False"),
                  Field(name="datalocation", domain=Domain(type="string", values=None), mandatory="True"),
                  # This is not ISO19139. Reference to definition of data
                  ]
             },
            {"type": "provenance",  # Reduced, from W3C Provenance Recommendation
             "fields":
                 [Field(name="agenttype", domain=Domain(type="string", values=["person", "software", "organization"]),
                        mandatory="True"),
                  Field(name="agent", domain=Domain(type="string", values=None), mandatory="True"),
                  Field(name="activities", domain=Domain(type="string", values=None), mandatory="True"),
                  Field(name="entities", domain=Domain(type="string", values=None), mandatory="False"),
                  ]
             },
            {"type": "bibliographic",  # From BibTex
             "fields":
                 [Field(name="entry_type", domain=Domain(type="string",
                                                         values=["article", "book", "booklet", "conference", "inbook",
                                                                 "incollection", "inproceedings", "mastersthesis", "misc",
                                                                 "phdtesis", "proceedings", "techreport", "unpublished"]),
                        mandatory="True"),
                  Field(name="address", domain=Domain(type="string", values=None),
                        mandatory="entry_type not in ('article', 'misc')"),
                  Field(name="annote", domain=Domain(type="string", values=None), mandatory="False"),
                  Field(name="author", domain=Domain(type="string", values=None), mandatory="entry_type not in ('misc')"),
                  Field(name="booktitle", domain=Domain(type="string", values=None),
                        mandatory="entry_type in ('incollection', 'inproceedings')"),
                  Field(name="chapter", domain=Domain(type="string", values=None),
                        mandatory="entry_type in ('inbook', 'incollection')"),
                  Field(name="crossref", domain=Domain(type="string", values=None), mandatory="False"),
                  Field(name="edition", domain=Domain(type="string", values=None), mandatory="False"),
                  Field(name="editor", domain=Domain(type="string", values=None),
                        mandatory="entry_type in ('book', 'inbook', 'incollection', 'inproceedings', 'proceedings')"),
                  Field(name="howpublished", domain=Domain(type="string", values=None), mandatory="False"),
                  Field(name="institution", domain=Domain(type="string", values=None),
                        mandatory="entry_type in ('techreport')"),
                  Field(name="journal", domain=Domain(type="string", values=None), mandatory="entry_type in ('article')"),
                  Field(name="key", domain=Domain(type="string", values=None), mandatory="False"),
                  Field(name="month", domain=Domain(type="string",
                                                    values=["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep",
                                                            "Oct", "Nov", "Dec"]), mandatory="False"),
                  Field(name="note", domain=Domain(type="string", values=None), mandatory="False"),
                  Field(name="number", domain=Domain(type="string", values=None), mandatory="False"),
                  Field(name="organization", domain=Domain(type="string", values=None), mandatory="False"),
                  Field(name="pages", domain=Domain(type="string", values=None), mandatory="False"),
                  Field(name="publisher", domain=Domain(type="string", values=None), mandatory="False"),
                  Field(name="school", domain=Domain(type="string", values=None), mandatory="False"),
                  Field(name="series", domain=Domain(type="string", values=None), mandatory="False"),
                  Field(name="title", domain=Domain(type="string", values=None), mandatory="entry_type not in ('misc')"),
                  Field(name="type", domain=Domain(type="string", values=None), mandatory="False"),
                  Field(name="url", domain=Domain(type="string", values=None), mandatory="False"),
                  # This one is not BibTex standard, but it is necessary
                  Field(name="volume", domain=Domain(type="string", values=None), mandatory="False"),
                  Field(name="year", domain=Domain(type="int", values=None),
                        mandatory="entry_type in ('article', 'book', 'inbook', 'incollection', 'inproceedings', 'mastersthesis', 'phdthesis', 'proceedings', 'techreport')")
                  ]
             }
            ]


def profile_field_name_sets():
    m = {}
    for prof in ref_prof:
        m[prof["type"]] = set([i.name.lower() for i in prof["fields"]])
    return m
