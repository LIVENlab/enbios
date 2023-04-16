import collections

# SDMX Concept can be: dimension, attribute or measure. Stored in "metadatasets" associated to a dataset by its name
SDMXConcept = collections.namedtuple('Concept', 'type name istime description code_list')
