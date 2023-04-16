import requests

base_url_v2 = "http://ec.europa.eu/eurostat/cache/GISCO/distribution/v2/"
base_url_v1 = "http://ec.europa.eu/eurostat/cache/GISCO/distribution/v2/"

# List of datasets available for each version
# base_url_v2+"/nuts/datasets.json"
#
# Download dataset: NUTS-YEAR or LAU-YEAR. Both are related. Maximum resolution.


lst = ["NUTS_LB_2013",  # Regions, mid point?
       "NUTS_SEPA_LI_2013",
       "NUTS_RG_60M_2013",  # Regions
       "NUTS_JOIN_LI_2013",
       "NUTS_BN_60M_2013"
       ]

# Download and cache tree of NUTS
# Misc Service to obtain the GeoJSON definition of a unit, or the direct URL, given the code

