import pandas as pd
import pandera.pandas as pa
from pandera import Check

schema = pa.DataFrameSchema(
    {
    "Processor": pa.Column(str, required=True, nullable=False),
    "Region": pa.Column(str, required=True, nullable=False),
    "@SimulationCarrier": pa.Column(str, required=True, nullable=False),
    "ParentProcessor": pa.Column(str, required=True, nullable=False),
    "@SimulationToEcoinventFactor": pa.Column(float,Check(lambda s: s.apply(lambda x: isinstance(x, (float,int)))),
                                              required=True,
                                              nullable=False),
    "Ecoinvent_key_code": pa.Column(str,Check(lambda s: s.apply(lambda x: isinstance(x, (str,float,int)))),
                                    required=True, nullable=False),
    "File_source": pa.Column(str, required=True, nullable=False),
    "geo_loc": pa.Column(str, required=True, nullable=False)
    },
    strict = False  # Allow extra columns
    )

calliope_cleaning_schema = pa.DataFrameSchema(
    {
        "full_name": pa.Column(str, required=True, nullable=False),
        "energy_value": pa.Column(float, required=True, nullable=False),
        "new_units":  pa.Column(str, required=True, nullable=False),
        "scenarios": pa.Column(str,Check(lambda s: s.apply(lambda x: isinstance(x, (str,float,int)))),
                                                           required=True, nullable=False)

    }, strict = True  )# Allow extra columns


methods_schema = pa.DataFrameSchema(
    {
        "Formula": pa.Column(str, required=True, nullable=False),
    },
    strict= False ) # Allow extra columns (i.e information about the method)

hierarchy_schema = pa.DataFrameSchema(
    {
        "Processor": pa.Column(str, required=True, nullable=False),
        "ParentProcessor": pa.Column(str, required=True, nullable=False),
        "Level": pa.Column(str,
                           required=True,
                           nullable=False,
                           checks=Check.str_matches(r"^n(-\d+)?$")
                           ),
    } # Level should be n or n-1, n-2 etc.
)