"""
init of describing characterizations (impact assessment methods) directly from an activity
"""

"""
    "amount": number,  ## This is the only required field
    "uncertainty type": int,
    "loc": number,
    "scale": number,
    "shape": number,
    "minimum": number,
    "maximum": number
    


    0: Undefined or unknown uncertainty.

    1: No uncertainty.

    2: Lognormal distribution. This is a tricky distribution to work with, but is very popular in LCA. The amount field is the median of the data, and the sigma field is the standard deviation of the data when it is log-transformed, i.e. the σ from the formula for the log-normal PDF.

    3: Normal distribution.

    4: Uniform distribution.

    5: Triangular distribution.

    6: Bernoulli distribution.

    7: Discrete uniform.

    8: Weibull.

    9: Gamma.

    10: Beta distribution.

    11: Generalized Extreme Value.

    12: Student’s T.

"""

uncertainty_schema = {
    "type": "object",
    "properties": {
        "amount": {"type": "number"},
        "uncertainty type": {
            "oneOf": [
                {"type": "number", "minimum": 0, "maximum": 12},
                {"type": "string",
                 "enum": ["undefined", "none", "lognormal", "normal", "uniform", "triangular", "bernoulli",
                          "discrete uniform", "weibull", "gamma", "beta", "generalized extreme value", "students t"]},
            ]
        },
        "loc": {"type": "number"},
        "scale": {"type": "number"},
        "shape": {"type": "number"},
        "minimum": {"type": "number"},
        "maximum": {"type": "number"},
    }
}

data = {
    "name": "process 1",
    "code": "process 1",
    "type": "biosphere",
    "characterization": {
        # basic
        ('CML v4.8 2016', 'climate change', 'global warming potential (GWP100)'): 1,
        # uncertainty
        ('CML v4.8 2016', 'climate change', 'global warming potential (GWP100)'): {
            "amount": 1,
            "uncertainty type": 3,
        },
        # location
        ('CML v4.8 2016', 'climate change', 'global warming potential (GWP100)'):
            {"amount": 1,
             "location": "cat"}
        ,
        # checks in an index.
        "global_warming": 1,
    }
}


def read_from_activity(data: dict, metho_index: dict[str, tuple]) -> list[tuple]:
    """
    read characterization data from an activity
    :param data: activity data
    :return: list of characterization data
    """
    characterization = data.get("characterization", {})
    if not isinstance(characterization, dict):
        raise ValueError("Characterization data must be a dictionary")
    # iterate over characterization data
    for key, value in characterization.items():
        # for each key, if its a tuple use that, if its a string, check in some other dict
        if isinstance(key, tuple):
            # check if the tuple is valid
            if not all(isinstance(x, str) for x in key):
                raise ValueError("Characterization keys must be tuples of strings")
        # if string check in some other dict
        elif isinstance(key, str):
            # check if the string is valid
            if key not in metho_index:
                print(f"Warning: {key} is not a valid characterization key")
            # get the tuple from the dict
            key = metho_index[key]

        # check if the value is valid
        if isinstance(value, dict):
            pass
