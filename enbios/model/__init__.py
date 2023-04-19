from enbios.const import TECHNOLOGY, SUBTECHNOLOGY, REGION, CARRIER, SCENARIO, SUBSCENARIO, TIME

g_default_subtech = "_"  # Constant for default subtech


class SimStructuralProcessorAttributes:
    """
    A class to store Processor attributes while a Sentinel datapackage is read
    and values are stored in a PartialRetrievalDictionary
    Attributes can be stored directly
    """
    def __init__(self, technology=None, region=None, carrier=None, scenario=None, time_=None,
                 subtechnology=None, subscenario=None):
        self.attrs = {}
        if technology:
            self.attrs[TECHNOLOGY] = technology
        if subtechnology:
            self.attrs[SUBTECHNOLOGY] = subtechnology
        if region:
            self.attrs[REGION] = region
        if carrier:
            self.attrs[CARRIER] = carrier
        if scenario:
            self.attrs[SCENARIO] = scenario
        if subscenario:
            self.attrs[SUBSCENARIO] = subscenario
        if time_:
            self.attrs[TIME] = time_

    @staticmethod
    def partial_key(technology=None, region=None, carrier=None, scenario=None, time=None,
                    subtechnology=None, subscenario=None):
        d = {}
        if technology:
            d["_t"] = technology
        if subtechnology:
            d["_st"] = subtechnology
        if region:
            d["_g"] = region
        if carrier:
            d["_c"] = carrier
        if scenario:
            d["_s"] = scenario
        if subscenario:
            d["_ss"] = subscenario
        if time:
            d["_d"] = time
        return d

    def key(self):
        return self.partial_key(self.attrs.get(TECHNOLOGY),
                                self.attrs.get(REGION),
                                self.attrs.get(CARRIER),
                                self.attrs.get(SCENARIO),
                                self.attrs.get(TIME),
                                self.attrs.get(SUBTECHNOLOGY),
                                self.attrs.get(SUBSCENARIO))

