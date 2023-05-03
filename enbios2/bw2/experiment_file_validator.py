import json
import logging
import sys
from copy import copy
from dataclasses import asdict
from typing import Optional, Union

import bw2data as bd
from bw2data.backends import Activity
from pint import UnitRegistry, Quantity
from pydantic.dataclasses import dataclass

logger = logging.Logger(__name__)
# add console.stream handler
logger.addHandler(logging.StreamHandler(sys.stdout))
logger.setLevel("INFO")


class Config:
    arbitrary_types_allowed = True


@dataclass
class ExperimentActivitiesGlobalConf:
    database: str


@dataclass
class ExperimentActivityId:
    database: Optional[str] = None
    code: Optional[str] = None
    # search and filter
    name: Optional[str] = None
    location: Optional[str] = None
    # additional filter
    unit: Optional[str] = None
    # internal-name
    alias: Optional[str] = None


@dataclass(config=Config)
class ExtendedExperimentActivity:
    id: ExperimentActivityId
    output: Optional[Union["ExperimentActivityOutput", "ExtendedExperimentActivityOutput"]] = None
    #
    orig_id: Optional[ExperimentActivityId] = None
    bw_activity: Optional[Activity] = None

    def fill_empty_fields(self):
        if not self.id.alias:
            self.id.alias = self.bw_activity["name"]
        if not self.id.code:
            self.id.code = self.bw_activity["code"]
        if not self.id.name:
            self.id.name = self.bw_activity["name"]
        if not self.id.location:
            self.id.location = self.bw_activity["location"]
        if not self.id.unit:
            self.id.unit = self.bw_activity["unit"]


@dataclass
class ExperimentActivityOutput:
    unit: str
    magnitude: float = 1.0


@dataclass(config=Config)
class ExtendedExperimentActivityOutput:
    unit: str
    magnitude: float = 1.0
    pint_quantity: Optional[Quantity] = None


@dataclass
class ExperimentActivity:
    id: ExperimentActivityId
    output: Optional[ExperimentActivityOutput] = None
    #
    orig_id: Optional[ExperimentActivityId] = None

    def check_exist(self, required_output: bool = False) -> ExtendedExperimentActivity:
        assert self.id.database in bd.databases, f"activity database does not exist: '{self.id.database}'"
        self.orig_id = copy(self.id)
        result = ExtendedExperimentActivity(asdict(self))
        if self.id.code:
            result.bw_activity = bd.Database(self.id.database).get(self.id.code)
        elif self.id.name:
            filters = {}
            if self.id.location:
                filters["location"] = self.id.location
                search_results = bd.Database(self.id.database).search(self.id.name, filter=filters)
            else:
                search_results = bd.Database(self.id.database).search(self.id.name)
            # print(len(search_results))
            # print(search_results)
            if self.id.unit:
                search_results = list(filter(lambda a: a["unit"] == self.id.unit, search_results))
            assert len(search_results) == 1, f"results : {len(search_results)}"
            result.bw_activity = search_results[0]

        if required_output:
            assert self.output is not None, f"Since there is no scenario, activity output is required: {self.orig_id}"
        return result


@dataclass
class BWMethod:
    description: str
    filename: str
    unit: str
    abbreviation: str
    num_cfs: int
    geocollections: list[str]


@dataclass
class ExperimentMethod:
    id: Union[list[str], tuple[str, ...]]
    alias: Optional[str] = None
    #
    bw_method: Optional[BWMethod] = None


@dataclass
class ExperimentHierarchyNode:
    children: Optional[
        Union[
            dict[str, "ExperimentHierarchyNode"],
            list[ExperimentActivityId]]]  # any activityId type


@dataclass
class ExperimentHierarchy:
    name: Optional[str]
    root: ExperimentHierarchyNode


@dataclass
class ExperimentScenario:
    activities: Union[
        list[
            tuple[ExperimentActivityId, ExperimentActivityOutput]],  # id to output
        dict[str, ExperimentActivityOutput]]  # alias to output

    methods: list[Union[str, ExperimentMethod]]
    alias: Optional[str] = None

    @staticmethod
    def alias_factory(index: int):
        return f"Scenario {index}"


@dataclass
class ExperimentData:
    bw_project: str
    activities: Union[list[ExperimentActivity], dict[str, Union[ExperimentActivitiesGlobalConf, ExperimentActivity]]]
    methods: Union[list[ExperimentMethod], dict[str, ExperimentMethod]]
    hierarchy: Optional[Union[ExperimentHierarchy, list[ExperimentHierarchy]]] = None
    scenarios: Optional[Union[list[ExperimentScenario], dict[str, ExperimentScenario]]] = None


class Experiment:
    ureg = UnitRegistry()

    def __init__(self, raw_data: ExperimentData):
        if raw_data.bw_project in bd.projects:
            bd.projects.set_current(raw_data.bw_project)
        self.raw_data = raw_data
        self.ureg = UnitRegistry()

        self.activitiesMap: dict[str, ExtendedExperimentActivity] = {}

        output_required = not raw_data.scenarios
        self.validate_activities(output_required)

        self.methods: dict[str, ExperimentMethod] = self.prepare_methods()
        self.validate_methods(self.methods)

        orig_activities_ids: list[tuple[ExperimentActivityId, ExtendedExperimentActivity]] = self.collect_orig_ids()

        self.validate_hierarchies(orig_activities_ids)
        self.validate_scenarios(list(self.activitiesMap.values()))

    def validate_activities(self, required_output: bool = False):
        # check if all activities exist
        activities = self.raw_data.activities
        # if activities is a list, convert validate and convert to dict
        if isinstance(activities, list):
            logger.debug("activity list")
            for activity in activities:
                ext_activity = activity.check_exist(required_output)
                assert activity.id.alias not in self.activitiesMap, f"Duplicate activity. {activity.id.alias} exists already. Try giving it a specific alias"
                self.activitiesMap[activity.id.alias] = ext_activity
        elif isinstance(activities, dict):
            logger.debug("activity dict")
            if "_all" in activities:
                conf: ExperimentActivitiesGlobalConf = activities["_all"]
                logger.debug(f"activity-configuration, {conf}")
                del activities["_all"]
                if conf.database:
                    for activity in activities.values():
                        activity.id.database = conf.database

            for activity in activities.values():
                ext_activity = activity.check_exist(required_output)
                self.activitiesMap[activity.id.alias] = ext_activity

        for activity in self.activitiesMap.values():
            activity.fill_empty_fields()

        # all codes should only appear once
        unique_activities = set([(a.id.database, a.id.code) for a in self.activitiesMap.values()])
        assert len(unique_activities) == len(activities), "Not all activities are unique"

        for activity in self.activitiesMap.values():
            activity: ExtendedExperimentActivity = activity
            if activity.output:
                Experiment.validate_output(activity.output, activity)

    def collect_orig_ids(self) -> list[tuple[ExperimentActivityId, ExtendedExperimentActivity]]:
        return [(activity.orig_id, activity) for activity in self.activitiesMap.values()]

    @staticmethod
    def validate_output(target_output: ExperimentActivityOutput, activity: ExtendedExperimentActivity):
        try:
            pint_target_unit = Experiment.ureg[target_output.unit]
            pint_target_quantity = target_output.magnitude * pint_target_unit
            #
            pint_activity_unit = Experiment.ureg[activity.id.unit]
            #
            target_output.pint_quantity = pint_target_quantity.to(pint_activity_unit)
        except Exception as err:
            raise Exception(f"Unit error, {err}; For activity: {activity.id}")

    def prepare_methods(self) -> dict[str, ExperimentMethod]:
        """
        give all methods some alias and turn them into a dict
        :return: map of alias -> method
        """
        if isinstance(self.raw_data.methods, dict):
            method_dict: dict[str, ExperimentMethod] = self.raw_data.methods
            for method_alias, method in method_dict.items():
                assert method.alias is None or method_alias == method.alias, f"Method: {method} must either have NO alias or the same as the key"
                method.alias = "_".join(method.id)
            return method_dict
        elif isinstance(self.raw_data.methods, list):
            method_list: list[ExperimentMethod] = self.raw_data.methods
            method_dict: dict[str, ExperimentMethod] = {}
            for method in method_list:
                if not method.alias:
                    method.alias = "_".join(method.id)
                method_dict[method.alias] = method
            return method_dict

    def validate_methods(self, methods: dict[str, ExperimentMethod]):
        # all methods must exist
        all_methods = bd.methods
        method_tree: dict[str, dict] = {}

        def build_method_tree():
            """make a tree search (tuple-part=level)"""
            if not method_tree:
                for bw_method in all_methods.keys():
                    # iter through tuple
                    current = method_tree
                    for part in bw_method:
                        current = current.setdefault(part, {})

        def tree_search(search_method_tuple: tuple[str]) -> dict:
            """search for a method in the tree. Is used when not all parts of a method are given"""
            build_method_tree()
            current = method_tree
            # print(method_tree)

            result = list(search_method_tuple)
            for index, part in enumerate(search_method_tuple):
                _next = current.get(part)
                assert _next, f"Method not found. Part: '{part}' does not exist for {list(search_method_tuple)[index - 1]}"
                current = _next

            while True:
                assert len(
                    current) <= 1, f"There is not unique method for '{result}', but options are '{current}'"
                if len(current) == 0:
                    break
                elif len(current) == 1:
                    _next = list(current.keys())[0]
                    result.append(_next)
                    current = current[_next]

            return all_methods.get(tuple(result))

        for alias, method in methods.items():
            method_tuple = tuple(method.id)
            bw_method = all_methods.get(method_tuple)
            if not bw_method and len(method_tuple) < 3:
                bw_method = tree_search(method_tuple)

            assert bw_method, f"Method with id: {method_tuple} does not exist"
            method.bw_method = BWMethod(**bw_method)

    def validate_hierarchies(self, orig_activities_ids: list[tuple[ExperimentActivityId, ExperimentActivity]]):

        def check_hierarchy(hierarchy: ExperimentHierarchy):
            def rec_find_leaf(node: ExperimentHierarchyNode) -> list[ExperimentActivityId]:
                # print(node)
                if isinstance(node.children, dict):
                    # merge all leafs of children
                    leafs = []
                    for child in node.children:
                        leafs.extend(rec_find_leaf(child))
                    return leafs
                elif isinstance(node.children, list):
                    return node.children

            leafs = rec_find_leaf(hierarchy.root)
            all_aliases = []
            for leaf in leafs:
                if isinstance(leaf, str):
                    assert leaf in self.activitiesMap, f"Hierarchy {hierarchy.name}, activity: {leaf} does not exist"
                    all_aliases.append(leaf)
                elif isinstance(leaf, dict):
                    # find the leaf dict in orig_activities_ids
                    for orig_id, activity in orig_activities_ids:
                        if orig_id == leaf:
                            all_aliases.append(activity.id.alias)
                            break
                    else:
                        assert False, f"Hierarchy {hierarchy.name}, activity: {leaf} does not exist"

        hierarchies = self.raw_data.hierarchy

        if not hierarchies:
            return

        if isinstance(hierarchies, list):
            # all activities must be in the hierarchy
            assert len(set(h.name for h in hierarchies)) == len(hierarchies), "Hierarchy names must be unique"
            for hierarchy in hierarchies:
                check_hierarchy(hierarchy)
        else:
            check_hierarchy(hierarchies)

    def validate_scenarios(self, defined_activities: list[ExtendedExperimentActivity]):
        """

        :param defined_activities:
        :return:
        """
        scenarios = self.raw_data.scenarios

        def validate_activities(scenario: ExperimentScenario):
            activities = scenario.activities
            # turn to alias dict
            if isinstance(activities, list):
                for activity in activities:
                    activity_id, activity_output = activity
                    for defined_activity in defined_activities:
                        if activity_id == defined_activity.orig_id:  # compare with id
                            activity_alias = defined_activity.id.alias  # assign alias
                            Experiment.validate_output(activity_output, defined_activity)
                pass  # todo

        def validate_scenario(scenario: ExperimentScenario, generate_name_index: Optional[int] = None):
            if generate_name_index:
                if not scenario.alias:
                    scenario.name = ExperimentScenario.alias_factory(generate_name_index)
            validate_activities(scenario)

        if isinstance(scenarios, list):
            scenarios: list[ExperimentScenario] = self.raw_data.scenarios
            for index, _scenario in enumerate(scenarios):
                validate_scenario(_scenario, index)
            pass
        elif isinstance(self.raw_data.scenarios, dict):
            scenarios: dict[str, ExperimentScenario] = self.raw_data.scenarios
            for alias, _scenario in scenarios.items():
                if _scenario.alias is not None and _scenario.alias != alias:
                    assert (False,
                            f"Scenario defines alias as dict-key: {alias} but also in the scenario object: {_scenario.alias}")
        else:
            assert False, "Scenario of wrong type. should be list or dict"
        # check activities
        # check outputs, units, unit match
        # check methods
        pass


if __name__ == "__main__":
    ed = ExperimentData(
        bw_project="ecoi_dbs",
        activities={
            "_all": ExperimentActivitiesGlobalConf(database="cutoff39"),
            "a": ExperimentActivity(
                id=ExperimentActivityId(
                    name="heat and power co-generation, wood chips, 6667 kW, state-of-the-art 2014",
                    location="DK",
                    unit='kilowatt hour'
                ),
                output=ExperimentActivityOutput("MWh", 30)
            )
        },
        methods=[ExperimentMethod(
            id=["Crustal Scarcity Indicator 2020", "material resources: metals/minerals"]
        )]
    )

    data = asdict(ed)

    # print(data)

    edR = ExperimentData(**data)
    Experiment(edR)

    e2 = Experiment(
        ExperimentData(
            bw_project="ecoi_dbs",
            activities=[ExperimentActivity(
                id=ExperimentActivityId(
                    name="heat and power co-generation, wood chips, 6667 kW, state-of-the-art 2014",
                    location="DK",
                    database="cutoff39",
                    unit='kilowatt hour'
                ),
                output=ExperimentActivityOutput(magnitude=1, unit="TWh")
            ),
            ],
            methods=[ExperimentMethod(
                id=('CML v4.8 2016 no LT', 'acidification no LT',
                    'acidification (incl. fate, average Europe total, A&B) no LT')
            ),
                ExperimentMethod(
                    id=('CML v4.8 2016 no LT', 'acidification no LT')
                )
            ],
            hierarchy=ExperimentHierarchy(**{
                "name": "h1",
                "root": ExperimentHierarchyNode(**{
                    "children": [
                        ExperimentActivityId(
                            **{"name": "heat and power co-generation, wood chips, 6667 kW, state-of-the-art 2014",
                               "location": "DK",
                               "database": "cutoff39",
                               "unit": 'kilowatt hour'})
                    ]
                })
            })
        ))

    # e3 = Experiment(
    #     ExperimentData(
    #         bw_project="ecoi_dbs",
    #         activities=[ExperimentActivity(
    #             id=ExperimentActivityId(
    #                 database="cutoff39",
    #                 code="4c1387042abc649885596438a1178036"
    #             )
    #         )],
    #         methods=[ExperimentMethod(
    #             id=["s", "s"]
    #         )]
    #     ))

    # print(e2.raw_data.activities[0])

print(json.dumps(
    ExperimentData.__pydantic_model__.schema()
))
