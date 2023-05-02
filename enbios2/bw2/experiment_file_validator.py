import logging
import sys
from copy import copy
from dataclasses import dataclass
from typing import Optional, Union

import bw2data as bd
from bw2data.backends import Activity
from pint import UnitRegistry, Quantity

logger = logging.Logger(__name__)
# add console.stream handler
logger.addHandler(logging.StreamHandler(sys.stdout))


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


@dataclass
class ExperimentActivityOutput:
    unit: str
    magnitude: float = 1.0
    #
    pint_quantity: Optional[Quantity] = None


@dataclass
class ExperimentActivity:
    id: ExperimentActivityId
    output: Optional[ExperimentActivityOutput] = None
    #
    orig_id: Optional[ExperimentActivityId] = None
    bw_activity: Optional[Activity] = None

    def check_exist(self, required_output: bool = False) -> Activity:
        assert self.id.database in bd.databases, f"activity database does not exist: '{self.id.database}'"
        self.orig_id = copy(self.id)
        if self.id.code:
            self.bw_activity = bd.Database(self.id.database).get(self.id.code)
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
                search_results = list(filter(lambda a: a._data["unit"] == self.id.unit, search_results))
            assert len(search_results) == 1, f"results : {len(search_results)}"
            self.bw_activity = search_results[0]

        if required_output:
            assert self.output is not None, f"Since there is no scenario, activity output is required: {self.orig_id}"
        return self.bw_activity

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
    bw_method = None


@dataclass
class ExperimentHierarchy:
    name: Optional[str]
    root: "ExperimentHierarchyNode"


@dataclass
class ExperimentHierarchyNode:
    children: Optional[
        Union[
            dict["ExperimentHierarchyNode"],  # use activity alias
            list[ExperimentActivityId]]]  # any activityId type


@dataclass
class Scenario:
    activities: Union[
        list[Union[tuple[
            ExperimentActivityId, ExperimentActivityOutput], ExperimentActivity]],  # id to output (# new activities allowed)
        dict[str, ExperimentActivityOutput]]  # alias to output

    methods: list[Union[str, ExperimentMethod]]


@dataclass
class ExperimentData:
    bw_project: str
    activities: Union[list[ExperimentActivity], dict[str, Union[ExperimentActivitiesGlobalConf, ExperimentActivity]]]
    methods: Union[list[ExperimentMethod], dict[str, ExperimentMethod]]
    hierarchy: Optional[Union[ExperimentHierarchy, list[ExperimentHierarchy]]] = None
    scenarios: Optional[Union[list[Scenario], dict[Scenario]]] = None


class Experiment:

    def __init__(self, raw_data: ExperimentData):
        if raw_data.bw_project in bd.projects:
            bd.projects.set_current(raw_data.bw_project)
        self.raw_data = raw_data
        self.ureg = UnitRegistry()

        self.activitiesMap: dict[str, ExperimentActivity] = {}
        output_required = not raw_data.scenarios
        self.validate_activities(output_required)

        self.methods: dict[str, ExperimentMethod] = self.prepare_methods()
        self.validate_methods(self.methods)

        self.validate_hierarchies()

    def validate_activities(self, required_output: bool = False):
        # check if all activities exist
        activities = self.raw_data.activities
        if isinstance(activities, list):
            logger.debug("activity list")
            for activity in activities:
                activity.check_exist(required_output)
                assert activity.id.alias not in self.activitiesMap, f"Duplicate activity. {activity.id.alias} exists already. Try giving it a specific alias"
                self.activitiesMap[activity.id.alias] = activity
        elif isinstance(activities, dict):
            logger.debug("activity dict")
            if "_all" in activities:
                conf = ExperimentActivitiesGlobalConf(**activities["_all"])
                logger.debug(f"activity-configuration, {conf}")
                del activities["_all"]
                if conf.database:
                    for activity in activities.values():
                        activity.id.database = conf.database
            self.activitiesMap = activities

            for activity in activities.values():
                activity.check_exist(required_output)

        for activity in self.activitiesMap.values():
            activity.fill_empty_fields()

        # all codes should only appear once
        unique_activities = set([(a.id.database, a.id.code) for a in activities])
        assert len(unique_activities) == len(activities), "Not all activities are unique"

        for activity in self.activitiesMap.values():
            activity: ExperimentActivity = activity
            if activity.output:
                self.validate_output(activity.output, activity)

    def validate_output(self, target_output: ExperimentActivityOutput, activity: ExperimentActivity):
        try:
            pint_target_unit = self.ureg[target_output.unit]
            pint_target_quantity = target_output.magnitude * pint_target_unit
            #
            pint_activity_unit = self.ureg[activity.id.unit]
            #
            target_output.pint_quantity = pint_target_quantity.to(pint_activity_unit)
        except Exception as err:
            raise Exception(f"Unit error, {err}; For activity: {activity.id}")

    def prepare_methods(self) -> dict[str, ExperimentMethod]:
        """
        give all methods their some alias
        :return:
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
            if not method_tree:
                for bw_method in all_methods.keys():
                    # iter through tuple
                    current = method_tree
                    for part in bw_method:
                        current = current.setdefault(part, {})

        def tree_search(search_method_tuple: tuple[str]) -> dict:
            build_method_tree()
            current = method_tree
            # print(method_tree)

            result = list(search_method_tuple)
            for index, part in enumerate(search_method_tuple):
                next = current.get(part)
                assert next, f"Method not found. Part: '{part}' does not exist for {list(search_method_tuple)[index - 1]}"
                current = next

            while True:
                assert len(
                    current) <= 1, f"There is not unique method for '{result}', but options are '{current}'"
                if len(current) == 0:
                    break
                elif len(current) == 1:
                    next = list(current.keys())[0]
                    result.append(next)
                    current = current[next]

            return all_methods.get(tuple(result))

        for alias, method in methods.items():
            method_tuple = tuple(method.id)
            bw_method = all_methods.get(method_tuple)
            if not bw_method and len(method_tuple) < 3:
                bw_method = tree_search(method_tuple)

            assert bw_method, f"Method with id: {method_tuple} does not exist"
            method.bw_method = BWMethod(**bw_method)

    def validate_hierarchies(self):
        hierarchies = self.raw_data.hierarchy
        # check on the last level
        # all activities must be in the hierarchy
        pass

    def validate_scenarios(self):
        # check activities
        # check outputs, units, unit match
        # check methods
        pass


if __name__ == "__main__":
    # Experiment(
    #     ExperimentData(
    #         bw_project="ecoi_dbs",
    #         activities={
    #             "_all": {
    #                 "database": "cutoff39"
    #             },
    #             "a": ExperimentActivity(
    #                 id=ExperimentActivityId(
    #                     name="heat and power co-generation, wood chips, 6667 kW, state-of-the-art 2014",
    #                     location="DK",
    #                     unit='kilowatt hour'
    #                 )
    #             )
    #         },
    #         methods=[ExperimentMethod(
    #             id=["s", "s"]
    #         )]
    #     ))

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
            ]
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
