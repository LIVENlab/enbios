from dataclasses import asdict, dataclass
from logging import getLogger
from typing import Optional

import bw2data as bd
from bw2data.backends import Activity
from pint import DimensionalityError, Quantity, UndefinedUnitError

from enbios import get_enbios_ureg
from enbios.bw2.util import bw_unit_fix, get_activity
from enbios.models.experiment_models import (
    ActivityOutput,
    ExperimentActivityData,
    ExperimentActivityId,
    ExtendedExperimentActivityData, ExperimentMethodData, ExperimentMethodPrepData,
    MethodsDataTypes, MethodsDataTypesExt,
)

logger = getLogger(__file__)

ureg = get_enbios_ureg()


@dataclass
class BWAdapterConfig:
    bw_project: str
    methods: MethodsDataTypesExt
    use_k_bw_distributions: Optional[int] = 1  # number of samples to use for monteCarlo
    bw_default_database: Optional[str] = None


def _bw_activity_search(activity: ExperimentActivityData) -> Activity:
    """
    Search for the activity in the brightway project
    :param activity:
    :return: brightway activity
    """
    id_ = activity.id
    bw_activity: Optional[Activity] = None
    if id_.code:
        if id_.database:
            bw_activity = bd.Database(id_.database).get_node(id_.code)
        else:
            bw_activity = get_activity(id_.code)
    elif id_.name:
        filters = {}
        search_in_dbs = [id_.database] if id_.database else bd.databases
        for db in search_in_dbs:
            if id_.location:
                filters["location"] = id_.location
                search_results = bd.Database(db).search(id_.name, filter=filters)
            else:
                search_results = bd.Database(db).search(id_.name)
            if id_.unit:
                search_results = list(
                    filter(lambda a: a["unit"] == id_.unit, search_results)
                )
            if len(search_results) == 1:
                bw_activity = search_results[0]
                break
            elif len(search_results) > 1:
                activities_str = "\n".join(
                    [f'{str(a)} - {a["code"]}' for a in search_results]
                )
                raise ValueError(
                    f"There are more than one activity with the same name, "
                    f"try including  "
                    f"the code of the activity you want to use:\n{activities_str}"
                )
    if not bw_activity:
        raise ValueError(f"No activity found for {activity.id}")
    return bw_activity


def validate_config(config: BWAdapterConfig):
    if config.use_k_bw_distributions < 1:
        raise ValueError(
            f"config.use_k_bw_distributions must be greater than 0, "
            f"but is {config.use_k_bw_distributions}"
        )

    def validate_bw_project_bw_database(
            bw_project: str, bw_default_database: Optional[str] = None
    ):
        if bw_project not in bd.projects:
            raise ValueError(f"Project {bw_project} not found")

        if bw_project in bd.projects:
            bd.projects.set_current(bw_project)

        if bw_default_database:
            if bw_default_database not in bd.databases:
                raise ValueError(
                    f"Database {bw_default_database} "
                    f"not found. Options are: {list(bd.databases)}"
                )

    # print("validate_bw_config***************", self.raw_data.bw_project)
    # if isinstance(config.bw_project, str):
    validate_bw_project_bw_database(
        config.bw_project, config.bw_default_database
    )


def validate_methods(config: BWAdapterConfig):
    def validate_method(
            method: ExperimentMethodData, alias: str
    ) -> ExperimentMethodPrepData:
        method.id = tuple(method.id)
        bw_method = bd.methods.get(method.id)
        if not bw_method:
            raise Exception(f"Method with id: {method.id} does not exist")
        if method.alias:
            if method.alias != alias:
                raise Exception(
                    f"Method alias: {method.alias} does not match with "
                    f"the given alias: {alias}"
                )
        else:
            method.alias = alias
        return ExperimentMethodPrepData(
            id=method.id, alias=method.alias, bw_method_unit=bw_method["unit"]
        )

    def prepare_methods(methods: MethodsDataTypes) -> dict[str, ExperimentMethodData]:
        # if not methods:
        #     methods = self.raw_data.methods
        method_dict: dict[str, ExperimentMethodData] = {}
        if isinstance(methods, dict):
            for method_alias, method in methods.items():
                method_dict[method_alias] = ExperimentMethodData(method, method_alias)
        elif isinstance(methods, list):
            method_list: list[ExperimentMethodData] = [ExperimentMethodData(**m) for m in methods]
            for method_ in method_list:
                alias = method_.alias if method_.alias else "_".join(method_.id)
                method__ = ExperimentMethodData(method_.id, alias)
                method_dict[method__.alias_] = method__
        return method_dict

    methods: dict[str, ExperimentMethodPrepData] = {
        alias: validate_method(method, alias)
        for alias, method in prepare_methods(config.methods).items()
    }
    return methods


def validate_activity_output(
        activity: ExtendedExperimentActivityData,
        target_output: ActivityOutput,
        # bw_activity: Activity,
        # activity_id: ExperimentActivityId,
) -> float:
    """
    validate and convert to the bw-activity unit
    :param activity:
    :param target_output:
    :return:
    """
    try:
        target_quantity: Quantity = (
                ureg.parse_expression(bw_unit_fix(target_output.unit), case_sensitive=False)
                * target_output.magnitude
        )
        bw_activity_unit = bw_activity["unit"]
        return target_quantity.to(bw_unit_fix(bw_activity_unit)).magnitude
    except UndefinedUnitError as err:
        logger.error(
            f"Cannot parse output unit '{target_output.unit}'- "
            f"of activity {activity_id}. {err}. "
            f"Consider the unit definition to 'enbios2/base/unit_registry.py'"
        )
        raise Exception(f"Unit error, {err}; For activity: {activity_id}")
    except DimensionalityError as err:
        logger.error(
            f"Cannot convert output of activity {activity_id}. -"
            f"From- \n{target_output}\n-To-"
            f"\n{bw_activity['unit']} (brightway unit)"
            f"\n{err}"
        )
        raise Exception(f"Unit error for activity: {activity_id}")


def validate_activity(
        activity: ExperimentActivityData,
        # default_id_attr: ExperimentActivityId,
        required_output: bool = False,
) -> "ExtendedExperimentActivityData":
    # get the brightway activity
    bw_activity = _bw_activity_search(activity)

    # create output: ActivityOutput and default_output_value
    if activity.output:
        if isinstance(activity.output, tuple):
            output = ActivityOutput(unit=activity.output[0], magnitude=activity.output[1])
        else:  # if isinstance(activity.output, ActivityOutput):
            output = activity.output

    else:
        output = ActivityOutput(unit=bw_unit_fix(bw_activity["unit"]), magnitude=1.0)
    default_output_value = validate_activity_output(output, bw_activity, activity.id)

    activity_dict = asdict(activity)
    activity_dict["output"] = asdict(output)
    result: ExtendedExperimentActivityData = ExtendedExperimentActivityData(
        **activity_dict,
        bw_activity=bw_activity,
        default_output_value=default_output_value,
    )

    result.id.fill_empty_fields(
        ["name", "code", "location", "unit", ("alias", "name")],
        **result.bw_activity.as_dict(),
    )
    if required_output:
        assert activity.output is not None, (
            f"Since there is no scenario, activity output is required: "
            f"{activity.orig_id}"
        )
    return result



def run(scenario: str) -> dict:
    return {}
