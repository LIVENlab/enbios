from dataclasses import asdict
from logging import getLogger
from typing import Optional

from bw2data.backends import Activity
import bw2data as bd
from pint import DimensionalityError, Quantity, UndefinedUnitError
from pydantic import ValidationError

from enbios.base.unit_registry import ureg
from enbios.bw2.util import bw_unit_fix, get_activity
from enbios.models.experiment_models import (
    ActivityOutput,
    ExperimentActivityData,
    ExperimentActivityId,
    ExtendedExperimentActivityData,
)

logger = getLogger(__file__)


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


def validate_brightway_output(
    target_output: ActivityOutput,
    bw_activity: Activity,
    activity_id: ExperimentActivityId,
) -> float:
    """
    validate and convert to the bw-activity unit
    :param target_output:
    :param bw_activity:
    :param activity_id:
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


def validate_brightway_activity(
    activity: ExperimentActivityData,
    default_id_attr: ExperimentActivityId,
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
    default_output_value = validate_brightway_output(output, bw_activity, activity.id)

    activity_dict = asdict(activity)
    activity_dict["output"] = asdict(output)
    result: ExtendedExperimentActivityData = ExtendedExperimentActivityData(
        **activity_dict,
        bw_activity=bw_activity,
        default_output_value=default_output_value,
    )
    result.id.fill_empty_fields(["alias"], **asdict(default_id_attr))

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
