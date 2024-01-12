import sys

from pydantic import ValidationError

from enbios.models.experiment_base_models import (
    ExperimentData,
    ExperimentHierarchyNodeData,
    ExperimentScenarioData,
)


def validate_experiment_data(data: dict) -> ExperimentData:
    try:
        return ExperimentData(**data)
    except ValidationError as err:
        print(err)

        try:
            ExperimentHierarchyNodeData(**data.get("hierarchy"))
            print("\nHierarchy is valid\n")
        except ValidationError as err:
            print(err)
            print(f"!!!\n!!!\nCheck: 'hierarchy': {err.errors()}\n\n")

        if scenarios := data.get("scenarios"):
            for idx, scenario in enumerate(scenarios):
                try:
                    sc = ExperimentScenarioData(**scenario)
                    print(f"Scenario {idx} ({sc.name}) is valid")
                except ValidationError as err:
                    print(err)
                    print(f"!!!\n!!!\nCheck: 'scenarios[{idx}]': {err.errors()}\n\n")

        # errors_sorted = sorted(err.errors(), key=lambda x: len(x["loc"]), reverse=True)[0]
        # print(f"!!!\n!!!\nCheck: '{errors_sorted['type']}': {errors_sorted['loc']}\n\n")
        sys.exit(1)
