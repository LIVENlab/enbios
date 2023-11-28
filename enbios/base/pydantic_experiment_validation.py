import sys

from pydantic import ValidationError

from enbios.models.experiment_models import ExperimentData


def validate_experiment_data(data: dict) -> ExperimentData:
    try:
        return ExperimentData(**data)
    except ValidationError as err:
        print(err)
        errors_sorted = sorted(err.errors(), key=lambda x: len(x["loc"]), reverse=True)[0]
        print(f"!!!\n!!!\nCheck: '{errors_sorted['type']}': {errors_sorted['loc']}\n\n")
        sys.exit()
