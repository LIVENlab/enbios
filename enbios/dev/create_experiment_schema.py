import json
from pathlib import Path

from enbios.generic.enbios2_logging import get_logger, get_module_name
from enbios.models.experiment_base_models import ExperimentData

logger = get_logger(get_module_name(__file__))


def create_experiment_schema():
    schema_file_path = Path(__file__).parent.parent.parent / "data/schema/experiment.schema.gen.json"
    schema_file_path.write_text(
        json.dumps(ExperimentData.model_json_schema(), indent=2, ensure_ascii=False),
        encoding="utf-8")
    logger.info(f"Created schema file {schema_file_path}")


if __name__ == "__main__":
    create_experiment_schema()
