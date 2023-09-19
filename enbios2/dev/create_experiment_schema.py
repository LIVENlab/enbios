from enbios2.const import BASE_SCHEMA_PATH
from enbios2.generic.enbios2_logging import get_logger, get_module_name
from enbios2.models.experiment_models import ExperimentData
from pydantic import TypeAdapter
import json

logger = get_logger(get_module_name(__file__))


def create_experiment_schema():
    schema_file_path = BASE_SCHEMA_PATH / "experiment.schema.gen.json"
    schema_file_path.write_text(
        json.dumps(TypeAdapter(ExperimentData).json_schema(),indent=2, ensure_ascii=False),
        encoding="utf-8")
    logger.info(f"Created schema file {schema_file_path}")


if __name__ == "__main__":
    create_experiment_schema()
