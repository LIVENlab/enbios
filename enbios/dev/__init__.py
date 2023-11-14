from enbios.const import BASE_SCHEMA_PATH
from enbios.generic.enbios2_logging import get_logger

logger = get_logger(__name__)

def run_all():
    BASE_SCHEMA_PATH.mkdir(parents=True, exist_ok=True)
    from enbios.dev.create_experiment_schema import create_experiment_schema

    create_experiment_schema()
    logger.warning("Tech tree schema not generated")
    # create_tech_tree_node_schema()


if __name__ == "__main__":
    run_all()
