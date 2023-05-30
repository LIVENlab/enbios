from enbios2.const import BASE_SCHEMA_PATH
from enbios2.generic.enbios2_logging import get_logger

logger = get_logger(__file__)

def run_all():
    BASE_SCHEMA_PATH.mkdir(parents=True, exist_ok=True)
    from enbios2.dev.create_experiment_schema import create_experiment_schema
    from enbios2.dev.create_tech_tree_node_schema import create_tech_tree_node_schema

    create_experiment_schema()
    logger.warning("Tech tree schema not generated")
    # create_tech_tree_node_schema()


if __name__ == "__main__":
    run_all()