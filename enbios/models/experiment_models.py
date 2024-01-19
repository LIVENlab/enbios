from dataclasses import field
from typing import Optional, Any

from pydantic import BaseModel, Field, model_validator

from enbios.models.experiment_base_models import StrictInputConfig, NodeOutput


class TechTreeNodeData(BaseModel):
    adapter: Optional[str] = None
    aggregator: Optional[str] = None
    # for
    config: Optional[Any] = Field(
        None, description="The identifies (method to find) a node"
    )

    @model_validator(mode="before")
    @classmethod
    def check_model(cls, data: Any) -> Any:
        if isinstance(data, dict):
            leaf_node = "adapter" in data and "config" in data
            non_leaf_node = "aggregator" in data
            assert leaf_node or non_leaf_node, (
                "Node must be either leaf ('id', 'adapter`) " "or non-leaf ('aggregator')"
            )
        return data


class ResultValue(NodeOutput):
    model_config = StrictInputConfig
    magnitude: Optional[float] = None
    multi_magnitude: Optional[list[float]] = field(default_factory=list)


class ScenarioResultNodeData(BaseModel):
    model_config = StrictInputConfig
    output: Optional[NodeOutput] = None
    results: dict[str, ResultValue] = field(default_factory=dict)
    adapter: Optional[str] = None
    aggregator: Optional[str] = None
