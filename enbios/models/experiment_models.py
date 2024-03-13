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
    def check_model(cls, data: Any) -> Any:
        if isinstance(data, dict):
            leaf_node = "adapter" in data and "config" in data
            non_leaf_node = "aggregator" in data
            assert leaf_node or non_leaf_node, (
                "Node must be either leaf ('id', 'adapter`) " "or non-leaf ('aggregator')"
            )
        return data


class ResultValue(BaseModel):
    model_config = StrictInputConfig
    unit: str
    magnitude: Optional[float] = None  # type: ignore
    multi_magnitude: Optional[list[float]] = field(default_factory=list)


class ScenarioResultNodeData(BaseModel):
    model_config = StrictInputConfig
    output: list[NodeOutput] = Field(default_factory=list)
    results: dict[str, ResultValue] = Field(default_factory=dict)
    adapter: Optional[str] = None
    aggregator: Optional[str] = None
