from dataclasses import field
from typing import Optional, Any

from pydantic import BaseModel, Field, model_validator, field_validator
from pydantic_core.core_schema import ValidationInfo

from enbios.const import DEFAULT_SUM_AGGREGATOR
from enbios.models.experiment_base_models import StrictInputConfig, ActivityOutput


class TechTreeNodeData(BaseModel):
    adapter: Optional[str] = None
    aggregator: Optional[str] = None
    # for
    config: Optional[Any] = Field(
        None, description="The identifies (method to find) an activity"
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

    @field_validator("aggregator", mode="before")
    @classmethod
    def add_default_aggregator(cls, v: str, values: ValidationInfo) -> str:
        if not values.data["adapter"]:
            return DEFAULT_SUM_AGGREGATOR
        return v


class ResultValue(ActivityOutput):
    model_config = StrictInputConfig
    magnitude: Optional[float] = None
    multi_magnitude: Optional[list[float]] = field(default_factory=list)


class ScenarioResultNodeData(BaseModel):
    model_config = StrictInputConfig
    output: Optional[ActivityOutput] = None
    results: dict[str, ResultValue] = field(default_factory=dict)
    adapter: Optional[str] = None
    aggregator: Optional[str] = None


Activity_Outputs = dict[str, float]
