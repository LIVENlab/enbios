from dataclasses import field
from typing import Optional, Any

from pydantic import BaseModel, Field, model_validator, field_validator, ConfigDict
from pydantic_core.core_schema import ValidationInfo

from enbios.const import DEFAULT_SUM_AGGREGATOR
from enbios.models.experiment_base_models import StrictInputConfig


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


class EnbiosQuantity(BaseModel):
    model_config = StrictInputConfig
    unit: str
    amount: float

    @model_validator(mode="before")
    @classmethod
    def check_format(cls, data: Any) -> Any:
        if isinstance(data, tuple):
            return {"unit": data[0], "amount": data[1]}
        return data


class ResultValue(EnbiosQuantity):
    model_config = StrictInputConfig
    model_config = ConfigDict(extra='forbid')
    amount: Optional[float] = None
    multi_amount: Optional[list[float]] = field(default_factory=list)


class ScenarioResultNodeData(BaseModel):
    model_config = StrictInputConfig
    output: Optional[EnbiosQuantity] = None
    results: dict[str, ResultValue] = field(default_factory=dict)
    adapter: Optional[str] = None
    aggregator: Optional[str] = None


Activity_Outputs = dict[str, float]
