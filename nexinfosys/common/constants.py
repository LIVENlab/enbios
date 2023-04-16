from enum import Enum

from typing import List


class BasicEnum(Enum):
    @classmethod
    def get_names(cls) -> List[str]:
        return list(cls.__members__.keys())


class Scope(BasicEnum):
    Total = 1
    Internal = 2
    External = 3


class SubsystemType(BasicEnum):
    Local = (1, Scope.Internal)
    Environment = (2, Scope.Internal)
    External = (3, Scope.External)
    ExternalEnvironment = (4, Scope.External)

    def is_same_scope(self, other: 'SubsystemType') -> bool:
        return self.value[1] == other.value[1]

    def is_internal_scope(self) -> bool:
        return self.value[1] == Scope.Internal

    def is_external_scope(self) -> bool:
        return self.value[1] == Scope.External

    @staticmethod
    def from_str(s: str) -> 'SubsystemType':
        if s.lower() == 'local':
            return SubsystemType.Local
        elif s.lower() == 'environment':
            return SubsystemType.Environment
        elif s.lower() == 'external':
            return SubsystemType.External
        elif s.lower() == 'externalenvironment':
            return SubsystemType.ExternalEnvironment
        else:
            raise NotImplementedError

