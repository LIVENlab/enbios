from dataclasses import dataclass
from typing import Optional


@dataclass
class LCISpoldDefinition:
    Processor: str
    EcoinventFilename: str
    EcoinventCarrierName: Optional[str]

    def __post_init__(self):
        # when Filename does not end with .spold, add it
        if not self.EcoinventFilename.endswith(".spold"):
            self.EcoinventFilename += ".spold"