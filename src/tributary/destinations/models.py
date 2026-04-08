from dataclasses import dataclass

@dataclass
class Destination:
    name: str
    type: str
    config: dict