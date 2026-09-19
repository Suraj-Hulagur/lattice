from pydantic import BaseModel
from enum import Enum

class NodeState(str, Enum):
    HEALTHY = "healthy"
    SUSPECTED = "suspected"
    FAILED = "failed"

class NodeInfo(BaseModel):
    id: str
    address: str
    state: NodeState
