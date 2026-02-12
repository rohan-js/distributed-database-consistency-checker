from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional


@dataclass
class Operation:
    op_type: str
    txn_id: Optional[str] = None
    key: Optional[str] = None
    value: Optional[Any] = None


@dataclass
class Transaction:
    txn_id: str
    status: str = "ACTIVE"
    operations: List[Operation] = field(default_factory=list)
    read_set: Dict[str, Any] = field(default_factory=dict)
    write_set: Dict[str, int] = field(default_factory=dict)
    direct_old_values: Dict[str, Optional[int]] = field(default_factory=dict)

    def begin(self) -> None:
        self.status = "ACTIVE"

    def commit(self) -> None:
        self.status = "COMMITTED"

    def rollback(self) -> None:
        self.status = "ABORTED"

    def add_operation(self, operation: Operation) -> None:
        self.operations.append(operation)

    def record_read(self, key: str, value: Any) -> None:
        self.read_set[key] = value

    def record_write(self, key: str, value: int) -> None:
        self.write_set[key] = value
