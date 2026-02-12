from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass
from typing import Dict, List, Optional


@dataclass(frozen=True)
class WALEntry:
    txn_id: str
    key: str
    old_value: Optional[int]
    new_value: int


class Database:
    """Simple in-memory key-value store with a write-ahead log."""

    def __init__(self, initial_accounts: Optional[Dict[str, int]] = None) -> None:
        self.accounts: Dict[str, int] = dict(initial_accounts or {})
        self.wal: List[WALEntry] = []

    def read(self, key: str) -> Optional[int]:
        return self.accounts.get(key)

    def write(
        self,
        key: str,
        value: int,
        txn_id: Optional[str] = None,
        log_wal: bool = False,
    ) -> WALEntry:
        old_value = self.accounts.get(key)
        self.accounts[key] = value
        entry = WALEntry(txn_id=txn_id or "SYSTEM", key=key, old_value=old_value, new_value=value)
        if log_wal:
            self.wal.append(entry)
        return entry

    def apply_wal_entry(self, entry: WALEntry, log_wal: bool = True) -> None:
        self.accounts[entry.key] = entry.new_value
        if log_wal:
            self.wal.append(entry)

    def append_wal(self, entry: WALEntry) -> None:
        self.wal.append(entry)

    def snapshot(self) -> Dict[str, int]:
        return deepcopy(self.accounts)
