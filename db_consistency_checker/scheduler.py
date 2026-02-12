from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from .database import Database, WALEntry
from .transaction import Operation, Transaction


@dataclass
class ExecutionRecord:
    order: int
    txn_id: str
    op_type: str
    key: Optional[str] = None
    value: Optional[Any] = None
    read_from_txn: Optional[str] = None
    read_from_uncommitted: bool = False


class Scheduler:
    """Deterministic interleaving scheduler that executes operations in file order."""

    def __init__(
        self,
        operations: List[Operation],
        database: Database,
        mode: str = "direct",
        replica_manager: Optional[Any] = None,
    ) -> None:
        normalized_mode = mode.lower()
        if normalized_mode not in {"direct", "buffered"}:
            raise ValueError("mode must be 'direct' or 'buffered'")

        self.operations = operations
        self.database = database
        self.mode = normalized_mode
        self.replica_manager = replica_manager
        self.transactions: Dict[str, Transaction] = {}
        self.execution_history: List[ExecutionRecord] = []

        # Tracks the latest uncommitted writer for each key in direct mode.
        self.uncommitted_writers: Dict[str, str] = {}

    def run(self) -> List[ExecutionRecord]:
        order = 1
        for op in self.operations:
            op_type = op.op_type.upper()

            if op_type in {"DELAY", "PARTITION", "RECOVER", "DELIVER", "WAIT", "SYNC"}:
                self._handle_network_operation(op)
                continue

            if not op.txn_id:
                raise ValueError(f"transaction id is required for operation: {op_type}")

            txn = self.transactions.get(op.txn_id)
            if op_type == "BEGIN":
                txn = Transaction(op.txn_id)
                txn.begin()
                self.transactions[op.txn_id] = txn
                self.execution_history.append(
                    ExecutionRecord(order=order, txn_id=op.txn_id, op_type="BEGIN")
                )
                order += 1
                continue

            if txn is None:
                raise ValueError(f"transaction {op.txn_id} used before BEGIN")

            txn.add_operation(op)

            if op_type == "READ":
                value, source_txn, is_dirty = self._execute_read(txn, op.key)
                self.execution_history.append(
                    ExecutionRecord(
                        order=order,
                        txn_id=txn.txn_id,
                        op_type="READ",
                        key=op.key,
                        value=value,
                        read_from_txn=source_txn,
                        read_from_uncommitted=is_dirty,
                    )
                )
            elif op_type == "RANGE_READ":
                range_value = self._execute_range_read(txn, str(op.key))
                self.execution_history.append(
                    ExecutionRecord(
                        order=order,
                        txn_id=txn.txn_id,
                        op_type="RANGE_READ",
                        key=op.key,
                        value=range_value,
                    )
                )
            elif op_type == "WRITE":
                if op.key is None or op.value is None:
                    raise ValueError("WRITE requires key and value")
                self._execute_write(txn, op.key, int(op.value))
                self.execution_history.append(
                    ExecutionRecord(
                        order=order,
                        txn_id=txn.txn_id,
                        op_type="WRITE",
                        key=op.key,
                        value=int(op.value),
                    )
                )
            elif op_type == "COMMIT":
                self._commit(txn)
                self.execution_history.append(
                    ExecutionRecord(order=order, txn_id=txn.txn_id, op_type="COMMIT")
                )
            elif op_type == "ROLLBACK":
                self._rollback(txn)
                self.execution_history.append(
                    ExecutionRecord(order=order, txn_id=txn.txn_id, op_type="ROLLBACK")
                )
            else:
                raise ValueError(f"unsupported operation: {op_type}")

            order += 1

        return self.execution_history

    def _execute_read(self, txn: Transaction, key: Optional[str]) -> tuple[Optional[int], Optional[str], bool]:
        if key is None:
            raise ValueError("READ requires key")

        source_txn: Optional[str] = None
        dirty = False

        if key in txn.write_set:
            value = txn.write_set[key]
            source_txn = txn.txn_id
        else:
            value = self.database.read(key)
            if self.mode == "direct":
                writer = self.uncommitted_writers.get(key)
                if writer and writer != txn.txn_id:
                    source_txn = writer
                    dirty = True

        txn.record_read(key, value)
        return value, source_txn, dirty

    def _execute_range_read(self, txn: Transaction, range_expr: str) -> List[str]:
        # Supported form: PREFIX* (e.g. A*)
        prefix = range_expr.rstrip("*")
        keys = sorted(k for k in self.database.accounts if k.startswith(prefix))
        txn.record_read(range_expr, keys)
        return keys

    def _execute_write(self, txn: Transaction, key: str, value: int) -> None:
        txn.record_write(key, value)

        if self.mode == "direct":
            if key not in txn.direct_old_values:
                txn.direct_old_values[key] = self.database.read(key)
            self.database.write(key, value, txn_id=txn.txn_id, log_wal=False)
            self.uncommitted_writers[key] = txn.txn_id

    def _commit(self, txn: Transaction) -> None:
        wal_entries: List[WALEntry] = []

        if self.mode == "buffered":
            for key, value in txn.write_set.items():
                entry = self.database.write(key, value, txn_id=txn.txn_id, log_wal=True)
                wal_entries.append(entry)
        else:
            for key, value in txn.write_set.items():
                entry = WALEntry(
                    txn_id=txn.txn_id,
                    key=key,
                    old_value=txn.direct_old_values.get(key),
                    new_value=value,
                )
                self.database.append_wal(entry)
                wal_entries.append(entry)
                if self.uncommitted_writers.get(key) == txn.txn_id:
                    self.uncommitted_writers.pop(key, None)

        txn.commit()

        if self.replica_manager:
            for entry in wal_entries:
                self.replica_manager.propagate(entry)

    def _rollback(self, txn: Transaction) -> None:
        if self.mode == "direct":
            for key, old_value in txn.direct_old_values.items():
                if old_value is None:
                    self.database.accounts.pop(key, None)
                else:
                    self.database.write(key, old_value, txn_id=txn.txn_id, log_wal=False)
                if self.uncommitted_writers.get(key) == txn.txn_id:
                    self.uncommitted_writers.pop(key, None)

        txn.write_set.clear()
        txn.rollback()

    def _handle_network_operation(self, op: Operation) -> None:
        if not self.replica_manager:
            return

        op_type = op.op_type.upper()
        replica = str(op.key) if op.key is not None else None

        if op_type == "DELAY":
            if replica is None or op.value is None:
                raise ValueError("DELAY requires replica and seconds")
            self.replica_manager.network.delay(replica, float(op.value))
        elif op_type == "PARTITION":
            if replica is None:
                raise ValueError("PARTITION requires replica")
            self.replica_manager.network.partition(replica)
        elif op_type == "RECOVER":
            if replica is None:
                raise ValueError("RECOVER requires replica")
            self.replica_manager.network.recover(replica)
        elif op_type == "DELIVER":
            self.replica_manager.network.deliver_pending()
        elif op_type == "WAIT":
            seconds = float(op.value or 0)
            self.replica_manager.network.advance_time(seconds)
            self.replica_manager.network.deliver_pending()
        elif op_type == "SYNC":
            self.replica_manager.sync_all()
