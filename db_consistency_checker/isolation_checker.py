from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Sequence

from .scheduler import ExecutionRecord


@dataclass(frozen=True)
class Violation:
    violation_type: str
    txn1: str
    txn2: str
    key: str
    detail_message: str


class IsolationChecker:
    def __init__(self, history: Sequence[ExecutionRecord]) -> None:
        self.history = list(history)

    def check_all(self) -> List[Violation]:
        violations: List[Violation] = []
        violations.extend(self.detect_dirty_reads())
        violations.extend(self.detect_non_repeatable_reads())
        violations.extend(self.detect_lost_updates())
        violations.extend(self.detect_phantom_reads())
        return violations

    def detect_dirty_reads(self) -> List[Violation]:
        violations: List[Violation] = []
        for record in self.history:
            if record.op_type != "READ":
                continue
            if record.read_from_uncommitted and record.read_from_txn:
                violations.append(
                    Violation(
                        violation_type="Dirty Read",
                        txn1=record.read_from_txn,
                        txn2=record.txn_id,
                        key=record.key or "",
                        detail_message=(
                            f"{record.txn_id} read key {record.key} from uncommitted write by "
                            f"{record.read_from_txn}"
                        ),
                    )
                )
        return violations

    def detect_non_repeatable_reads(self) -> List[Violation]:
        violations: List[Violation] = []
        reads_by_txn_key: Dict[tuple[str, str], List[ExecutionRecord]] = {}

        for record in self.history:
            if record.op_type == "READ" and record.key is not None:
                reads_by_txn_key.setdefault((record.txn_id, record.key), []).append(record)

        for (txn_id, key), reads in reads_by_txn_key.items():
            reads.sort(key=lambda r: r.order)
            for first, second in zip(reads, reads[1:]):
                if first.value == second.value:
                    continue

                writer = None
                for mid in self.history:
                    if not (first.order < mid.order < second.order):
                        continue
                    if mid.op_type == "WRITE" and mid.key == key and mid.txn_id != txn_id:
                        writer = mid.txn_id
                        break

                if writer:
                    violations.append(
                        Violation(
                            violation_type="Non-Repeatable Read",
                            txn1=txn_id,
                            txn2=writer,
                            key=key,
                            detail_message=(
                                f"{txn_id} read {key} as {first.value}, then {second.value} after "
                                f"{writer} updated it"
                            ),
                        )
                    )
        return violations

    def detect_lost_updates(self) -> List[Violation]:
        violations: List[Violation] = []

        writes_by_key: Dict[str, List[ExecutionRecord]] = {}
        reads_by_txn_key: Dict[tuple[str, str], List[ExecutionRecord]] = {}

        for record in self.history:
            if record.op_type == "WRITE" and record.key is not None:
                writes_by_key.setdefault(record.key, []).append(record)
            if record.op_type == "READ" and record.key is not None:
                reads_by_txn_key.setdefault((record.txn_id, record.key), []).append(record)

        for key, writes in writes_by_key.items():
            writes.sort(key=lambda r: r.order)
            for first, second in zip(writes, writes[1:]):
                if first.txn_id == second.txn_id:
                    continue

                reads_t1 = [r for r in reads_by_txn_key.get((first.txn_id, key), []) if r.order < first.order]
                reads_t2 = [r for r in reads_by_txn_key.get((second.txn_id, key), []) if r.order < second.order]
                if not reads_t1 or not reads_t2:
                    continue

                # Common lost-update signature: both transactions derived writes from the same prior read value.
                if reads_t1[-1].value == reads_t2[-1].value:
                    violations.append(
                        Violation(
                            violation_type="Lost Update",
                            txn1=first.txn_id,
                            txn2=second.txn_id,
                            key=key,
                            detail_message=(
                                f"{second.txn_id} overwrote {first.txn_id}'s update on {key}; "
                                f"both read base value {reads_t1[-1].value}"
                            ),
                        )
                    )
        return violations

    def detect_phantom_reads(self) -> List[Violation]:
        violations: List[Violation] = []
        range_reads: Dict[tuple[str, str], List[ExecutionRecord]] = {}

        for record in self.history:
            if record.op_type == "RANGE_READ" and record.key is not None:
                range_reads.setdefault((record.txn_id, record.key), []).append(record)

        for (txn_id, range_expr), reads in range_reads.items():
            reads.sort(key=lambda r: r.order)
            prefix = range_expr.rstrip("*")
            for first, second in zip(reads, reads[1:]):
                if first.value == second.value:
                    continue

                writer = None
                for mid in self.history:
                    if not (first.order < mid.order < second.order):
                        continue
                    if mid.op_type == "WRITE" and mid.key and mid.key.startswith(prefix) and mid.txn_id != txn_id:
                        writer = mid.txn_id
                        break

                if writer:
                    violations.append(
                        Violation(
                            violation_type="Phantom Read",
                            txn1=txn_id,
                            txn2=writer,
                            key=range_expr,
                            detail_message=(
                                f"{txn_id} observed different row set for {range_expr} after {writer} inserted/updated"
                            ),
                        )
                    )
        return violations
