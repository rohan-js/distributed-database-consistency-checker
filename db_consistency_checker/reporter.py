from __future__ import annotations

from dataclasses import dataclass
from typing import List

from .history_graph import GraphResult
from .isolation_checker import Violation
from .replica_manager import ReplicaDivergence


@dataclass(frozen=True)
class ReportResult:
    text: str
    has_failures: bool


class Reporter:
    RED = "\033[91m"
    GREEN = "\033[92m"
    YELLOW = "\033[93m"
    RESET = "\033[0m"

    def generate(
        self,
        violations: List[Violation],
        graph_result: GraphResult,
        divergences: List[ReplicaDivergence],
        color: bool = False,
    ) -> ReportResult:
        lines: List[str] = ["Distributed Database Consistency Report", "=" * 40]

        has_failures = bool(violations) or (not graph_result.is_serializable) or bool(divergences)

        lines.append(self._marker_line("Isolation Checks", not violations, color))
        if violations:
            for violation in violations:
                lines.append(
                    f"  - {violation.violation_type}: {violation.detail_message} "
                    f"(txn1={violation.txn1}, txn2={violation.txn2}, key={violation.key})"
                )

        lines.append(self._marker_line("Conflict Serializability", graph_result.is_serializable, color))
        if graph_result.graph_edges:
            lines.append("  Edges:")
            for edge in graph_result.graph_edges:
                lines.append(
                    f"  - {edge.source} -> {edge.target} [{edge.conflict_type}] key={edge.key}"
                )
        if graph_result.cycles:
            lines.append("  Cycles:")
            for cycle in graph_result.cycles:
                lines.append(f"  - {' -> '.join(cycle)}")

        lines.append(self._marker_line("Replica Divergence", not divergences, color))
        if divergences:
            for divergence in divergences:
                lines.append(
                    f"  - {divergence.replica}: expected={divergence.expected}, actual={divergence.actual}"
                )

        lines.append(self._marker_line("Overall", not has_failures, color))
        return ReportResult(text="\n".join(lines), has_failures=has_failures)

    def _marker_line(self, section: str, passed: bool, color: bool) -> str:
        marker = "[PASS]" if passed else "[FAIL]"
        if not color:
            return f"{marker} {section}"

        if passed:
            decorated = f"{self.GREEN}{marker}{self.RESET}"
        else:
            decorated = f"{self.RED}{marker}{self.RESET}"
        return f"{decorated} {section}"
