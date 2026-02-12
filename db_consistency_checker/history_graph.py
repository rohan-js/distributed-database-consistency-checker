from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Sequence, Set, Tuple

from .scheduler import ExecutionRecord


@dataclass(frozen=True)
class GraphEdge:
    source: str
    target: str
    conflict_type: str
    key: str


@dataclass(frozen=True)
class GraphResult:
    is_serializable: bool
    cycles: List[List[str]]
    graph_edges: List[GraphEdge]


class HistoryGraph:
    def __init__(self, history: Sequence[ExecutionRecord]) -> None:
        self.history = list(history)

    def analyze(self) -> GraphResult:
        committed = {record.txn_id for record in self.history if record.op_type == "COMMIT"}

        ops = [
            r
            for r in self.history
            if r.txn_id in committed and r.op_type in {"READ", "WRITE"} and r.key is not None
        ]

        edges: Set[Tuple[str, str, str, str]] = set()
        for i, first in enumerate(ops):
            for second in ops[i + 1 :]:
                if first.txn_id == second.txn_id or first.key != second.key:
                    continue

                conflict_type = self._conflict_type(first.op_type, second.op_type)
                if conflict_type:
                    edges.add((first.txn_id, second.txn_id, conflict_type, first.key))

        graph_edges = [GraphEdge(*edge) for edge in sorted(edges)]
        cycles = self._find_cycles(graph_edges)
        return GraphResult(is_serializable=not cycles, cycles=cycles, graph_edges=graph_edges)

    @staticmethod
    def _conflict_type(op1: str, op2: str) -> str:
        if op1 == "WRITE" and op2 == "READ":
            return "WR"
        if op1 == "READ" and op2 == "WRITE":
            return "RW"
        if op1 == "WRITE" and op2 == "WRITE":
            return "WW"
        return ""

    def _find_cycles(self, edges: Sequence[GraphEdge]) -> List[List[str]]:
        adjacency: Dict[str, Set[str]] = {}
        for edge in edges:
            adjacency.setdefault(edge.source, set()).add(edge.target)
            adjacency.setdefault(edge.target, set())

        cycles: Set[Tuple[str, ...]] = set()

        def dfs(node: str, stack: List[str], on_stack: Set[str]) -> None:
            stack.append(node)
            on_stack.add(node)

            for neighbor in adjacency.get(node, set()):
                if neighbor in on_stack:
                    idx = stack.index(neighbor)
                    cycle = stack[idx:] + [neighbor]
                    cycles.add(self._normalize_cycle(cycle))
                elif neighbor not in stack:
                    dfs(neighbor, stack, on_stack)

            stack.pop()
            on_stack.remove(node)

        for node in adjacency:
            dfs(node, [], set())

        return [list(cycle) for cycle in sorted(cycles)]

    @staticmethod
    def _normalize_cycle(cycle: List[str]) -> Tuple[str, ...]:
        # Drop duplicate closing node for normalization then restore at the end.
        if len(cycle) > 1 and cycle[0] == cycle[-1]:
            cycle = cycle[:-1]

        if not cycle:
            return tuple()

        min_idx = min(range(len(cycle)), key=lambda i: cycle[i])
        rotated = cycle[min_idx:] + cycle[:min_idx]
        return tuple(rotated + [rotated[0]])
