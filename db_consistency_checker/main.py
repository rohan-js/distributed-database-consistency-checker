from __future__ import annotations

import argparse
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from .database import Database
from .history_graph import GraphResult, HistoryGraph
from .isolation_checker import IsolationChecker, Violation
from .replica_manager import ReplicaDivergence, ReplicaManager
from .reporter import Reporter
from .scheduler import ExecutionRecord, Scheduler
from .transaction import Operation


def parse_scenario(path: str) -> Tuple[Dict[str, int], str, List[str], List[Operation]]:
    initial_accounts: Dict[str, int] = {}
    mode = "direct"
    replicas = ["ReplicaA", "ReplicaB"]
    operations: List[Operation] = []

    for line_no, raw_line in enumerate(Path(path).read_text().splitlines(), start=1):
        line = raw_line.split("#", 1)[0].strip()
        if not line:
            continue

        parts = line.split()
        command = parts[0].upper()

        if command == "INIT":
            for assignment in parts[1:]:
                if "=" not in assignment:
                    raise ValueError(f"line {line_no}: INIT expects KEY=VALUE pairs")
                key, raw_value = assignment.split("=", 1)
                initial_accounts[key] = int(raw_value)
            continue

        if command == "MODE":
            if len(parts) != 2:
                raise ValueError(f"line {line_no}: MODE expects one argument")
            mode = parts[1].lower()
            continue

        if command == "REPLICAS":
            if len(parts) < 2:
                raise ValueError(f"line {line_no}: REPLICAS expects at least one name")
            replicas = parts[1:]
            continue

        if command in {"DELAY", "PARTITION", "RECOVER", "DELIVER", "WAIT", "SLEEP", "SYNC"}:
            operations.append(_parse_network_operation(parts, line_no))
            continue

        operations.append(_parse_transaction_operation(parts, line_no))

    return initial_accounts, mode, replicas, operations


def _parse_transaction_operation(parts: List[str], line_no: int) -> Operation:
    if len(parts) < 2:
        raise ValueError(f"line {line_no}: invalid transaction operation")

    txn_id = parts[0]
    op = parts[1].upper()

    if op in {"BEGIN", "COMMIT", "ROLLBACK"}:
        return Operation(op_type=op, txn_id=txn_id)

    if op in {"READ", "RANGE_READ"}:
        if len(parts) != 3:
            raise ValueError(f"line {line_no}: {op} expects key/range")
        return Operation(op_type=op, txn_id=txn_id, key=parts[2])

    if op == "WRITE":
        if len(parts) == 3 and "=" in parts[2]:
            key, raw_value = parts[2].split("=", 1)
        elif len(parts) == 4:
            key = parts[2]
            raw_value = parts[3]
        else:
            raise ValueError(
                f"line {line_no}: WRITE expects either 'WRITE KEY VALUE' or 'WRITE KEY=VALUE'"
            )
        return Operation(op_type="WRITE", txn_id=txn_id, key=key, value=int(raw_value))

    raise ValueError(f"line {line_no}: unknown transaction op '{op}'")


def _parse_network_operation(parts: List[str], line_no: int) -> Operation:
    op = parts[0].upper()

    if op == "DELAY":
        if len(parts) != 3:
            raise ValueError(f"line {line_no}: DELAY expects replica and seconds")
        return Operation(op_type="DELAY", key=parts[1], value=float(parts[2]))

    if op in {"PARTITION", "RECOVER"}:
        if len(parts) != 2:
            raise ValueError(f"line {line_no}: {op} expects a replica")
        return Operation(op_type=op, key=parts[1])

    if op in {"WAIT", "SLEEP"}:
        if len(parts) != 2:
            raise ValueError(f"line {line_no}: {op} expects seconds")
        return Operation(op_type="WAIT", value=float(parts[1]))

    if op in {"DELIVER", "SYNC"}:
        if len(parts) != 1:
            raise ValueError(f"line {line_no}: {op} takes no arguments")
        return Operation(op_type=op)

    raise ValueError(f"line {line_no}: unknown network op '{op}'")


def run_scenario(scenario_path: str, color: bool = False) -> Tuple[
    List[ExecutionRecord],
    List[Violation],
    GraphResult,
    List[ReplicaDivergence],
    str,
]:
    initial_accounts, mode, replica_names, operations = parse_scenario(scenario_path)

    primary_db = Database(initial_accounts)
    replica_manager = ReplicaManager(primary_db, replica_names)

    scheduler = Scheduler(
        operations=operations,
        database=primary_db,
        mode=mode,
        replica_manager=replica_manager,
    )
    history = scheduler.run()

    checker = IsolationChecker(history)
    violations = checker.check_all()

    graph_result = HistoryGraph(history).analyze()
    divergences = replica_manager.check_divergence()

    report = Reporter().generate(
        violations=violations,
        graph_result=graph_result,
        divergences=divergences,
        color=color,
    )
    return history, violations, graph_result, divergences, report.text


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Distributed Database Consistency Checker")
    parser.add_argument("scenario", help="Path to scenario file")
    parser.add_argument("--color", action="store_true", help="Enable ANSI color output")
    return parser


def main(argv: Optional[List[str]] = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    _, _, _, _, report_text = run_scenario(args.scenario, color=args.color)
    print(report_text)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
