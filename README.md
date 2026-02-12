# Distributed Database Consistency Checker

A production-style reliability testing tool that simulates a replicated banking database, executes interleaved transactions, and reports consistency violations.

## Features

- Deterministic transaction interleaving (`direct` and `buffered` execution modes)
- Write-ahead logging (WAL) on the primary database
- Isolation anomaly detection:
  - Dirty reads
  - Non-repeatable reads
  - Lost updates
  - Phantom reads (range-based)
- Conflict-serializability analysis with cycle detection (precedence graph)
- Replica consistency simulation:
  - Delay
  - Partition
  - Recovery
  - Forced sync/convergence
- Readable terminal report with `[PASS]/[FAIL]` sections

## Repository Layout

- `db_consistency_checker/database.py` in-memory DB + WAL
- `db_consistency_checker/transaction.py` transaction and operation models
- `db_consistency_checker/scheduler.py` interleaving executor
- `db_consistency_checker/isolation_checker.py` anomaly detection
- `db_consistency_checker/history_graph.py` conflict graph analysis
- `db_consistency_checker/network_simulator.py` network fault simulation
- `db_consistency_checker/replica_manager.py` primary/replica propagation
- `db_consistency_checker/reporter.py` final report generation
- `db_consistency_checker/main.py` CLI orchestration
- `scenarios/` sample scenario files
- `tests/` pytest suite

## Quick Start

1. Create and activate a virtual environment.
2. Install package and dev dependencies:

```bash
python3 -m pip install -e .[dev]
```

3. Run a scenario:

```bash
python3 -m db_consistency_checker.main scenarios/full_scenario.txt
```

You can also use the root wrapper:

```bash
python3 main.py scenarios/full_scenario.txt
```

## Scenario Format

Supported directives:

- `INIT A=100 B=50`
- `MODE direct` or `MODE buffered`
- `REPLICAS ReplicaA ReplicaB`
- Transaction ops:
  - `T1 BEGIN`
  - `T1 READ A`
  - `T1 RANGE_READ A*`
  - `T1 WRITE A 80` or `T1 WRITE A=80`
  - `T1 COMMIT`
  - `T1 ROLLBACK`
- Network ops:
  - `DELAY ReplicaB 5`
  - `PARTITION ReplicaA`
  - `RECOVER ReplicaA`
  - `WAIT 5`
  - `DELIVER`
  - `SYNC`

Lines can include comments using `#`.

## Test

```bash
python3 -m pytest tests -v
```

## Example Verification Commands

```bash
python3 -m db_consistency_checker.main scenarios/lost_update.txt
python3 -m db_consistency_checker.main scenarios/dirty_read.txt
python3 -m db_consistency_checker.main scenarios/non_repeatable_read.txt
python3 -m db_consistency_checker.main scenarios/non_serializable.txt
python3 -m db_consistency_checker.main scenarios/replica_divergence.txt
python3 -m db_consistency_checker.main scenarios/full_scenario.txt
```

## License

MIT. See `LICENSE`.
