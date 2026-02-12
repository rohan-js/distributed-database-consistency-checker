from db_consistency_checker.database import Database
from db_consistency_checker.scheduler import Scheduler
from db_consistency_checker.transaction import Operation


def test_scheduler_lost_update_interleaving_history_and_state() -> None:
    db = Database({"A": 100})
    operations = [
        Operation("BEGIN", "T1"),
        Operation("BEGIN", "T2"),
        Operation("READ", "T1", key="A"),
        Operation("READ", "T2", key="A"),
        Operation("WRITE", "T1", key="A", value=90),
        Operation("WRITE", "T2", key="A", value=80),
        Operation("COMMIT", "T1"),
        Operation("COMMIT", "T2"),
    ]

    history = Scheduler(operations=operations, database=db, mode="direct").run()

    assert [record.op_type for record in history] == [
        "BEGIN",
        "BEGIN",
        "READ",
        "READ",
        "WRITE",
        "WRITE",
        "COMMIT",
        "COMMIT",
    ]
    assert db.read("A") == 80
    assert len(db.wal) == 2
