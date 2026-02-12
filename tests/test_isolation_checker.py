from pathlib import Path

from db_consistency_checker.main import run_scenario


SCENARIOS = Path(__file__).resolve().parents[1] / "scenarios"


def _violation_types(path: str) -> set[str]:
    _, violations, _, _, _ = run_scenario(str(SCENARIOS / path))
    return {v.violation_type for v in violations}


def test_dirty_read_detected() -> None:
    assert "Dirty Read" in _violation_types("dirty_read.txt")


def test_non_repeatable_read_detected() -> None:
    assert "Non-Repeatable Read" in _violation_types("non_repeatable_read.txt")


def test_lost_update_detected() -> None:
    assert "Lost Update" in _violation_types("lost_update.txt")
