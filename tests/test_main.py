from pathlib import Path

from db_consistency_checker.main import run_scenario


SCENARIOS = Path(__file__).resolve().parents[1] / "scenarios"


def test_full_scenario_produces_report_sections() -> None:
    _, _, _, _, report = run_scenario(str(SCENARIOS / "full_scenario.txt"))

    assert "Distributed Database Consistency Report" in report
    assert "Isolation Checks" in report
    assert "Conflict Serializability" in report
    assert "Replica Divergence" in report
