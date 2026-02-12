from pathlib import Path

from db_consistency_checker.main import run_scenario


SCENARIOS = Path(__file__).resolve().parents[1] / "scenarios"


def test_non_serializable_cycle_detected() -> None:
    _, _, graph, _, _ = run_scenario(str(SCENARIOS / "non_serializable.txt"))

    assert graph.is_serializable is False
    assert graph.cycles
