from pathlib import Path

from db_consistency_checker.main import run_scenario


SCENARIOS = Path(__file__).resolve().parents[1] / "scenarios"


def test_replica_divergence_detected_during_partition() -> None:
    _, _, _, divergences, _ = run_scenario(str(SCENARIOS / "replica_divergence.txt"))

    replicas = {d.replica for d in divergences}
    assert "ReplicaB" in replicas
