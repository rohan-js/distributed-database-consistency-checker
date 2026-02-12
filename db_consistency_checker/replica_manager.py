from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Optional

from .database import Database, WALEntry
from .network_simulator import NetworkSimulator


@dataclass(frozen=True)
class ReplicaDivergence:
    replica: str
    expected: Dict[str, int]
    actual: Dict[str, int]


class ReplicaManager:
    def __init__(self, primary: Database, replica_names: Optional[List[str]] = None) -> None:
        self.primary = primary
        self.replicas: Dict[str, Database] = {
            name: Database(primary.snapshot()) for name in (replica_names or ["ReplicaA", "ReplicaB"])
        }
        self.network = NetworkSimulator(list(self.replicas), self._deliver_to_replica)

    def propagate(self, wal_entry: WALEntry) -> None:
        for replica in self.replicas:
            self.network.send_write(replica, wal_entry)

    def sync_all(self) -> None:
        # Simulates eventual convergence by forcing delivery of all queued updates,
        # then hard-aligning snapshots to primary for deterministic reconciliation.
        self.network.flush_all()
        primary_snapshot = self.primary.snapshot()
        for replica in self.replicas.values():
            replica.accounts = dict(primary_snapshot)

    def check_divergence(self) -> List[ReplicaDivergence]:
        expected = self.primary.snapshot()
        divergences: List[ReplicaDivergence] = []
        for replica_name, replica_db in self.replicas.items():
            actual = replica_db.snapshot()
            if actual != expected:
                divergences.append(
                    ReplicaDivergence(replica=replica_name, expected=expected, actual=actual)
                )
        return divergences

    def _deliver_to_replica(self, replica: str, entry: WALEntry) -> None:
        self.replicas[replica].apply_wal_entry(entry)
