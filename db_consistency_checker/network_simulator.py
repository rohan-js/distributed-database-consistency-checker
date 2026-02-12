from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from enum import Enum
from typing import Callable, Dict, List

from .database import WALEntry


class ReplicaState(str, Enum):
    CONNECTED = "CONNECTED"
    PARTITIONED = "PARTITIONED"
    DELAYED = "DELAYED"


@dataclass
class QueuedWrite:
    entry: WALEntry
    deliver_at: float


class NetworkSimulator:
    """Virtual-time network simulator for replication links."""

    def __init__(self, replicas: List[str], deliver_callback: Callable[[str, WALEntry], None]) -> None:
        self.deliver_callback = deliver_callback
        self.clock: float = 0.0
        self.state: Dict[str, ReplicaState] = {name: ReplicaState.CONNECTED for name in replicas}
        self.delay_seconds: Dict[str, float] = {name: 0.0 for name in replicas}
        self.pending: Dict[str, List[QueuedWrite]] = defaultdict(list)
        self.partition_backlog: Dict[str, List[WALEntry]] = defaultdict(list)

    def delay(self, replica: str, seconds: float) -> None:
        self._require_replica(replica)
        self.state[replica] = ReplicaState.DELAYED
        self.delay_seconds[replica] = max(0.0, float(seconds))

    def partition(self, replica: str) -> None:
        self._require_replica(replica)
        self.state[replica] = ReplicaState.PARTITIONED

    def recover(self, replica: str) -> None:
        self._require_replica(replica)
        self.state[replica] = ReplicaState.CONNECTED
        self.delay_seconds[replica] = 0.0

        backlog = self.partition_backlog.pop(replica, [])
        for entry in backlog:
            self.deliver_callback(replica, entry)

        delayed = sorted(self.pending.get(replica, []), key=lambda q: q.deliver_at)
        self.pending[replica] = []
        for queued in delayed:
            self.deliver_callback(replica, queued.entry)

    def send_write(self, replica: str, entry: WALEntry) -> None:
        self._require_replica(replica)
        state = self.state[replica]

        if state == ReplicaState.CONNECTED:
            self.deliver_callback(replica, entry)
        elif state == ReplicaState.DELAYED:
            deliver_at = self.clock + self.delay_seconds[replica]
            self.pending[replica].append(QueuedWrite(entry=entry, deliver_at=deliver_at))
        else:  # PARTITIONED
            self.partition_backlog[replica].append(entry)

    def advance_time(self, seconds: float) -> None:
        self.clock += max(0.0, float(seconds))

    def deliver_pending(self) -> None:
        for replica, queue in list(self.pending.items()):
            if not queue:
                continue

            still_pending: List[QueuedWrite] = []
            due: List[QueuedWrite] = []
            for queued in queue:
                if queued.deliver_at <= self.clock and self.state[replica] != ReplicaState.PARTITIONED:
                    due.append(queued)
                else:
                    still_pending.append(queued)

            self.pending[replica] = still_pending
            for queued in sorted(due, key=lambda q: q.deliver_at):
                self.deliver_callback(replica, queued.entry)

    def flush_all(self) -> None:
        for replica in list(self.state):
            self.recover(replica)

    def _require_replica(self, replica: str) -> None:
        if replica not in self.state:
            raise ValueError(f"unknown replica: {replica}")
