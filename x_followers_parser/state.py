"""Checkpoint / resume for bulk runs. Stdlib only.

State file is JSONL: one settled result object per line.
Only *settled* rows are stored (successes + permanent failures like
"user not found"). Transient failures (429/5xx/timeout) are NOT stored,
so a resumed run retries exactly the handles that never settled.
"""
import json
import os
from typing import Dict, List, Tuple

from .providers import is_transient_error


def _is_settled(row: dict) -> bool:
    """Only successes and permanent failures settle. Transient rows
    (429/5xx/timeout) must be retried, so they are never loaded as done."""
    return bool(row.get("ok")) or not is_transient_error(
        str(row.get("error") or ""))


def load_state(path: str) -> Dict[str, dict]:
    """Load settled rows keyed by lowercased username. Missing file -> {}.

    Rows that are neither ok nor permanently failed (e.g. transient errors
    from a hand-edited file) are ignored so --resume retries them.
    """
    done: Dict[str, dict] = {}
    if not path or not os.path.exists(path):
        return done
    with open(path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                row = json.loads(line)
            except ValueError:
                continue
            if not isinstance(row, dict):
                continue
            # strip internal fields (e.g. retry_after from older versions /
            # hand-edited files) so resumed rows match the documented schema.
            row.pop("retry_after", None)
            user = str(row.get("username") or "").lower()
            if user and user not in done and _is_settled(row):
                done[user] = row
    return done


class StateWriter:
    """Append-only buffered JSONL writer. Call close() (or use as context)."""

    def __init__(self, path: str, flush_every: int = 50):
        self.path = path
        self.flush_every = max(1, flush_every)
        self._buf: List[str] = []
        d = os.path.dirname(os.path.abspath(path))
        os.makedirs(d, exist_ok=True)
        self._f = open(path, "a", encoding="utf-8")

    def append(self, row: dict) -> None:
        self._buf.append(json.dumps(row, ensure_ascii=False))
        if len(self._buf) >= self.flush_every:
            self.flush()

    def flush(self) -> None:
        if self._buf:
            self._f.write("\n".join(self._buf) + "\n")
            self._f.flush()
            self._buf = []

    def close(self) -> None:
        try:
            self.flush()
        finally:
            self._f.close()

    def __enter__(self) -> "StateWriter":
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()


def merge_in_order(handles: List[str], fresh: Dict[str, dict],
                   settled: Dict[str, dict]) -> Tuple[List[dict], int]:
    """Merge fresh rows with previously settled rows, preserving input order.

    Returns (rows, n_resumed). Fresh rows win over settled ones.
    """
    rows = []
    resumed = 0
    for h in handles:
        key = h.lower()
        if key in fresh:
            rows.append(fresh[key])
        elif key in settled:
            rows.append(settled[key])
            resumed += 1
        # else: aborted before this handle was attempted -> dropped from output
    return rows, resumed
