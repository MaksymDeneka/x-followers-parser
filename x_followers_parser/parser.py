"""Core orchestration: concurrent fetch + retries + bulk controls. No I/O formats here."""
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import asdict
from typing import Callable, Dict, List, Optional

from .handles import normalize_handle, parse_handles
from .providers import (BaseProvider, FetchResult, batch_head,
                        call_fetch_many, is_transient_error, pad_results)
from .ratelimit import RateLimiter


class BulkAborted(Exception):
    """Raised when too many consecutive transient failures suggest the
    provider is down. `partial` holds fresh rows completed before aborting
    (keyed by lowercased username); checkpointed rows are already on disk."""

    def __init__(self, completed: int, total: int, partial: Dict[str, dict]):
        super().__init__(
            "aborted after %d/%d handles: provider looks down "
            "(see --abort-after, --provider chains, --resume)" % (completed, total))
        self.completed = completed
        self.total = total
        self.partial = partial


def get_followers(
    handles: List[str],
    provider: BaseProvider,
    max_workers: int = 5,
    delay: float = 0.0,
    timeout: int = 15,
    retries: int = 2,
    rps: Optional[float] = None,
    chunk_delay: Optional[float] = None,
    on_result: Optional[Callable[[dict], None]] = None,
    on_progress: Optional[Callable[[int, int], None]] = None,
    abort_after: Optional[int] = None,
) -> List[Dict]:
    """Fetch follower counts for normalized handles.

    Returns a list of dicts preserving input order:
    {"username", "followers_count", "name", "following_count",
     "tweet_count", "ok", "error", "status"}.
    status is "active" | "suspended" | "not_found" | "error":
    suspended/not_found are definitive answers (settled, never retried).
    Retries transient failures (network/HTTP 429/5xx) with backoff.

    Bulk controls:
      rps: shared requests/sec cap across workers (takes precedence over delay).
      chunk_delay: pause between official-API batch calls (default: delay or 1.0).
      on_result: called from the main thread with each completed row
        (use for checkpointing).
      on_progress: called from the main thread as (done, total).
      abort_after: abort the run after N *consecutive* transient failures.
    """
    usernames = parse_handles(handles)
    if not usernames:
        return []

    # Batching provider (official API, possibly as a chain head) serves
    # 100 handles per call — sequential chunk loop, no thread pool needed.
    if batch_head(provider) is not None:
        return _get_followers_batched(
            usernames, provider, timeout=timeout,
            chunk_delay=chunk_delay if chunk_delay is not None else (delay or 1.0),
            on_result=on_result, on_progress=on_progress, abort_after=abort_after)

    limiter = RateLimiter(rps if rps is not None else (1.0 / delay if delay > 0 else 0.0))
    stop = threading.Event()
    ordered: Dict[str, Dict] = {}
    done = 0
    consec_transient = 0

    def _work(u: str):
        if stop.is_set():
            return None  # skipped: run aborted, resume will retry
        return _fetch_one_with_retries(provider, u, timeout, retries, limiter, stop)

    with ThreadPoolExecutor(max_workers=max(1, max_workers)) as pool:
        pending = {pool.submit(_work, u): u for u in usernames}
        try:
            for fut in as_completed(pending):
                u = pending[fut]
                try:
                    res = fut.result()
                except Exception as e:  # never let one handle kill the batch
                    res = FetchResult(username=u, ok=False, error="request failed: %s" % e)
                if res is None:  # skipped after abort
                    continue
                row = _row(res)
                ordered[u.lower()] = row
                done += 1
                if on_result is not None:
                    on_result(row)
                if on_progress is not None:
                    on_progress(done, len(usernames))
                if res.ok or not is_transient_error(res.error or ""):
                    consec_transient = 0
                else:
                    consec_transient += 1
                    if abort_after and consec_transient >= abort_after:
                        stop.set()
                        for f in pending:
                            f.cancel()
                        raise BulkAborted(done, len(usernames), ordered)
        finally:
            stop.set()  # release any limiter-waiting workers on the way out
    return [ordered[u.lower()] for u in usernames if u.lower() in ordered]


def _get_followers_batched(usernames: List[str], provider: BaseProvider,
                           timeout: int, chunk_delay: float,
                           on_result, on_progress, abort_after) -> List[Dict]:
    ordered: Dict[str, Dict] = {}
    done = 0
    consec_transient = 0
    for i in range(0, len(usernames), 100):
        chunk = usernames[i:i + 100]
        try:
            raw = call_fetch_many(provider, chunk, timeout=timeout,
                                  chunk_delay=0.0)  # pacing handled here
        except Exception as e:
            # Same guarantee as the threaded path: a provider bug fails the
            # chunk transiently (retried on --resume) instead of tracebacking
            # the whole run.
            raw = [FetchResult(username=u, ok=False,
                               error="request failed: %s" % e) for u in chunk]
        results = pad_results(chunk=chunk, results=raw)
        for u, r in zip(chunk, results):
            row = _row(r)
            ordered[u.lower()] = row
            done += 1
            if on_result is not None:
                on_result(row)
            if r.ok or not is_transient_error(r.error or ""):
                consec_transient = 0
            else:
                consec_transient += 1
                if abort_after and consec_transient >= abort_after:
                    raise BulkAborted(done, len(usernames), ordered)
        if on_progress is not None:
            on_progress(done, len(usernames))
        if chunk_delay and i + 100 < len(usernames):
            time.sleep(chunk_delay)
    return [ordered[u.lower()] for u in usernames]


def _fetch_one_with_retries(provider: BaseProvider, username: str,
                            timeout: int, retries: int,
                            limiter: Optional[RateLimiter] = None,
                            stop: Optional[threading.Event] = None) -> Optional[FetchResult]:
    last: Optional[FetchResult] = None
    for attempt in range(max(0, retries) + 1):
        if stop is not None and stop.is_set():
            return last  # aborted: keep prior attempts' result (or None)
        if limiter is not None:
            if not limiter.wait(stop):
                return last
        try:
            res = provider.fetch_one(username, timeout=timeout)
        except Exception as e:
            # Unclassifiable failure: label transient so it is retried and
            # never checkpointed as settled (an empty/strange str(e) must not
            # look permanent).
            res = FetchResult(username=username, ok=False,
                              error="request failed: %s" % e)
        if res.ok or not is_transient_error(res.error or ""):
            return res
        if limiter is not None and _looks_rate_limited(res.error or ""):
            limiter.cool_down(res.retry_after or 60.0)
        last = res
        if attempt < retries:
            _sleep_or_stop(min(2 ** attempt, 8), stop)
    return last or FetchResult(username=username, ok=False, error="failed")


def _row(res: FetchResult) -> Dict:
    """Public row dict. Drops internal fields (retry_after) so output and
    checkpoint schema stays {"username","followers_count","name",
    "following_count","tweet_count","ok","error"} as documented."""
    row = asdict(res)
    row.pop("retry_after", None)
    return row


def _sleep_or_stop(seconds: float, stop: Optional[threading.Event]) -> None:
    """Backoff sleep that wakes early when the run is aborted."""
    if stop is None:
        time.sleep(seconds)
    else:
        stop.wait(timeout=seconds)


def _looks_rate_limited(msg: str) -> bool:
    m = msg.lower()
    return "429" in m or "rate limit" in m or "rate-limit" in m


# Re-export for `from .parser import ...` convenience
__all__ = ["get_followers", "BulkAborted", "FetchResult",
           "normalize_handle", "parse_handles"]
