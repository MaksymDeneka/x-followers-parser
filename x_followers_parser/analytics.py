"""Post analytics: recent posts per handle -> engagement stats. Stdlib only.

Needs just a handle: profile lookup gives followers, timeline pages give
per-post views/likes/reposts/replies/bookmarks/quotes. Summaries carry
sum/mean/median/min/max per metric plus top-N posts by views, so a pool
of accounts can be compared from one CSV/JSONL.
"""
import statistics
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Callable, Dict, List, Optional

from .handles import parse_handles
from .parser import BulkAborted
from .providers import (BaseProvider, FetchResult, ProviderError, RateLimited,
                        is_transient_error)
from .ratelimit import RateLimiter

METRICS = ("views", "likes", "reposts", "replies", "bookmarks", "quotes")


def _describe(vals: List[int]) -> Dict[str, Optional[float]]:
    """sum/mean/median/min/max over non-null values (None when empty)."""
    vals = [v for v in vals if v is not None]
    if not vals:
        return {"n": 0, "sum": 0, "mean": None, "median": None,
                "min": None, "max": None}
    return {"n": len(vals), "sum": sum(vals),
            "mean": sum(vals) / len(vals),
            "median": statistics.median(vals),
            "min": min(vals), "max": max(vals)}


def summarize(posts: List[dict], top_n: int = 5) -> Dict[str, object]:
    """Stats over analyzed (own, non-repost unless included upstream) posts."""
    stats = {m: _describe([p.get(m) for p in posts]) for m in METRICS}
    eng = [(p.get("likes") or 0) + (p.get("reposts") or 0)
           + (p.get("replies") or 0) + (p.get("quotes") or 0)
           + (p.get("bookmarks") or 0) for p in posts]
    stats["engagement"] = _describe(eng) if posts else _describe([])
    stamps = [p["created_timestamp"] for p in posts
              if p.get("created_timestamp")]
    span_days = ((max(stamps) - min(stamps)) / 86400.0) if len(stamps) >= 2 else 0.0
    by_time = sorted(posts, key=lambda p: (p.get("created_timestamp") is not None,
                                           p.get("created_timestamp") or 0))
    top = sorted(posts, key=lambda p: (p.get("views") is not None,
                                       p.get("views") or 0),
                 reverse=True)[:max(0, top_n)]
    return {"n_posts": len(posts), "metrics": stats,
            "date_from": by_time[0].get("created_at") if by_time else None,
            "date_to": by_time[-1].get("created_at") if by_time else None,
            "span_days": round(span_days, 2),
            "posts_per_day": round(len(posts) / span_days, 2) if span_days >= 1 else float(len(posts)),
            "top_posts": [dict(p) for p in top]}


def get_post_analytics(
    handles: List[str],
    provider: BaseProvider,
    max_workers: int = 4,
    max_posts: int = 150,
    top_n: int = 5,
    include_reposts: bool = False,
    with_replies: bool = False,
    timeout: int = 15,
    retries: int = 2,
    rps: Optional[float] = 8.0,
    on_result: Optional[Callable[[dict], None]] = None,
    on_progress: Optional[Callable[[int, int], None]] = None,
    on_posts: Optional[Callable[[str, List[dict]], None]] = None,
    abort_after: Optional[int] = None,
) -> List[Dict]:
    """Fetch recent posts per handle and summarize engagement.

    Flow per handle (sequential inside one worker): profile lookup for
    followers/status, then timeline pages until max_posts raw items or the
    cursor runs out. Reposts of others' posts are excluded from stats by
    default (counted as n_reposts_skipped) so own-post medians stay fair.
    Returns summary rows in input order; full post lists go to on_posts
    (used for --posts-dump) instead of the rows to keep CSVs comparable.
    """
    usernames = parse_handles(handles)
    if not usernames:
        return []
    if not getattr(provider, "supports_timeline", False):
        raise ProviderError(
            "provider %r has no post timeline; post analytics needs "
            "fxtwitter (free, --provider fxtwitter)" % provider.name)
    limiter = RateLimiter(rps if rps is not None else 0.0)
    stop = threading.Event()
    ordered: Dict[str, Dict] = {}
    done = 0
    consec_transient = 0

    def _work(u: str):
        if stop.is_set():
            return None
        return _fetch_account(provider, u, max_posts=max_posts, top_n=top_n,
                              include_reposts=include_reposts,
                              with_replies=with_replies, timeout=timeout,
                              retries=retries, limiter=limiter, stop=stop,
                              on_posts=on_posts)

    with ThreadPoolExecutor(max_workers=max(1, max_workers)) as pool:
        pending = {pool.submit(_work, u): u for u in usernames}
        try:
            for fut in as_completed(pending):
                u = pending[fut]
                try:
                    row = fut.result()
                except Exception as e:
                    row = {"username": u, "ok": False, "status": "error",
                           "error": "request failed: %s" % e}
                if row is None:
                    continue
                ordered[u.lower()] = row
                done += 1
                if on_result is not None:
                    on_result(row)
                if on_progress is not None:
                    on_progress(done, len(usernames))
                if row.get("ok") or not is_transient_error(row.get("error") or ""):
                    consec_transient = 0
                else:
                    consec_transient += 1
                    if abort_after and consec_transient >= abort_after:
                        stop.set()
                        for f in pending:
                            f.cancel()
                        raise BulkAborted(done, len(usernames), ordered)
        finally:
            stop.set()
    return [ordered[u.lower()] for u in usernames if u.lower() in ordered]


def _fetch_account(provider: BaseProvider, username: str, max_posts: int,
                   top_n: int, include_reposts: bool, with_replies: bool,
                   timeout: int, retries: int, limiter: RateLimiter,
                   stop: threading.Event,
                   on_posts: Optional[Callable[[str, List[dict]], None]]) -> Optional[Dict]:
    if stop.is_set():
        return None
    profile = _profile_with_retries(provider, username, timeout, retries,
                                    limiter, stop)
    if profile is None:
        return None
    if not profile.ok:
        row = {"username": username, "ok": False,
               "status": profile.status or "error",
               "error": profile.error or "lookup failed"}
        if is_transient_error(profile.error or ""):
            row["error"] = profile.error
        return row
    raw: List[dict] = []
    cursor: Optional[str] = None
    pages = 0
    error: Optional[str] = None
    # ~30/page observed (upstream picks its own size); +2 headroom pages.
    max_pages = max(1, max_posts // 10 + 2)
    while len(raw) < max_posts and pages < max_pages:
        if stop.is_set():
            return None
        limiter.wait(stop)
        if stop.is_set():
            return None
        try:
            page, cursor = provider.fetch_timeline_page(
                username, count=min(100, max(20, max_posts - len(raw))),
                cursor=cursor, timeout=timeout, with_replies=with_replies)
        except RateLimited as e:
            limiter.cool_down(e.retry_after or 60.0)
            error = str(e)
            if not _retry_sleep(retries, pages, stop):
                break
            pages += 1
            continue
        except ProviderError as e:
            error = str(e)
            if not is_transient_error(error):
                break  # permanent (suspended/private mid-run): keep profile
            if not _retry_sleep(retries, pages, stop):
                break
            pages += 1
            continue
        except Exception as e:
            error = "request failed: %s" % e
            if not _retry_sleep(retries, pages, stop):
                break
            pages += 1
            continue
        pages += 1
        if not page:
            break
        raw.extend(page)
        if not cursor:
            break
    raw = raw[:max_posts]
    if on_posts is not None and raw:
        try:
            on_posts(profile.username or username, raw)
        except Exception:
            pass
    skipped = sum(1 for p in raw if p.get("is_repost"))
    analyzed = raw if include_reposts else [p for p in raw if not p.get("is_repost")]
    summary = summarize(analyzed, top_n=top_n)
    row = {"username": profile.username or username, "ok": True,
           "status": "active", "error": None,
           "followers_count": profile.followers_count, "name": profile.name,
           "following_count": profile.following_count,
           "tweet_count": profile.tweet_count,
           "n_fetched": len(raw), "n_reposts_skipped": 0 if include_reposts else skipped,
           "include_reposts": include_reposts,
           "truncated": bool(cursor) and len(raw) >= max_posts,
           "timeline_error": error if not analyzed and error else None,
           **summary}
    return row


def _profile_with_retries(provider: BaseProvider, username: str, timeout: int,
                          retries: int, limiter: RateLimiter,
                          stop: threading.Event) -> Optional[FetchResult]:
    last: Optional[FetchResult] = None
    for attempt in range(max(0, retries) + 1):
        if stop.is_set():
            return last
        limiter.wait(stop)
        if stop.is_set():
            return last
        try:
            res = provider.fetch_one(username, timeout=timeout)
        except Exception as e:
            res = FetchResult(username=username, ok=False,
                              error="request failed: %s" % e)
        if res.ok or not is_transient_error(res.error or ""):
            return res
        if "429" in (res.error or "") or "rate limit" in (res.error or "").lower():
            limiter.cool_down(res.retry_after or 60.0)
        last = res
        if attempt < retries:
            _sleep_or_stop(min(2 ** attempt, 8), stop)
    return last


def _retry_sleep(retries: int, pages: int, stop: threading.Event) -> bool:
    """True to retry a failed timeline page (bounded by retries)."""
    if pages >= max(0, retries):
        return False
    _sleep_or_stop(min(2 ** pages, 8), stop)
    return not (stop is not None and stop.is_set())


def _sleep_or_stop(seconds: float, stop: Optional[threading.Event]) -> None:
    if stop is None:
        time.sleep(seconds)
    else:
        stop.wait(timeout=seconds)


__all__ = ["METRICS", "get_post_analytics", "summarize"]
