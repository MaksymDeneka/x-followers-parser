"""Data providers: FxTwitter (no auth) + official X API v2 (bearer token).

Only stdlib is used so the parser runs with a bare `python3`.
"""
import json
import os
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

USER_AGENT = "x-followers-parser/0.1.0 (+https://x.com)"


@dataclass
class FetchResult:
    username: str
    followers_count: Optional[int] = None
    name: Optional[str] = None
    following_count: Optional[int] = None
    tweet_count: Optional[int] = None
    ok: bool = False
    error: Optional[str] = None
    # server-suggested wait (seconds) when this failure was a 429; lets the
    # caller cool down by the right amount instead of a fixed guess.
    retry_after: Optional[float] = None
    # Account status: "active" | "suspended" | "not_found" | "error".
    # Auto-derived from ok/error unless passed explicitly.
    status: str = "error"

    def __post_init__(self) -> None:
        if self.ok:
            self.status = "active"
        elif not self.status or self.status == "error":
            self.status = classify_status(self.error or "")


def classify_status(error: str) -> str:
    """Map a failure message to an account status.

    - "suspended": provider says the account was suspended
      (FxTwitter reason/message, X API "User has been suspended", ...).
    - "not_found": unknown / deleted / never existed.
    - "error": anything else (transient or unclassifiable).
    Suspended/not_found are permanent: never retried, checkpointed as
    settled, and never fail over to the next provider in a chain.
    """
    low = (error or "").lower()
    if "suspend" in low:
        return "suspended"
    # NOTE: no numeric-code matching here on purpose: e.g. "code\":50"
    # is a prefix-substring of "\"code\":500", which would misclassify a
    # transient HTTP 500 as permanently not found.
    if ("not found" in low or "no user" in low or "does not exist" in low
            or "unknown user" in low or "could not find user" in low
            or "http 404" in low):
        return "not_found"
    return "error"


def _http_get_json(url: str, headers: Optional[Dict[str, str]] = None,
                   timeout: int = 15) -> object:
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT, **(headers or {})})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return json.loads(resp.read().decode("utf-8", "replace"))
    except urllib.error.HTTPError as e:
        try:
            body = e.read().decode("utf-8", "replace")[:500]
        except Exception:
            body = ""
        if e.code == 429:
            retry_after = _retry_after_from_headers(e.headers)
            raise RateLimited("HTTP 429 rate limited%s" % (
                " (retry in %ds)" % int(retry_after) if retry_after else ""),
                retry_after=retry_after)
        raise ProviderError("HTTP %s: %s" % (e.code, body or e.reason))
    except urllib.error.URLError as e:
        raise ProviderError("network error: %s" % getattr(e, "reason", e))
    except TimeoutError:
        raise ProviderError("request timed out")
    except OSError as e:
        # socket.timeout and other low-level failures (OSError subclasses
        # that escape urlopen unwrapped on some versions).
        raise ProviderError("network error: %s" % e)
    except json.JSONDecodeError:
        raise ProviderError("invalid JSON response")


class ProviderError(Exception):
    pass


class RateLimited(ProviderError):
    """HTTP 429. retry_after is seconds until safe to retry (may be None)."""

    def __init__(self, message: str, retry_after: Optional[float] = None):
        super().__init__(message)
        self.retry_after = retry_after


def is_transient_error(msg: str) -> bool:
    """True for errors worth retrying / failing over (vs permanent ones like
    'user not found', 'suspended', 'invalid handle')."""
    m = (msg or "").lower()
    if any(k in m for k in ("429", "too many requests", "rate limit", "rate-limit",
                            "500", "502", "503", "504", "timed out", "timeout",
                            "network error", "connection", "temporarily",
                            "request failed", "over capacity", "try again")):
        return True
    return False


def _retry_after_from_headers(headers) -> Optional[float]:
    """Extract wait time from 429 response headers (x-rate-limit-reset or Retry-After)."""
    if not headers:
        return None
    try:
        reset = headers.get("x-rate-limit-reset")
        if reset:
            wait = float(reset) - time.time()
            if wait > 0:
                return min(wait + 1.0, 16 * 60.0)
        retry = headers.get("Retry-After")
        if retry:
            return min(max(float(retry), 1.0), 16 * 60.0)
    except (ValueError, TypeError):
        pass
    return None


class BaseProvider:
    name = "base"
    # Throughput/cost hints for --dry-run. requests_per_handle is HTTP calls
    # per handle; cost_per_1k_usd is None when unknown/free.
    requests_per_handle = 1.0
    cost_per_1k_usd: Optional[float] = None
    rate_note = ""
    # True when fetch_many serves many handles per HTTP call (official batch).
    batches = False
    # True when fetch_timeline_page serves recent posts (post analytics).
    supports_timeline = False

    def fetch_one(self, username: str, timeout: int = 15) -> FetchResult:
        raise NotImplementedError

    def fetch_many(self, usernames: List[str], timeout: int = 15) -> List[FetchResult]:
        # Default: sequential single fetches. Subclasses may override (batching).
        return [self.fetch_one(u, timeout=timeout) for u in usernames]

    def fetch_timeline_page(self, username: str, count: int = 20,
                            cursor: Optional[str] = None,
                            timeout: int = 15,
                            with_replies: bool = False,
                            ) -> Tuple[List[dict], Optional[str]]:
        """One page of the account's recent posts (newest first).

        Returns (posts, next_cursor). Posts are normalized dicts:
        {"id","url","text","created_at","created_timestamp","views","likes",
         "reposts","replies","bookmarks","quotes","is_repost","author"}.
        next_cursor is None when the timeline is exhausted.
        Raises ProviderError on failure (message classifies transient vs
        permanent via is_transient_error / classify_status).
        Default: not supported (only providers with a timeline implement it).
        """
        raise ProviderError(
            "provider %r has no post-timeline endpoint" % self.name)


def _fx_error(username: str, data: dict) -> FetchResult:
    """Failure row for an FxTwitter non-user payload.

    Suspended accounts surface as reason/profile_embed "suspended" and/or a
    "User is suspended" message; unknown accounts as code 404 / "not found".
    The "Suspended: " prefix guarantees classify_status() sees the signal
    even if the message wording ever changes.
    """
    msg = data.get("message") or "user not found / suspended / private"
    reason = data.get("reason") or ""
    embed = data.get("profile_embed") or ""
    code = data.get("code")
    if (reason == "suspended" or embed == "suspended"
            or "suspend" in str(msg).lower()):
        return FetchResult(username=username, ok=False,
                           error="Suspended: %s" % msg)
    if code is not None and code != 404:
        # e.g. {"code": 500, ...}: keep the numeric code in the message so
        # it classifies transient (retryable), not settled.
        return FetchResult(username=username, ok=False,
                           error="HTTP %s: %s" % (code, msg))
    return FetchResult(username=username, ok=False, error=str(msg))


def _fx_post(item: dict, fallback_author: str = "?") -> Optional[dict]:
    """Normalize one FxTwitter timeline entry to a post dict.

    Returns None for non-post entries (thread-group wrappers etc.).
    A repost of someone else's post surfaces with `reposted_by` set and
    the original author in `author` — flagged as is_repost so analytics
    can exclude amplified content from own-post stats.
    View/like/... counters may be absent on tombstones; missing stays None
    (stats skip Nones rather than counting them as zero).
    """
    if not isinstance(item, dict) or item.get("type") not in ("status", None):
        # "thread" group entries (only with groupthreads) and tombstones
        # carry no per-post counters worth analyzing.
        if item.get("type") != "status":
            return None
    author = item.get("author") or {}
    return {
        "id": str(item.get("id") or ""),
        "url": item.get("url") or "",
        "text": item.get("text") or "",
        "created_at": item.get("created_at") or "",
        "created_timestamp": _to_int(item.get("created_timestamp")),
        "views": _to_int(item.get("views")),
        "likes": _to_int(item.get("likes")),
        "reposts": _to_int(item.get("reposts")),
        "replies": _to_int(item.get("replies")),
        "bookmarks": _to_int(item.get("bookmarks")),
        "quotes": _to_int(item.get("quotes")),
        "is_repost": item.get("reposted_by") is not None,
        "author": author.get("screen_name") or fallback_author,
    }


class FxTwitterProvider(BaseProvider):
    """Free, no-auth provider backed by the FxTwitter API.

    GET https://api.fxtwitter.com/2/profile/{handle}
      -> {"user": {"followers": N, ...}} on success;
         404 {"code":404,"message":"User not found"} when unknown;
         404 {"code":404,"message":"User is suspended","reason":"suspended"}
         (and/or "profile_embed":"suspended") when suspended.
    Good for small/bulk public lookups without an X developer account —
    and the best free signal for suspension audits.
    Unofficial: be polite (shared rps cap, few workers) and chain to a
    paid provider if you need SLA-grade reliability.
    """
    name = "fxtwitter"
    BASE = "https://api.fxtwitter.com/2/profile"
    supports_timeline = True
    requests_per_handle = 1.0
    cost_per_1k_usd = 0.0
    rate_note = ("~1000 req/min per IP (1 req/handle). "
                 "Unofficial: back off on 429, shard across IPs for >10k.")

    def fetch_one(self, username: str, timeout: int = 15) -> FetchResult:
        url = "%s/%s" % (self.BASE, urllib.parse.quote(username, safe=""))
        try:
            data = _http_get_json(url, timeout=timeout)
        except RateLimited as e:
            return FetchResult(username=username, ok=False, error=str(e),
                               retry_after=e.retry_after)
        except ProviderError as e:
            # HTTP errors embed the response body, e.g.
            # 'HTTP 404: {"code":404,"message":"User is suspended","reason":"suspended"}'
            return FetchResult(username=username, ok=False, error=str(e))
        if not isinstance(data, dict):
            return FetchResult(username=username, ok=False, error="unexpected response shape")
        if data.get("user") is None:
            return _fx_error(username, data)
        try:
            user = data["user"]
            followers = user.get("followers")
            if followers is None:
                return FetchResult(username=username, ok=False, error="no followers field in response")
            return FetchResult(
                username=user.get("screen_name") or username,
                followers_count=int(followers),
                name=user.get("name"),
                following_count=_to_int(user.get("following")),
                # v2 profile uses "statuses"; tolerate legacy "tweets"
                tweet_count=_to_int(user.get("statuses", user.get("tweets"))),
                ok=True,
            )
        except (ValueError, TypeError, AttributeError) as e:
            return FetchResult(username=username, ok=False, error="parse error: %s" % e)

    def fetch_timeline_page(self, username: str, count: int = 20,
                            cursor: Optional[str] = None,
                            timeout: int = 15,
                            with_replies: bool = False,
                            ) -> Tuple[List[dict], Optional[str]]:
        """One page of GET /2/profile/{handle}/statuses (newest first).

        Upstream picks its own page size (often more than `count`), so
        callers must truncate to their budget. A null/unchanged bottom
        cursor or empty results means the timeline is exhausted.
        Suspended/missing accounts raise permanent errors; 429/5xx raise
        transient ones (both classified by is_transient_error).
        """
        params = {"count": str(max(1, min(int(count or 20), 100)))}
        if cursor:
            params["cursor"] = cursor
        if with_replies:
            params["with_replies"] = "1"
        url = "%s/%s/statuses?%s" % (
            self.BASE, urllib.parse.quote(username, safe=""),
            urllib.parse.urlencode(params))
        data = _http_get_json(url, timeout=timeout)  # raises on 429/5xx
        if not isinstance(data, dict):
            raise ProviderError("unexpected response shape")
        if data.get("code") not in (200, None) or not isinstance(
                data.get("results"), list):
            # error envelope, e.g. suspended timeline or "search unavailable"
            raise ProviderError(str(data.get("message") or "timeline unavailable"))
        posts = [p for p in (_fx_post(it, username) for it in data["results"])
                 if p is not None]
        nxt = (data.get("cursor") or {}).get("bottom")
        if nxt == cursor:  # unchanged cursor: upstream has no more pages
            nxt = None
        return posts, nxt


def _sleep_incremental(seconds: float, slice_s: float = 5.0) -> None:
    """Sleep in short slices so KeyboardInterrupt stays responsive during
    long 429 waits (a single 15-min time.sleep would feel hung)."""
    end = time.monotonic() + max(0.0, seconds)
    while True:
        remaining = end - time.monotonic()
        if remaining <= 0:
            return
        time.sleep(min(remaining, slice_s))


def _api_errors_map(data: object) -> Dict[str, dict]:
    """Index an X API {"errors": [{"value","detail","title"}]} list by
    lowercased value (finding: keys were case-sensitive while usernames are
    compared lowercased)."""
    errors: Dict[str, dict] = {}
    if isinstance(data, dict):
        for e in data.get("errors") or []:
            if isinstance(e, dict):
                errors[str(e.get("value", "")).lower()] = e
    return errors


def _api_error_detail(data: object, username: str) -> str:
    """Best-effort message for a non-envelope X API error payload."""
    detail = _api_errors_map(data).get(username.lower(), {})
    msg = detail.get("detail") or detail.get("title")
    if msg:
        return str(msg)[:200]
    if isinstance(data, dict):
        for key in ("detail", "title", "error"):
            if data.get(key):
                return str(data[key])[:200]
    return "user not found"


class XApiV2Provider(BaseProvider):
    """Official X API v2 provider. Needs a bearer token.

    Single: GET https://api.x.com/2/users/by/username/{u}?user.fields=public_metrics,...
    Batch:  GET https://api.x.com/2/users/by?usernames=a,b,c&user.fields=...
    (up to 100 usernames per call — used automatically by fetch_many).
    """
    name = "x-api-v2"
    BASE = "https://api.x.com"
    batches = True
    requests_per_handle = 0.01  # batch endpoint: 1 call per 100 handles
    cost_per_1k_usd = 10.0  # user reads billed $0.010/resource (2026 pay-per-use)
    rate_note = ("300 req/15min per app (batch: 100 handles/req -> ~30k handles/15min). "
                 "Tokens from the same app share quota; rotation only helps across apps.")

    def __init__(self, bearer_token: Optional[str] = None):
        raw = (bearer_token or os.environ.get("X_BEARER_TOKEN")
               or os.environ.get("TWITTER_BEARER_TOKEN") or "")
        # comma-separated rotation across (ideally different-app) tokens
        tokens = [t.strip() for t in str(raw).split(",") if t.strip()]
        if not tokens:
            raise ProviderError(
                "missing bearer token (pass --bearer or set X_BEARER_TOKEN)")
        self.tokens = tokens
        self._next_token = 0
        self._token_lock = threading.Lock()

    def _headers(self) -> Dict[str, str]:
        with self._token_lock:
            tok = self.tokens[self._next_token % len(self.tokens)]
            self._next_token += 1
        return {"Authorization": "Bearer %s" % tok}

    def _parse_user(self, u: dict) -> FetchResult:
        pm = u.get("public_metrics") or {}
        followers = pm.get("followers_count")
        if followers is None:
            return FetchResult(username=u.get("username", "?"), ok=False,
                               error="no public_metrics in response")
        return FetchResult(
            username=u.get("username", "?"),
            followers_count=int(followers),
            name=u.get("name"),
            following_count=_to_int(pm.get("following_count")),
            tweet_count=_to_int(pm.get("tweet_count")),
            ok=True,
        )

    def fetch_one(self, username: str, timeout: int = 15) -> FetchResult:
        url = ("%s/2/users/by/username/%s?%s" % (
            self.BASE, urllib.parse.quote(username, safe=""),
            urllib.parse.urlencode({"user.fields": "public_metrics"})))
        try:
            data = _http_get_json(url, headers=self._headers(), timeout=timeout)
        except RateLimited as e:
            return FetchResult(username=username, ok=False, error=str(e),
                               retry_after=e.retry_after)
        except ProviderError as e:
            return FetchResult(username=username, ok=False, error=str(e))
        if isinstance(data, dict) and "data" in data and isinstance(data["data"], dict):
            try:
                return self._parse_user(data["data"])
            except (ValueError, TypeError) as e:
                return FetchResult(username=username, ok=False, error="parse error: %s" % e)
        # error shape: {"errors": [{"value": ..., "detail": ..., "title": ...}]}
        return FetchResult(username=username, ok=False,
                           error=_api_error_detail(data, username))

    def fetch_many(self, usernames: List[str], timeout: int = 15,
                     chunk_delay: float = 1.0, max_waits: int = 2,
                     wait_budget: float = 900.0,
                     chunk_retries: int = 2) -> List[FetchResult]:
        # Batch in chunks of 100 (API limit). Transient chunk failures
        # (5xx/network) are retried with backoff; 429s sleep until reset
        # within a bounded wait budget, then the chunk fails transiently
        # (retried on --resume) instead of stalling the run.
        out: List[FetchResult] = []
        waited_total = 0.0
        for ci, i in enumerate(range(0, len(usernames), 100)):
            chunk = usernames[i:i + 100]
            waits = 0
            attempt = 0
            while True:
                url = ("%s/2/users/by?%s" % (
                    self.BASE, urllib.parse.urlencode({
                        "usernames": ",".join(chunk),
                        "user.fields": "public_metrics",
                    })))
                try:
                    data = _http_get_json(url, headers=self._headers(), timeout=timeout)
                except RateLimited as e:
                    waits += 1
                    want = e.retry_after if e.retry_after else 60.0
                    allowed = min(want, max(0.0, wait_budget - waited_total))
                    if waits > max_waits or allowed <= 0:
                        out.extend(FetchResult(username=u, ok=False, error=str(e),
                                               retry_after=e.retry_after) for u in chunk)
                        break
                    _sleep_incremental(allowed)
                    waited_total += allowed
                    continue  # retry same chunk (rotates to next token)
                except ProviderError as e:
                    if is_transient_error(str(e)) and attempt < chunk_retries:
                        attempt += 1
                        _sleep_incremental(min(2 ** attempt, 8))
                        continue
                    out.extend(FetchResult(username=u, ok=False, error=str(e)) for u in chunk)
                    break
                users = (data.get("data") if isinstance(data, dict) else None) or []
                errors = _api_errors_map(data)
                by_name = {u.get("username", "").lower(): u for u in users if isinstance(u, dict)}
                for u in chunk:
                    hit = by_name.get(u.lower())
                    if hit is not None:
                        try:
                            out.append(self._parse_user(hit))
                        except (ValueError, TypeError) as e:
                            out.append(FetchResult(username=u, ok=False, error="parse error: %s" % e))
                    else:
                        err = errors.get(u.lower()) or {}
                        msg = err.get("detail") or err.get("title") or "user not found"
                        out.append(FetchResult(username=u, ok=False, error=str(msg)[:200]))
                break
            # pace batch calls (300 req / 15 min budget) + politeness
            if ci < (len(usernames) - 1) // 100 and chunk_delay:
                time.sleep(chunk_delay)
        return out


class TwitterApiIOProvider(BaseProvider):
    """Third-party bulk-friendly provider. Needs an API key.

    GET https://api.twitterapi.io/twitter/user/info?userName={handle}
    with `X-API-Key` header -> {"followers_count": N, ...} (or {"data": {...}}).
    ~$0.18/1k profiles (2026), no fixed per-endpoint window published;
    still backs off on 429. Best value for thousands of handles.
    """
    name = "twitterapi.io"
    BASE = "https://api.twitterapi.io"
    requests_per_handle = 1.0
    cost_per_1k_usd = 0.18
    rate_note = ("no fixed per-endpoint quota published; general throttling, "
                 "back off on 429. ~$0.18/1k profiles.")

    def __init__(self, api_key: Optional[str] = None):
        key = (api_key or os.environ.get("TWITTERAPI_IO_KEY")
               or os.environ.get("X_API_KEY") or "").strip()
        if not key:
            raise ProviderError(
                "missing API key (pass --api-key or set TWITTERAPI_IO_KEY)")
        self.api_key = key

    def fetch_one(self, username: str, timeout: int = 15) -> FetchResult:
        url = "%s/twitter/user/info?%s" % (
            self.BASE, urllib.parse.urlencode({"userName": username}))
        try:
            data = _http_get_json(
                url, headers={"X-API-Key": self.api_key}, timeout=timeout)
        except RateLimited as e:
            return FetchResult(username=username, ok=False, error=str(e),
                               retry_after=e.retry_after)
        except ProviderError as e:
            return FetchResult(username=username, ok=False, error=str(e))
        if not isinstance(data, dict):
            return FetchResult(username=username, ok=False, error="unexpected response shape")
        if data.get("status") == "error":
            return FetchResult(username=username, ok=False,
                               error=str(data.get("msg") or "lookup failed")[:200])
        # tolerant: fields may sit top-level or inside a {"data": {...}} envelope,
        # under followers_count/followers, userName/username, etc.
        p = data.get("data") if isinstance(data.get("data"), dict) else data
        # unavailable accounts (suspended / deactivated): no counters, but a
        # reason — e.g. unavailableReason "suspended".
        if p.get("unavailable"):
            reason = (p.get("unavailableReason") or p.get("message")
                      or "account unavailable")
            if "suspend" in str(reason).lower():
                return FetchResult(username=username, ok=False,
                                   error="Suspended: %s" % reason)
            return FetchResult(username=username, ok=False,
                               error="Unavailable: %s" % reason)
        followers = p.get("followers_count")
        if followers is None:  # explicit null falls back to "followers"
            followers = p.get("followers")
        if followers is None:
            return FetchResult(username=username, ok=False,
                               error="no followers field in response")
        try:
            return FetchResult(
                username=(p.get("userName") or p.get("username")
                          or p.get("screen_name") or username),
                followers_count=int(followers),
                name=p.get("name"),
                following_count=_to_int(p.get("following_count", p.get("following"))),
                tweet_count=_to_int(p.get("tweet_count", p.get("statuses_count",
                                     p.get("statusesCount")))),
                ok=True,
            )
        except (ValueError, TypeError, AttributeError) as e:
            return FetchResult(username=username, ok=False, error="parse error: %s" % e)


class ChainProvider(BaseProvider):
    """Fallback chain: try providers in order, per handle.

    Only *transient* failures (429/5xx/timeout/network) fall through to the
    next provider. Permanent ones (not found, suspended, invalid) are
    returned immediately so the chain never wastes paid calls on dead handles.
    """

    def __init__(self, providers: List[BaseProvider]):
        if not providers:
            raise ProviderError("empty provider chain")
        self.providers = providers

    @property
    def batches(self) -> bool:  # type: ignore[override]
        return getattr(self.providers[0], "batches", False)

    @property
    def supports_timeline(self) -> bool:  # type: ignore[override]
        return any(getattr(p, "supports_timeline", False)
                   for p in self.providers)

    @property
    def name(self) -> str:  # type: ignore[override]
        return "+".join(p.name for p in self.providers)

    @property
    def requests_per_handle(self) -> float:  # type: ignore[override]
        return self.providers[0].requests_per_handle

    @property
    def cost_per_1k_usd(self):  # type: ignore[override]
        return self.providers[0].cost_per_1k_usd

    @property
    def rate_note(self) -> str:  # type: ignore[override]
        return "chain: " + " -> ".join(
            "%s (%s)" % (p.name, p.rate_note or "n/a") for p in self.providers)

    def fetch_one(self, username: str, timeout: int = 15) -> FetchResult:
        last: Optional[FetchResult] = None
        for p in self.providers:
            try:
                res = p.fetch_one(username, timeout=timeout)
            except ProviderError as e:
                res = FetchResult(username=username, ok=False, error=str(e))
            except Exception as e:
                # Bug in a provider (not a provider-reported failure):
                # stay transient so the next provider still gets a chance.
                res = FetchResult(username=username, ok=False,
                                  error="request failed: %s: %s" % (
                                      type(e).__name__, e))
            if res.ok or not is_transient_error(res.error or ""):
                return res
            last = res
        return last or FetchResult(username=username, ok=False, error="all providers failed")

    def fetch_many(self, usernames: List[str], timeout: int = 15,
                   **kwargs) -> List[FetchResult]:
        # If the head of the chain batches (official API), use it, then
        # fail over only the transiently-failed handles to the rest.
        head, rest = self.providers[0], self.providers[1:]
        try:
            head_results = call_fetch_many(head, usernames, timeout=timeout, **kwargs)
        except Exception as e:
            # Batch head blew up (buggy custom provider): synthesize transient
            # rows so the failover loop below still tries the rest instead of
            # propagating and losing the whole chunk.
            head_results = [FetchResult(username=u, ok=False,
                                        error="request failed: %s: %s" % (
                                            type(e).__name__, e))
                            for u in usernames]
        results = pad_results(chunk=usernames, results=head_results)
        if not rest:
            return results
        out = []
        for u, r in zip(usernames, results):
            if r.ok or not is_transient_error(r.error or ""):
                out.append(r)
            else:
                out.append(ChainProvider(rest).fetch_one(u, timeout=timeout))
        return out

    def fetch_timeline_page(self, username: str, count: int = 20,
                            cursor: Optional[str] = None,
                            timeout: int = 15,
                            with_replies: bool = False,
                            ) -> Tuple[List[dict], Optional[str]]:
        # Timelines don't batch: use the first chain member that has one,
        # failing over to the next member only on transient errors.
        last_err: Optional[str] = None
        for p in self.providers:
            if not getattr(p, "supports_timeline", False):
                continue
            try:
                return p.fetch_timeline_page(
                    username, count=count, cursor=cursor, timeout=timeout,
                    with_replies=with_replies)
            except ProviderError as e:
                last_err = str(e)
                if not is_transient_error(last_err):
                    raise
            except Exception as e:
                last_err = "request failed: %s: %s" % (type(e).__name__, e)
        raise ProviderError(last_err or "no chain member has a post timeline")


_PROVIDER_ALIASES = {
    "fxtwitter": "fxtwitter", "fx": "fxtwitter", "free": "fxtwitter",
    "x-api-v2": "x-api-v2", "x-api": "x-api-v2", "official": "x-api-v2",
    "twitterapi.io": "twitterapi.io", "twitterapiio": "twitterapi.io",
    "tio": "twitterapi.io",
}


def batch_head(provider: BaseProvider) -> Optional[BaseProvider]:
    """Return the batching provider for this (possibly chained) provider.

    Used to route bulk runs through the 100-handle batch endpoint instead of
    per-handle calls, and to pace/estimate them as batches.
    """
    if getattr(provider, "batches", False):
        members = getattr(provider, "providers", None)
        if isinstance(members, list) and members:
            return members[0] if getattr(members[0], "batches", False) else None
        return provider
    return None


def call_fetch_many(provider: BaseProvider, chunk: List[str], timeout: int = 15,
                    **kwargs) -> List[FetchResult]:
    """Call fetch_many passing only the kwargs its signature accepts.

    Avoids try/except-TypeError dispatch, which would re-execute the chunk
    (duplicate HTTP calls / double billing) when the TypeError comes from
    *inside* the call rather than from the signature.
    """
    import inspect
    try:
        params = inspect.signature(provider.fetch_many).parameters
    except (TypeError, ValueError):
        return provider.fetch_many(chunk, timeout=timeout)
    accepts_var_kw = any(p.kind == inspect.Parameter.VAR_KEYWORD
                         for p in params.values())
    supported = {k: v for k, v in kwargs.items()
                 if k in params or accepts_var_kw}
    return provider.fetch_many(chunk, timeout=timeout, **supported)


def pad_results(chunk: List[str], results,
                note: str = "provider returned fewer rows than handles") -> List[FetchResult]:
    """Align a fetch_many return with its chunk (no silent drops, no crashes).

    Tolerates buggy/custom providers: a non-list return, a short list, or
    non-FetchResult items all become transient failures for the affected
    handles, so they are retried on --resume instead of tracebacking the run.
    """
    if not isinstance(results, list):
        results = []
    out: List[FetchResult] = []
    for i, u in enumerate(chunk):
        r = results[i] if i < len(results) else None
        if not isinstance(r, FetchResult):
            detail = note if r is None else "bad row type %s" % type(r).__name__
            r = FetchResult(username=u, ok=False,
                            error="request failed: %s" % detail)
        elif not isinstance(r.username, str) or not r.username:
            r = FetchResult(username=u, ok=False,
                            error="request failed: bad row username")
        out.append(r)
    if len(results) > len(chunk):
        out = out[:len(chunk)]
    if len(results) != len(chunk):
        for r in out[len(results):]:
            r.error = "%s (%d/%d rows)" % (r.error, len(results), len(chunk))
    return out


def _build_single(name: str, bearer_token: Optional[str],
                  api_key: Optional[str]) -> BaseProvider:
    key = _PROVIDER_ALIASES.get(name)
    if key == "fxtwitter":
        return FxTwitterProvider()
    if key == "x-api-v2":
        return XApiV2Provider(bearer_token=bearer_token)
    if key == "twitterapi.io":
        return TwitterApiIOProvider(api_key=api_key)
    raise ProviderError(
        "unknown provider %r (use fxtwitter|x-api-v2|twitterapi.io, "
        "comma-separated for a fallback chain)" % name)


def get_provider(name: str = "auto", bearer_token: Optional[str] = None,
                 api_key: Optional[str] = None) -> BaseProvider:
    """Resolve provider by name. Comma-separated names build a fallback chain.

    'auto' uses X API v2 when a bearer token is present, else free FxTwitter.
    e.g. get_provider("x-api-v2,fxtwitter"), get_provider("twitterapi.io,fxtwitter").
    """
    name = (name or "auto").lower()
    if name == "auto":
        has_token = bool((bearer_token or os.environ.get("X_BEARER_TOKEN")
                          or os.environ.get("TWITTER_BEARER_TOKEN") or "").strip())
        name = "x-api-v2" if has_token else "fxtwitter"
    parts = [p.strip().lower() for p in name.replace("+", ",").split(",") if p.strip()]
    if not parts:
        raise ProviderError("empty provider name")
    providers = [_build_single(p, bearer_token, api_key) for p in parts]
    if len(providers) == 1:
        return providers[0]
    # dedupe while preserving order
    seen, uniq = set(), []
    for p in providers:
        if p.name not in seen:
            seen.add(p.name)
            uniq.append(p)
    return ChainProvider(uniq)


def _to_int(v) -> Optional[int]:
    try:
        return int(v) if v is not None else None
    except (ValueError, TypeError):
        return None
