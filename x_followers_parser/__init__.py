"""x_followers_parser: handles in -> follower counts out. No UI."""

from .handles import (creds_field, is_url, looks_like_creds, normalize_handle,
                        parse_handles, parse_handles_strict)
from .parser import BulkAborted, get_followers
from .providers import (ChainProvider, FetchResult, FxTwitterProvider,
                        RateLimited, TwitterApiIOProvider, XApiV2Provider,
                        classify_status, get_provider, is_transient_error)
from .ratelimit import RateLimiter
from .state import StateWriter, load_state

__all__ = [
    "BulkAborted",
    "ChainProvider",
    "FetchResult",
    "FxTwitterProvider",
    "RateLimited",
    "RateLimiter",
    "StateWriter",
    "TwitterApiIOProvider",
    "XApiV2Provider",
    "classify_status",
    "creds_field",
    "get_followers",
    "get_provider",
    "is_transient_error",
    "is_url",
    "load_state",
    "looks_like_creds",
    "normalize_handle",
    "parse_handles",
    "parse_handles_strict",
]

__version__ = "0.1.0"
