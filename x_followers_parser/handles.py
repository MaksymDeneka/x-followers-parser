"""Handle normalization shared by core + CLI."""
import re
from typing import Iterable, List, Optional

_HANDLE_RE = re.compile(r"^[A-Za-z0-9_]{1,15}$")
_URL_RE = re.compile(
    r"(?<![A-Za-z0-9_.-])(?:https?://)?(?:www\.)?(?:x\.com|twitter\.com)"
    r"/([A-Za-z0-9_]{1,15})(?![A-Za-z0-9_])",
    re.IGNORECASE,
)


def is_url(text: str) -> bool:
    """True when the text is (or contains) an x.com/twitter.com profile URL."""
    return bool(_URL_RE.search(text or ""))


def creds_field(line: str, index: int = 0) -> Optional[str]:
    """Extract one :-separated field from a credentials/combo row.

    Only this field is ever used — passwords, emails, tokens in the other
    fields are never read beyond splitting the line.
    Returns None when the field is missing or blank.
    """
    parts = str(line or "").split(":")
    if len(parts) < 2 or index < 0 or index >= len(parts):
        return None
    field = parts[index].strip().strip("\"'")
    return field or None


def looks_like_creds(lines: Iterable[str]) -> bool:
    """File-level detection: True when most lines look like credentials rows.

    A line counts when it contains ":" yet is neither a profile URL nor
    (redundantly) a bare handle — handles and URLs never contain colons, so
    a handle list scores ~0 while a combo dump scores ~1. Majority vote so
    one odd line can't flip a whole file.
    """
    content = [ln.strip() for ln in (lines or []) if (ln or "").strip()]
    if not content:
        return False
    hits = sum(1 for ln in content if ":" in ln and not is_url(ln))
    return hits / len(content) >= 0.5


def normalize_handle(raw: str) -> str:
    """Normalize one raw input to a bare username (no @).

    Accepts: '@elonmusk', 'elonmusk', 'https://x.com/elonmusk',
    'twitter.com/elonmusk/status/123', '  @ElonMusk  '.
    Raises ValueError if nothing usable remains.
    """
    s = (raw or "").strip()
    if not s:
        raise ValueError("empty handle")
    if s.startswith("#"):
        raise ValueError("comment line")
    # URL form wins: pull the first path segment after domain
    m = _URL_RE.search(s)
    if m:
        return m.group(1)
    # strip leading @s and trailing path/query fragments
    s = s.lstrip("@").strip()
    s = re.split(r"[\s,;/?#]+", s, maxsplit=1)[0]
    s = s.strip().lstrip("@")
    # strip a stray @-prefix domain leftover like 'x.com:handle'
    if ":" in s:
        s = s.rsplit(":", 1)[-1].lstrip("@")
    if not _HANDLE_RE.match(s):
        raise ValueError("invalid handle %r (must match [A-Za-z0-9_]{1,15})" % raw)
    return s


def parse_handles(items: Iterable[str]) -> List[str]:
    """Flatten free-form inputs into normalized usernames.

    Splits on commas, semicolons and whitespace, drops blanks/comments,
    dedupes case-insensitively while preserving first-seen order.
    Invalid entries are skipped (CLI reports them when they come from argv).
    """
    seen = set()
    out: List[str] = []
    for item in items or []:
        if item is None:
            continue
        text = str(item).strip()
        if not text or text.startswith("#"):
            continue
        # split one line like "elonmusk, @XDevelopers; https://x.com/nasa" into parts
        parts = re.split(r"[,\s;]+", text)
        for part in parts:
            part = part.strip().strip("\"'")
            if not part or part.startswith("#"):
                continue
            try:
                h = normalize_handle(part)
            except ValueError:
                continue
            key = h.lower()
            if key not in seen:
                seen.add(key)
                out.append(h)
    return out


def parse_handles_strict(items: Iterable[str]) -> List[str]:
    """Like parse_handles but raises on the first invalid entry.

    Used for explicit CLI args so typos fail loudly instead of
    silently disappearing.
    """
    out: List[str] = []
    for item in items or []:
        if item is None:
            continue
        for part in re.split(r"[,\s;]+", str(item).strip()):
            part = part.strip().strip("\"'")
            if not part:
                continue
            out.append(normalize_handle(part))  # raises ValueError
    # dedupe preserving order
    seen = set()
    deduped = []
    for h in out:
        if h.lower() not in seen:
            seen.add(h.lower())
            deduped.append(h)
    return deduped
