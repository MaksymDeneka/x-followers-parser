"""CLI: handles in -> follower counts out. No UI, just stdout/file."""
import argparse
import csv
import io
import json
import math
import os
import re
import sys
import time
from typing import Dict, List, Optional, Tuple

from .handles import (creds_field, is_url, looks_like_creds, parse_handles,
                        parse_handles_strict)
from .parser import BulkAborted, get_followers
from .providers import (ChainProvider, FxTwitterProvider, ProviderError,
                        TwitterApiIOProvider, XApiV2Provider, batch_head,
                        get_provider, is_transient_error)
from .state import StateWriter, load_state, merge_in_order

PROVIDER_CLASSES = {
    "fxtwitter": FxTwitterProvider,
    "x-api-v2": XApiV2Provider,
    "twitterapi.io": TwitterApiIOProvider,
}

# Default shared request rate (req/s) when neither --rps nor --delay is given.
DEFAULT_RPS = {"fxtwitter": 8.0, "twitterapi.io": 5.0}


EXAMPLES = """examples:
  x-followers elonmusk NASA
  x-followers -i handles.txt -o followers.csv
  x-followers -i credentials.txt --suspended-only -o suspended.csv
  x-followers -i big.txt --format jsonl -o out.jsonl --state run.jsonl
  x-followers -i big.txt --state run.jsonl --resume -o out.jsonl
  x-followers -i big.txt --dry-run
  cat handles.txt | x-followers --format csv
"""


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="x-followers",
        description="Look up X (Twitter) follower counts. "
                    "Input: handles via args, -i file, or stdin. "
                    "Output: table/csv/json/jsonl to stdout or -o file. "
                    "Bulk-ready: --provider chains, --state/--resume, --shard, --dry-run.",
        epilog=EXAMPLES,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    p.add_argument("handles", nargs="*", help="handles, @handles or x.com URLs (comma/space separated)")
    p.add_argument("-i", "--input", help="file with handles (txt/csv, one per line or comma-separated, # comments ignored)")
    p.add_argument("--input-format", choices=["auto", "handles", "creds"], default="auto",
                   help="input layout (default: auto). creds: every row is a :-separated "
                        "credentials string, handle taken from --creds-field")
    p.add_argument("--creds-field", type=int, default=0,
                   help="which :-separated field holds the handle in creds input (default: 0). "
                        "Only this field is used; passwords/tokens are never sent anywhere")
    p.add_argument("-o", "--output", help="write output to file instead of stdout")
    p.add_argument("--format", choices=["table", "csv", "json", "jsonl"], default="table",
                   help="output format (default: table; jsonl streams one object per line, best for 1000s)")
    p.add_argument("--provider", default="auto",
                   help="provider or comma-separated fallback chain (default: auto). "
                        "Choices: fxtwitter (free, no key), x-api-v2 (official, needs --bearer), "
                        "twitterapi.io (cheap bulk, needs --api-key). "
                        "e.g. --provider x-api-v2,fxtwitter")
    p.add_argument("--bearer", default=None,
                   help="X API bearer token, comma-separated for rotation "
                        "(or set X_BEARER_TOKEN / TWITTER_BEARER_TOKEN)")
    p.add_argument("--api-key", default=None,
                   help="twitterapi.io key (or set TWITTERAPI_IO_KEY)")
    p.add_argument("--workers", type=int, default=8, help="concurrency for per-handle providers (default: 8)")
    p.add_argument("--rps", type=float, default=None,
                   help="shared requests/sec cap across workers "
                        "(defaults: fxtwitter 8, twitterapi.io 5)")
    p.add_argument("--delay", type=float, default=None,
                   help="legacy: 1/delay used as shared rps when --rps is unset; "
                        "for x-api-v2 it paces batch calls (default 1.0s)")
    p.add_argument("--timeout", type=int, default=15, help="per-request timeout seconds (default: 15)")
    p.add_argument("--retries", type=int, default=2, help="retries on transient errors (default: 2)")
    p.add_argument("--state", default=None,
                   help="checkpoint file (JSONL). Settled rows are appended as they complete; "
                        "re-running with --resume skips them")
    p.add_argument("--resume", action="store_true",
                   help="skip handles already settled in --state (requires --state)")
    p.add_argument("--shard", default=None,
                   help="process a slice only, as I/N (e.g. 2/4). Fan out across machines/IPs")
    p.add_argument("--suspended-only", action="store_true",
                   help="output only suspended accounts (bulk suspension audit; "
                        "exits 1 when any are found, 0 when none)")
    p.add_argument("--progress-every", type=int, default=100,
                   help="stderr progress line every N completed handles (default: 100)")
    p.add_argument("--abort-after", type=int, default=100,
                   help="abort after N consecutive transient failures, 0 disables (default: 100)")
    p.add_argument("--dry-run", action="store_true",
                   help="print the bulk plan (calls, time, cost estimate) and exit")
    p.add_argument("-q", "--quiet", action="store_true", help="only errors to stderr, no progress notes")
    return p


HEADER_WORDS = ("handle", "handles", "username", "usernames", "screen_name")

# Second-column cells that look like exported data, not handles:
# numbers with optional thousand separators / count suffixes ("123", "92M").
_DATA_CELL_RE = re.compile(r"^[\d][\d\s,._kKmMbB]*$")


def _is_export(rows: List[List[str]]) -> bool:
    """True when rows look like an exported table (handles in column 0,
    data elsewhere) rather than a free-form handle list.

    Every non-blank row must have 2+ cells with a data-like second cell,
    so `elonmusk, nasa\\njack` (a comma list across lines) still flattens
    to all three handles instead of dropping "nasa".
    """
    content = [r for r in rows if any((c or "").strip() for c in r)]
    if len(content) < 1:
        return False
    for row in content:
        if len(row) < 2:
            return False
        second = (row[1] or "").strip()
        if second and not _DATA_CELL_RE.match(second):
            return False
    return True


def resolve_input_format(lines: List[str], requested: str) -> str:
    """Resolve "auto" to "creds" or "handles" via file-level detection."""
    if requested in ("creds", "handles"):
        return requested
    return "creds" if looks_like_creds(lines) else "handles"


def extract_creds_fields(lines: List[str], index: int) -> List[str]:
    """Pull one field per credentials row; drop blanks and header words."""
    out = []
    for ln in lines:
        field = creds_field(ln, index)
        if field and field.strip().lower() not in HEADER_WORDS:
            out.append(field)
    return out


def read_input_file(path: str, input_format: str = "auto",
                    creds_field_index: int = 0) -> List[str]:
    with open(path, encoding="utf-8-sig") as f:
        lines = [ln.strip() for ln in f.read().splitlines()]
    # drop blanks and full-line comments first so a comma inside a
    # comment (e.g. "# a, b") never triggers CSV parsing
    content = [ln for ln in lines if ln and not ln.lstrip().startswith("#")]
    if resolve_input_format(content, input_format) == "creds":
        # combo dump: handles come from one :-separated field per row;
        # no CSV sniffing (passwords/hashes are opaque payload, not columns).
        return extract_creds_fields(content, creds_field_index)
    text = "\n".join(content)
    if "," in text or '"' in text:
        rows = list(csv.reader(io.StringIO(text)))
        # a header row (e.g. "handle,followers_count" or "username,name")
        # proves this is an export table: handles live in column 0.
        had_header = bool(rows and (rows[0][0] if rows[0] else "").strip().lower()
                          in HEADER_WORDS)
        if had_header:
            rows = rows[1:]
        if had_header or _is_export(rows):
            # export: handles live in the FIRST column; other columns are
            # data, not handles.
            return [r[0].strip() for r in rows
                    if r and r[0].strip() and not r[0].lstrip().startswith("#")]
        # otherwise a free-form handle list: flatten every cell
        items: List[str] = []
        for row in rows:
            for cell in row:
                cell = (cell or "").strip()
                if cell and not cell.lstrip().startswith("#"):
                    items.append(cell)
        return items
    # plain one-per-line txt: still drop a lone header line ("handle")
    if content and content[0].strip().lower() in HEADER_WORDS:
        content = content[1:]
    return list(content)


def maybe_creds_argv(item: str, args: argparse.Namespace) -> Optional[str]:
    """Extract the handle field from a credentials string passed as an arg.

    Returns None when the item is not credentials-shaped (plain handle/URL).
    Warns once: secrets on a command line are visible to other local users
    via `ps` — prefer -i FILE.
    """
    if ":" not in (item or "") or is_url(item):
        return None
    if args.input_format == "handles":
        return None
    field = creds_field(item, args.creds_field)
    if field is None:
        return None
    print("warning: credentials on the command line are visible to other "
          "local users; prefer -i FILE", file=sys.stderr)
    return field


def collect_handles(args: argparse.Namespace) -> List[str]:
    raw: List[str] = []
    if args.handles:
        argv_items = []
        for item in args.handles:
            extracted = maybe_creds_argv(item, args)
            argv_items.append(extracted if extracted is not None else item)
        # strict: typos in explicit args should fail loudly
        try:
            strict = parse_handles_strict(argv_items)
        except ValueError as e:
            print("error: %s" % e, file=sys.stderr)
            sys.exit(2)
        raw.extend(strict)
    if args.input:
        if not os.path.exists(args.input):
            print("error: input file not found: %s" % args.input, file=sys.stderr)
            sys.exit(2)
        raw.extend(parse_handles(read_input_file(
            args.input, args.input_format, args.creds_field)))
    if not args.handles and not args.input and not sys.stdin.isatty():
        lines = [ln.strip() for ln in sys.stdin.read().splitlines()]
        lines = [ln for ln in lines if ln and not ln.lstrip().startswith("#")]
        if resolve_input_format(lines, args.input_format) == "creds":
            lines = extract_creds_fields(lines, args.creds_field)
        raw.extend(parse_handles(lines))
    # final dedupe preserving order (case-insensitive)
    seen, out = set(), []
    for h in raw:
        if h.lower() not in seen:
            seen.add(h.lower())
            out.append(h)
    return out


def apply_shard(handles: List[str], spec: Optional[str]) -> Tuple[List[str], str]:
    if not spec:
        return handles, "1/1"
    try:
        i, n = spec.split("/")
        i, n = int(i), int(n)
        assert 1 <= i <= n
    except (ValueError, AssertionError):
        print("error: --shard must be I/N with 1 <= I <= N (got %r)" % spec,
              file=sys.stderr)
        sys.exit(2)
    size = int(math.ceil(len(handles) / n)) if handles else 0
    return handles[(i - 1) * size:i * size], "%d/%d" % (i, n)


def chain_head_name(provider) -> str:
    if isinstance(provider, ChainProvider):
        return provider.providers[0].name
    return provider.name


def uses_batches(provider) -> bool:
    """True when the run goes through the 100-handle batch endpoint —
    pure official provider or a chain headed by one."""
    return batch_head(provider) is not None


def resolve_rates(provider, args: argparse.Namespace) -> Tuple[Optional[float], float]:
    """Return (rps for per-handle path, chunk_delay for official batch path)."""
    if uses_batches(provider):
        return None, (args.delay if args.delay is not None else 1.0)
    if args.rps is not None:
        return args.rps, 0.0
    if args.delay is not None:
        # --delay 0/negative means no pacing (RateLimiter treats rps<=0 as unlimited)
        return (1.0 / args.delay if args.delay > 0 else 0.0), 0.0
    return DEFAULT_RPS.get(chain_head_name(provider), 5.0), 0.0


def estimate(provider, n: int, rps: Optional[float], chunk_delay: float,
             workers: int) -> Tuple[float, str, str]:
    """Return (est_seconds, calls_desc, cost_desc) for --dry-run."""
    if uses_batches(provider):
        calls = int(math.ceil(n / 100.0)) if n else 0
        secs = calls * (chunk_delay + 0.5)
        cost = n * 0.010
        return secs, "%d batch calls (100 handles/call)" % calls, "$%.2f @ $0.010/handle" % cost
    head = chain_head_name(provider)
    cls = PROVIDER_CLASSES.get(head)
    rph = (cls.requests_per_handle if cls else 1.0) or 1.0
    calls = int(math.ceil(n * rph))
    # rps<=0 means unpaced: wall time is latency-bound, report ~0s estimate
    eff_rps = min(rps or 0.0, 1000.0)
    secs = calls / eff_rps if eff_rps > 0 else 0.0
    per_1k = cls.cost_per_1k_usd if cls else None
    if per_1k is None:
        cost_desc = "unknown"
    elif per_1k == 0:
        cost_desc = "$0.00 (free)"
    else:
        cost_desc = "$%.2f @ $%.3f/1k" % (n / 1000.0 * per_1k, per_1k)
    return secs, "%d calls (%.2g/handle)" % (calls, rph), cost_desc


def format_table(rows: List[dict]) -> str:
    if not rows:
        return "(no results)"
    user_w = max([len("username")] + [len(r.get("username") or "") for r in rows])
    fol_w = max([len("followers")] + [len(str(r.get("followers_count") or "-")) for r in rows])
    lines = ["%-*s  %*s  %s" % (user_w, "username", fol_w, "followers", "status")]
    for r in rows:
        if r.get("ok"):
            fol = str(r.get("followers_count"))
            status = r.get("name") or "OK"
        else:
            fol = "-"
            msg = r.get("error") or "failed"
            label = {"suspended": "SUSPENDED", "not_found": "NOT FOUND"}.get(
                r.get("status") or "", "ERROR")
            status = "%s: %s" % (label, msg)
        lines.append("%-*s  %*s  %s" % (user_w, r.get("username"), fol_w, fol, status))
    return "\n".join(lines)


def format_csv(rows: List[dict]) -> str:
    buf = io.StringIO()
    w = csv.writer(buf, lineterminator="\n")
    w.writerow(["username", "followers_count", "name", "following_count", "tweet_count",
                "status", "ok", "error"])
    for r in rows:
        w.writerow([r.get("username"), r.get("followers_count") if r.get("ok") else "",
                    r.get("name") or "", r.get("following_count") or "",
                    r.get("tweet_count") or "", r.get("status") or "",
                    "true" if r.get("ok") else "false",
                    "" if r.get("ok") else (r.get("error") or "")])
    return buf.getvalue()


def format_jsonl(rows: List[dict]) -> str:
    return "\n".join(json.dumps(r, ensure_ascii=False) for r in rows)


def emit(output: str, dest: Optional[str]) -> None:
    if dest:
        with open(dest, "w", encoding="utf-8", newline="") as f:
            f.write(output)
            if not output.endswith("\n"):
                f.write("\n")
    else:
        sys.stdout.write(output)
        if not output.endswith("\n"):
            sys.stdout.write("\n")


def fmt_duration(secs: float) -> str:
    if secs < 60:
        return "~%ds" % int(math.ceil(secs))
    if secs < 3600:
        return "~%dm" % int(math.ceil(secs / 60))
    return "~%.1fh" % (secs / 3600)


def main(argv: Optional[List[str]] = None) -> int:
    args = build_parser().parse_args(argv)
    if args.resume and not args.state:
        print("error: --resume requires --state FILE", file=sys.stderr)
        return 2
    if args.retries is not None and args.retries < 0:
        print("error: --retries must be >= 0 (got %d)" % args.retries,
              file=sys.stderr)
        return 2

    handles = collect_handles(args)
    if not handles:
        # friendlier than a bare error for interactive use: show full help
        if sys.stdin.isatty() and not args.handles and not args.input:
            build_parser().print_help()
        else:
            print("error: no handles given. Pass handles, use -i FILE, or pipe via stdin.\n"
                  "example: x-followers elonmusk XDevelopers",
                  file=sys.stderr)
        return 2
    handles, shard_desc = apply_shard(handles, args.shard)

    settled: Dict[str, dict] = {}
    if args.state and (args.resume or os.path.exists(args.state)):
        if args.resume:
            settled = load_state(args.state)
        elif os.path.exists(args.state) and os.path.getsize(args.state) > 0:
            print("error: state file %s already exists: use --resume to continue it "
                  "or pick a fresh --state" % args.state, file=sys.stderr)
            return 2
    todo = [h for h in handles if h.lower() not in settled]

    # Resolve provider lazily for --dry-run (plan works even without keys).
    provider = None
    provider_err = None
    try:
        provider = get_provider(args.provider, bearer_token=args.bearer,
                                api_key=args.api_key)
    except ProviderError as e:
        provider_err = str(e)

    if args.dry_run:
        return dry_run(args, handles, todo, settled, shard_desc,
                       provider, provider_err)

    if provider is None:
        print("error: %s" % provider_err, file=sys.stderr)
        return 2

    if not todo:
        if not args.quiet:
            print("nothing to fetch: %d handle(s) already settled in %s" % (
                len(handles), args.state), file=sys.stderr)
        rows, _ = merge_in_order(handles, {}, settled)
        if args.suspended_only:
            rows = [r for r in rows if (r.get("status") or "") == "suspended"]
        render(rows, args)
        # same exit code as a fresh run over identical output
        return 1 if any(not r.get("ok") for r in rows) else 0

    rps, chunk_delay = resolve_rates(provider, args)
    if not args.quiet:
        extra = " (shard %s)" % shard_desc if shard_desc != "1/1" else ""
        resumed = " (%d resumed)" % len(settled) if settled else ""
        print("looking up %d handle(s)%s%s via %s..." % (
            len(todo), resumed, extra, provider.name), file=sys.stderr)

    start = time.time()
    state_writer = StateWriter(args.state) if args.state else None

    def on_result(row: dict) -> None:
        # checkpoint only settled rows; transient failures are retried on --resume
        if state_writer is not None and (
                row.get("ok") or not is_transient_error(row.get("error") or "")):
            state_writer.append(row)

    def on_progress(done: int, total: int) -> None:
        if args.quiet:
            return
        if done % max(1, args.progress_every) != 0 and done != total:
            return
        el = max(time.time() - start, 0.001)
        rate = done / el
        eta = (total - done) / rate if rate > 0 else 0
        print("  %d/%d (%.0f%%) %.1f/s eta %s" % (
            done, total, 100.0 * done / total, rate, fmt_duration(eta)),
            file=sys.stderr)

    aborted = False
    try:
        def _collect(row: dict) -> None:
            on_result(row)

        rows_todo = get_followers(
            todo, provider, max_workers=args.workers,
            delay=args.delay or 0.0, timeout=args.timeout, retries=args.retries,
            rps=rps, chunk_delay=chunk_delay, on_result=_collect,
            on_progress=on_progress,
            abort_after=(args.abort_after or None))
        # key by INPUT handle: providers may return a different casing (or
        # "?" on error payloads); merge_in_order looks rows up by input.
        fresh = dict(zip([h.lower() for h in todo], rows_todo))
    except BulkAborted as e:
        aborted = True
        fresh = dict(e.partial)
        print("ABORTED: %s" % e, file=sys.stderr)
        print("progress checkpointed to %s; resume with: --state %s --resume" % (
            args.state or "(no --state! re-run with --state to enable resume)",
            args.state or "FILE"), file=sys.stderr)
    finally:
        if state_writer is not None:
            state_writer.close()

    rows, n_resumed = merge_in_order(handles, fresh, settled)
    # attempted = every handle either fetched now or resumed (filtering below
    # must not count as "not attempted").
    missing = sum(1 for h in handles
                  if h.lower() not in fresh and h.lower() not in settled)
    if args.suspended_only:
        rows = [r for r in rows if (r.get("status") or "") == "suspended"]
        if not args.quiet:
            print("suspension audit: %d suspended of %d checked" % (
                len(rows), len(handles)), file=sys.stderr)
    render(rows, args)

    failed = sum(1 for r in rows if not r.get("ok"))
    if not args.quiet:
        el = time.time() - start
        print("done: %d ok, %d failed%s in %s%s" % (
            len(rows) - failed, failed,
            ", %d not attempted" % missing if missing else "",
            fmt_duration(el).lstrip("~"),
            " (%d resumed)" % n_resumed if n_resumed else ""), file=sys.stderr)
    if aborted or missing:
        return 3
    return 1 if failed else 0


def render(rows: List[dict], args: argparse.Namespace) -> None:
    if args.format == "json":
        emit(json.dumps(rows, indent=2, ensure_ascii=False), args.output)
    elif args.format == "jsonl":
        emit(format_jsonl(rows), args.output)
    elif args.format == "csv":
        emit(format_csv(rows), args.output)
    else:
        emit(format_table(rows), args.output)


def dry_run(args: argparse.Namespace, handles: List[str], todo: List[str],
            settled: Dict[str, dict], shard_desc: str,
            provider, provider_err: Optional[str]) -> int:
    if provider is None:
        # plan must work without keys: describe from class metadata
        return dry_run_unresolved(args, handles, todo, settled, shard_desc,
                                  provider_err)
    print("dry-run bulk plan")
    print("  handles: %d (%d to fetch%s)" % (
        len(handles), len(todo),
        ", %d resumed from state" % len(settled) if settled else ""))
    if shard_desc != "1/1":
        print("  shard: %s" % shard_desc)
    rps, chunk_delay = resolve_rates(provider, args)
    secs, calls, cost = estimate(provider, len(todo), rps, chunk_delay, args.workers)
    print("  provider chain: %s" % provider.name)
    print("  http calls (est): %s" % calls)
    if uses_batches(provider):
        print("  pacing: %.1fs between batch calls" % chunk_delay)
    else:
        print("  pacing: %s rps shared across %d workers" % (
            rps if rps else "unlimited", args.workers))
    print("  wall time (est): %s" % fmt_duration(secs))
    print("  cost (est): %s" % cost)
    if isinstance(provider, ChainProvider):
        for p in provider.providers:
            print("  limit [%s]: %s" % (p.name, p.rate_note or "n/a"))
    elif getattr(provider, "rate_note", ""):
        print("  limit: %s" % provider.rate_note)
    if args.state:
        print("  checkpoints: %s%s" % (
            args.state, " (resume enabled)" if args.resume else ""))
    return 0


def dry_run_unresolved(args: argparse.Namespace, handles: List[str], todo: List[str],
                       settled: Dict[str, dict], shard_desc: str,
                       provider_err: Optional[str]) -> int:
    """Plan output when provider keys are missing — no instantiation needed."""
    from .providers import _PROVIDER_ALIASES  # internal map of valid names
    print("dry-run bulk plan")
    print("  handles: %d (%d to fetch%s)" % (
        len(handles), len(todo),
        ", %d resumed from state" % len(settled) if settled else ""))
    if shard_desc != "1/1":
        print("  shard: %s" % shard_desc)
    spec = (args.provider or "auto").lower()
    names = [n.strip() for n in spec.replace("+", ",").split(",") if n.strip()]
    if spec == "auto":
        names = ["<auto: x-api-v2 if token else fxtwitter>"]
    bad_names = []
    for n in names:
        key = _PROVIDER_ALIASES.get(n, n)
        cls = PROVIDER_CLASSES.get(key)
        if cls:
            per_1k = cls.cost_per_1k_usd
            cost = "unknown" if per_1k is None else ("$0.00 (free)" if per_1k == 0
                                                    else "$%.3f/1k" % per_1k)
            print("  provider [%s]: ~%g calls/handle, %s" % (n, cls.requests_per_handle, cost))
            if cls.rate_note:
                print("    limit: %s" % cls.rate_note)
        else:
            bad_names.append(n)
            print("  provider [%s]: unknown (valid: fxtwitter|x-api-v2|twitterapi.io)" % n)
    if bad_names:
        # typos must fail like any other usage error, even in dry-run
        return 2
    # estimate off the chain head (or fxtwitter defaults for auto)
    head = _PROVIDER_ALIASES.get(names[0], names[0]) if names else "fxtwitter"
    cls = PROVIDER_CLASSES.get(head)
    if cls is not None and head == "x-api-v2":
        calls = int(math.ceil(len(todo) / 100.0)) if todo else 0
        print("  http calls (est): %d batch calls (100 handles/call)" % calls)
        print("  cost (est): $%.2f @ $0.010/handle" % (len(todo) * 0.010))
    elif cls is not None:
        per_1k = cls.cost_per_1k_usd or 0.0
        print("  http calls (est): ~%d" % int(math.ceil(len(todo) * cls.requests_per_handle)))
        print("  cost (est): $%.2f" % (len(todo) / 1000.0 * per_1k))
    print("  keys: ERROR: %s" % provider_err)
    print("  (add keys to run; estimates above assume the chain head serves all)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
