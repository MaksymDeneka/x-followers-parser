# x-followers-parser

Handles in → follower counts out. CLI only, no UI. Stdlib-only Python (no `pip install` needed).

## Setup (done once, already done on this machine)

The `x-followers` command works from any directory:

```bash
ln -s /Users/dmnn/projects/x-followers-parser/x-followers /opt/homebrew/bin/x-followers
```

(Alternative for library use: `pip install -e .` in the project dir.)

## Quick start

```bash
# which accounts in my credentials file are suspended?
x-followers -i x-credentials.txt --suspended-only -o suspended.csv

# follower counts for the whole list (resumable)
x-followers -i x-credentials.txt -o followers.csv --state run.jsonl

# single lookup (free provider, no key)
x-followers elonmusk NASA

# full help with examples
x-followers --help
```

Input accepts bare handles, `@handles`, or `x.com/` / `twitter.com/` URLs,
comma/space/semicolon separated, one per line or inline. `#` lines ignored.

Credential/combo dumps (`handle:password:email:...` per row) are detected
automatically — the handle is taken from the first `:`-separated field:

```bash
x-followers -i credentials.txt --suspended-only --format csv
```

`--input-format {auto,handles,creds}` overrides detection; `--creds-field N`
picks a different field (default 0). Only that one field is ever used —
passwords, emails and tokens are never sent anywhere, only the handle leaves
the machine (as the public profile lookup itself). Prefer `-i FILE` over
argv: secrets on a command line are visible to other local users via `ps`.

Output formats: `table` (default, human-readable), `csv`, `json`, `jsonl`
(one object per line — best for 1000s of rows).
Every row carries `status`: `active` | `suspended` | `not_found` | `error`.
Suspended/not_found are definitive answers (settled, never retried).
Exit codes: `0` full success, `1` some lookups failed, `2` usage error,
`3` aborted (provider looks down, progress checkpointed — resume it).

## Providers

| provider | auth | notes |
|---|---|---|
| `auto` (default) | — | official X API v2 when `X_BEARER_TOKEN` is set, else free FxTwitter |
| `fxtwitter` | none | free, unofficial. `GET /2/profile/{handle}`; explicit suspended signal (`reason: suspended`). Best free pick for suspension audits |
| `x-api-v2` | bearer token | official `GET /2/users/by...?user.fields=public_metrics`. Up to 100 usernames batched per call; single lookups report suspension detail, batch results may lump suspended as not found (see below) |
| `twitterapi.io` | API key (`--api-key` / `TWITTERAPI_IO_KEY`) | third-party, cheapest paid bulk (~$0.18/1k profiles); `unavailableReason: suspended` signal |

Comma-separated `--provider` builds a **fallback chain**: only *transient*
failures (429/5xx/timeout) fail over; permanent ones (not found, suspended)
return immediately so the chain never wastes paid calls on dead handles.

```bash
export X_BEARER_TOKEN="..."   # or TWITTER_BEARER_TOKEN, or pass --bearer
x-followers --provider x-api-v2 -i handles.example.txt

# official first, free fallback if X throttles
x-followers --provider x-api-v2,fxtwitter -i big.txt \
  -o out.csv --format csv --state run.jsonl
```

## Bulk: thousands of handles

Rate limits and costs (verified Aug 2026):

| provider | limit | cost | 10k handles ≈ |
|---|---|---|---|
| `fxtwitter` | ~1000 req/min per IP, 1 req/handle | free | ~20 min single IP, $0 |
| `x-api-v2` | 300 req/15min per app, 100 handles/req → ~30k handles/15min | $0.010/handle | ~5 min, ~$100 |
| `twitterapi.io` | no fixed window (back off on 429) | ~$0.00018/handle | minutes, ~$1.80 |

Notes: official tokens from the *same app* share quota — `--bearer` rotation
only helps across apps. FxTwitter responses are Cloudflare-cached (~1h), so
counts can lag slightly; shard across IPs past ~10k handles.

Recommended flow for a big list:

```bash
# 1. plan first: calls, wall time, cost estimate (works without keys)
x-followers -i big.txt --provider fxtwitter --dry-run
x-followers -i big.txt --provider twitterapi.io,fxtwitter --dry-run

# 2. run with checkpoints (jsonl streams well for 1000s of rows)
x-followers -i big.txt --format jsonl -o out.jsonl \
  --state run.jsonl --progress-every 200

# 3. if it dies or aborts halfway, resume retries only what never settled
x-followers -i big.txt --format jsonl -o out.jsonl \
  --state run.jsonl --resume

# 4. fan out across 4 machines/IPs (each runs its slice with its own state)
x-followers -i big.txt --shard 1/4 --state run.s1.jsonl -o out.s1.jsonl --format jsonl
# ... then concatenate out.s*.jsonl
```

Knobs: `--rps` (shared req/s cap, defaults 8 fxtwitter / 5 tio),
`--workers` (default 8), `--timeout`, `--retries`,
`--abort-after N` (abort after N consecutive transient failures, default 100 —
protects a 10k run from burning 20 min on a dead provider),
`--shard I/N`, `--state/--resume`, `--progress-every`, `--dry-run`.

## Suspension audit

```bash
# which of these accounts are suspended?
x-followers -i big.txt --suspended-only --format csv -o suspended.csv
# exits 1 when any are found, 0 when none — script-friendly
```
Suspended accounts show as `SUSPENDED` in tables and `"status": "suspended"`
in csv/json/jsonl; dead handles show `NOT FOUND`. Suspended/not_found are
permanent verdicts: they never trigger retries, never fail over to the next
provider in a chain, and are checkpointed as settled (a `--resume` won't
re-ask them). Caveat: the official API's *batch* endpoint may report a
suspended handle as plain not found — for suspension audits prefer
`--provider fxtwitter` (explicit `reason: suspended`, verified live) or
`twitterapi.io` (`unavailableReason`), optionally chained
(`--provider twitterapi.io,fxtwitter`).

## Post analytics

Yes — a handle is all it takes. Profile lookup resolves followers while
the timeline endpoint returns each recent post with its views, likes,
reposts, replies, bookmarks and quotes:

```bash
# top posts + view stats (sum/mean/median/max) per account, comparison table
x-followers -i accounts.txt --posts --max-posts 150 --top-n 5

# same as CSV for spreadsheets, plus every post for drill-down
x-followers -i accounts.txt --posts --max-posts 150 --format csv \
  -o stats.csv --posts-dump posts.jsonl --state posts.jsonl --resume
```

Notes: free `--provider fxtwitter` timeline (~6 calls per 150 posts,
still $0). Reposts of *other* people's posts are excluded from stats by
default (counted as `n_reposts_skipped`; `--include-reposts` keeps them).
`--posts` needs a timeline provider (`fxtwitter`); uses its own
`--state` file, not one from follower runs.

## Library use

```python
from x_followers_parser import get_followers
from x_followers_parser.providers import FxTwitterProvider

rows = get_followers(["elonmusk", "@nasa"], FxTwitterProvider())
for r in rows:
    print(r["username"], r["followers_count"], r["error"] if not r["ok"] else "OK")
```

## Options

```
x-followers --help
```

`--workers`, `--rps`, `--delay`, `--timeout`, `--retries`, `-o/--output`, `-q/--quiet` do what they say.
